(ns lotuc.examples.akka.persistence-shopping-cart
  (:require
   [lotuc.akka.javadsl.actor :as javadsl.actor]
   [lotuc.akka.system :refer [create-system-from-config]])
  (:import
   (akka.actor.typed SupervisorStrategy)
   (akka.pattern StatusReply)
   (akka.persistence.typed PersistenceId)
   (akka.persistence.typed.scaladsl Effect EventSourcedBehavior RetentionCriteria)
   (java.time Duration)))

(defn- make-summary [{:keys [items checkout-time] :as state}]
  {:action :Summary :items items :checked-out? (boolean checkout-time)})

(defn then-run [effect f]
  (.thenRun effect (reify scala.Function1 (apply [_ v] (f v)))))

(defn checked-out-shopping-cart
  [_cart-id state {:keys [action reply-to] :as command}]
  (case action
    :Get
    (.tell reply-to (make-summary state))
    :AddItem
    (.tell reply-to (StatusReply/error "Can't add an item to an already checked out shopping cart"))
    :RemoveItem
    (.tell reply-to (StatusReply/error "Can't remove an item from an already checked out shopping cart"))
    :AdjustItemQuantity
    (.tell reply-to (StatusReply/error "Can't adjust item on an already checked out shopping cart"))
    :Checkout
    (.tell reply-to (StatusReply/error "Can't checkout already checked out shopping cart")))
  (Effect/none))

(defn open-shopping-cart
  [cart-id {:keys [items] :as state} {:keys [action reply-to] :as command}]
  (case action
    :Get
    (do (.tell reply-to (make-summary state))
        (Effect/none))
    :AddItem
    (let [{:keys [item-id quantity]} command]
      (cond
        (items item-id)
        (do (.tell reply-to (StatusReply/error (format "Item '%s' was already added to this shopping cart" item-id)))
            (Effect/none))

        (neg? quantity)
        (do (.tell reply-to (StatusReply/error "Quantity must be greater than zero"))
            (Effect/none))

        :else
        (-> (Effect/persist {:ev-type :ItemAdded :cart-id cart-id :item-id item-id :quantity quantity})
            (then-run #(.tell reply-to (StatusReply/success (make-summary %)))))))
    :RemoveItem
    (let [{:keys [item-id]} command]
      (if (items item-id)
        (-> (Effect/persist {:ev-type :ItemRemoved :cart-id cart-id :item-id item-id})
            (then-run #(.tell reply-to (StatusReply/success (make-summary %)))))
        (do (.tell reply-to (StatusReply/success (make-summary state)))
            (Effect/none))))
    :AdjustItemQuantity
    (let [{:keys [item-id quantity]} command]
      (cond
        (neg? quantity)
        (do (.tell reply-to (StatusReply/error "Quantity must be greater than zero"))
            (Effect/none))

        (items item-id)
        (-> (Effect/persist {:ev-type :ItemQuantityAdjusted :cart-id cart-id :item-id item-id :new-quantity quantity})
            (then-run #(.tell reply-to (StatusReply/success (make-summary %)))))

        :else
        (do (.tell reply-to (StatusReply/error (format "Cannot adjust quantity for item %s. Item not present on cart" item-id)))
            (Effect/none))))
    :Checkout
    (if (empty? items)
      (do (.tell reply-to (StatusReply/error "Cannot checkout an empty shopping cart"))
          (Effect/none))
      (-> (Effect/persist {:ev-type :CheckedOut :cart-id cart-id :event-time (System/currentTimeMillis)})
          (then-run #(.tell reply-to (StatusReply/success (make-summary %))))))))

(defn handle-event [state {:keys [ev-type] :as ev}]
  (case ev-type
    :ItemAdded            (update state :items assoc  (:item-id ev) (:quantity ev))
    :ItemRemoved          (update state :items dissoc (:item-id ev))
    :ItemQuantityAdjusted (update state :items assoc  (:item-id ev) (:new-quantity ev))
    :CheckedOut           (assoc  state :checkout-time (:event-time ev))))

(defn shopping-cart [cart-id]
  (-> (EventSourcedBehavior/apply
       (PersistenceId/apply "ShoppingCart" cart-id)
       {:items {} :checkout-time nil}
       (reify scala.Function2
         (apply [_ state command]
           (if (:checkout-time state)
             (checked-out-shopping-cart cart-id state command)
             (open-shopping-cart cart-id state command))))
       (reify scala.Function2
         (apply [_ state event]
           (handle-event state event))))
      (.withRetention (RetentionCriteria/snapshotEvery 100 3))
      (.onPersistFailure (SupervisorStrategy/restartWithBackoff
                          (Duration/ofMillis 200) (Duration/ofSeconds 5) 0.1))))

(defn startup [cart-id port]
  (create-system-from-config
   (shopping-cart cart-id)
   "PersistenceShoppingCart"
   "persistence-shopping-cart.conf"
   {"akka.remote.artery.canonical.port" port}))

(defn- ask* [system msg]
  (-> (javadsl.actor/ask system
                         (fn [reply-to] (assoc msg :reply-to reply-to))
                         (Duration/ofSeconds 5)
                         (.scheduler system))
      (.get)))

(defn get-cart [system]
  (ask* system {:action :Get}))

(defn add-item [system item-id quantity]
  (ask* system {:action :AddItem :item-id item-id :quantity quantity}))

(defn remove-item [system item-id]
  (ask* system {:action :RemoveItem :item-id item-id}))

(defn adjust-item-quantity [system item-id quantity]
  (ask* system {:action :AdjustItemQuantity :item-id item-id :quantity quantity}))

(defn checkout [system]
  (ask* system {:action :Checkout}))

(comment
  (def s0 (startup "user-0" 25251))

  (get-cart s0)
  (checkout s0)
  (add-item s0 "product-42" 2)
  (adjust-item-quantity s0 "product-42" 3)
  (remove-item s0 {:product-id "product-42"})

  (.terminate s0))
