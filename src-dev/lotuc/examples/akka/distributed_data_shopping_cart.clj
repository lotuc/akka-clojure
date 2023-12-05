(ns lotuc.examples.akka.distributed-data-shopping-cart
  (:require
   [lotuc.akka.actor.scaladsl :as dsl]
   [lotuc.akka.actor.typed.actor-ref :as actor-ref]
   [lotuc.akka.actor.typed.scaladsl.ask-pattern :as scaladsl.ask-pattern]
   [lotuc.akka.cluster.ddata.lww-map :as ddata.lww-map]
   [lotuc.akka.cluster.ddata.scaladsl :as ddata-dsl]
   [lotuc.akka.cluster.scaladsl :as cluster-dsl]
   [lotuc.akka.cnv :as cnv]
   [lotuc.akka.common.log :refer [slf4j-log]]
   [lotuc.akka.actor.typed.actor-system :as actor-system]))

;;; https://developer.lightbend.com/start/?group=akka&project=akka-samples-distributed-data-java
;;; ShoppingCart

(set! *warn-on-reflection* true)

(def write-majority {:dtype :replicator-consistency :w :majority :timeout "3.sec"})
(def read-majority {:dtype :replicator-consistency :r :majority :timeout "3.sec"})

(defn response-adapter
  ([action]   (comp #(assoc % :action action) cnv/->clj))
  ([action m] (comp #(merge (assoc % :action action) m) cnv/->clj)))

(defmacro info [ctx msg & args]
  `(slf4j-log (dsl/log ~ctx) info ~msg ~@args))

(defn- shopping-cart* [ctx node replicator user-id]
  (let [data-key {:dtype :ddata-key :ddata-type :lww-map :key-id (str "cart-" user-id)}]
    (letfn [(on-get-cart [{:keys [reply-to]}]
              (cluster-dsl/ask-get
               replicator
               (fn [reply-to]
                 {:dkey data-key
                  :consistency read-majority
                  :reply-to reply-to})
               (response-adapter :GetResponse {:reply-to reply-to}))
              :same)
            (update-cart [cart {:keys [product-id title quantity] :as item}]
              (let [v (if-some [exiting-item (ddata.lww-map/get cart product-id)]
                        {:product-id product-id :title title
                         :quantity (+ (:quantity exiting-item) quantity)}
                        item)]
                (ddata.lww-map/put cart node product-id v)))
            (on-add-item [{:keys [item]}]
              (cluster-dsl/ask-update
               replicator
               (fn [reply-to]
                 {:dkey data-key
                  :initial {:dtype :ddata :ddata-type :lww-map}
                  :consistency write-majority
                  :reply-to reply-to
                  :modify (fn [cart] (update-cart cart item))})
               (response-adapter :UpdateResponse))
              :same)
            (on-remove-item [{:keys [product-id]}]
              (cluster-dsl/ask-get
               replicator
               (fn [reply-to]
                 {:dkey data-key :consistency read-majority :reply-to reply-to})
               (response-adapter :InternalRemoveItem {:product-id product-id}))
              :same)
            (remove-item [product-id]
              (cluster-dsl/ask-update
               replicator
               (fn [reply-to]
                 {:dkey data-key
                  :consistency write-majority
                  :initial {:dtype :ddata :ddata-type :lww-map}
                  :reply-to reply-to
                  :modify (fn [cart] (ddata.lww-map/remove cart node product-id))})
               (response-adapter :UpdateResponse)))
            (on-get-response [{:keys [response-type data reply-to]}]
              (case response-type
                :success
                (actor-ref/tell reply-to {:action :Cart :items
                                          (ddata.lww-map/get-entries data)})

                :not-found
                (actor-ref/tell reply-to {:action :Cart :items #{}})

                :failure
                (cluster-dsl/ask-get
                 replicator
                 (fn [reply-to]
                   {:dkey data-key
                    :consistency {:dtype :replicator-consistency :r :local}
                    :reply-to reply-to})
                 (response-adapter :GetResponse {:reply-to reply-to})))
              :same)
            (on-internal-remove-item [{:keys [response-type product-id]}]
              (case response-type
                :success (remove-item product-id)
                ;; ReadMajority failed, fall back to best effort local value
                :failure (remove-item product-id)
                ;; Nothing to remove
                :not-found nil
                nil)
              :same)
            (on-update-response [{:keys [response-type] :as m}]
              (case response-type
                ;; ok
                :success nil
                ;; will eventually be replicated
                :timeout nil
                :data-deleted nil
                (throw (IllegalStateException. (str "Unexpected failure: " m))))
              :same)]

      (dsl/receive-message
       (fn [{:keys [action] :as m}]
         (info ctx "guardian recv: {} {}" action m)
         (cond
           ;; Public APIs
           (= action :GetCart) (on-get-cart m)
           (= action :AddItem) (on-add-item m)
           (= action :RemoveItem) (on-remove-item m)

           ;; Internal messages
           (= action :GetResponse) (on-get-response m)
           (= action :InternalRemoveItem) (on-internal-remove-item m)
           (= action :UpdateResponse) (on-update-response m)))))))

(defn shopping-cart [user-id]
  (dsl/setup
   (fn [ctx]
     (ddata-dsl/with-replicator-message-adapter
       (fn [replicator]
         (let [system (dsl/system ctx)
               node (.selfUniqueAddress (cluster-dsl/get-distributed-data system))]
           (shopping-cart* ctx node replicator user-id)))))))

(defn startup [user-id port]
  (actor-system/create-system-from-config
   (shopping-cart user-id)
   "ClusterSystem"
   "cluster-application"
   {"akka.remote.artery.canonical.port" port}))

(defn- ask* [system msg]
  (scaladsl.ask-pattern/ask
   system
   (fn [reply-to] (assoc msg :reply-to reply-to))
   "5.sec"
   (actor-system/scheduler system)))

(defn get-cart [system]
  (ask* system {:action :GetCart}))

(defn add-item [system item]
  (actor-ref/tell system {:action :AddItem :item item}))

(defn remove-item [system {:keys [product-id]}]
  (actor-ref/tell system {:action :RemoveItem :product-id product-id}))

(comment
  (do (def s0 (startup "user-0" 25251))
      (def s1 (startup "user-1" 25252)))

  @(get-cart s0)
  (remove-item s0 {:product-id "product-42"})
  (add-item s0 {:product-id "product-42" :title "42" :quantity 2})

  (do (.terminate s0)
      (.terminate s1)))
