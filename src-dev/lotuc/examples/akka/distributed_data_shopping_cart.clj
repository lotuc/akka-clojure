(ns lotuc.examples.akka.distributed-data-shopping-cart
  (:require
   [lotuc.akka.cluster.ddata :as cluster.ddata]
   [lotuc.akka.common.log :refer [slf4j-log]]
   [lotuc.akka.javadsl.actor :as javadsl.actor]
   [lotuc.akka.javadsl.actor.behaviors :as behaviors]
   [lotuc.akka.javadsl.ddata :as javadsl.ddata]
   [lotuc.akka.system :refer [create-system-from-config]])
  (:import
   (java.time Duration)))

;;; https://developer.lightbend.com/start/?group=akka&project=akka-samples-distributed-data-java
;;; ShoppingCart

(set! *warn-on-reflection* true)

(def write-majority (javadsl.ddata/clj->data {:dtype :ReplicatorWriteMajority
                                              :timeout (Duration/ofSeconds 3)}))
(def read-majority (javadsl.ddata/clj->data {:dtype :ReplicatorReadMajority
                                             :timeout (Duration/ofSeconds 3)}))

(defn response-adapter
  ([typ] (comp #(assoc % :action typ) javadsl.ddata/->clj))
  ([typ m] (comp #(merge (assoc % :action typ) m) javadsl.ddata/->clj)))

(defmacro info [ctx msg & args]
  `(slf4j-log (.getLog ~ctx) info ~msg ~@args))

(defn- tell [^akka.actor.typed.ActorRef target msg]
  (.tell target msg))

(defn- shopping-cart* [^akka.actor.typed.javadsl.ActorContext ctx
                       ^akka.cluster.ddata.SelfUniqueAddress node
                       replicator
                       user-id]
  (let [data-key (cluster.ddata/create-key :LWWMap (str "cart-" user-id))]
    (letfn [(on-get-cart [{:keys [reply-to]}]
              (javadsl.ddata/ask-get replicator
                                     (fn [reply-to] {:dtype :ReplicatorGet :dkey data-key
                                                     :consistency read-majority :reply-to reply-to})
                                     (response-adapter :GetResponse {:reply-to reply-to}))
              :same)
            (update-cart [^akka.cluster.ddata.LWWMap cart {:keys [product-id title quantity] :as item}]
              (->> (if (.contains cart product-id)
                     (let [exiting-item (.get (.get cart product-id))
                           new-quantity (+ (:quantity exiting-item) quantity)]
                       {:product-id product-id :title title :quantity new-quantity})
                     item)
                   (.put cart node product-id)))
            (on-add-item [{:keys [item]}]
              (javadsl.ddata/ask-update replicator
                                        (fn [reply-to]
                                          {:dtype :ReplicatorUpdate :dkey data-key
                                           :initial (cluster.ddata/create-ddata {:dtype :LWWMap})
                                           :consistency write-majority
                                           :reply-to reply-to
                                           :modify (fn [cart] (update-cart cart item))})
                                        (response-adapter :UpdateResponse))
              :same)
            (on-remove-item [{:keys [product-id]}]
              (javadsl.ddata/ask-get replicator
                                     (fn [reply-to]
                                       {:dtype :ReplicatorGet :dkey data-key
                                        :consistency read-majority :reply-to reply-to})
                                     (response-adapter :InternalRemoveItem {:product-id product-id}))
              :same)
            (remove-item [product-id]
              (javadsl.ddata/ask-update replicator
                                        (fn [reply-to]
                                          {:dtype :ReplicatorUpdate :dkey data-key
                                           :consistency write-majority
                                           :initial (cluster.ddata/create-ddata {:dtype :LWWMap})
                                           :reply-to reply-to
                                           :modify (fn [^akka.cluster.ddata.LWWMap cart] (.remove cart node product-id))})
                                        (response-adapter :UpdateResponse)))
            (on-get-response [{:keys [dtype ^akka.cluster.ddata.LWWMap data reply-to]}]
              (case dtype
                :ReplicatorGetSuccess
                (tell reply-to {:action :Cart :items (into {} (.. data getEntries values))})

                :ReplicatorNotFound
                (tell reply-to {:action :Cart :items #{}})

                :ReplicatorGetFailure
                (javadsl.ddata/ask-get replicator
                                       (fn [reply-to]
                                         {:dtype :ReplicatorGet :dkey data-key
                                          :consistency (javadsl.ddata/clj->data {:dtype :ReplicatorReadLocal$})
                                          :reply-to reply-to})
                                       (response-adapter :GetResponse {:reply-to reply-to})))
              :same)
            (on-internal-remove-item [{:keys [dtype product-id]}]
              (case dtype
                :ReplicatorGetSuccess (remove-item product-id)
                ;; ReadMajority failed, fall back to best effort local value
                :ReplicatorGetFailure (remove-item product-id)
                ;; Nothing to remove
                :ReplicatorNotFound nil
                nil)
              :same)
            (on-update-response [{:keys [dtype] :as m}]
              (case dtype
                ;; ok
                :ReplicatorUpdateSuccess nil
                ;; will eventually be replicated
                :ReplicatorUpdateTimeout nil
                :ReplicatorUpdateFailure (throw (IllegalStateException. (str "Unexpected failure: " m)))
                nil)
              :same)]

      (behaviors/receive-message
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
  (behaviors/setup
   (fn [^akka.actor.typed.javadsl.ActorContext ctx]
     (javadsl.ddata/with-replicator-message-adaptor
       (fn [replicator]
         (let [node (.selfUniqueAddress (javadsl.ddata/get-distributed-data
                                         (.. ctx getSystem)))]
           (shopping-cart* ctx node replicator user-id)))))))

(defn startup [user-id port]
  (create-system-from-config
   (shopping-cart user-id)
   "ClusterSystem"
   "cluster-application"
   {"akka.remote.artery.canonical.port" port}))

(defn- ask* [^akka.actor.typed.ActorSystem system msg]
  (-> (javadsl.actor/ask system
                         (fn [reply-to] (assoc msg :reply-to reply-to))
                         (Duration/ofSeconds 5)
                         (.scheduler system))
      (.get)))

(defn get-cart [^akka.actor.typed.ActorSystem system]
  (ask* system {:action :GetCart}))

(defn add-item [^akka.actor.typed.ActorSystem system item]
  (.tell system {:action :AddItem :item item}))

(defn remove-item [^akka.actor.typed.ActorSystem system {:keys [product-id]}]
  (.tell system {:action :RemoveItem :product-id product-id}))

(comment
  (do (def s0 (startup "user-0" 25251))
      (def s1 (startup "user-1" 25252)))

  (get-cart s0)
  (remove-item s0 {:product-id "product-42"})
  (add-item s0 {:product-id "product-42" :title "42" :quantity 2})

  (do (.terminate s0)
      (.terminate s1)))
