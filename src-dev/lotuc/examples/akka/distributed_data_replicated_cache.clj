(ns lotuc.examples.akka.distributed-data-replicated-cache
  (:require
   [lotuc.akka.javadsl.actor.behaviors :as behaviors]
   [lotuc.akka.cluster.ddata :as cluster.ddata]
   [lotuc.akka.javadsl.ddata :as javadsl.ddata]
   [lotuc.akka.javadsl.actor :as javadsl.actor]
   [lotuc.akka.system :refer [create-system-from-config]])
  (:import
   (java.time Duration)))

;;; https://developer.lightbend.com/start/?group=akka&project=akka-samples-distributed-data-java
;;; Replicated Cache

(set! *warn-on-reflection* true)

(defn response-adapter
  ([typ] (comp #(assoc % :action typ) javadsl.ddata/->clj))
  ([typ m] (comp #(merge (assoc % :action typ) m) javadsl.ddata/->clj)))

(defn- tell [^akka.actor.typed.ActorRef target msg]
  (.tell target msg))

(defn- replicated-cache* [^akka.cluster.ddata.SelfUniqueAddress node
                          replicator]
  (letfn [(data-key [k] (cluster.ddata/create-key :LWWMap (str "cache-" (mod (abs (hash k)) 100))))
          (receive-put-in-cache [{:keys [key value]}]
            (javadsl.ddata/ask-update replicator
                                      (fn [reply-to]
                                        {:dtype :ReplicatorUpdate :dkey (data-key key)
                                         :initial (cluster.ddata/create-ddata {:dtype :LWWMap})
                                         :consistency (javadsl.ddata/clj->data {:dtype :ReplicatorWriteLocal$})
                                         :reply-to reply-to
                                         :modify (fn [^akka.cluster.ddata.LWWMap v] (.put v node key value))})
                                      (response-adapter :UpdateResponse))
            :same)
          (receive-evict [{:keys [key]}]
            (javadsl.ddata/ask-update replicator
                                      (fn [reply-to]
                                        {:dtype :ReplicatorUpdate :dkey (data-key key)
                                         :initial (cluster.ddata/create-ddata {:dtype :LWWMap})
                                         :consistency (javadsl.ddata/clj->data {:dtype :ReplicatorWriteLocal$})
                                         :reply-to reply-to
                                         :modify (fn [^akka.cluster.ddata.LWWMap v] (.remove v node key))})
                                      (response-adapter :UpdateResponse))
            :same)
          (receive-get-from-cache [{:keys [key reply-to]}]
            (javadsl.ddata/ask-get replicator
                                   (fn [reply-to]
                                     {:dtype :ReplicatorGet :dkey (data-key key)
                                      :consistency (javadsl.ddata/clj->data {:dtype :ReplicatorReadLocal$})
                                      :reply-to reply-to})
                                   (response-adapter :GetResponse {:reply-to reply-to :key key}))
            :same)
          (on-get-response [{:keys [dtype key ^akka.cluster.ddata.LWWMap data reply-to]}]
            (some->> (case dtype
                       :ReplicatorGetSuccess
                       (let [r (.get data key)]
                         {:action :Cached
                          :key key
                          :value (if (.isDefined r) [:ok (.get r)] [:empty])})

                       :ReplicatorNotFound
                       [:empty])
                     (tell reply-to))
            :same)]
    (behaviors/receive-message
     (fn [{:keys [action] :as m}]
       (case action
         :PutInCache (receive-put-in-cache m)
         :Evict (receive-evict m)
         :GetFromCache (receive-get-from-cache m)

         :GetResponse (on-get-response m)
         :UpdateResponse :same
         :same)))))

(defn replicated-cache []
  (behaviors/setup
   (fn [^akka.actor.typed.javadsl.ActorContext ctx]
     (javadsl.ddata/with-replicator-message-adaptor
       (fn [replicator]
         (let [node (.selfUniqueAddress (javadsl.ddata/get-distributed-data
                                         (.. ctx getSystem)))]
           (replicated-cache* node replicator)))))))

(defn startup [port]
  (create-system-from-config
   (replicated-cache)
   "ClusterSystem"
   "cluster-application"
   {"akka.remote.artery.canonical.port" port}))

(defn- ask* [^akka.actor.typed.ActorSystem system msg]
  (-> (javadsl.actor/ask system
                         (fn [reply-to] (assoc msg :reply-to reply-to))
                         (Duration/ofSeconds 5)
                         (.scheduler system))
      (.get)))

(defn put-in-cache [^akka.actor.typed.ActorSystem system {:keys [key value]}]
  (.tell system {:action :PutInCache :key key :value value}))

(defn evict [^akka.actor.typed.ActorSystem system {:keys [key]}]
  (.tell system {:action :Evict :key key}))

(defn get-from-cache [^akka.actor.typed.ActorSystem system {:keys [key]}]
  (ask* system {:action :GetFromCache :key key}))

(comment
  (do (def s0 (startup 25251))
      (def s1 (startup 25252)))

  (put-in-cache s0 {:key "hello" :value "world"})
  (evict s1 {:key "hello"})

  (get-from-cache s0 {:key "hello"})
  (get-from-cache s1 {:key "hello"})

  (get-from-cache s1 {:key "42"})

  (do (.terminate s0)
      (.terminate s1)))
