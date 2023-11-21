(ns lotuc.examples.akka.distributed-data-replicated-cache
  (:require
   [lotuc.akka.behaviors :as behaviors]
   [lotuc.akka.ddata :as ddata]
   [lotuc.akka.ddata-java-dsl :as ddata-java-dsl]
   [lotuc.akka.java-dsl :as java-dsl]
   [lotuc.akka.system :refer [create-system-from-config]])
  (:import
   (java.time Duration)))

;;; https://developer.lightbend.com/start/?group=akka&project=akka-samples-distributed-data-java
;;; Replicated Cache

(set! *warn-on-reflection* true)

(defn response-adapter
  ([typ] (comp #(assoc % :action typ) ddata-java-dsl/->clj))
  ([typ m] (comp #(merge (assoc % :action typ) m) ddata-java-dsl/->clj)))

(defn- tell [^akka.actor.typed.ActorRef target msg]
  (.tell target msg))

(defn- replicated-cache* [^akka.cluster.ddata.SelfUniqueAddress node
                          replicator]
  (letfn [(data-key [k] (ddata/create-key :LWWMap (str "cache-" (mod (abs (hash k)) 100))))
          (receive-put-in-cache [{:keys [key value]}]
            (ddata-java-dsl/ask-update replicator
                                       (fn [reply-to]
                                         {:dtype :ReplicatorUpdate :dkey (data-key key)
                                          :initial (ddata/create-ddata {:dtype :LWWMap})
                                          :consistency (ddata-java-dsl/clj->data {:dtype :ReplicatorWriteLocal$})
                                          :reply-to reply-to
                                          :modify (fn [^akka.cluster.ddata.LWWMap v] (.put v node key value))})
                                       (response-adapter :UpdateResponse))
            :same)
          (receive-evict [{:keys [key]}]
            (ddata-java-dsl/ask-update replicator
                                       (fn [reply-to]
                                         {:dtype :ReplicatorUpdate :dkey (data-key key)
                                          :initial (ddata/create-ddata {:dtype :LWWMap})
                                          :consistency (ddata-java-dsl/clj->data {:dtype :ReplicatorWriteLocal$})
                                          :reply-to reply-to
                                          :modify (fn [^akka.cluster.ddata.LWWMap v] (.remove v node key))})
                                       (response-adapter :UpdateResponse))
            :same)
          (receive-get-from-cache [{:keys [key reply-to]}]
            (ddata-java-dsl/ask-get replicator
                                    (fn [reply-to]
                                      {:dtype :ReplicatorGet :dkey (data-key key)
                                       :consistency (ddata-java-dsl/clj->data {:dtype :ReplicatorReadLocal$})
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
     (ddata-java-dsl/with-replicator-message-adaptor
       (fn [replicator]
         (let [node (.selfUniqueAddress (ddata-java-dsl/get-distributed-data
                                         (.. ctx getSystem)))]
           (replicated-cache* node replicator)))))))

(defn startup [port]
  (create-system-from-config
   (replicated-cache)
   "ClusterSystem"
   "cluster-application"
   {"akka.remote.artery.canonical.port" port}))

(defn- ask* [^akka.actor.typed.ActorSystem system msg]
  (-> (java-dsl/ask system
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
