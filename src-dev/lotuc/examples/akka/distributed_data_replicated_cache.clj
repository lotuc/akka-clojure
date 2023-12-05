(ns lotuc.examples.akka.distributed-data-replicated-cache
  (:require
   [lotuc.akka.actor.scaladsl :as dsl]
   [lotuc.akka.actor.typed.actor-ref :as actor-ref]
   [lotuc.akka.actor.typed.actor-system :as actor-system]
   [lotuc.akka.actor.typed.scaladsl.ask-pattern :as scaladsl.ask-pattern]
   [lotuc.akka.cluster.ddata.lww-map :as ddata.lww-map]
   [lotuc.akka.cluster.ddata.scaladsl :as ddata-dsl]
   [lotuc.akka.cluster.scaladsl :as cluster-dsl]
   [lotuc.akka.cnv :as cnv]
   [lotuc.akka.common.slf4j :refer [slf4j-log]]))

;;; https://developer.lightbend.com/start/?group=akka&project=akka-samples-distributed-data-java
;;; Replicated Cache

(set! *warn-on-reflection* true)

(defn response-adapter
  ([typ] (comp #(assoc % :action typ) cnv/->clj))
  ([typ m] (comp #(merge (assoc % :action typ) m) cnv/->clj)))

(defmacro info [ctx msg & args]
  `(slf4j-log (dsl/log ~ctx) info ~msg ~@args))

(defn- replicated-cache* [ctx node replicator]
  (letfn [(data-key [k]
            {:dtype :ddata-key :ddata-type :lww-map
             :key-id (str "cache-" (mod (abs (hash k)) 100))})
          (receive-put-in-cache [{:keys [k v]}]
            (info ctx "put in cache: {} -> {}" k v)
            (cluster-dsl/ask-update
             replicator
             (fn [reply-to]
               {:dkey (data-key k)
                :initial {:dtype :ddata :ddata-type :lww-map}
                :consistency {:dtype :replicator-consistency :w :local}
                :reply-to reply-to
                :modify #(ddata.lww-map/put % node k v)})
             (response-adapter :UpdateResponse))
            :same)
          (receive-evict [{:keys [k]}]
            (info ctx "evict: {}" k)
            (cluster-dsl/ask-update
             replicator
             (fn [reply-to]
               {:dkey (data-key k)
                :initial {:dtype :ddata :ddata-type :lww-map}
                :consistency {:dtype :replicator-consistency :w :local}
                :reply-to reply-to
                :modify #(ddata.lww-map/remove % node k)})
             (response-adapter :UpdateResponse))
            :same)
          (receive-get-from-cache [{:keys [k reply-to]}]
            (cluster-dsl/ask-get
             replicator
             (fn [reply-to]
               {:dkey (data-key k)
                :consistency {:dtype :replicator-consistency :r :local}
                :reply-to reply-to})
             (response-adapter :GetResponse {:reply-to reply-to :k k}))
            :same)
          (on-get-response [{:keys [response-type k data reply-to] :as m}]
            (case response-type
              :success
              (actor-ref/tell reply-to
                              {:action :Cached
                               :k k :v (ddata.lww-map/get data k)})
              :not-found
              (actor-ref/tell reply-to
                              {:k k :v nil}))
            :same)]
    (dsl/receive-message
     (fn [{:keys [action] :as m}]
       (info ctx "guardian recv: {}" m)
       (case action
         :PutInCache (receive-put-in-cache m)
         :Evict (receive-evict m)
         :GetFromCache (receive-get-from-cache m)

         :GetResponse (on-get-response m)
         :UpdateResponse :same
         :same)))))

(defn replicated-cache []
  (dsl/setup
   (fn [ctx]
     (ddata-dsl/with-replicator-message-adapter
       (fn [replicator]
         (let [system (dsl/system ctx)
               node (.selfUniqueAddress (cluster-dsl/get-distributed-data system))]
           (replicated-cache* ctx node replicator)))))))

(defn startup [port]
  (actor-system/create-system-from-config
   (replicated-cache)
   "ClusterSystem"
   "cluster-application"
   {"akka.remote.artery.canonical.port" port}))

(defn- ask* [^akka.actor.typed.ActorSystem system msg]
  (scaladsl.ask-pattern/ask
   system
   (fn [reply-to] (assoc msg :reply-to reply-to))
   "5.sec"
   (actor-system/scheduler system)))

(defn put-in-cache [^akka.actor.typed.ActorSystem system {:keys [k v]}]
  (actor-ref/tell system {:action :PutInCache :k k :v v}))

(defn evict [^akka.actor.typed.ActorSystem system {:keys [k]}]
  (actor-ref/tell system {:action :Evict :k k}))

(defn get-from-cache [^akka.actor.typed.ActorSystem system {:keys [k]}]
  (ask* system {:action :GetFromCache :k k}))

(comment
  (do (def s0 (startup 25251))
      (def s1 (startup 25252)))

  (put-in-cache s0 {:k "hello" :v "world"})
  (put-in-cache s0 {:k "lotuc" :v "42"})
  (evict s1 {:k "hello"})

  @(get-from-cache s0 {:k "hello"})
  @(get-from-cache s1 {:k "hello"})

  @(get-from-cache s1 {:k "lotuc"})
  @(get-from-cache s1 {:k "42"})

  (do (.terminate s0)
      (.terminate s1)))
