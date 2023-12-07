(ns lotuc.examples.akka.distributed-data-replicated-metrics
  (:require
   [lotuc.akka.actor.scaladsl :as dsl]
   [lotuc.akka.actor.typed.actor-ref :as actor-ref]
   [lotuc.akka.actor.typed.actor-system :as actor-system]
   [lotuc.akka.actor.typed.event-stream :as event-stream]
   [lotuc.akka.cluster.ddata.lww-map :as ddata.lww-map]
   [lotuc.akka.cluster.ddata.scaladsl :as ddata-dsl]
   [lotuc.akka.cluster.member :as cluster.member]
   [lotuc.akka.cluster.scaladsl :as cluster-dsl]
   [lotuc.akka.cluster.cluster-event]
   [lotuc.akka.cluster.typed.cluster :as cluster]
   [lotuc.akka.cnv :as cnv]
   [lotuc.akka.common.slf4j :refer [slf4j-log]])
  (:import
   (java.lang.management ManagementFactory MemoryMXBean)))

;;; https://developer.lightbend.com/start/?group=akka&project=akka-samples-distributed-data-java
;;; Replicated Metrics

(set! *warn-on-reflection* true)

(def used-heap-key {:dtype :ddata-key :ddata-type :lww-map :key-id "used-heap"})
(def max-heap-key  {:dtype :ddata-key :ddata-type :lww-map :key-id "max-heap"})

(defn response-adapter
  ([action]   (comp #(assoc % :action action) cnv/->clj))
  ([action m] (comp #(merge (assoc % :action action) m) cnv/->clj)))

(defn- node-key [^akka.actor.Address address]
  (format "%s:%s" (.. address host get) (.. address port get)))

(defmacro info [ctx msg & args]
  `(slf4j-log (dsl/log ~ctx) info ~msg ~@args))

(defn replicated-metrics*
  [{:keys [context replicator node cluster event-stream self-node-key
           ^MemoryMXBean memory-m-bean
           !max-heap !nodes-in-cluster]}]
  (letfn [(receive-tick []
            (let [heap (.getHeapMemoryUsage memory-m-bean)
                  used (.getUsed heap)
                  max (.getMax heap)]
              (doto replicator
                (cluster-dsl/ask-update
                 (fn [reply-to]
                   {:dkey used-heap-key
                    :initial {:dtype :ddata :ddata-type :lww-map}
                    :consistency {:dtype :replicator-consistency :w :local}
                    :modify #(ddata.lww-map/put % node self-node-key used)
                    :reply-to reply-to})
                 (response-adapter :UpdateResponse))
                (cluster-dsl/ask-update
                 (fn [reply-to]
                   {:dkey max-heap-key
                    :initial {:dtype :ddata :ddata-type :lww-map}
                    :consistency {:dtype :replicator-consistency :w :local}
                    :modify #(cond-> %
                               (not= max (ddata.lww-map/get % self-node-key))
                               (ddata.lww-map/put node self-node-key max))
                    :reply-to reply-to})
                 (response-adapter :UpdateResponse)))
              :same))
          (receive-cleanup []
            (let [nodes-at-cleanup @!nodes-in-cluster
                  cleanup-removed (fn [data]
                                    (loop [result data
                                           [k & ks] (keys (ddata.lww-map/get-entries data))]
                                      (if k
                                        (do (when-not (contains? nodes-at-cleanup k)
                                              (ddata.lww-map/remove result node k))
                                            (recur result ks))
                                        result)))]
              (doto replicator
                (cluster-dsl/ask-update
                 (fn [reply-to]
                   {:dkey used-heap-key
                    :initial {:dtype :ddata :ddata-type :lww-map}
                    :consistency {:dtype :replicator-consistency :w :local}
                    :reply-to reply-to
                    :modify cleanup-removed})
                 (response-adapter :UpdateResponse))
                (cluster-dsl/ask-update
                 (fn [reply-to]
                   {:dkey max-heap-key
                    :initial {:dtype :ddata :ddata-type :lww-map}
                    :consistency {:dtype :replicator-consistency :w :local}
                    :reply-to reply-to
                    :modify cleanup-removed})
                 (response-adapter :UpdateResponse))))
            :same)
          (on-subscribe-response [{:keys [response-type dkey data]}]
            (when (= response-type :changed)
              (condp = dkey
                max-heap-key
                (reset! !max-heap (ddata.lww-map/get-entries data))

                used-heap-key
                (loop [percent-per-node {}
                       [[k v :as entry] & entries] (ddata.lww-map/get-entries data)]
                  (if entry
                    (recur (if-some [m (get @!max-heap k)]
                             (assoc percent-per-node k (/ (* 100.0 v) m))
                             percent-per-node)
                           entries)
                    (->> {:action :UsedHeap
                          :percent-per-node percent-per-node}
                         (event-stream/publish-command)
                         (actor-ref/tell event-stream))))))
            :same)
          (receive-member-up [{:keys [member]}]
            (swap! !nodes-in-cluster conj (node-key (cluster.member/address member)))
            :same)
          (receive-member-removed [{:keys [member]}]
            (let [member-node-key (node-key (cluster.member/address member))]
              (swap! !nodes-in-cluster disj member-node-key)
              (if (= member-node-key self-node-key)
                :stopped
                :same)))]

    (dsl/receive-message
     (fn [{:keys [action dtype] :as m}]
       (case (if (keyword? m) m action)
         :Tick (receive-tick)
         :Cleanup (receive-cleanup)
         :SubscribeResponse (on-subscribe-response m)
         :UpdateResponse :same
         (let [{:keys [ev-type] :as m} (cnv/->clj m)]
           (case ev-type
             :member-up (receive-member-up m)
             :member-removed (receive-member-removed m))))
       :same))))

(defn watcher-behavior []
  (dsl/receive
   (fn [ctx {:keys [action percent-per-node]}]
     (when (= action :UsedHeap)
       (info ctx "event stream watcher: percent-per-node: {}" percent-per-node))
     :same)))

(defn replicated-metrics
  [measure-interval cleanup-interval]
  (dsl/setup
   (fn [context]
     (dsl/with-timers
       (fn [timers]
         (doto timers
           (dsl/start-timer {:msg :Tick :timer-key :Tick :fix-rate measure-interval})
           (dsl/start-timer {:msg :Cleanup :timer-key :Cleanup :fix-rate cleanup-interval}))
         (ddata-dsl/with-replicator-message-adapter
           (fn [replicator]
             (let [system (dsl/system context)
                   cluster (cluster/get-cluster system)
                   node (.selfUniqueAddress (cluster-dsl/get-distributed-data system))
                   self (dsl/self context)
                   self-node-key (node-key (.. cluster selfMember address))
                   watcher (dsl/spawn context (watcher-behavior) "watcher")
                   event-stream (actor-system/event-stream system)]

               (doto replicator
                 (cluster-dsl/subscribe-ddata used-heap-key (response-adapter :SubscribeResponse))
                 (cluster-dsl/subscribe-ddata max-heap-key (response-adapter :SubscribeResponse)))

               (doto (cluster/subscriptions cluster)
                 (actor-ref/tell (cluster/create-subscribe self :member-removed))
                 (actor-ref/tell (cluster/create-subscribe self :member-up)))

               (info context "start watching event stream")
               (actor-ref/tell event-stream (event-stream/subscribe-command watcher))

               (replicated-metrics* {:context context
                                     :replicator replicator
                                     :node node
                                     :cluster cluster
                                     :self-node-key self-node-key
                                     :event-stream event-stream
                                     :memory-m-bean (ManagementFactory/getMemoryMXBean)
                                     :!max-heap (atom {})
                                     :!nodes-in-cluster (atom #{})})))))))))

(defn startup [port measure-interval cleanup-interval]
  (actor-system/create-system-from-config
   (replicated-metrics measure-interval cleanup-interval)
   "ClusterSystem"
   "cluster-application"
   {"akka.remote.artery.canonical.port" port}))

(comment
  (do (def s0 (startup 25251 "1.sec" "3.sec"))
      (def s1 (startup 25252 "1.sec" "3.sec")))

  (do (.terminate s0)
      (.terminate s1)))
