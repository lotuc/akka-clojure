(ns lotuc.examples.akka.distributed-data-replicated-metrics
  (:require
   [lotuc.akka.cluster :as cluster]
   [lotuc.akka.cluster.ddata :as cluster.ddata]
   [lotuc.akka.common.log :refer [slf4j-log]]
   [lotuc.akka.javadsl.actor :as javadsl.actor]
   [lotuc.akka.javadsl.actor.behaviors :as behaviors]
   [lotuc.akka.javadsl.ddata :as javadsl.ddata]
   [lotuc.akka.system :refer [create-system-from-config]])
  (:import
   (akka.actor.typed.eventstream EventStream$Publish)
   (akka.cluster ClusterEvent$MemberRemoved ClusterEvent$MemberUp)
   (java.lang.management ManagementFactory)
   (java.time Duration)
   (akka.actor.typed.eventstream EventStream$Subscribe)))

;;; https://developer.lightbend.com/start/?group=akka&project=akka-samples-distributed-data-java
;;; Replicated Metrics

(def used-heap-key (cluster.ddata/create-key :LWWMap "used-heap"))
(def max-heap-key (cluster.ddata/create-key :LWWMap "max-heap"))

(defn response-adapter
  ([typ] (comp #(assoc % :action typ) javadsl.ddata/->clj))
  ([typ m] (comp #(merge (assoc % :action typ) m) javadsl.ddata/->clj)))

(defn- node-key [address]
  (format "%s:%s" (.. address host get) (.. address port get)))

(defmacro info [ctx msg & args]
  `(slf4j-log (.getLog ~ctx) info ~msg ~@args))

(defn replicated-metrics*
  [{:keys [context replicator node cluster self-node-key memory-m-bean
           !max-heap !nodes-in-cluster]}]
  (letfn [(receive-tick []
            (let [heap (.getHeapMemoryUsage memory-m-bean)
                  used (.getUsed heap)
                  max (.getMax heap)]
              (doto replicator
                (javadsl.ddata/ask-update (fn [reply-to]
                                            {:dtype :ReplicatorUpdate :dkey used-heap-key
                                             :initial (cluster.ddata/create-ddata {:dtype :LWWMap})
                                             :consistency (javadsl.ddata/clj->data {:dtype :ReplicatorWriteLocal$})
                                             :modify (fn [curr] (.put curr node self-node-key used))
                                             :reply-to reply-to})
                                          (response-adapter :UpdateResponse))
                (javadsl.ddata/ask-update (fn [reply-to]
                                            {:dtype :ReplicatorUpdate :dkey max-heap-key
                                             :initial (cluster.ddata/create-ddata {:dtype :LWWMap})
                                             :consistency (javadsl.ddata/clj->data {:dtype :ReplicatorWriteLocal$})
                                             :modify (fn [curr]
                                                       (if (and (.contains curr self-node-key)
                                                                (= max (.get (.get curr self-node-key))))
                                                         curr
                                                         (.put curr node self-node-key max)))
                                             :reply-to reply-to})
                                          (response-adapter :UpdateResponse)))
              :same))
          (receive-cleanup []
            (let [nodes-at-cleanup @!nodes-in-cluster
                  cleanup-removed (fn [data]
                                    (loop [result data
                                           [k & ks] (.. data getEntries keySet)]
                                      (if k
                                        (do (when-not (.contains nodes-at-cleanup k)
                                              (.remove result node k))
                                            (recur result ks))
                                        result)))]
              (doto replicator
                (javadsl.ddata/ask-update (fn [reply-to]
                                            {:dtype :ReplicatorUpdate :dkey used-heap-key
                                             :initial (cluster.ddata/create-ddata {:dtype :LWWMap})
                                             :consistency (javadsl.ddata/clj->data {:dtype :ReplicatorWriteLocal$})
                                             :reply-to reply-to
                                             :modify cleanup-removed})
                                          (response-adapter :UpdateResponse))
                (javadsl.ddata/ask-update (fn [reply-to]
                                            {:dtype :ReplicatorUpdate :dkey max-heap-key
                                             :initial (cluster.ddata/create-ddata {:dtype :LWWMap})
                                             :consistency (javadsl.ddata/clj->data {:dtype :ReplicatorWriteLocal$})
                                             :reply-to reply-to
                                             :modify cleanup-removed})
                                          (response-adapter :UpdateResponse))))
            :same)
          (on-subscribe-response [{:keys [dtype dkey data]}]
            (when (= dtype :ReplicatorChanged)
              (cond
                (= dkey max-heap-key)
                (reset! !max-heap (.getEntries data))

                (= dkey used-heap-key)
                (loop [percent-per-node {}
                       [[k v :as entry] & entries] (seq (.getEntries data))]
                  (if entry
                    (recur (if-some [m (get @!max-heap k)]
                             (assoc percent-per-node k (/ (* 100.0 v) m))
                             percent-per-node)
                           entries)
                    (.tell (.. context getSystem eventStream)
                           (EventStream$Publish. {:action :UsedHeap
                                                  :percent-per-node percent-per-node}))))))
            :same)
          (receive-member-up [m]
            (swap! !nodes-in-cluster conj (node-key (.. m member address)))
            :same)
          (receive-member-removed [m]
            (let [addr (.. m member address)]
              (swap! !nodes-in-cluster disj (node-key addr))
              (if (= addr (.. cluster selfMember uniqueAddress address))
                :stopped
                :same)))]

    (behaviors/receive-message
     (fn [{:keys [action] :as m}]
       (cond
         (= m :Tick) (receive-tick)
         (= m :Cleanup) (receive-cleanup)

         (= action :SubscribeResponse) (on-subscribe-response m)
         (= action :UpdateResponse) :same

         (instance? ClusterEvent$MemberUp m) (receive-member-up m)
         (instance? ClusterEvent$MemberRemoved m) (receive-member-removed m))))))

(defn watcher-behavior []
  (behaviors/receive
   (fn [ctx {:keys [action percent-per-node]}]
     (when (= action :UsedHeap)
       (info ctx "event stream watcher: percent-per-node: {}" percent-per-node))
     :same)))

(defn replicated-metrics
  [measure-interval cleanup-interval]
  (behaviors/setup
   (fn [{:keys [context timers]}]
     (doto timers
       (javadsl.actor/start-timer :Tick    {:timer-key :Tick
                                            :timer-type :fix-rate
                                            :interval measure-interval})
       (javadsl.actor/start-timer :Cleanup {:timer-key :Cleanup
                                            :timer-type :fix-rate
                                            :interval cleanup-interval}))
     (javadsl.ddata/with-replicator-message-adaptor
       (fn [replicator]
         (let [system (.getSystem context)
               cluster (cluster/get-cluster system)
               node (.selfUniqueAddress (javadsl.ddata/get-distributed-data system))
               self (.getSelf context)
               self-node-key (node-key (.. cluster selfMember address))
               watcher (.spawn context (watcher-behavior) "watcher")]
           (doto replicator
             (javadsl.ddata/subscribe used-heap-key (response-adapter :SubscribeResponse))
             (javadsl.ddata/subscribe max-heap-key (response-adapter :SubscribeResponse)))
           (doto (.subscriptions cluster)
             (.tell (cluster/create-subscribe self ClusterEvent$MemberRemoved))
             (.tell (cluster/create-subscribe self ClusterEvent$MemberUp)))

           (info context "start watching event stream")
           (.tell (.. system eventStream) (EventStream$Subscribe. Object watcher))

           (replicated-metrics* {:context context
                                 :replicator replicator
                                 :node node
                                 :cluster cluster
                                 :self-node-key self-node-key
                                 :memory-m-bean (ManagementFactory/getMemoryMXBean)
                                 :!max-heap (atom {})
                                 :!nodes-in-cluster (atom #{})})))))
   {:with-timer true}))

(defn startup [port measure-interval cleanup-interval]
  (create-system-from-config
   (replicated-metrics measure-interval cleanup-interval)
   "ClusterSystem"
   "cluster-application"
   {"akka.remote.artery.canonical.port" port}))

(comment
  (do (def s0 (startup 25251 (Duration/ofSeconds 1) (Duration/ofSeconds 3)))
      (def s1 (startup 25252 (Duration/ofSeconds 1) (Duration/ofSeconds 3))))

  (do (.terminate s0)
      (.terminate s1)))
