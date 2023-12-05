(ns lotuc.examples.akka.cluster-simple
  (:require
   [lotuc.akka.actor.scaladsl :as dsl]
   [lotuc.akka.cluster.typed.cluster :as cluster]
   [lotuc.akka.common.slf4j :refer [slf4j-log]]
   [lotuc.akka.actor.typed.actor-system :as actor-system]
   [lotuc.akka.actor.typed.actor-ref :as actor-ref]
   [lotuc.akka.cnv :as cnv]))

(set! *warn-on-reflection* true)

;;; https://developer.lightbend.com/start/?group=akka&project=akka-samples-cluster-java
;;; simple

(defmacro info [ctx msg & args]
  `(slf4j-log (dsl/log ~ctx) info ~msg ~@args))

(defn cluster-listener []
  (dsl/setup
   (fn [ctx]
     (let [self (dsl/self ctx)
           cluster (cluster/get-cluster (dsl/system ctx))]
       (doto (.subscriptions cluster)
         (actor-ref/tell (cluster/create-subscribe self :member-event))
         (actor-ref/tell (cluster/create-subscribe self :member-rechability-event)))
       (dsl/receive-message
        (fn [m]
          (let [{:keys [dtype ev-type member] :as ev} (cnv/->clj m)]
            (when (= dtype :cluster-event)
              (case ev-type
                :member-up
                (info ctx "Member is up: {}" member)
                :member-removed
                (info ctx "Member is removed: {} after {}" member (:previous-status ev))
                :member-unrechable
                (info ctx "Member detected as unreachable: {}" member)
                :member-rechable
                (info ctx "Member back to reachable: {}" member)
                nil)))
          :same))))))

(def root-behavior
  (dsl/setup
   (fn [ctx]
     (dsl/spawn ctx (cluster-listener) "ClusterListener")
     :empty)))

(defn startup [port]
  (actor-system/create-system-from-config
   root-behavior
   "ClusterSystem"
   "cluster-application.conf"
   {"akka.remote.artery.canonical.port" port}))

(comment
  (do
    (def s0 (startup 25251))
    (def s1 (startup 25252))
    (def s2 (startup 0)))
  (do
    (actor-system/terminate s0)
    (actor-system/terminate s1)
    (actor-system/terminate s2)))
