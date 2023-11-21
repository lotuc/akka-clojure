(ns lotuc.examples.akka.cluster-simple
  (:require
   [lotuc.akka.behaviors :as behaviors]
   [lotuc.akka.cluster :as cluster]
   [lotuc.akka.system :refer [create-system-from-config]])
  (:import
   (akka.cluster ClusterEvent$MemberEvent ClusterEvent$ReachabilityEvent)))

(set! *warn-on-reflection* true)

;;; https://developer.lightbend.com/start/?group=akka&project=akka-samples-cluster-java
;;; simple

(defmacro info [ctx msg & args]
  `(.info (.getLog ~ctx) ~msg (into-array Object [~@args])))

(defn cluster-listener []
  (behaviors/setup
   (fn [^akka.actor.typed.javadsl.ActorContext ctx]
     (let [self (.getSelf ctx)
           cluster (cluster/get-cluster (.getSystem ctx))]
       (doto (.subscriptions cluster)
         (.tell (cluster/create-subscribe self ClusterEvent$MemberEvent))
         (.tell (cluster/create-subscribe self ClusterEvent$ReachabilityEvent)))
       (behaviors/receive-message
        (fn [m] (info ctx "recv: {} - {}" (class m) (bean m))))))))

(def root-behavior
  (behaviors/setup
   (fn [^akka.actor.typed.javadsl.ActorContext ctx]
     (.spawn ctx (cluster-listener) "ClusterListener")
     :empty)))

(defn startup [port]
  (create-system-from-config
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
    (.terminate s0)
    (.terminate s1)
    (.terminate s2)))
