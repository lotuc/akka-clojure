(ns org.lotuc.akka-clojure-examples.cluster-simple
  (:require
   [org.lotuc.akka-clojure :as a])
  (:import
   (com.typesafe.config ConfigFactory)
   (akka.actor.typed ActorSystem)
   (akka.cluster.typed Cluster Subscribe)
   (akka.cluster ClusterEvent$MemberEvent
                 ClusterEvent$ReachabilityEvent)))

;;; https://developer.lightbend.com/start/?group=akka&project=akka-samples-cluster-java
;;; simple

(a/setup cluster-listener []
  (let [cluster (Cluster/get (a/system))]
    (doto (.subscriptions cluster)
      (a/tell (Subscribe/create (a/self) ClusterEvent$MemberEvent))
      (a/tell (Subscribe/create (a/self) ClusterEvent$ReachabilityEvent)))
    (a/receive-message
     (fn [m] (a/info "recv: {} - {}" (class m) (bean m))))))

(a/setup root-behavior []
  (a/spawn (cluster-listener) "ClusterListener")
  :empty)

(defn startup [port]
  (let [overrides {"akka.remote.artery.canonical.port" port}
        config (-> (ConfigFactory/parseMap overrides)
                   ;; load application.conf by default
                   (.withFallback (ConfigFactory/load "cluster-application.conf")))]
    (ActorSystem/create (root-behavior) "ClusterSystem" config)))

(comment
  (do
    (def s0 (startup 25251))
    (def s1 (startup 25252))
    (def s2 (startup 0)))
  (do
    (.terminate s0)
    (.terminate s1)
    (.terminate s2)))
