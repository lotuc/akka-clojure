(ns lotuc.akka-clojure-examples.cluster-simple
  (:require
   [lotuc.akka-clojure :as a]
   [lotuc.akka.system :refer [create-system-from-config]]
   [lotuc.akka.cluster :as cluster])
  (:import
   (akka.cluster ClusterEvent$MemberEvent ClusterEvent$ReachabilityEvent)))

;;; https://developer.lightbend.com/start/?group=akka&project=akka-samples-cluster-java
;;; simple

(a/setup cluster-listener []
  (let [cluster (a/cluster)]
    (doto (.subscriptions cluster)
      (a/tell (cluster/create-subscribe (a/self) ClusterEvent$MemberEvent))
      (a/tell (cluster/create-subscribe (a/self) ClusterEvent$ReachabilityEvent)))
    (a/receive-message
     (fn [m] (a/info "recv: {} - {}" (class m) (bean m))))))

(a/setup root-behavior []
  (a/spawn (cluster-listener) "ClusterListener")
  :empty)

(defn startup [port]
  (create-system-from-config
   (root-behavior)
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
