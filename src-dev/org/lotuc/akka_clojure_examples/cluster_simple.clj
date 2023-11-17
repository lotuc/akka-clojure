(ns org.lotuc.akka-clojure-examples.cluster-simple
  (:require
   [org.lotuc.akka-clojure :as a]
   [org.lotuc.akka.system :refer [create-system-from-config]])
  (:import
   (akka.cluster ClusterEvent$MemberEvent ClusterEvent$ReachabilityEvent)
   (akka.cluster.typed Cluster Subscribe)))

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
