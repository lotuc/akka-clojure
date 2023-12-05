(ns lotuc.examples.akka-clojure.cluster-simple
  (:require
   [lotuc.akka-clojure :as a]
   [lotuc.akka.cluster.typed.cluster :as cluster]
   [lotuc.akka.cluster.cluster-event]
   [lotuc.akka.cnv :as cnv]
   [lotuc.akka.actor.typed.actor-system :as actor-system]))

(set! *warn-on-reflection* true)

;;; https://developer.lightbend.com/start/?group=akka&project=akka-samples-cluster-java
;;; simple

(a/setup cluster-listener []
  (let [cluster (a/cluster)]
    (doto (cluster/subscriptions cluster)
      (a/tell (cluster/create-subscribe (a/self) :member-event))
      (a/tell (cluster/create-subscribe (a/self) :member-rechability-event)))
    (a/receive-message
     (fn [m]
       (a/info "recv: {}" (cnv/->clj m))
       :same))))

(a/setup root-behavior []
  (a/spawn (cluster-listener) "ClusterListener")
  :empty)

(defn startup [port]
  (actor-system/create-system-from-config
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
