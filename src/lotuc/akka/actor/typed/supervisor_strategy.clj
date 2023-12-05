(ns lotuc.akka.actor.typed.supervisor-strategy
  (:require
   [lotuc.akka.cnv :as cnv]
   [lotuc.akka.common.scala :as scala])
  (:import
   [akka.actor.typed SupervisorStrategy]))

(set! *warn-on-reflection* true)

(defn supervisor-strategy? [v] (instance? SupervisorStrategy v))

(defmethod cnv/->akka :SupervisorStrategy
  [{:keys [strategy] :as v}]
  (or ({:resume (SupervisorStrategy/resume)
        :stop (SupervisorStrategy/stop)
        :restart (SupervisorStrategy/restart)}
       strategy)
      (and (= strategy :backoff)
           (SupervisorStrategy/restartWithBackoff
            (scala/->scala.concurrent.duration.FiniteDuration (:min v))
            (scala/->scala.concurrent.duration.FiniteDuration (:max v))
            ^double (:random-factor v)))
      (and (instance? SupervisorStrategy v) v)))
