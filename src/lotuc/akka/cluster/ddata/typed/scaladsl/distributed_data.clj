(ns lotuc.akka.cluster.ddata.typed.scaladsl.distributed-data
  (:require
   [lotuc.akka.actor.scaladsl :as dsl])
  (:import
   (akka.cluster.ddata.typed.scaladsl DistributedData)))

(defn with-replicator-message-adapter [factory]
  (DistributedData/withReplicatorMessageAdapter
   (reify scala.Function1
     (apply [_ replicator-adapter]
       (dsl/->behavior (factory replicator-adapter))))))
