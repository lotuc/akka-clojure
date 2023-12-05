(ns lotuc.akka.cluster.typed.scaladsl.distributed-data
  (:import
   [akka.actor.typed ActorSystem]
   (akka.cluster.ddata.typed.scaladsl DistributedData)))

(set! *warn-on-reflection* true)

(defn get-distributed-data ^DistributedData [^ActorSystem system]
  (DistributedData/get system))
