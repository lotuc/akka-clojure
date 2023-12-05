(ns lotuc.akka.cluster.ddata.scaladsl
  (:require
   [potemkin]
   [lotuc.akka.cluster.ddata.typed.scaladsl.distributed-data]
   [lotuc.akka.cluster.ddata.replicator]))

(potemkin/import-vars
 [lotuc.akka.cluster.ddata.typed.scaladsl.distributed-data
  with-replicator-message-adapter]
 [lotuc.akka.cluster.ddata.replicator
  ->replicator-consistency])
