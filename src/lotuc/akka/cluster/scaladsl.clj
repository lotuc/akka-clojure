(ns lotuc.akka.cluster.scaladsl
  (:require
   [potemkin]
   [lotuc.akka.cluster.typed.scaladsl.distributed-data]
   [lotuc.akka.cluster.typed.scaladsl.replicator-message-adapter]))

(potemkin/import-vars
 [lotuc.akka.cluster.typed.scaladsl.distributed-data
  get-distributed-data]
 [lotuc.akka.cluster.typed.scaladsl.replicator-message-adapter
  ask-delete
  ask-get
  ask-replica-count
  ask-update
  subscribe-ddata
  unsubscribe-ddata])
