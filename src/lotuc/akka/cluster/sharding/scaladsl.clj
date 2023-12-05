(ns lotuc.akka.cluster.sharding.scaladsl
  (:require
   [potemkin]
   [lotuc.akka.cluster.sharding.typed.scaladsl.cluster-sharding]))

(potemkin/import-vars
 [lotuc.akka.cluster.sharding.typed.scaladsl.cluster-sharding
  ->EntityTypeKey
  get-cluster-sharding
  entity-ref-for])
