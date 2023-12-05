(ns lotuc.akka.cluster.sharding.typed.scaladsl.cluster-sharding
  (:require
   [lotuc.akka.actor.scaladsl :as dsl]
   [lotuc.akka.common.scala :as scala])
  (:import
   (akka.actor.typed ActorSystem)
   (akka.cluster.sharding.typed.scaladsl ClusterSharding
                                         Entity
                                         EntityContext
                                         EntityTypeKey)))

(defn ->EntityTypeKey
  ([k]
   (->EntityTypeKey k (scala.reflect.ClassTag/Any)))
  ([k class-tag-like]
   (or (and (instance? EntityTypeKey k) k)
       (and (string? k) (EntityTypeKey/apply k (scala/->scala.reflect.ClassTag class-tag-like)))
       (throw (ex-info (str "illegal EntityTypeKey: " (type k)) {:value k})))))

(defn get-cluster-sharding ^ClusterSharding [^ActorSystem system]
  (ClusterSharding/apply system))

(defn create-entity [entity-type-key-like create-behavior]
  (Entity/apply
   (->EntityTypeKey entity-type-key-like)
   (reify scala.Function1
     (apply [_ entity-context]
       (let [^EntityContext ctx entity-context]
         (dsl/->behavior (create-behavior {:entity-type-key (.entityTypeKey ctx)
                                           :entity-id (.entityId ctx)
                                           :shard (.shard ctx)})))))))

(defn entity-ref-for
  ([^ClusterSharding sharding entity-type-key-like ^String entity-id]
   (.entityRefFor sharding (->EntityTypeKey entity-type-key-like) entity-id)))
