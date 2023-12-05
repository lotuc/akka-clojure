(ns lotuc.akka.actor.typed.scaladsl.routers
  (:require
   [lotuc.akka.actor.typed.receptionist :refer [->ServiceKey]])
  (:import
   (akka.actor.typed Behavior)
   (akka.actor.typed.scaladsl Routers
                              PoolRouter
                              GroupRouter))
  (:require
   [lotuc.akka.common.scala :as scala]))

(set! *warn-on-reflection* true)

(defn group-router ^GroupRouter [service-key-like]
  (Routers/group (->ServiceKey service-key-like)))

(defn group-with-random-routing
  ([^GroupRouter router]
   (.withRandomRouting router))
  ([^GroupRouter router prefer-local-routees]
   (.withRandomRouting router (boolean prefer-local-routees))))

(defn group-with-round-robin-routing [^GroupRouter router]
  (.withRoundRobinRouting router))

(defn group-with-consistent-hashing-routing [^GroupRouter router
                                             ^long virtual-nodes-factor
                                             mapping-fn]
  (.withConsistentHashingRouting router virtual-nodes-factor
                                 (scala/->scala.function 1 mapping-fn)))

;;;

(defn pool-with-random-routing [^PoolRouter router]
  (.withRandomRouting router))

(defn pool-with-round-robin-routing [^PoolRouter router]
  (.withRoundRobinRouting router))

(defn pool-router ^PoolRouter [^Behavior behavior ^long n]
  (Routers/pool n behavior))

(defn pool-with-pool-size [^PoolRouter router ^long n]
  (.withPoolSize router n))

(defn pool-with-broadcast-predicate [^PoolRouter router predicate]
  (.withBroadcastPredicate router (scala/->scala.function 1 predicate)))

(defn pool-with-consistent-hashing-routing [^PoolRouter router
                                            ^long virtual-nodes-factor
                                            mapping-fn]
  (.withConsistentHashingRouting router virtual-nodes-factor
                                 (scala/->scala.function 1 mapping-fn)))
