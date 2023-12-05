(ns lotuc.akka.cluster.typed.cluster-singleton
  (:import
   (akka.cluster.typed ClusterSingleton ClusterSingletonSettings SingletonActor))
  (:require
   [lotuc.akka.common.scala :as scala]))

(set! *warn-on-reflection* true)

(defn get-cluster-singleton
  ^ClusterSingleton [system]
  (ClusterSingleton/get system))

(defn singleton-actor-of
  ([behavior name]
   (SingletonActor/of behavior name))
  ([behavior name {:keys [props settings stop-message]}]
   (cond-> (SingletonActor/of behavior name)
     props (.withProps props)
     settings (.withSettings settings)
     stop-message (.withStopMessage stop-message))))

(defn create-cluster-singleton-setting
  (^ClusterSingletonSettings [system]
   (ClusterSingletonSettings/create system))
  (^ClusterSingletonSettings
   [system {:keys [buffer-size
                   data-center
                   hand-over-retry-interval
                   lease-settings
                   role
                   removal-margin]
            :as opts}]
   (cond-> (ClusterSingletonSettings/create system)
     buffer-size
     (.withBufferSize buffer-size)

     hand-over-retry-interval
     (.withHandoverRetryInterval
      (scala/->scala.concurrent.duration.FiniteDuration hand-over-retry-interval))

     lease-settings
     (.withLeaseSettings lease-settings)

     removal-margin
     (.withRemovalMargin
      (scala/->scala.concurrent.duration.FiniteDuration removal-margin))

     (some? data-center)
     (as-> $ (let [^ClusterSingletonSettings v $]
               (if (false? data-center)
                 (.withNoDataCenter v)
                 (.withDataCenter v data-center))))

     (some? role)
     (as-> $ (if (false? role)
               (.withNoRole $)
               (.withRole $ role))))))
