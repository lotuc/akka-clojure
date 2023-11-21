(ns lotuc.akka.cluster
  (:import
   (akka.cluster.typed Cluster Subscribe
                       ClusterSingleton
                       ClusterSingletonSettings
                       SingletonActor)))

(set! *warn-on-reflection* true)

;;; ClusterEvent
;;; https://doc.akka.io/japi/akka/current/akka/cluster/ClusterEvent.html

;;; Subscibe
;;; https://doc.akka.io/japi/akka/current/akka/cluster/typed/Subscribe.html

(defn create-subscribe [subscriber-actor event-class]
  (Subscribe/create subscriber-actor event-class))

;;; Cluster
;;; https://doc.akka.io/japi/akka/current/akka/cluster/typed/Cluster.html

(defn get-cluster ^akka.cluster.typed.Cluster [system]
  (Cluster/get system))

;;; ClusterSingleton
;;; https://doc.akka.io/japi/akka/current/akka/cluster/typed/ClusterSingleton.html
;;; for testing purpose

(defn get-cluster-singleton ^akka.cluster.typed.ClusterSingleton [system]
  (ClusterSingleton/get system))

;;; ClusterSingletonSettings
;;; https://doc.akka.io/japi/akka/current/akka/cluster/typed/ClusterSingletonSettings.html

(defn create-cluster-singleton-setting
  ([system] (ClusterSingletonSettings/create system))
  ([system {:keys [buffer-size
                   data-center
                   ^java.time.Duration hand-over-retry-interval
                   lease-settings
                   role
                   ^java.time.Duration removal-margin]
            :as opts}]
   (cond-> (ClusterSingletonSettings/create system)
     buffer-size
     (.withBufferSize buffer-size)

     hand-over-retry-interval
     (.withHandoverRetryInterval hand-over-retry-interval)

     lease-settings
     (.withLeaseSettings lease-settings)

     removal-margin
     (.withRemovalMargin removal-margin)

     (some? data-center)
     (as-> $ (let [^akka.cluster.typed.ClusterSingletonSettings v $]
               (if (false? data-center)
                 (.withNoDataCenter v)
                 (.withDataCenter v data-center))))

     (some? role)
     (as-> $ (if (false? role)
               (.withNoRole $)
               (.withRole $ role))))))

;;; SingletonActor
;;; https://doc.akka.io/japi/akka/current/akka/cluster/typed/SingletonActor.html

(defn singleton-actor-of
  ([behavior name]
   (SingletonActor/of behavior name))
  ([behavior name {:keys [props settings stop-message]}]
   (cond-> (SingletonActor/of behavior name)
     props (.withProps props)
     settings (.withSettings settings)
     stop-message (.withStopMessage stop-message))))
