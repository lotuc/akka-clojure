(ns org.lotuc.akka.cluster
  (:import
   (akka.cluster.typed Cluster Subscribe
                       ClusterSingleton
                       ClusterSingletonSettings
                       SingletonActor)))

;;; ClusterEvent
;;; https://doc.akka.io/japi/akka/current/akka/cluster/ClusterEvent.html

;;; Subscibe
;;; https://doc.akka.io/japi/akka/current/akka/cluster/typed/Subscribe.html

(defn create-subscribe [subscriber-actor event-class]
  (Subscribe/create subscriber-actor event-class))

;;; Cluster
;;; https://doc.akka.io/japi/akka/current/akka/cluster/typed/Cluster.html

(defn get-cluster [system]
  (Cluster/get system))

;;; ClusterSingleton
;;; https://doc.akka.io/japi/akka/current/akka/cluster/typed/ClusterSingleton.html
;;; for testing purpose

(defn get-cluster-singleton
  [system]
  (ClusterSingleton/get system))

;;; ClusterSingletonSettings
;;; https://doc.akka.io/japi/akka/current/akka/cluster/typed/ClusterSingletonSettings.html

(defn create-cluster-singleton-setting
  ([system] (ClusterSingletonSettings/create system))
  ([system {:keys [buffer-size
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
     (.withHandoverRetryInterval hand-over-retry-interval)

     lease-settings
     (.withLeaseSettings​ lease-settings)

     removal-margin
     (.withRemovalMargin​ removal-margin)

     (some? data-center)
     (as-> $ (if (false? data-center)
               (.withNoDataCenter $)
               (.withDataCenter $ data-center)))

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
