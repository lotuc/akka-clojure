(ns lotuc.akka.actor.typed.receptionist
  (:require
   [lotuc.akka.cnv :as cnv]
   [lotuc.akka.common.scala :as scala])
  (:import
   (akka.actor.typed ActorRef ActorSystem)
   (akka.actor.typed.receptionist Receptionist Receptionist$Listing)
   (akka.actor.typed.receptionist ServiceKey)))

(set! *warn-on-reflection* true)

(defn ->ServiceKey
  ([k] (->ServiceKey k (scala.reflect.ClassTag/Any)))
  ([k class-tag-like]
   (or (and (instance? ServiceKey k) k)
       (and (string? k) (ServiceKey/apply k (scala/->scala.reflect.ClassTag class-tag-like)))
       (throw (ex-info (str "illegal ServiceKey: " (type k)) {:value k})))))

(defn get-receptionist [^ActorSystem system]
  (Receptionist/get system))

(defn register-service
  ([service-key-like ^ActorRef service]
   (Receptionist/register (->ServiceKey service-key-like) service))
  ([service-key-like ^ActorRef service ^ActorRef reply-to]
   (Receptionist/register (->ServiceKey service-key-like) service reply-to)))

(defn deregister-service
  ([service-key-like ^ActorRef service]
   (Receptionist/deregister (->ServiceKey service-key-like) service))
  ([service-key-like ^ActorRef service ^ActorRef reply-to]
   (Receptionist/deregister (->ServiceKey service-key-like) service reply-to)))

(defn subscribe-service [service-key-like ^ActorRef subscriber]
  (Receptionist/subscribe (->ServiceKey service-key-like) subscriber))

(defn find-service [service-key-like ^ActorRef reply-to]
  (Receptionist/find (->ServiceKey service-key-like) reply-to))

(defn registered [service-key-like ^ActorRef service]
  (Receptionist/registered (->ServiceKey service-key-like) service))

(defn deregistered [service-key-like ^ActorRef service]
  (Receptionist/deregistered (->ServiceKey service-key-like) service))

(defn receptionist-listing? [v]
  (instance? Receptionist$Listing v))

(defn receptionist-listing-get-key [^Receptionist$Listing v]
  (.getKey v))

(defn receptionist-listing-service-were-added-or-removed? [^Receptionist$Listing v]
  (.servicesWereAddedOrRemoved v))

(defn receptionist-listing-for-key? [^Receptionist$Listing v k]
  (.getServiceInstances v (->ServiceKey k)))

(defn receptionist-listing-get-service-instances [^Receptionist$Listing v k]
  (.getServiceInstances v (->ServiceKey k)))

(defn receptionist-listing-get-all-service-instances [^Receptionist$Listing v k]
  (.getAllServiceInstances v (->ServiceKey k)))

(defmethod cnv/->clj Receptionist$Listing [^Receptionist$Listing v]
  {:dtype :receptionist-listing
   :service-key (.getKey v)
   :services-were-added-or-removed? (.servicesWereAddedOrRemoved v)
   :for-key? (fn [k] (.isForKey v (->ServiceKey k)))
   :get-service-instances (fn [k] (.getServiceInstances v (->ServiceKey k)))
   :get-all-service-instances (fn [k] (.getAllServiceInstances v (->ServiceKey k)))})
