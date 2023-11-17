(ns org.lotuc.akka.receptionist
  (:import
   (akka.actor.typed.receptionist Receptionist ServiceKey)))

;;; ServiceKey
;;; https://doc.akka.io/japi/akka/current/akka/actor/typed/receptionist/ServiceKey.html

(defn create-service-key
  ([clazz id] (ServiceKey/create clazz id))
  ([id] (ServiceKey/create Object id)))

;;; Receptionist
;;; https://doc.akka.io/japi/akka/current/akka/actor/typed/receptionist/Receptionist.html

(defn register
  "Create a registration command which can be sent
  to [receptionist]([`get-receptionist`]))."
  ([service-key service-instance]
   (if (string? service-key)
     (Receptionist/register (create-service-key service-key) service-instance)
     (Receptionist/register service-key service-instance)))
  ([service-key service-instance reply-to]
   (if (string? service-key)
     (Receptionist/register (create-service-key service-key) service-instance reply-to)
     (Receptionist/register service-key service-instance reply-to))))

(defn subscribe
  "Create a subscribe command when sent to receptionist, the subscriber will
  receive [ReceptionList.Listing](https://doc.akka.io/japi/akka/current/akka/actor/typed/receptionist/Receptionist.Listing.html)
  updates."
  [service-key subscriber]
  (if (string? service-key)
    (Receptionist/subscribe (create-service-key service-key) subscriber)
    (Receptionist/subscribe service-key subscriber)))
