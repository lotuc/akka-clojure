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

(defn get-receptionist [system]
  (Receptionist/get system))

(defn register
  "Create a command when sent to [receptionist]([`get-receptionist`])), it
  replies [ReceptionList.Listing](https://doc.akka.io/japi/akka/current/akka/actor/typed/receptionist/Receptionist.Listing.html)
  to `reply-to`."
  ([service-key service-instance]
   (Receptionist/register service-key service-instance))
  ([service-key service-instance reply-to]
   (Receptionist/register service-key service-instance reply-to)))

(defn subscribe
  "The subscriber will receive [ReceptionList.Listing](https://doc.akka.io/japi/akka/current/akka/actor/typed/receptionist/Receptionist.Listing.html)."
  [service-key subscriber]
  (if (string? service-key)
    (Receptionist/subscribe (create-service-key service-key) subscriber)
    (Receptionist/subscribe service-key subscriber)))
