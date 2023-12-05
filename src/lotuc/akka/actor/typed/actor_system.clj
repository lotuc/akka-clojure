(ns lotuc.akka.actor.typed.actor-system
  (:import
   (akka.actor.typed ActorRef
                     ActorSystem
                     Behavior
                     Props)
   (akka.actor Address)
   (akka.actor.typed.receptionist Receptionist)
   (com.typesafe.config ConfigFactory)))

(set! *warn-on-reflection* true)

(defn create-system ^ActorSystem [guardian-behavior name]
  (ActorSystem/create guardian-behavior name))

(defn scheduler [^ActorSystem system]
  (.scheduler system))

(defn create-system-from-config
  "Create system with given config file.

  ```Clojure
  (create-system-from-config
    a-behavior
    \"system-name\"
    \"cluster-transformation.conf\"    ; the .conf extension can be ignored
    {\"akka.remote.artery.canonical.port\" port})
  ```"
  (^ActorSystem
   [^akka.actor.typed.Behavior guardian-behavior
    ^String name
    ^String config-resource-base-name]
   (create-system-from-config guardian-behavior name config-resource-base-name nil))
  (^ActorSystem
   [^akka.actor.typed.Behavior guardian-behavior
    ^String name
    ^String config-resource-base-name
    ^java.util.Map overrides-config]
   (if (and (not config-resource-base-name) (not overrides-config))
     (ActorSystem/create guardian-behavior name)
     (let [config (cond
                    (and config-resource-base-name overrides-config)
                    (-> (ConfigFactory/parseMap overrides-config)
                        (.withFallback (ConfigFactory/load config-resource-base-name)))

                    config-resource-base-name
                    (ConfigFactory/load config-resource-base-name)

                    :else
                    (ConfigFactory/parseMap overrides-config))]
       (ActorSystem/create guardian-behavior name config)))))

(defn terminate [^ActorSystem system]
  (.terminate system))

(defn system-actor-of
  ^ActorRef [^ActorSystem system ^Behavior behavior ^String actor-name]
  (.systemActorOf system behavior actor-name (Props/empty)))

(defn receptionist
  ^Receptionist [^ActorSystem system]
  (.receptionist system))

(defn event-stream
  ^ActorRef [^ActorSystem system]
  (.eventStream system))

(defn address
  ^Address [^ActorSystem system]
  (.address system))
