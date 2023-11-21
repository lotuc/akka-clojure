(ns lotuc.akka.system
  (:import
   (akka.actor.typed ActorSystem)
   (com.typesafe.config ConfigFactory)))

(set! *warn-on-reflection* true)

;;; ActorSystem
;;; https://doc.akka.io/japi/akka/current/akka/actor/typed/ActorSystem.html
;;; ConfigFactory
;;; https://lightbend.github.io/config/latest/api/com/typesafe/config/ConfigFactory.html

(defn create-system [guardian-behavior name]
  (ActorSystem/create guardian-behavior name))

(defn create-system-from-config
  "Create system with given config file.

  ```Clojure
  (create-system-from-config
    a-behavior
    \"system-name\"
    \"cluster-transformation.conf\"    ; the .conf extension can be ignored
    {\"akka.remote.artery.canonical.port\" port})
  ```"
  ([^akka.actor.typed.Behavior guardian-behavior
    ^String name
    ^String config-resource-base-name]
   (create-system-from-config guardian-behavior name config-resource-base-name nil))
  ([^akka.actor.typed.Behavior guardian-behavior
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
