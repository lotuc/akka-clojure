(ns lotuc.akka.system
  (:import
   (akka.actor.typed ActorSystem)
   (com.typesafe.config ConfigFactory)))

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
  ([guardian-behavior name config-file]
   (create-system-from-config guardian-behavior name config-file nil))
  ([guardian-behavior name resource-base-name overrides-config]
   (cond
     (and (not resource-base-name) (not overrides-config))
     (ActorSystem/create guardian-behavior name)

     (and resource-base-name overrides-config)
     (ActorSystem/create guardian-behavior name
                         (-> (ConfigFactory/parseMap overrides-config)
                             (.withFallback (ConfigFactory/load resource-base-name))))

     resource-base-name
     (ActorSystem/create guardian-behavior name
                         (ConfigFactory/load resource-base-name))

     :else
     (ActorSystem/create guardian-behavior name
                         (ConfigFactory/parseMap overrides-config)))))
