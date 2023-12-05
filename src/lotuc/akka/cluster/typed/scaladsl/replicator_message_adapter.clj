(ns lotuc.akka.cluster.typed.scaladsl.replicator-message-adapter
  (:require
   [lotuc.akka.cluster.ddata.key :as ddata.key]
   [lotuc.akka.cluster.typed.scaladsl.replicator :as dsl.replicator]
   [lotuc.akka.common.scala :as scala])
  (:import
   (akka.cluster.ddata.typed.scaladsl ReplicatorMessageAdapter)))

(defmacro ^:private ->request-adapter [command f]
  `(reify scala.Function1
     (apply [_ v#]
       (let [r# (~f v#)]
         (when r#
           (if (map? r#)
             (dsl.replicator/->replica-dsl-command
              (assoc r# :command ~command))
             r#))))))

(defn ask-delete
  [^ReplicatorMessageAdapter adapter create-request response-adapter]
  (.askDelete adapter
              (->request-adapter :delete create-request)
              (scala/->scala.function 1 response-adapter)))

(defn ask-get
  [^ReplicatorMessageAdapter adapter create-request response-adapter]
  (.askGet adapter
           (->request-adapter :get create-request)
           (scala/->scala.function 1 response-adapter)))

(defn ask-replica-count
  [^ReplicatorMessageAdapter adapter create-request response-adapter]
  (.askReplicaCount adapter
                    (->request-adapter :get-replica-count create-request)
                    (scala/->scala.function 1 response-adapter)))

(defn ask-update
  [^ReplicatorMessageAdapter adapter create-request response-adapter]
  (.askUpdate adapter
              (->request-adapter :update create-request)
              (scala/->scala.function 1 response-adapter)))

(defn subscribe-ddata
  [^ReplicatorMessageAdapter adapter ddata-key-like response-adapter]
  (.subscribe adapter
              (ddata.key/->ddata-key ddata-key-like)
              (scala/->scala.function 1 response-adapter)))

(defn unsubscribe-ddata
  [^ReplicatorMessageAdapter adapter ddata-key-like]
  (.unsubscribe adapter (ddata.key/->ddata-key ddata-key-like)))
