(ns lotuc.akka.cluster.ddata.flag
  (:require
   [lotuc.akka.cluster.ddata.key :as ddata.key]
   [lotuc.akka.cluster.ddata.replicated-data :as ddata.replicated-data]
   [lotuc.akka.cnv :as cnv])
  (:import
   (akka.cluster.ddata FlagKey Flag)))

(defmethod ddata.replicated-data/->replicated-data* :flag [_]
  (Flag/create))

(defn switch-on [^Flag v] (.switchOn v))

(defn enabled? [^Flag v] (.enabled v))

;;; key

(defmethod ddata.key/->ddata-key* :flag [{:keys [key-id]}]
  (FlagKey/apply key-id))

(defmethod cnv/->clj FlagKey [^FlagKey v]
  {:dtype :ddata-key :ddata-type :flag :key-id (._id v)})
