(ns lotuc.akka.cluster.ddata.or-map
  (:require
   [lotuc.akka.cluster.ddata.key :as ddata.key]
   [lotuc.akka.cluster.ddata.replicated-data :as ddata.replicated-data]
   [lotuc.akka.cnv :as cnv])
  (:import
   (akka.cluster.ddata ORMap ORMapKey)))

(defmethod ddata.replicated-data/->replicated-data* :or-map
  [{:keys []}]
  (ORMap/create))

;;; key

(defmethod ddata.key/->ddata-key* :or-map [{:keys [key-id]}]
  (ORMapKey/apply key-id))

(defmethod cnv/->clj ORMapKey [^ORMapKey v]
  {:dtype :ddata-key :ddata-type :or-map :key-id (._id v)})
