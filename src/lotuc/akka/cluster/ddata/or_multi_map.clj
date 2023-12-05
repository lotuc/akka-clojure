(ns lotuc.akka.cluster.ddata.or-multi-map
  (:require
   [lotuc.akka.cluster.ddata.key :as ddata.key]
   [lotuc.akka.cluster.ddata.replicated-data :as ddata.replicated-data]
   [lotuc.akka.cnv :as cnv])
  (:import
   (akka.cluster.ddata ORMultiMap ORMultiMapKey)))

(defmethod ddata.replicated-data/->replicated-data* :or-multi-map
  [_]
  (ORMultiMap/create))

;;; key

(defmethod ddata.key/->ddata-key* :or-multi-map [{:keys [key-id]}]
  (ORMultiMapKey/apply key-id))

(defmethod cnv/->clj ORMultiMapKey [^ORMultiMapKey v]
  {:dtype :ddata-key :ddata-type :or-multi-map :key-id (._id v)})
