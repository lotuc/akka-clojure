(ns lotuc.akka.cluster.ddata.or-set
  (:require
   [lotuc.akka.cluster.ddata.key :as ddata.key]
   [lotuc.akka.cluster.ddata.replicated-data :as ddata.replicated-data]
   [lotuc.akka.cnv :as cnv])
  (:import
   (akka.cluster.ddata ORSet ORSetKey)))

(defmethod ddata.replicated-data/->replicated-data* :or-set
  [_]
  (ORSet/create))

;;; key

(defmethod ddata.key/->ddata-key* :or-set [{:keys [key-id]}]
  (ORSetKey/apply key-id))

(defmethod cnv/->clj ORSetKey [^ORSetKey v]
  {:dtype :ddata-key :ddata-type :or-set :key-id (._id v)})
