(ns lotuc.akka.cluster.ddata.g-set
  (:require
   [lotuc.akka.cluster.ddata.key :as ddata.key]
   [lotuc.akka.cluster.ddata.replicated-data :as ddata.replicated-data]
   [lotuc.akka.cnv :as cnv])
  (:import
   (akka.cluster.ddata GSet GSetKey)))

(defmethod ddata.replicated-data/->replicated-data* :g-set [_]
  (GSet/create))

;;; key

(defmethod ddata.key/->ddata-key* :g-set [{:keys [key-id]}]
  (GSetKey/apply key-id))

(defmethod cnv/->clj GSetKey [^GSetKey v]
  {:dtype :ddata-key :ddata-type :g-set :key-id (._id v)})
