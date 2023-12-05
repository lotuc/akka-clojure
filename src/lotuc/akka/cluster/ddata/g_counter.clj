(ns lotuc.akka.cluster.ddata.g-counter
  (:require
   [lotuc.akka.cluster.ddata.key :as ddata.key]
   [lotuc.akka.cluster.ddata.replicated-data :as ddata.replicated-data]
   [lotuc.akka.cnv :as cnv])
  (:import
   (akka.cluster.ddata GCounter GCounterKey)))

(defmethod ddata.replicated-data/->replicated-data* :g-counter [_]
  (GCounter/create))

;;; key

(defmethod ddata.key/->ddata-key* :g-counter [{:keys [key-id]}]
  (GCounterKey/apply key-id))

(defmethod cnv/->clj GCounterKey [^GCounterKey v]
  {:dtype :ddata-key :ddata-type :g-counter :key-id (._id v)})
