(ns lotuc.akka.cluster.ddata.pn-counter
  (:require
   [lotuc.akka.cluster.ddata.key :as ddata.key]
   [lotuc.akka.cluster.ddata.replicated-data :as ddata.replicated-data]
   [lotuc.akka.cnv :as cnv])
  (:import
   (akka.cluster.ddata PNCounter PNCounterKey)))

(defmethod ddata.replicated-data/->replicated-data* :pn-counter [_]
  (PNCounter/create))

;;; key

(defmethod ddata.key/->ddata-key* :pn-counter [{:keys [key-id]}]
  (PNCounterKey/apply key-id))

(defmethod cnv/->clj PNCounterKey [^PNCounterKey v]
  {:dtype :ddata-key :ddata-type :pn-counter :key-id (._id v)})
