(ns lotuc.akka.cluster.ddata.replicated-data
  (:require
   [lotuc.akka.cnv :as cnv])
  (:import
   [akka.cluster.ddata ReplicatedData]))

(defmulti ->replicated-data* :ddata-type)

(defmethod cnv/->akka :ddata
  [{:keys [ddata-type] :as v}]
  (->replicated-data* v))

(defn ->replicated-data [replicated-data-like?]
  (if (instance? ReplicatedData replicated-data-like?)
    replicated-data-like?
    (let [r (cnv/->akka replicated-data-like?)]
      (assert (instance? ReplicatedData r))
      r)))

(comment
  (require '[lotuc.akka.cluster.ddata.flag])
  (cnv/->akka {:dtype :ddata :ddata-type :flag}))
