(ns lotuc.akka.cluster.ddata.lww-register
  (:require
   [lotuc.akka.cluster.ddata.key :as ddata.key]
   [lotuc.akka.cluster.ddata.replicated-data :as ddata.replicated-data]
   [lotuc.akka.cnv :as cnv])
  (:import
   (akka.cluster.ddata
    LWWRegister
    LWWRegister$Clock
    LWWRegisterKey
    SelfUniqueAddress)))

(defmethod ddata.replicated-data/->replicated-data* :lww-register
  [{:keys [^SelfUniqueAddress node ^LWWRegister$Clock clock value]}]
  (if clock
    (LWWRegister/create node value clock)
    (LWWRegister/create node value)))

;;; key

(defmethod ddata.key/->ddata-key* :lww-register [{:keys [key-id]}]
  (LWWRegisterKey/apply key-id))

(defmethod cnv/->clj LWWRegisterKey [^LWWRegisterKey v]
  {:dtype :ddata-key :ddata-type :lww-register :key-id (._id v)})
