(ns lotuc.akka.ddata
  (:import
   (akka.cluster.ddata FlagKey Flag
                       PNCounterMapKey PNCounterMap
                       GCounterKey GCounter
                       GSetKey GSet
                       LWWMapKey LWWMap
                       LWWRegisterKey LWWRegister LWWRegister$Clock)))

(set! *warn-on-reflection* true)

;;; https://doc.akka.io/japi/akka/current/akka/cluster/ddata/ReplicatedData.html

;;; TODO: more to add

(defmulti create-key (fn [dtype _k] dtype))
(defmulti create-ddata (fn [d] (:dtype d)))

(defmethod create-key :Flag [_ k] (FlagKey/create k))
(defmethod create-ddata :Flag [_] (Flag/create))

(defmethod create-key :PNCounterMap [_ k] (PNCounterMapKey/create k))
(defmethod create-ddata :PNCounterMap [_] (PNCounterMap/create))

(defmethod create-key :GCounter [_ k] (GCounterKey/create k))
(defmethod create-ddata :GCounter [_] (GCounter/create))

(defmethod create-key :GSet [_ k] (GSetKey/create k))
(defmethod create-ddata :GSet [_] (GSet/create))

(defmethod create-key :LWWMap [_ k] (LWWMapKey/create k))
(defmethod create-ddata :LWWMap [_] (LWWMap/create))

(defmethod create-key :LWWRegister [_ k] (LWWRegisterKey/create k))
(defmethod create-ddata :LWWRegister [{:keys [^akka.cluster.ddata.SelfUniqueAddress node
                                              value
                                              ^LWWRegister$Clock clock]}]
  (if clock
    (LWWRegister/create node value)
    (LWWRegister/create node value clock)))
