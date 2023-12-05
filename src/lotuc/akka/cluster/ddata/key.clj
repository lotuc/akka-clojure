(ns lotuc.akka.cluster.ddata.key
  (:require
   [lotuc.akka.cnv :as cnv])
  (:import
   (akka.cluster.ddata Key)))

(defmulti ->ddata-key* :ddata-type)

(defn ddata-key? [v] (instance? Key v))

(defmethod cnv/->akka :ddata-key
  [{:keys [ddata-type] :as v}]
  (->ddata-key* v))

(defn ->ddata-key [ddata-key-like]
  (if (instance? Key ddata-key-like)
    ddata-key-like
    (let [r (cnv/->akka ddata-key-like)]
      (assert (instance? Key r))
      r)))

(defn same-key? [k0 k1]
  (= (->ddata-key k0) (->ddata-key k1)))

(comment
  (require '[lotuc.akka.cluster.ddata.flag])
  (cnv/->akka {:dtype :ddata-key :ddata-type :flag :key-id "abc"}))
