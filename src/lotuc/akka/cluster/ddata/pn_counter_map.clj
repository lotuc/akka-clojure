(ns lotuc.akka.cluster.ddata.pn-counter-map
  (:require
   [lotuc.akka.cluster.ddata.key :as ddata.key]
   [lotuc.akka.cluster.ddata.replicated-data :as ddata.replicated-data]
   [lotuc.akka.cnv :as cnv])
  (:import
   (akka.cluster UniqueAddress)
   (akka.cluster.ddata PNCounterMap PNCounterMapKey SelfUniqueAddress)))

(defmethod ddata.replicated-data/->replicated-data* :pn-counter-map [_]
  (PNCounterMap/create))

(defn increment [^PNCounterMap v node key ^long delta]
  (cond
    (instance? SelfUniqueAddress node)
    (.increment v ^SelfUniqueAddress node key delta)

    (instance? UniqueAddress)
    (.increment v ^UniqueAddress node key delta)

    :else
    (throw (ex-info (str "illegal node type: " (type node)) {:value node}))))

(defn get-entries [^PNCounterMap v]
  (into {} (.getEntries v)))

;;; key

(defmethod ddata.key/->ddata-key* :pn-counter-map [{:keys [key-id]}]
  (PNCounterMapKey/apply key-id))

(defmethod cnv/->clj PNCounterMapKey [^PNCounterMapKey v]
  {:dtype :ddata-key :ddata-type :pn-counter-map :key-id (._id v)})
