(ns lotuc.akka.cluster.ddata.lww-map
  (:refer-clojure :exclude [contains? get remove])
  (:require
   [lotuc.akka.cluster.ddata.key :as ddata.key]
   [lotuc.akka.cluster.ddata.replicated-data :as ddata.replicated-data]
   [lotuc.akka.cnv :as cnv])
  (:import
   (akka.cluster.ddata LWWMap LWWMapKey LWWRegister$Clock
                       SelfUniqueAddress)))

(set! *warn-on-reflection* true)

(defmethod ddata.replicated-data/->replicated-data* :lww-map [_]
  (LWWMap/create))

(defn contains? [^LWWMap m k]
  (.contains m k))

(defn get [^LWWMap m k]
  (let [v (.get m k)]
    (when (.isDefined v)
      (.get v))))

(defn put
  ([^LWWMap m ^SelfUniqueAddress node k v]
   {:pre [(some? v)]}
   (.put m node k v))
  ([^LWWMap m ^SelfUniqueAddress node k v ^LWWRegister$Clock clock]
   {:pre [(some? v)]}
   (.put m node k v clock)))

(defn get-entries [^LWWMap m]
  (into {} (.getEntries m)))

(defn size [^LWWMap m]
  (.size m))

(defn remove [^LWWMap m ^SelfUniqueAddress node k]
  (.remove m node k))

(defn is-empty [^LWWMap m]
  (.isEmpty m))

;;; key

(defmethod ddata.key/->ddata-key* :lww-map [{:keys [key-id]}]
  (LWWMapKey/apply key-id))

(defmethod cnv/->clj LWWMapKey [^LWWMapKey v]
  {:dtype :ddata-key :ddata-type :lww-map :key-id (._id v)})
