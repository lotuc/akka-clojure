(ns lotuc.akka.cnv)

(defmulti ->clj
  "returns a map with `dtype` key indicating data type."
  class)

(defmulti ->akka
  "convert a value to akka related object."
  (fn [v] (or (:dtype v) v)))

;;; signals
;;; [[lotuc.akka.actor.typed.message-and-signals/signal?]]

;;; receptionist
;;; [[lotuc.akka.actor.typed.receptionist/receptionist-listing?]]

;;; supervisor strategy
;;; [[lotuc.akka.actor.typed.supervisor-strategy/supervisor-strategy?]]

;;; cluster.ddata.replicator
;;; [[lotuc.akka.cluster.ddata.replicator]]

;;; cluster.ddata.key
;;; [[lotuc.akka.cluster.ddata.key/ddata-key?]]

;;; cluster.ddata.replicated-data
;;; [[lotuc.akka.cluster.ddata.replicated-data]]

;;; cluster event
;;; [[lotuc.akka.cluster.cluster-event/cluster-event?]]
;;; [[lotuc.akka.cluster.cluster-event/member-reachability-event?]]
;;; [[lotuc.akka.cluster.cluster-event/dc-reachability-event?]]
;;; [[lotuc.akka.cluster.cluster-event/member-event?]]
