(ns lotuc.akka.cluster.typed.scaladsl.replicator
  (:require
   [lotuc.akka.cluster.ddata.replicated-data :as ddata.replicated-data]
   [lotuc.akka.cluster.ddata.key :as ddata.key]
   [lotuc.akka.cluster.ddata.replicator :as ddata.replicator])
  (:import
   (akka.cluster.ddata.typed.scaladsl
    Replicator$Delete
    Replicator$Get
    Replicator$Subscribe
    Replicator$Unsubscribe
    Replicator$Update
    Replicator$GetReplicaCount
    Replicator$FlushChanges$)))

(set! *warn-on-reflection* true)

(defn replicator-get-command
  [{:keys [dkey consistency reply-to]}]
  (let [dkey (some-> dkey ddata.key/->ddata-key)
        consistency (some-> consistency ddata.replicator/->replicator-consistency)]
    (Replicator$Get. dkey consistency reply-to)))

(defn replicator-subscribe-command
  [{:keys [dkey reply-to]}]
  (let [dkey (some-> dkey ddata.key/->ddata-key)]
    (Replicator$Subscribe. dkey reply-to)))

(defn replicator-unsubscribe-command
  [{:keys [dkey reply-to]}]
  (let [dkey (some-> dkey ddata.key/->ddata-key)]
    (Replicator$Unsubscribe. dkey reply-to)))

(defn replicator-update-command
  [{:keys [dkey initial consistency reply-to modify] :as v}]
  (let [dkey (some-> dkey ddata.key/->ddata-key)
        initial (some-> initial ddata.replicated-data/->replicated-data)
        consistency (some-> consistency ddata.replicator/->replicator-consistency)
        reply-to reply-to
        modify (reify scala.Function1
                 (apply [_ v]
                   (let [^scala.Option v v]
                     (if (.isEmpty v)
                       initial
                       (cond-> (.get v) modify modify)))))]
    (Replicator$Update. dkey consistency reply-to modify)))

(comment
  (replicator-update-command
   {:dkey {:dtype :ddata-key :ddata-type :flag :key-id "42"}
    :initial {:dtype :ddata :ddata-type :flag}
    :consistency {:dtype :replicator-consistency :w :all :timeout "3.sec"}
    :reply-to nil}))

(defn replicator-delete-command [{:keys [dkey consistency reply-to]}]
  (let [dkey (some-> dkey ddata.key/->ddata-key)
        consistency (some-> consistency ddata.replicator/->replicator-consistency)]
    (Replicator$Delete. dkey consistency reply-to)))

(defn replicator-get-replica-count-command [{:keys [reply-to]}]
  (Replicator$GetReplicaCount. reply-to))

(defn replicator-flush-changes-command [_]
  (Replicator$FlushChanges$.))

(defn ->replica-dsl-command [{:keys [command] :as v}]
  (case command
    :get (replicator-get-command v)
    :subscribe (replicator-subscribe-command v)
    :unsubscribe (replicator-unsubscribe-command v)
    :update (replicator-update-command v)
    :delete (replicator-delete-command v)
    :get-replica-count (replicator-get-replica-count-command v)
    :flush-changes (replicator-flush-changes-command v)))
