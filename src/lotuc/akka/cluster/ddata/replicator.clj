(ns lotuc.akka.cluster.ddata.replicator
  (:require
   [lotuc.akka.cnv :as cnv]
   [lotuc.akka.common.scala :as scala]
   [lotuc.akka.cluster.ddata.replicated-data :as ddata.replicated-data]
   [lotuc.akka.cluster.ddata.key :as ddata.key]
   [lotuc.akka.cluster.ddata.replicator :as ddata.replicator])
  (:import
   (akka.cluster.ddata Replicator

                       Replicator$ReadConsistency
                       Replicator$ReadFrom
                       Replicator$ReadMajority
                       Replicator$ReadMajorityPlus
                       Replicator$ReadAll

                       Replicator$WriteConsistency
                       Replicator$WriteTo
                       Replicator$WriteMajority
                       Replicator$WriteMajorityPlus
                       Replicator$WriteAll

                       Replicator$Command
                       Replicator$ReplicatorMessage

                       Replicator$Get
                       Replicator$GetSuccess
                       Replicator$NotFound
                       Replicator$GetFailure
                       Replicator$GetDataDeleted

                       Replicator$Subscribe
                       Replicator$Unsubscribe
                       Replicator$Changed
                       Replicator$Deleted
                       Replicator$Expired

                       Replicator$Update
                       Replicator$UpdateSuccess
                       Replicator$UpdateTimeout
                       Replicator$UpdateDataDeleted
                       Replicator$ModifyFailure
                       Replicator$StoreFailure

                       Replicator$Delete
                       Replicator$DeleteSuccess
                       Replicator$ReplicationDeleteFailure
                       Replicator$DataDeleted)))

(set! *warn-on-reflection* true)

(defn ->replicator-command-response
  [{:keys [comresponse-type-type dkey request data] :as v}]
  (case [comresponse-type-type]
    [:get :success]
    (Replicator$GetSuccess. dkey request data)
    [:get :not-found]
    (Replicator$NotFound. dkey request)
    [:get :failure]
    (Replicator$GetFailure. dkey request)
    [:get :data-deleted]
    (Replicator$GetDataDeleted. dkey request)

    [:subscribe :changed]
    (Replicator$Changed. dkey data)
    [:subscribe :deleted]
    (Replicator$Deleted. dkey)
    [:subscribe :expired]
    (Replicator$Expired. dkey)

    [:update :success]
    (Replicator$UpdateSuccess. dkey request)
    [:update :data-deleted]
    (Replicator$UpdateDataDeleted. dkey request)
    ;; failures
    [:update :timeout]
    (Replicator$UpdateTimeout. dkey request)
    [:update :modify-failure]
    (Replicator$ModifyFailure. dkey (:error-message v) (:cause v) request)
    [:update :store-failure]
    (Replicator$StoreFailure. dkey request)

    [:delete :success]
    (Replicator$DeleteSuccess. dkey request)
    [:delete :delete-failure]
    (Replicator$ReplicationDeleteFailure. dkey request)
    [:delete :data-deleted]
    (Replicator$DataDeleted. dkey request)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; replicator-consistency

(defn replicator-consistency? [v]
  (or (instance? Replicator$ReadConsistency v)
      (instance? Replicator$WriteConsistency v)))

(defn ->replicator-consistency
  [consistency-like]
  {:post [replicator-consistency?]}
  (if (replicator-consistency? consistency-like)
    consistency-like
    (cnv/->akka consistency-like)))

(defmethod cnv/->akka :replicator-consistency
  [{:keys [timeout r w] :as v}]
  {:pre [(or r w)]}
  (let [timeout (some-> timeout scala/->scala.concurrent.duration.FiniteDuration)]
    (case [(if r :read :write) (or r w)]
      [:read :local]
      (Replicator/readLocal)
      [:read :from]
      (Replicator$ReadFrom. ^long (:n v) timeout)
      [:read :majority]
      (Replicator$ReadMajority.
       timeout
       (or ^long (:min-cap v)
           (Replicator/DefaultMajorityMinCap)))
      [:read :majority-plus]
      (Replicator$ReadMajorityPlus.
       timeout
       ^long (:additional v)
       (or ^long (:min-cap v)
           (Replicator/DefaultMajorityMinCap)))
      [:read :all]
      (Replicator$ReadAll. timeout)

      [:write :local]
      (Replicator/writeLocal)
      [:write :to]
      (Replicator$WriteTo. ^long (:n v) timeout)
      [:write :majority]
      (Replicator$WriteMajority.
       timeout
       (or ^long (:min-cap v)
           (Replicator/DefaultMajorityMinCap)))
      [:write :majority-plus]
      (Replicator$WriteMajorityPlus.
       timeout
       ^long (:additional v)
       (or ^long (:min-cap v)
           (Replicator/DefaultMajorityMinCap)))
      [:write :all]
      (Replicator$WriteAll. timeout))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; replicator-op

(defn replicator-command? [v] (instance? Replicator$Command v))
(defn replicator-message? [v] (instance? Replicator$ReplicatorMessage v))

(defn ->replicator-command [v]
  {:post [#(instance? Replicator$Command %)]}
  (if (instance? Replicator$Command v) v (cnv/->akka v)))

(defmethod cnv/->akka :replicator-command
  [{:keys [command dkey consistency reply-to request] :as v}]
  (let [dkey (some-> dkey ddata.key/->ddata-key)
        consistency (some-> consistency ddata.replicator/->replicator-consistency)]
    (case command
      :get
      (Replicator$Get. dkey consistency)
      :subscribe
      (Replicator$Subscribe. dkey reply-to)
      :unsubscribe
      (Replicator$Unsubscribe. dkey reply-to)
      :update
      (let [request (java.util.Optional/ofNullable request)
            initial (some-> (:initial v) ddata.replicated-data/->replicated-data)
            modify (reify java.util.function.Function
                     (apply [_ v] (if-some [f (:modify v)] (f v) v)))]
        (Replicator$Update. dkey initial consistency request modify))
      :delete
      (Replicator$Delete. dkey consistency)
      :get-replica-count
      (Replicator/getReplicaCount)
      :flush-changes
      (Replicator/flushChanges))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; replicator-op-response

;;; ->akka

(defmethod cnv/->akka :replicator-command-response [v]
  (->replicator-command-response v))

;;; ->clj

(defmethod cnv/->clj Replicator$GetSuccess [^Replicator$GetSuccess v]
  {:dtype :replicator-command-response :command :get :response-type :success
   :dkey (cnv/->clj (.key v)) :request (.request v) :data (.dataValue v)})

(defmethod cnv/->clj Replicator$NotFound [^Replicator$NotFound v]
  {:dtype :replicator-command-response :command :get :response-type :not-found
   :dkey (cnv/->clj (.key v)) :request (.request v)})

(defmethod cnv/->clj Replicator$GetFailure [^Replicator$GetFailure v]
  {:dtype :replicator-command-response :command :get :response-type :get-failure
   :dkey (cnv/->clj (.key v)) :request (.request v)})

(defmethod cnv/->clj Replicator$GetDataDeleted [^Replicator$GetDataDeleted v]
  {:dtype :replicator-command-response :command :get :response-type :data-deleted
   :dkey (cnv/->clj (.key v)) :request (.request v)})

(defmethod cnv/->clj Replicator$Changed [^Replicator$Changed v]
  {:dtype :replicator-command-response :command :subscribe :response-type :changed
   :dkey (cnv/->clj (.key v)) :data (.dataValue v)})

(defmethod cnv/->clj Replicator$Deleted [^Replicator$Deleted v]
  {:dtype :replicator-command-response :command :subscribe :response-type :deleted
   :dkey (cnv/->clj (.key v))})

(defmethod cnv/->clj Replicator$Expired [^Replicator$Expired v]
  {:dtype :replicator-command-response :command :subscribe :response-type :expired
   :dkey (cnv/->clj (.key v))})

(defmethod cnv/->clj Replicator$UpdateSuccess [^Replicator$UpdateSuccess v]
  {:dtype :replicator-command-response :command :update :response-type :success
   :dkey (cnv/->clj (.key v)) :request (.request v)})

(defmethod cnv/->clj Replicator$UpdateTimeout [^Replicator$UpdateTimeout v]
  {:dtype :replicator-command-response :command :update :response-type :timeout
   :dkey (cnv/->clj (.key v)) :request (.request v)})

(defmethod cnv/->clj Replicator$UpdateDataDeleted [^Replicator$UpdateDataDeleted v]
  {:dtype :replicator-command-response :command :update :response-type :data-deleted
   :dkey (cnv/->clj (.key v)) :request (.request v)})

(defmethod cnv/->clj Replicator$ModifyFailure [^Replicator$ModifyFailure v]
  {:dtype :replicator-command-response :command :update :response-type :data-deleted
   :dkey (cnv/->clj (.key v)) :request (.request v)
   :cause (.cause v) :error-message (.errorMessage v)})

(defmethod cnv/->clj Replicator$StoreFailure [^Replicator$StoreFailure v]
  {:dtype :replicator-command-response :command :update :response-type :store-failure
   :dkey (cnv/->clj (.key v)) :request (.request v)})

(defmethod cnv/->clj Replicator$DeleteSuccess [^Replicator$DeleteSuccess v]
  {:dtype :replicator-command-response :command :delete :response-type :success
   :dkey (cnv/->clj (.key v)) :request (.request v)})

(defmethod cnv/->clj Replicator$ReplicationDeleteFailure [^Replicator$ReplicationDeleteFailure v]
  {:dtype :replicator-command-response :command :delete :response-type :failure
   :dkey (cnv/->clj (.key v)) :request (.request v)})

(defmethod cnv/->clj Replicator$DataDeleted [^Replicator$DataDeleted v]
  {:dtype :replicator-command-response :command :delete :response-type :data-deleted
   :dkey (cnv/->clj (.key v)) :request (.request v)})

(comment
  (cnv/->akka {:dtype :replicator-consistency
               :r :local})
  (cnv/->akka {:dtype :replicator-consistency
               :r :all
               :timeout "1.sec"}))
