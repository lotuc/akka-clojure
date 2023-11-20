(ns lotuc.akka.ddata-java-dsl
  (:import
   (akka.cluster.ddata.typed.javadsl DistributedData)))

(set! *warn-on-reflection* true)

(defprotocol ReplicatorMessageAdapter
  (ask-delete [_ create-request response-adapter])
  (ask-get [_ create-request response-adapter])
  (ask-replica-count [_ create-request response-adapter])
  (ask-update [_ create-request response-adapter])
  (subscribe [_ ddata-key response-adapter])
  (unsubscribe [_ ddata-key]))

(defprotocol DataToCljCnv
  (->clj [_]))

(defprotocol CljToDataCnv
  (clj->data [_]))

(defmacro >java-util-function [f]
  `(reify java.util.function.Function (apply [_ v#] (~f v#))))

(defmacro >request-adapter [f]
  `(reify java.util.function.Function
     (apply [_ v#]
       (let [r# (~f v#)]
         (cond-> r#
           (map? r#) clj->data)))))

(defn with-replicator-message-adaptor [factory]
  (DistributedData/withReplicatorMessageAdapter (>java-util-function factory)))

(defn get-distributed-data ^DistributedData [^akka.actor.typed.ActorSystem system]
  (DistributedData/get system))

;;; https://doc.akka.io/japi/akka/current/akka/cluster/ddata/typed/javadsl/ReplicatorMessageAdapter.html
(extend-type akka.cluster.ddata.typed.javadsl.ReplicatorMessageAdapter
  ReplicatorMessageAdapter
  (ask-delete [this create-request response-adapter]
    (.askDelete this (>request-adapter create-request) (>java-util-function response-adapter)))
  (ask-get [this create-request response-adapter]
    (.askGet this (>request-adapter create-request) (>java-util-function response-adapter)))
  (ask-replica-count [this create-request response-adapter]
    (.askReplicaCount this (>request-adapter create-request) (>java-util-function response-adapter)))
  (ask-update [this create-request response-adapter]
    (.askUpdate this (>request-adapter create-request) (>java-util-function response-adapter)))
  (subscribe [this ddata-key response-adapter]
    (.subscribe this ddata-key (reify akka.japi.function.Function
                                 (apply [_ v] (response-adapter v)))))
  (unsubscribe [this ddata-key]
    (.unsubscribe this ddata-key)))

;;; https://doc.akka.io/japi/akka/current/akka/cluster/ddata/typed/javadsl/Replicator.html
(extend-protocol DataToCljCnv

  ;; Replicator.Command
  ;; https://doc.akka.io/japi/akka/current/akka/cluster/ddata/typed/javadsl/Replicator.Command.html

  akka.cluster.ddata.typed.javadsl.Replicator$Get
  (->clj [d] {:dtype :ReplicatorGet :dkey (.key d) :consistency (.consistency d) :reply-to (.replyTo d)})

  akka.cluster.ddata.typed.javadsl.Replicator$GetReplicaCount
  (->clj [d] {:dtype :ReplicatorGetReplicaCount :reply-to (.replyTo d)})

  akka.cluster.ddata.typed.javadsl.Replicator$Update
  (->clj [d] {:dtype :ReplicatorUpdate :dkey (.key d) :consistency (.writeConsistency d)
              :reply-to (.replyTo d) :modify (.modify d)})

  akka.cluster.ddata.typed.javadsl.Replicator$Delete
  (->clj [d] {:dtype :ReplicatorDelete :dkey (.key d) :consistency (.consistency d) :reply-to (.replyTo d)})

  akka.cluster.ddata.typed.javadsl.Replicator$FlushChanges$
  (->clj [_] {:dtype :ReplicatorFlushChanges$})

  akka.cluster.ddata.typed.javadsl.Replicator$Subscribe
  (->clj [d] {:dtype :ReplicatorSubscribe :dkey (.key d) :subscriber (.subscriber d)})

  akka.cluster.ddata.typed.javadsl.Replicator$Unsubscribe
  (->clj [d] {:dtype :ReplicatorUnsubscribe :dkey (.key d) :subscriber (.subscriber d)})

  ;; Replicator.SubscribeResponse
  ;; https://doc.akka.io/japi/akka/current/akka/cluster/ddata/typed/javadsl/Replicator.SubscribeResponse.html
  akka.cluster.ddata.typed.javadsl.Replicator$Changed
  (->clj [d] {:dtype :ReplicatorChanged :dkey (.key d) :data (.dataValue d)})

  akka.cluster.ddata.typed.javadsl.Replicator$Deleted
  (->clj [d] {:dtype :ReplicatorDeleted :dkey (.key d)})

  akka.cluster.ddata.typed.javadsl.Replicator$Expired
  (->clj [d] {:dtype :ReplicatorExpired :dkey (.key d)})

  ;; Replicator.GetResponse
  ;; https://doc.akka.io/japi/akka/current/akka/cluster/ddata/typed/javadsl/Replicator.GetResponse.html
  akka.cluster.ddata.typed.javadsl.Replicator$GetSuccess
  (->clj [d] {:dtype :ReplicatorGetSuccess :dkey (.key d) :data (.dataValue d)})

  akka.cluster.ddata.typed.javadsl.Replicator$GetDataDeleted
  (->clj [d] {:dtype :ReplicatorGetDataDeleted :dkey (.key d)})

  akka.cluster.ddata.typed.javadsl.Replicator$GetFailure
  (->clj [d] {:dtype :ReplicatorGetFailure :dkey (.key d)})

  akka.cluster.ddata.typed.javadsl.Replicator$NotFound
  (->clj [d] {:dtype :ReplicatorNotFound :dkey (.key d)})

  ;; Replicator.UpdateResponse
  ;; https://doc.akka.io/japi/akka/current/akka/cluster/ddata/typed/javadsl/Replicator.UpdateResponse.html
  akka.cluster.ddata.typed.javadsl.Replicator$UpdateSuccess
  (->clj [d] {:dtype :ReplicatorUpdateSuccess :dkey (.key d)})

  akka.cluster.ddata.typed.javadsl.Replicator$UpdateDataDeleted
  (->clj [d] {:dtype :ReplicatorUpdateDataDeleted :dkey (.key d)})

  ;; StoreFailure, ModifyFailure, UpdateTimeout
  akka.cluster.ddata.typed.javadsl.Replicator$UpdateFailure
  (->clj [_d] {:dtype :ReplicatorUpdateFailure})

  akka.cluster.ddata.typed.javadsl.Replicator$StoreFailure
  (->clj [d] {:dtype :ReplicatorStoreFailure :dkey (.key d)})

  akka.cluster.ddata.typed.javadsl.Replicator$ModifyFailure
  (->clj [d] {:dtype :ReplicatorModifyFailure :dkey (.key d) :cause (.cause d) :error-message (.errorMessage d)})

  akka.cluster.ddata.typed.javadsl.Replicator$UpdateTimeout
  (->clj [d] {:dtype :ReplicatorUpdateTimeout :dkey (.key d)})

  ;; Replicator.ReadConsistency
  ;; https://doc.akka.io/japi/akka/current/akka/cluster/ddata/typed/javadsl/Replicator.ReadConsistency.html
  akka.cluster.ddata.typed.javadsl.Replicator$ReadAll
  (->clj [d] {:dtype :ReplicatorReadAll :timeout (.timeout d)})

  akka.cluster.ddata.typed.javadsl.Replicator$ReadFrom
  (->clj [d] {:dtype :ReplicatorReadFrom :timeout (.timeout d) :n (.n d)})

  akka.cluster.ddata.typed.javadsl.Replicator$ReadLocal$
  (->clj [_d] {:dtype :ReplicatorReadLocal$})

  akka.cluster.ddata.typed.javadsl.Replicator$ReadMajority
  (->clj [d] {:dtype :ReplicatorReadMajority :timeout (.timeout d) :min-cap (.minCap d)})

  ;; Replicator.WriteConsistency
  ;; https://doc.akka.io/japi/akka/current/akka/cluster/ddata/typed/javadsl/Replicator.WriteConsistency.html
  akka.cluster.ddata.typed.javadsl.Replicator$WriteAll
  (-clj [d] {:dtype :ReplicatorWriteAll :timeout (.timeout d)})

  akka.cluster.ddata.typed.javadsl.Replicator$WriteTo
  (-clj [d] {:dtype :ReplicatorWriteTo :timeout (.timeout d) :n (.n d)})

  akka.cluster.ddata.typed.javadsl.Replicator$WriteLocal$
  (-clj [_d] {:dtype :ReplicatorWriteLocal$})

  akka.cluster.ddata.typed.javadsl.Replicator$WriteMajority
  (-clj [d] {:dtype :ReplicatorWriteMajority :timeout (.timeout d) :min-cap (.minCap d)})

  ;; Replicator.ReplicaCount (Current number of replicas)
  ;; https://doc.akka.io/japi/akka/current/akka/cluster/ddata/typed/javadsl/Replicator.ReplicaCount.html
  akka.cluster.ddata.typed.javadsl.Replicator$ReplicaCount
  (->clj [d] {:dtype :ReplicatorReplicaCount :n (.n d)}))

(defmulti ^:private clj->data* (fn [d] (:dtype d)))

(extend-type clojure.lang.IPersistentMap
  CljToDataCnv
  (clj->data [m] (clj->data* m)))

;;; only implements Command & Consistency now

(defmethod clj->data* :ReplicatorGet
  [{:keys [dkey consistency reply-to]}]
  (akka.cluster.ddata.typed.javadsl.Replicator$Get. dkey consistency reply-to))

(defmethod clj->data* :ReplicatorGetReplicaCount
  [{:keys [reply-to]}]
  (akka.cluster.ddata.typed.javadsl.Replicator$GetReplicaCount. reply-to))

(defmethod clj->data* :ReplicatorUpdate
  [{:keys [dkey initial consistency reply-to modify]}]
  (akka.cluster.ddata.typed.javadsl.Replicator$Update.
   dkey initial consistency reply-to
   (reify java.util.function.Function
     (apply [_ v] (if modify (modify v) v)))))

(defmethod clj->data* :ReplicatorDelete
  [{:keys [dkey consistency reply-to]}]
  (akka.cluster.ddata.typed.javadsl.Replicator$Delete. dkey consistency reply-to))

(defmethod clj->data* :ReplicatorFlushChanges$
  [_]
  (akka.cluster.ddata.typed.javadsl.Replicator$FlushChanges$.))

(defmethod clj->data* :ReplicatorSubscribe
  [{:keys [dkey subscriber]}]
  (akka.cluster.ddata.typed.javadsl.Replicator$Subscribe. dkey subscriber))

(defmethod clj->data* :ReplicatorUnsubscribe
  [{:keys [dkey subscriber]}]
  (akka.cluster.ddata.typed.javadsl.Replicator$Unsubscribe. dkey subscriber))

(defmethod clj->data* :ReplicatorReadAll
  [{:keys [timeout]}]
  (akka.cluster.ddata.typed.javadsl.Replicator$ReadAll. timeout))

(defmethod clj->data* :ReplicatorReadFrom
  [{:keys [n timeout]}]
  (akka.cluster.ddata.typed.javadsl.Replicator$ReadFrom. n timeout))

(defmethod clj->data* :ReplicatorReadLocal$
  [_]
  (akka.cluster.ddata.typed.javadsl.Replicator$ReadLocal$.))

(defmethod clj->data* :ReplicatorReadMajority
  [{:keys [min-cap timeout]}]
  (if min-cap
    (akka.cluster.ddata.typed.javadsl.Replicator$ReadMajority. timeout min-cap)
    (akka.cluster.ddata.typed.javadsl.Replicator$ReadMajority. timeout)))

(defmethod clj->data* :ReplicatorWriteAll
  [{:keys [timeout]}]
  (akka.cluster.ddata.typed.javadsl.Replicator$WriteAll. timeout))

(defmethod clj->data* :ReplicatorWriteTo
  [{:keys [n timeout]}]
  (akka.cluster.ddata.typed.javadsl.Replicator$WriteTo. n timeout))

(defmethod clj->data* :ReplicatorWriteLocal$
  [_]
  (akka.cluster.ddata.typed.javadsl.Replicator$WriteLocal$.))

(defmethod clj->data* :ReplicatorWriteMajority
  [{:keys [timeout min-cap]}]
  (if min-cap
    (akka.cluster.ddata.typed.javadsl.Replicator$WriteMajority. timeout min-cap)
    (akka.cluster.ddata.typed.javadsl.Replicator$WriteMajority. timeout)))
