(ns lotuc.examples.akka.distributed-data-voting-service
  (:require
   [lotuc.akka.cluster.ddata :as cluster.ddata]
   [lotuc.akka.common.log :refer [slf4j-log]]
   [lotuc.akka.javadsl.actor :as javadsl.actor]
   [lotuc.akka.javadsl.actor.behaviors :as behaviors]
   [lotuc.akka.javadsl.ddata :as javadsl.ddata]
   [lotuc.akka.system :refer [create-system-from-config]])
  (:import
   (java.time Duration)))

;;; https://developer.lightbend.com/start/?group=akka&project=akka-samples-distributed-data-java
;;; VotingService

(set! *warn-on-reflection* true)

(def opened-key (cluster.ddata/create-key :Flag "contest-opened"))
(def closed-key (cluster.ddata/create-key :Flag "contest-closed"))
(def counters-key (cluster.ddata/create-key :PNCounterMap "contest-counters"))

;;; https://doc.akka.io/japi/akka/current/akka/cluster/ddata/typed/javadsl/Replicator.html
(def write-all (javadsl.ddata/clj->data {:dtype :ReplicatorWriteAll :timeout (Duration/ofSeconds 5)}))
(def read-all (javadsl.ddata/clj->data {:dtype :ReplicatorReadAll :timeout (Duration/ofSeconds 3)}))
(defn write-local [] (javadsl.ddata/clj->data {:dtype :ReplicatorWriteLocal$}))

(defn tell [^akka.actor.typed.ActorRef target msg]
  (.tell target msg))

(defn response-adapter
  ([typ] (comp #(assoc % :action typ) javadsl.ddata/->clj))
  ([typ m] (comp #(merge (assoc % :action typ) m) javadsl.ddata/->clj)))

(defmacro info [ctx msg & args]
  `(slf4j-log (.getLog ~ctx) info ~msg ~@args))

(defn voting-service* [^akka.actor.typed.javadsl.ActorContext ctx
                       ^akka.cluster.ddata.SelfUniqueAddress node
                       replicator-flag
                       replicator-counters]
  (letfn [(receive-open []
            (info ctx "receive-open")
            (javadsl.ddata/ask-update replicator-flag
                                      (fn [reply-to]
                                        {:dtype :ReplicatorUpdate :dkey opened-key
                                         :initial (cluster.ddata/create-ddata {:dtype :Flag})
                                         :consistency write-all
                                         :reply-to reply-to
                                         :modify (fn [^akka.cluster.ddata.Flag v] (.switchOn v))})
                                      (response-adapter :UpdateResponse))
            (become-open))

          (on-subscribe-response [{:keys [dtype dkey data] :as m}]
            (info ctx "on-subscribe-response: {}" m)
            (or (when (= dtype :ReplicatorChanged)
                  (let [^akka.cluster.ddata.Flag data data]
                    (condp = dkey
                      opened-key (when (. data enabled) (become-open))
                      closed-key (when (. data enabled) (partial match-get-votes-impl false)))))
                :same))

          (on-get-response [open? {:keys [dtype dkey reply-to data] :as m}]
            (info ctx "on-get-response: {}" m)
            (or (some->> (case (when (= dkey counters-key) dtype)
                           :ReplicatorGetSuccess {:action :Votes
                                                  :result (.getEntries ^akka.cluster.ddata.PNCounterMap data)
                                                  :open? open?}
                           :ReplicatorNotFound   {:action :Votes
                                                  :results {}
                                                  :open? open?}
                           ;; skip
                           :ReplicatorGetFailure nil)
                         (tell reply-to))
                :same))

          (receive-get-votes-empty [{:keys [reply-to]}]
            (info ctx "receive-get-votes-empty")
            (tell reply-to {:action :Votes :result {} :open? false})
            :same)

          (receive-get-votes [{:keys [reply-to]}]
            (info ctx "receive-get-votes")
            (javadsl.ddata/ask-get replicator-counters
                                   (fn [reply-to]
                                     {:dtype :ReplicatorGet
                                      :dkey counters-key
                                      :consistency read-all
                                      :reply-to reply-to})
                                   (response-adapter :GetResponse {:reply-to reply-to}))
            :same)

          (receive-vote [{:keys [participant]}]
            (info ctx "receive-vote: {}" participant)
            (javadsl.ddata/ask-update replicator-counters
                                      (fn [reply-to]
                                        {:dtype :ReplicatorUpdate :dkey counters-key
                                         :initial (cluster.ddata/create-ddata {:dtype :PNCounterMap})
                                         :consistency (write-local)
                                         :reply-to reply-to
                                         :modify (fn [^akka.cluster.ddata.PNCounterMap v]
                                                   (.increment v node participant 1))})
                                      (response-adapter :UpdateResponse))
            :same)

          (receive-close []
            (info ctx "receive-close")
            (javadsl.ddata/ask-update replicator-flag
                                      (fn [reply-to]
                                        {:dtype :ReplicatorUpdate :dkey closed-key
                                         :initial (cluster.ddata/create-ddata {:dtype :Flag})
                                         :consistency write-all
                                         :reply-to reply-to
                                         :modify (fn [^akka.cluster.ddata.Flag v] (.switchOn v))})
                                      (response-adapter :UpdateResponse))
            (behaviors/setup (partial match-get-votes-impl false)))

          (match-open [handle-message]
            (behaviors/receive-message
             (fn [{:keys [action] :as m}]
               (info ctx "match-open: {}" action)
               (cond
                 (= action :Vote) (receive-vote m)
                 (= action :UpdateResponse) (do (info ctx "ignored: {}" m) :same)
                 (= m :Close) (receive-close)
                 (= action :SubscribeResponse) (on-subscribe-response m)
                 :else (handle-message m)))))

          (match-get-votes-impl [open? {:keys [action] :as m}]
            (cond
              (= action :GetVotes) (receive-get-votes m)
              (= action :GetResponse) (on-get-response open? m)
              (= action :UpdateResponse) (do (info ctx "ignored: {}" m) :same)))

          (become-open []
            (doto replicator-flag
              (javadsl.ddata/unsubscribe opened-key)
              (javadsl.ddata/subscribe closed-key (response-adapter :SubscribeResponse)))
            (match-open (partial match-get-votes-impl true)))]

    (javadsl.ddata/subscribe
     replicator-flag opened-key
     (response-adapter :SubscribeResponse))

    (behaviors/receive-message
     (fn [{:keys [action] :as m}]
       (info ctx "guardian: {}" action)
       (cond
         (= m :Open) (receive-open)
         (= action :SubscribeResponse) (on-subscribe-response m)
         (= action :GetVotes) (receive-get-votes-empty m))))))

(defn voting-service []
  (behaviors/setup
   (fn [^akka.actor.typed.javadsl.ActorContext ctx]
     (javadsl.ddata/with-replicator-message-adaptor
       (fn [replicator-flag]
         (javadsl.ddata/with-replicator-message-adaptor
           (fn [replicator-counters]
             (let [node (.selfUniqueAddress (javadsl.ddata/get-distributed-data
                                             (.. ctx getSystem)))]
               (voting-service* ctx node replicator-flag replicator-counters)))))))))

(defn startup [port]
  (create-system-from-config
   (voting-service)
   "ClusterSystem"
   "cluster-transformation"
   {"akka.remote.artery.canonical.port" port}))

(defn get-votes [^akka.actor.typed.ActorSystem system]
  (-> (javadsl.actor/ask
       system
       (fn [reply-to] {:action :GetVotes :reply-to reply-to})
       (Duration/ofSeconds 5)
       (.scheduler system))
      (.get)))

(comment
  (do (def s0 (startup 25251))
      (def s1 (startup 25252)))

  (.tell s0 :Open)
  (.tell s1 :Open)

  (get-votes s0)
  (get-votes s1)

  (.tell s0 {:action :Vote :participant "00"})
  (.tell s0 {:action :Vote :participant "01"})
  (.tell s1 {:action :Vote :participant "00"})
  (.tell s1 {:action :Vote :participant "01"})

  (do (.terminate s0)
      (.terminate s1)))
