(ns lotuc.examples.akka.distributed-data-voting-service
  (:require
   [lotuc.akka.actor.typed.actor-ref :as actor-ref]
   [lotuc.akka.cluster.ddata.flag :as ddata.flag]
   [lotuc.akka.cluster.ddata.pn-counter-map :as ddata.pn-counter-map]
   [lotuc.akka.cluster.ddata.scaladsl :as ddata-dsl]
   [lotuc.akka.cluster.scaladsl :as cluster-dsl]
   [lotuc.akka.cnv :as cnv]
   [lotuc.akka.common.slf4j :refer [slf4j-log]]
   [lotuc.akka.actor.scaladsl :as dsl]
   [lotuc.akka.actor.typed.scaladsl.ask-pattern :as scaladsl.ask-pattern]
   [lotuc.akka.actor.typed.actor-system :as actor-system]))

;;; https://developer.lightbend.com/start/?group=akka&project=akka-samples-distributed-data-java
;;; VotingService

(set! *warn-on-reflection* true)

(def opened-key   {:dtype :ddata-key :ddata-type :flag :key-id "contest-opened"})
(def closed-key   {:dtype :ddata-key :ddata-type :flag :key-id "contest-closed"})
(def counters-key {:dtype :ddata-key :ddata-type :pn-counter-map :key-id "contest-counters"})

(def write-all   {:dtype :replicator-consistency :w :all :timeout "5.sec"})
(def read-all    {:dtype :replicator-consistency :r :all :timeout "3.sec"})
(def write-local {:dtype :replicator-consistency :w :local})

(defn response-adapter
  ([typ] (comp #(assoc % :action typ) cnv/->clj))
  ([typ m] (comp #(merge (assoc % :action typ) m) cnv/->clj)))

(defmacro info [ctx msg & args]
  `(slf4j-log (dsl/log ~ctx) info ~msg ~@args))

(defn voting-service* [ctx
                       node
                       replicator-flag
                       replicator-counters]
  (letfn [(receive-open []
            (info ctx "receive-open")
            (cluster-dsl/ask-update
             replicator-flag
             (fn [reply-to]
               {:dkey opened-key
                :initial {:dtype :ddata :ddata-type :flag}
                :consistency write-all
                :reply-to reply-to
                :modify ddata.flag/switch-on})
             (response-adapter :UpdateResponse))
            (become-open))

          (on-subscribe-response [{:keys [command response-type dkey data] :as m}]
            (info ctx "on-subscribe-response: {}" m)
            (when (= [command response-type] [:subscribe :changed])
              (condp = dkey
                opened-key (when (ddata.flag/enabled? data)
                             (become-open))
                closed-key (when (ddata.flag/enabled? data)
                             (partial match-get-votes-impl false))))
            :same)

          (on-get-response [open? {:keys [command dkey response-type reply-to data] :as m}]
            (info ctx "on-get-response: {}" m)
            (when (and (= dkey counters-key) (= command :get))
              (case response-type
                :success
                (->> {:action :Votes :open? open?
                      :result (ddata.pn-counter-map/get-entries data)}
                     (actor-ref/tell reply-to))
                :not-found
                (->> {:action :Votes :open? open?
                      :results {}}
                     (actor-ref/tell reply-to))
                :get-failure nil))
            :same)

          (receive-get-votes-empty [{:keys [reply-to]}]
            (info ctx "receive-get-votes-empty")
            (actor-ref/tell reply-to {:action :Votes :result {} :open? false})
            :same)

          (receive-get-votes [{:keys [reply-to]}]
            (info ctx "receive-get-votes")
            (cluster-dsl/ask-get
             replicator-counters
             (fn [reply-to]
               {:dkey counters-key
                :consistency read-all
                :reply-to reply-to})
             (response-adapter :GetResponse {:reply-to reply-to}))
            :same)

          (receive-vote [{:keys [participant]}]
            (info ctx "receive-vote: {}" participant)
            (cluster-dsl/ask-update
             replicator-counters
             (fn [reply-to]
               {:dkey counters-key
                :initial {:dtype :ddata :ddata-type :pn-counter-map}
                :consistency write-local
                :reply-to reply-to
                :modify #(ddata.pn-counter-map/increment % node participant 1)})
             (response-adapter :UpdateResponse))
            :same)

          (receive-close []
            (info ctx "receive-close")
            (cluster-dsl/ask-update
             replicator-flag
             (fn [reply-to]
               {:dkey closed-key
                :initial {:dtype :ddata :ddata-type :flag}
                :consistency write-all
                :reply-to reply-to
                :modify ddata.flag/switch-on})
             (response-adapter :UpdateResponse))
            (dsl/setup (partial match-get-votes-impl false)))

          (match-open [handle-message]
            (dsl/receive-message
             (fn [{:keys [action] :as m}]
               (info ctx "match-open: {}" m)
               (case (if (keyword? m) m action)
                 :Vote (receive-vote m)
                 :UpdateResponse (do (info ctx "ignored: {}" m) :same)
                 :Close (receive-close)
                 :SubscribeResponse (on-subscribe-response m)
                 (handle-message m)))))

          (match-get-votes-impl [open? {:keys [action] :as m}]
            (case action
              :GetVotes (receive-get-votes m)
              :GetResponse (on-get-response open? m)
              :UpdateResponse (do (info ctx "ignored: {}" m) :same)
              nil))

          (become-open []
            (doto replicator-flag
              (cluster-dsl/unsubscribe-ddata opened-key)
              (cluster-dsl/subscribe-ddata closed-key (response-adapter :SubscribeResponse)))
            (match-open (partial match-get-votes-impl true)))]

    (cluster-dsl/subscribe-ddata
     replicator-flag opened-key (response-adapter :SubscribeResponse))

    (dsl/receive-message
     (fn [{:keys [action] :as m}]
       (info ctx "guardian: {}" action)
       (case (if (keyword? m) m action)
         :Open (receive-open)
         :SubscribeResponse (on-subscribe-response m)
         :GetVotes (receive-get-votes-empty m))))))

(defn voting-service []
  (dsl/setup
   (fn [ctx]
     (ddata-dsl/with-replicator-message-adapter
       (fn [replicator-flag]
         (ddata-dsl/with-replicator-message-adapter
           (fn [replicator-counters]
             (let [system (dsl/system ctx)
                   node (.selfUniqueAddress (cluster-dsl/get-distributed-data system))]
               (voting-service* ctx node replicator-flag replicator-counters)))))))))

(defn startup [port]
  (actor-system/create-system-from-config
   (voting-service)
   "ClusterSystem"
   "cluster-transformation"
   {"akka.remote.artery.canonical.port" port}))

(defn get-votes [^akka.actor.typed.ActorSystem system]
  (-> (scaladsl.ask-pattern/ask
       system
       (fn [reply-to] {:action :GetVotes :reply-to reply-to})
       "5.sec"
       (.scheduler system))))

(comment
  (do (def s0 (startup 25251))
      (def s1 (startup 25252)))

  (actor-ref/tell s0 :Open)
  (actor-ref/tell s1 :Open)

  @(get-votes s0)
  @(get-votes s1)

  (actor-ref/tell s0 {:action :Vote :participant "00"})
  (actor-ref/tell s0 {:action :Vote :participant "01"})
  (actor-ref/tell s1 {:action :Vote :participant "00"})
  (actor-ref/tell s1 {:action :Vote :participant "01"})

  (do (.terminate s0)
      (.terminate s1)))
