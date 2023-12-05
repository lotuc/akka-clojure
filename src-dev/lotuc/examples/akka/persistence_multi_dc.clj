(ns lotuc.examples.akka.persistence-multi-dc
  (:require
   [clojure.set :as set]
   [lotuc.akka.actor.scaladsl :as dsl]
   [lotuc.akka.actor.typed.actor-system :as actor-system]
   [lotuc.akka.actor.typed.scaladsl.ask-pattern :as scaladsl.ask-pattern])
  (:import
   (akka.cluster.sharding.typed ReplicatedEntityProvider ReplicatedShardingExtension)
   (akka.pattern StatusReply)
   (akka.persistence.cassandra.query.scaladsl CassandraReadJournal)
   (akka.persistence.typed RecoveryCompleted ReplicaId ReplicationId)
   (akka.persistence.typed.crdt ORSet)
   (akka.persistence.typed.scaladsl Effect EventSourcedBehavior ReplicatedEventSourcing)))

;;; https://developer.lightbend.com/start/?group=akka&project=akka-samples-persistence-dc-java

(def all-replicas (-> (scala.collection.immutable.HashSet.)
                      (.$plus (ReplicaId/apply "eu-west"))
                      (.$plus (ReplicaId/apply "eu-central"))))

(defn then-run [effect f]
  (.thenRun effect (reify scala.Function1 (apply [_ v] (f v)))))

(defn auction* [{:keys [context
                        replication-context
                        timers]}
                closing-at
                responsible-for-closing?
                initial-bid]
  (letfn [(should-close? [state]
            (and responsible-for-closing?
                 (when (= :Closing (:phase state))
                   (let [finished-at-replica (:finished-at-replica state)
                         all-done (empty? (set/difference all-replicas finished-at-replica))]
                     all-done))))
          (recovery-completed [state]
            (when (should-close? state)
              (.tell (.getSelf context) :Close))
            (let [millis-until-closing (- closing-at (.currentTimeMillis replication-context))]
              (->> {:timer-key :Finish
                    :timer-type :single
                    :delay (str millis-until-closing ".millis")}
                   (dsl/start-timer timers :Finish))))

          (read-only-command-handler [{:keys [phase highest-bid highest-counter-offer] :as state}
                                      {:keys [action reply-to] :as command}]
            (case action
              :GetHighestBid
              (do (.tell reply-to (assoc highest-bid :offer highest-counter-offer))
                  (Effect/none))

              :IsClosed
              (do (.tell reply-to (= phase :Closed))
                  (Effect/none))

              :Finish
              (-> (Effect/persist {:ev-type :AuctionFinished
                                   :at-replica (.replicaId replication-context)})
                  (then-run (fn [_] (.tell reply-to :ok))))

              :Close
              (do (assert (should-close? state))
                  (-> (Effect/persist {:ev-type :WinnerDecided
                                       :at-replica (.replicaId replication-context)
                                       :winning-bid highest-bid
                                       :highest-counter-offer highest-counter-offer})
                      (then-run (fn [_] (.tell reply-to :ok)))))

              (Effect/unhandled)))

          (running-command-handler [{:keys [phase highest-bid highest-counter-offer] :as state}
                                    {:keys [action reply-to] :as command}]
            (case action
              :OfferBid
              (let [{:keys [bidder offer]} command]
                (-> (Effect/persist {:ev-type :BidRegistered
                                     :bid {:bidder bidder
                                           :offer offer
                                           :timestamp (System/currentTimeMillis)
                                           :origin-replica (.replicaId replication-context)}})
                    (then-run (fn [_] (.tell reply-to :ok)))))
              :GetHighestBid
              (do (.tell reply-to highest-bid)
                  (Effect/none))
              :Finish
              (-> (Effect/persist {:ev-type :AuctionFinished
                                   :at-replica (.replicaId replication-context)})
                  (then-run (fn [_] (.tell reply-to :ok))))
              :Close
              (Effect/unhandled)
              :IsClosed
              (do (.tell reply-to false)
                  (Effect/none))))

          (is-higher-bid [a b]
            (or (> (:offer a) (:offer b))
                (and (= (:offer a) (:offer b)) (< (:timestamp a) (:timestamp b))) ; first wins
                (and (= (:offer a) (:offer b)) (< (:timestamp a) (:timestamp b))
                     (< (.compareTo (.id (:origin-replica a)) (.id (:origin-replica b))) 0))))

          (apply-event [{:keys [highest-bid phase] :as state}
                        {:keys [ev-type] :as event}]
            (case ev-type
              :BidRegistered
              (let [bid (:bid event)]
                (cond
                  (is-higher-bid bid highest-bid)
                  (do (assert (not= phase :Closed))
                      (assoc state
                             :highest-bid bid
                             :highest-counter-offer (:offer highest-bid)))
                  (is-higher-bid highest-bid bid)
                  (do (assert (not= phase :Closed))
                      (assoc state
                             :highest-counter-offer (max (:offer bid) (:offer highest-bid))))))

              :AuctionFinished
              (case phase
                :Running (->> {:action :Closing
                               :finished-at-replica #{(:at-replica event)}}
                              (assoc state :phase))
                :Closing (->> {:action :Closing
                               :finished-at-replica (conj
                                                     (:finished-at-replica event)
                                                     (:at-replica event))}
                              (assoc state :phase))
                state)

              :WinnerDecided
              (assoc state :phase :Closed)))

          (event-triggers [{:keys [ev-type] :as event} new-state]
            (case ev-type
              :AuctionFinished
              (when (= :Closing (:phase new-state))
                (if ((:finished-at-replica new-state) (.replicaId replication-context))
                  (when (should-close? new-state)
                    (.tell (.self context) :Close))
                  (.tell (.self context) :Finish)))
              nil))]

    (-> (EventSourcedBehavior/apply
         (.persistenceId replication-context)
         {:phase :Running :highest-bid initial-bid :highest-counter-offer (:offer initial-bid)}
         (reify scala.Function2
           (apply [_ {:keys [phase] :as state} command]
             (println (str "!!! " phase "<<" command))
             (case phase
               :Closing (read-only-command-handler state command)
               :Running (running-command-handler state command))))
         (reify scala.Function2
           (apply [_ state event]
             (let [state' (apply-event state event)]
               (when-not (.recoveryRunning replication-context)
                 (event-triggers event state'))
               state'))))

        (.receiveSignal
         (reify scala.PartialFunction
           (isDefinedAt [_ _t] true)
           (apply [_ t]
             (let [[state signal] [(._1 t) (._2 t)]]
               (when (instance? RecoveryCompleted signal)
                 (recovery-completed state)))))))))

(defn auction [replication-id initial-bid closing-at responsible-for-closing]
  (dsl/setup
   (fn [context]
     (dsl/with-timers
       (fn [timers]
         (ReplicatedEventSourcing/commonJournalConfig
          replication-id
          all-replicas
          (CassandraReadJournal/Identifier)
          (reify scala.Function1
            (apply [_ replication-ctx]
              (-> {:context context :timers timers :replication-context replication-ctx}
                  (auction* closing-at responsible-for-closing initial-bid))))))))))

(defn bank-account-event-sourced-behavior [replication-ctx context]
  (EventSourcedBehavior/apply
   (.persistenceId replication-ctx)
   {:balance 0}
   (reify scala.Function2
     (apply [_ {balance :amount :as state}
             {:keys [action reply-to amount] :as command}]
       (case action
         :Deposit
         (-> (Effect/persist {:ev-type :Deposited :amount amount})
             (then-run (fn [_] (.tell reply-to (StatusReply/ack)))))
         :Withdraw
         (if (>= (- balance amount) 0)
           (-> (Effect/persist {:ev-type :Withdrawn :amount amount})
               (then-run (fn [_] (.tell reply-to (StatusReply/ack)))))
           (-> (Effect/none)
               (then-run (fn [_] (.tell reply-to (StatusReply/error "insufficent funds"))))))
         :GetBalance
         (-> (Effect/none)
             (then-run (fn [_] (.tell reply-to (StatusReply/success balance)))))
         :AlertOverdrawn
         (Effect/persist {:ev-type :Overdrawn :amount (:amount command)}))))
   (reify scala.Function2
     (apply [_ state {:keys [ev-type amount] :as event}]
       (let [{balance :amount :as state'}
             (case ev-type
               :Deposited (update state :amount (fnil + 0) amount)
               :Withdrawn (update state :amount (fnil - 0) amount)
               :Overdrawn state)]
         (when (and
                ;; this event happened concurrently with other events already processed
                (.concurrent replication-ctx)
                ;; if we only want to do the side effect in a single DC
                (= (.replicaId replication-ctx) (ReplicaId/apply "eu-central"))
                ;; probably want to avoid re-execution of side effects during recovery
                (not (.recoveryRunning replication-ctx))
                ;; there's a chance we may have gone into the overdraft due to concurrent
                ;; events due to concurrent requests or a network partition
                (< balance 0))
           (.tell (.getSelf context) {:action :AlertOverdrawn :amount balance}))
         state')))))

(defn bank-account [replication-id]
  (dsl/setup
   (fn [ctx]
     (ReplicatedEventSourcing/commonJournalConfig
      replication-id
      all-replicas
      (CassandraReadJournal/Identifier)
      (reify scala.Function1
        (apply [_ replication-ctx]
          (bank-account-event-sourced-behavior replication-ctx ctx)))))))

(defn- thumbs-up-counter* [replication-id]
  (EventSourcedBehavior/apply
   (.persistenceId replication-id)
   {:users #{}}
   (reify scala.Function2
     (apply [_ state {:keys [action] :as command}]
       (case action
         :GiveThumbsUp
         (let [{:keys [user-id reply-to]} command]
           (-> (Effect/persist {:ev-type :GaveThumbsUp :user-id user-id})
               (then-run (fn [state'] (.tell reply-to (count (:users state')))))))
         :GetCount
         (let [{:keys [reply-to]} command]
           (.tell reply-to (count (:users state)))
           (Effect/none))
         :GetUsers
         (let [{:keys [reply-to]} command]
           (.tell reply-to state)
           (Effect/none)))))
   (reify scala.Function2
     (apply [_ state {:keys [ev-type] :as event}]
       (case ev-type
         :GaveThumbsUp (update state :users conj (:user-id event)))))))

(defn thumbs-up-counter [replication-id]
  (ReplicatedEventSourcing/commonJournalConfig
   replication-id
   all-replicas
   (CassandraReadJournal/Identifier)
   (reify scala.Function1
     (apply [_ _replication-ctx]
       (thumbs-up-counter* replication-id)))))

(defn movie-watch-list [entity-id replica-id]
  (ReplicatedEventSourcing/commonJournalConfig
   (ReplicationId/apply "movies" entity-id replica-id)
   all-replicas
   (CassandraReadJournal/Identifier)
   (reify scala.Function1
     (apply [_ replication-ctx]
       (EventSourcedBehavior/apply
        (.persistenceId replication-ctx)
        (ORSet/empty replica-id)
        (reify scala.Function2
          (apply [_ state {:keys [action] :as command}]
            (case action
              :AddMovie
              (Effect/persist (.add state (:movie-id command)))
              :RemoveMovie
              (Effect/persist (.remove state (:movie-id command)))
              :GetMovieList
              (do (.tell (:reply-to command) {:action :MovieList :movie-ids (.elements state)})
                  (Effect/none)))))
        (reify scala.Function2
          (apply [_ state {:keys [ev-type] :as event}]
            (.applyOperations state event))))))))

(def thumbs-up-counter-provider
  (ReplicatedEntityProvider/perDataCenter
   "counter" all-replicas
   (reify scala.Function1
     (apply [_ replication-id]
       (thumbs-up-counter replication-id)))
   (scala.reflect.ClassTag/Any)))

(def bank-account-provider
  (ReplicatedEntityProvider/perDataCenter
   "account" all-replicas
   (reify scala.Function1
     (apply [_ replication-id]
       (bank-account replication-id)))
   (scala.reflect.ClassTag/Any)))

(defn start-node [port dc]
  (actor-system/create-system-from-config
   (dsl/receive-message
    (fn [_] :same))
   "PersistenceMultiDc"
   "persistence-multi-dc.conf"
   {"akka.remote.artery.canonical.port" port
    "akka.cluster.multi-data-center.self-data-center" dc}))

(defn- ask* [provider system dc resource-id msg]
  (let [replicated-sharding (ReplicatedShardingExtension/apply system)
        sharding (.init replicated-sharding provider)
        ref (-> (.entityRefsFor sharding resource-id)
                (.get (ReplicaId. dc))
                (.get))]
    (-> (scaladsl.ask-pattern/ask
         ref
         (fn [reply-to] (assoc msg
                               :resource-id resource-id
                               :reply-to reply-to))
         "5.sec"
         (actor-system/scheduler system)))))

(def ask-thumbs-up (partial ask* thumbs-up-counter-provider))
(def ask-bank-account (partial ask* bank-account-provider))

(comment
  ;; If you're running Clojure REPL with Java 17, you'll need start another
  ;; Clojure REPL with Java 8/11 (tested myself), and start the testing
  ;; cassandra instance with the following code there (or you can use your own
  ;; existing cassandra instance).

  ;; clojure -M:cassandra-launcher
  (do (require '[clojure.java.io :as io])
      (import '(akka.persistence.cassandra.testkit CassandraLauncher))
      (CassandraLauncher/start (io/file "target/cassandra-db")
                               (CassandraLauncher/DefaultTestConfigResource)
                               false
                               9042)))

(comment
  (do (def s0 (start-node 2551 "eu-west"))
      (def s1 (start-node 2552 "eu-central")))

  (do (.terminate s0)
      (.terminate s1))

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  ;; Thumbs up example

  ;; op on one dc
  @(ask-thumbs-up s0 "eu-west" "a" {:action :GetUsers})
  @(ask-thumbs-up s0 "eu-west" "a" {:action :GetCount})
  @(ask-thumbs-up s0 "eu-west" "a" {:action :GiveThumbsUp :user-id "lotuc"})
  @(ask-thumbs-up s0 "eu-west" "a" {:action :GiveThumbsUp :user-id "42"})

  ;; try retreive from another dc
  @(ask-thumbs-up s1 "eu-central" "a" {:action :GetUsers})
  @(ask-thumbs-up s1 "eu-central" "a" {:action :GetCount})

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  ;; Bank transaction example

  @(ask-bank-account s0 "eu-west" "a" {:action :GetBalance})
  @(ask-bank-account s0 "eu-west" "a" {:action :Deposit :amount 42})
  @(ask-bank-account s0 "eu-west" "a" {:action :Withdraw :amount 42})

  (defn get-dc-balances []
    {"eu-west" (try (.getValue @(ask-bank-account s0 "eu-west" "a" {:action :GetBalance})) (catch Exception e e))
     "eu-central" (try (.getValue @(ask-bank-account s1 "eu-central" "a" {:action :GetBalance})) (catch Exception e e))})

  ;; you can find inconsistency for different dc here (may need to run multiple
  ;; times)
  (do @(ask-bank-account s0 "eu-west" "a" {:action :Withdraw :amount 2})
      (get-dc-balances))

  ;; reset balance to 0 for testing
  (let [v (.getValue @(ask-bank-account s0 "eu-west" "a" {:action :GetBalance}))]
    (cond (neg? v) @(ask-bank-account s0 "eu-west" "a" {:action :Deposit :amount (abs v)})
          (pos? v) @(ask-bank-account s0 "eu-west" "a" {:action :Withdraw :amount v})))

  (do @(ask-bank-account s0 "eu-west" "a" {:action :Deposit :amount 14})
      @(ask-bank-account s0 "eu-west" "a" {:action :Withdraw :amount 14})
      @(ask-bank-account s1 "eu-central" "a" {:action :Withdraw :amount 14})
      ;; two dcs both withdraw the last 14
      (get-dc-balances)))
