(ns lotuc.examples.akka.cluster-stats
  (:require
   [clojure.string :as s]
   [lotuc.akka.actor.typed.actor-ref :as actor-ref]
   [lotuc.akka.actor.typed.receptionist :as receptionist]
   [lotuc.akka.actor.scaladsl :as dsl]
   [lotuc.akka.cluster.member :as cluster.member]
   [lotuc.akka.cluster.typed.cluster :as cluster]
   [lotuc.akka.cluster.typed.cluster-singleton :as cluster-singleton]
   [lotuc.akka.common.slf4j :refer [slf4j-log]]
   [lotuc.akka.actor.typed.actor-system :as actor-system]))

(set! *warn-on-reflection* true)

;;; https://developer.lightbend.com/start/?group=akka&project=akka-samples-cluster-java
;;; stats

(defmacro info [ctx msg & args]
  `(slf4j-log (dsl/log ~ctx) info ~msg ~@args))

(def stats-service-key "StatsService")

(defn- stats-worker*
  [{:keys [context timers]}]
  (info context "Worker starting up")
  (dsl/start-timer timers :EvictCache {:timer-key :EvictCache
                                       :timer-type :fix-rate
                                       :interval "30.sec"})
  (let [!cache (atom {})]
    (dsl/receive-message
     (fn [{:keys [action] :as m}]
       (cond
         (= m :EvictCache)
         (do (reset! !cache {})
             :same)

         (= action :Process)
         (let [{:keys [word ^akka.actor.typed.ActorRef reply-to]} m]
           (info context "Worker processing request [{}]" word)
           (let [length (get (swap! !cache update word (fn [v] (or v (count word)))) word)]
             (.tell reply-to {:action :Processed :word word :length length}))
           :same)

         :else
         (do (info context "Unhandled message {}" m)
             :unhandled))))))

(defn stats-worker []
  (dsl/setup
   (fn [ctx]
     (dsl/with-timers
       (fn [timers]
         (stats-worker* {:context ctx :timers timers}))))))

(defn stats-aggregator [words workers reply-to]
  (dsl/setup
   (fn [ctx]
     (let [expected-responses (count words)
           self (dsl/self ctx)
           !results (atom [])]
       (dsl/set-receive-timeout ctx "3.sec" :Timeout)
       (doseq [word words]
         (actor-ref/tell workers {:action :Process :word word :reply-to self}))

       (dsl/receive-message
        (fn [{:keys [action] :as m}]
          (cond
            (= m :Timeout)
            (do (actor-ref/tell reply-to {:action :JobFailed :reason "Service unavailable, try again later"})
                :same)

            (= action :Processed)
            (let [results (swap! !results conj (:length m))
                  result-size (count results)]
              (if (= result-size expected-responses)
                (let [sum (reduce + results)
                      mean-word-length (/ (double sum) result-size)]
                  (actor-ref/tell reply-to {:action :JobResult :mean-word-length mean-word-length})
                  :stopped)
                :same))

            :else
            (do (info ctx "Unhandled message {}" m)
                :unhandled))))))))

(defn stats-service [workers]
  (dsl/receive
   (fn [ctx {:keys [action] :as m}]
     (cond
       (= m :Stop)
       :stopped

       (= action :ProcessText)
       (let [{:keys [text reply-to]} m
             words (s/split text #" ")]
         (dsl/spawn-anonymous ctx (stats-aggregator words workers reply-to))
         :same)

       :else
       (do (info ctx "Unhandled message {}" m)
           :unhandled)))))

(defn- stats-service-client*
  [service {:keys [context timers]}]
  (dsl/start-timer timers :Tick {:timer-key :Tick
                                 :timer-type :fix-rate
                                 :interval "2.sec"})
  (dsl/receive-message
   (fn [{:keys [action] :as m}]
     (cond
       (= m :Tick)
       (do (info context "Sending process request")
           (actor-ref/tell service {:action :ProcessText
                                    :text "this is the text that will be analyzed"
                                    :reply-to (dsl/self context)})
           :same)

       (= action :JobResult)
       (do (info context "Service result: {}" (:mean-word-length m))
           :same)

       :else
       (do (info context "Unhandled message: {}" m)
           :unhandled)))))

(defn stats-service-client [service]
  (dsl/setup
   (fn [ctx]
     (dsl/with-timers
       (fn [timers]
         (stats-service-client* service {:context ctx :timers timers}))))))

;;; corresponds to original example's App.java
(defn root-behavior []
  (dsl/setup
   (fn [ctx]
     (let [system (dsl/system ctx)
           cluster (cluster/get-cluster system)
           self-member (.selfMember cluster)]
       (cond
         (cluster.member/has-role self-member "compute")
         (let [number-of-workers
               (.. system settings config
                   (getInt "stats-service.workers-per-node"))

               worker-pool-behavior
               (-> (dsl/pool-router (stats-worker) number-of-workers)
                   (dsl/pool-with-consistent-hashing-routing 1 :word))

               workers (dsl/spawn ctx worker-pool-behavior "WorkerRouter")
               service (dsl/spawn ctx (stats-service workers) "StatsService")]
           (actor-ref/tell (.receptionist system)
                           (receptionist/register-service stats-service-key service))
           :empty)

         (.hasRole self-member "client")
         (let [service-router (dsl/spawn ctx (dsl/group-router stats-service-key) "ServiceRouter")]
           (dsl/spawn ctx (stats-service-client service-router) "Client")
           :empty))))))

(def worker-service-key "Worker")

;;; corresponds to original example's AppOneMaster.java
(defn root-behavior-one-master []
  (dsl/setup
   (fn [ctx]
     (let [system (dsl/system ctx)
           cluster (cluster/get-cluster system)
           service-behavior (dsl/setup
                             (fn [singleton-ctx]
                               (let [worker-group-behavior
                                     (-> (dsl/group-router worker-service-key)
                                         (dsl/group-with-consistent-hashing-routing 1 :word))

                                     workers-router
                                     (dsl/spawn singleton-ctx worker-group-behavior "WorkersRouter")]
                                 (stats-service workers-router))))
           service-singleton (->> {:stop-message :Stop
                                   :settings (cluster-singleton/create-cluster-singleton-setting system {:role "compute"})}
                                  (cluster-singleton/singleton-actor-of service-behavior "StatsService"))
           service-proxy (-> (cluster-singleton/get-cluster-singleton system)
                             (.init service-singleton))
           self-member (.selfMember cluster)]
       (cond
         (cluster.member/has-role self-member "compute")
         (let [number-of-workers (.. system settings config
                                     (getInt "stats-service.workers-per-node"))]
           (info ctx "Starting {} workers" number-of-workers)
           (doseq [i (range 4)]
             (let [worker (dsl/spawn ctx (stats-worker) (str "StatsWorker" i))]
               (actor-ref/tell (.receptionist system)
                               (receptionist/register-service worker-service-key worker))))
           :empty)

         (cluster.member/has-role self-member "client")
         (do (dsl/spawn ctx (stats-service-client service-proxy) "Client")
             :empty)

         :else
         :empty)))))

(defn startup [behavior role port]
  (actor-system/create-system-from-config
   behavior
   "ClusterSystem"
   "cluster-application.conf"
   {"akka.remote.artery.canonical.port" port
    "stats-service.workers-per-node" 2
    "akka.cluster.roles" [role]}))

(comment
  (let [behavior (root-behavior-one-master)]
    (def s0 (startup behavior "compute" 25251))
    (def s1 (startup behavior "compute" 25252))
    (def s2 (startup behavior "client" 0)))

  (let [behavior (root-behavior)]
    (def s0 (startup behavior "compute" 25251))
    (def s1 (startup behavior "compute" 25252))
    (def s2 (startup behavior "client" 0)))

  (do (.terminate s0)
      (.terminate s1)
      (.terminate s2)))
