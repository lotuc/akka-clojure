(ns lotuc.examples.akka.cluster-stats
  (:require
   [clojure.string :as s]
   [lotuc.akka.actor.receptionist :as actor.receptionist]
   [lotuc.akka.cluster :as cluster]
   [lotuc.akka.common.log :refer [slf4j-log]]
   [lotuc.akka.javadsl.actor.behaviors :as behaviors]
   [lotuc.akka.system :refer [create-system-from-config]])
  (:import
   (akka.actor.typed.javadsl Routers)
   (java.time Duration)))

(set! *warn-on-reflection* true)

;;; https://developer.lightbend.com/start/?group=akka&project=akka-samples-cluster-java
;;; stats

(defmacro info [ctx msg & args]
  `(slf4j-log (.getLog ~ctx) info ~msg ~@args))

(def stats-service-key (actor.receptionist/create-service-key "StatsService"))

(defn- stats-worker*
  [{:keys [^akka.actor.typed.javadsl.ActorContext context
           ^akka.actor.typed.javadsl.TimerScheduler timers]}]
  (info context "Worker starting up")
  (.startTimerWithFixedDelay timers :EvictCache :EvictCache (Duration/ofSeconds 30))
  (let [!cache (atom {})]
    (behaviors/receive-message
     (fn [{:keys [action] :as m}]
       (cond
         (= m :EvictCache)
         (do (reset! !cache {}) :same)

         (= action :Process)
         (let [{:keys [word ^akka.actor.typed.ActorRef reply-to]} m]
           (info context "Worker processing request [{}]" word)
           (let [length (get (swap! !cache update word (fn [v] (or v (count word)))) word)]
             (.tell reply-to {:action :Processed :word word :length length}))
           :same))))))

(defn stats-worker ^akka.actor.typed.Behavior []
  (behaviors/setup stats-worker* {:with-timer true}))

(defn stats-aggregator [words
                        ^akka.actor.typed.ActorRef workers
                        ^akka.actor.typed.ActorRef reply-to]
  (behaviors/setup
   (fn [^akka.actor.typed.javadsl.ActorContext ctx]
     (let [expected-responses (count words)
           !results (atom [])]
       (.setReceiveTimeout ctx (Duration/ofSeconds 3) :Timeout)
       (doseq [word words]
         (.tell workers {:action :Process :word word :reply-to (.getSelf ctx)}))

       (behaviors/receive-message
        (fn [{:keys [action] :as m}]
          (cond
            (= m :Timeout)
            (.tell reply-to {:action :JobFailed :reason "Service unavailable, try again later"})

            (= action :Processed)
            (let [results (swap! !results conj (:length m))
                  result-size (count results)]
              (if (= result-size expected-responses)
                (let [sum (reduce + results)
                      mean-word-length (/ (double sum) result-size)]
                  (.tell reply-to {:action :JobResult :mean-word-length mean-word-length})
                  :stopped)
                :same)))))))))

(defn stats-service [workers]
  (behaviors/receive
   (fn [^akka.actor.typed.javadsl.ActorContext ctx {:keys [action] :as m}]
     (cond
       (= m :Stop)
       :stopped

       (= action :ProcessText)
       (let [{:keys [text reply-to]} m
             words (s/split text #" ")]
         (.spawnAnonymous ctx (stats-aggregator words workers reply-to))
         :same)))))

(defn- stats-service-client*
  [^akka.actor.typed.ActorRef service
   {:keys [^akka.actor.typed.javadsl.ActorContext context
           ^akka.actor.typed.javadsl.TimerScheduler timers]}]
  (.startTimerWithFixedDelay timers :Tick :Tick (Duration/ofSeconds 2))
  (behaviors/receive-message
   (fn [{:keys [action] :as m}]
     (cond
       (= m :Tick)
       (do (info context "Sending process request")
           (.tell service {:action :ProcessText
                           :text "this is the text that will be analyzed"
                           :reply-to (.getSelf context)})
           :same)

       (= action :JobResult)
       (info context "Service result: {}" (:mean-word-length m))))))

(defn stats-service-client [service]
  (-> (partial stats-service-client* service)
      (behaviors/setup {:with-timer true})))

;;; corresponds to original example's App.java
(defn root-behavior []
  (behaviors/setup
   (fn [^akka.actor.typed.javadsl.ActorContext ctx]
     (let [cluster (cluster/get-cluster (.getSystem ctx))
           self-member (.selfMember cluster)]
       (cond
         (.hasRole self-member "compute")
         (let [number-of-workers
               (.. ctx getSystem settings config
                   (getInt "stats-service.workers-per-node"))

               worker-pool-behavior
               (-> (Routers/pool number-of-workers (.narrow (stats-worker)))
                   (.withConsistentHashingRouting
                    1 (reify java.util.function.Function
                        (apply [_ process] (:word process)))))

               workers (.spawn ctx worker-pool-behavior "WorkerRouter")
               service (.spawn ctx (stats-service (.narrow workers)) "StatsService")]
           (.. ctx getSystem receptionist
               (tell (actor.receptionist/register stats-service-key (.narrow service))))
           :empty)

         (.hasRole self-member "client")
         (let [service-router (.spawn ctx (Routers/group stats-service-key) "ServiceRouter")]
           (.spawn ctx (stats-service-client service-router) "Client")
           :empty))))))

(def worker-service-key (actor.receptionist/create-service-key Object "Worker"))

;;; corresponds to original example's AppOneMaster.java
(defn root-behavior-one-master []
  (behaviors/setup
   (fn [^akka.actor.typed.javadsl.ActorContext ctx]
     (let [system (.getSystem ctx)
           cluster (cluster/get-cluster system)
           service-behavior (behaviors/setup
                             (fn [^akka.actor.typed.javadsl.ActorContext singleton-ctx]
                               (let [worker-group-behavior
                                     (-> (Routers/group worker-service-key)
                                         (.withConsistentHashingRouting
                                          1 (reify java.util.function.Function
                                              (apply [_ process] (:word process)))))

                                     workers-router
                                     (.spawn singleton-ctx worker-group-behavior
                                             "WorkersRouter")]
                                 (stats-service workers-router))))
           service-singleton (->> {:stop-message :Stop
                                   :settings (cluster/create-cluster-singleton-setting
                                              system {:role "compute"})}
                                  (cluster/singleton-actor-of
                                   service-behavior "StatsService"))
           service-proxy (-> (cluster/get-cluster-singleton (.getSystem ctx))
                             (.init service-singleton))
           self-member (.selfMember cluster)]
       (cond
         (.hasRole self-member "compute")
         (let [number-of-workers
               (.. ctx getSystem settings config
                   (getInt "stats-service.workers-per-node"))]
           (info ctx "Starting {} workers" number-of-workers)
           (doseq [i (range 4)]
             (let [worker (.spawn ctx (stats-worker) (str "StatsWorker" i))]
               (.. ctx getSystem receptionist
                   (tell (actor.receptionist/register worker-service-key (.narrow worker))))))
           :empty)

         (.hasRole self-member "client")
         (do (.spawn ctx (stats-service-client (.narrow service-proxy)) "Client")
             :empty))))))

(defn startup [behavior role port]
  (create-system-from-config
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
