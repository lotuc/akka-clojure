(ns org.lotuc.akka-clojure-examples.cluster-stats
  (:require
   [clojure.string :as s]
   [org.lotuc.akka-clojure :as a]
   [org.lotuc.akka.system :refer [create-system-from-config]])
  (:import
   (akka.actor.typed.javadsl Routers)
   (akka.actor.typed.receptionist Receptionist ServiceKey)
   (akka.cluster.typed ClusterSingleton ClusterSingletonSettings)
   (akka.cluster.typed SingletonActor)
   (akka.cluster.typed Cluster)
   (java.time Duration)))

;;; https://developer.lightbend.com/start/?group=akka&project=akka-samples-cluster-java
;;; stats

(def stats-service-key (ServiceKey/create Object "StatsService"))

(a/setup stats-worker [] {:with-timer true}
  (a/info "Worker starting up")
  (a/start-timer :EvictCache {:timer-key :EvictCache
                              :timer-type :fix-delay
                              :delay (Duration/ofSeconds 30)})
  (let [!cache (atom {})]
    (a/receive-message
     (fn [{:keys [action] :as m}]
       (cond
         (= m :EvictCache)
         (do (reset! !cache {}) :same)

         (= action :Process)
         (let [{:keys [word reply-to]} m]
           (a/info "Worker processing request [{}]" word)
           (let [length (get (swap! !cache update word (fn [v] (or v (count word)))) word)]
             (a/tell reply-to {:action :Processed :word word :length length}))
           :same))))))

(a/setup stats-aggregator [words workers reply-to]
  (let [expected-responses (count words)
        !results (atom [])]
    (.setReceiveTimeout (a/actor-context) (Duration/ofSeconds 3) :Timeout)
    (doseq [word words]
      (a/tell workers {:action :Process :word word :reply-to (a/self)}))

    (a/receive-message
     (fn [{:keys [action] :as m}]
       (cond
         (= m :Timeout)
         (a/tell reply-to {:action :JobFailed :reason "Service unavailable, try again later"})

         (= action :Processed)
         (let [results (swap! !results conj (:length m))
               result-size (count results)]
           (if (= result-size expected-responses)
             (let [sum (reduce + results)
                   mean-word-length (/ (double sum) result-size)]
               (a/tell reply-to {:action :JobResult :mean-word-length mean-word-length})
               :stopped)
             :same)))))))

(defn stats-service [workers]
  (a/receive-message
   (fn [{:keys [action] :as m}]
     (cond
       (= m :Stop)
       :stopped

       (= action :ProcessText)
       (let [{:keys [text reply-to]} m
             words (s/split text #" ")]
         (a/spawn (stats-aggregator words workers reply-to))
         :same)))))

(a/setup stats-service-client [service] {:with-timer true}
  (a/start-timer :Tick {:timer-type :fix-delay
                        :timer-key :Tick
                        :delay (Duration/ofSeconds 2)})
  (a/receive-message
    (fn [{:keys [action] :as m}]
      (cond
        (= m :Tick)
        (do (a/info "Sending process request")
            (a/tell service {:action :ProcessText
                             :text "this is the text that will be analyzed"
                             :reply-to (a/self)})
            :same)

        (= action :JobResult)
        (a/info "Service result: {}" (:mean-word-length m))))))

;;; corresponds to original example's App.java
(a/setup root-behavior []
  (let [system (a/system)
        cluster (Cluster/get (a/system))
        self-member (.selfMember cluster)]
    (cond
      (.hasRole self-member "compute")
      (let [number-of-workers
            (.. system settings config
                (getInt "stats-service.workers-per-node"))

            worker-pool-behavior
            (-> (Routers/pool number-of-workers (stats-worker))
                (.withConsistentHashingRouting
                 1 (reify java.util.function.Function
                     (apply [_ process] (:word process)))))

            workers (a/spawn worker-pool-behavior "WorkerRouter")
            service (a/spawn (stats-service workers) "StatsService")]
        (.. system receptionist
            (a/tell (Receptionist/register stats-service-key service)))
        :empty)

      (.hasRole self-member "client")
      (let [service-router (a/spawn (Routers/group stats-service-key) "ServiceRouter")]
        (a/spawn (stats-service-client service-router) "Client")
        :empty))))

(def worker-service-key (ServiceKey/create Object "Worker"))

(a/setup routed-stat-service []
  (let [worker-group-behavior
        (-> (Routers/group worker-service-key)
            (.withConsistentHashingRouting
             1 (reify java.util.function.Function
                 (apply [_ process] (:word process)))))

        workers-router
        (a/spawn worker-group-behavior "WorkersRouter")]
    (stats-service workers-router)))

;;; corresponds to original example's AppOneMaster.java
(a/setup root-behavior-one-master []
  (let [system (a/system)
        cluster (Cluster/get system)
        singleton-settings (-> (ClusterSingletonSettings/create system)
                               (.withRole "compute"))
        service-singleton (-> (SingletonActor/of (routed-stat-service) "StatsService")
                              (.withStopMessage :Stop)
                              (.withSettings singleton-settings))
        service-proxy (-> (ClusterSingleton/get system)
                          (.init service-singleton))
        self-member (.selfMember cluster)]
    (cond
      (.hasRole self-member "compute")
      (let [number-of-workers
            (.. system settings config
                (getInt "stats-service.workers-per-node"))]
        (a/info "Starting {} workers" number-of-workers)
        (doseq [i (range 4)]
          (let [worker (a/spawn (stats-worker) (str "StatsWorker" i))]
            (.. system receptionist
                (a/tell (Receptionist/register worker-service-key (.narrow worker))))))
        :empty)

      (.hasRole self-member "client")
      (do (a/spawn (stats-service-client (.narrow service-proxy)) "Client")
          :empty))))

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
