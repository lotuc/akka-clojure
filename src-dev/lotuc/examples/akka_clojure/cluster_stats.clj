(ns lotuc.examples.akka-clojure.cluster-stats
  (:require
   [clojure.string :as s]
   [lotuc.akka-clojure :as a]
   [lotuc.akka.actor.scaladsl :as dsl]
   [lotuc.akka.actor.typed.actor-system :as actor-system]
   [lotuc.akka.actor.typed.receptionist :as receptionist]
   [lotuc.akka.cluster.typed.cluster-singleton :as cluster-singleton]))

(set! *warn-on-reflection* true)

;;; https://developer.lightbend.com/start/?group=akka&project=akka-samples-cluster-java
;;; stats

(def stats-service-key "StatsService")

(a/setup stats-worker [] {:with-timer true}
  (a/info "Worker starting up")
  (a/start-timer :EvictCache {:timer-key :EvictCache
                              :timer-type :fix-delay
                              :delay "30.sec"})
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
           :same)

         :else :same)))))

(a/setup stats-aggregator [words workers reply-to]
  (let [expected-responses (count words)
        !results (atom [])]
    (dsl/set-receive-timeout (a/actor-context) "3.sec" :Timeout)
    (doseq [word words]
      (a/tell workers {:action :Process :word word :reply-to (a/self)}))

    (a/receive-message
     (fn [{:keys [action] :as m}]
       (cond
         (= m :Timeout)
         (do (a/tell reply-to {:action :JobFailed :reason "Service unavailable, try again later"})
             :same)

         (= action :Processed)
         (let [results (swap! !results conj (:length m))
               result-size (count results)]
           (if (= result-size expected-responses)
             (let [sum (reduce + results)
                   mean-word-length (/ (double sum) result-size)]
               (a/tell reply-to {:action :JobResult :mean-word-length mean-word-length})
               :stopped)
             :same))

         :else :same)))))

(defn stats-service [workers]
  (a/receive-message
   (fn [{:keys [action] :as m}]
     (cond
       (= m :Stop) :stopped

       (= action :ProcessText)
       (let [{:keys [text reply-to]} m
             words (s/split text #" ")]
         (a/spawn (stats-aggregator words workers reply-to))
         :same)

       :else :same))))

(a/setup stats-service-client [service] {:with-timer true}
  (a/start-timer :Tick {:timer-type :fix-delay
                        :timer-key :Tick
                        :delay "2.sec"})
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
        (do (a/info "Service result: {}" (:mean-word-length m))
            :same)

        :else :same))))

;;; corresponds to original example's App.java
(a/setup root-behavior []
  (let [system (a/system)
        cluster (a/cluster)
        self-member (.selfMember cluster)]
    (cond
      (.hasRole self-member "compute")
      (let [number-of-workers
            (.. system settings config
                (getInt "stats-service.workers-per-node"))

            worker-pool-behavior
            (-> (dsl/pool-router  (stats-worker) number-of-workers)
                (dsl/pool-with-consistent-hashing-routing 1 :word))

            workers (a/spawn worker-pool-behavior "WorkerRouter")
            service (a/spawn (stats-service workers) "StatsService")]
        (.. system receptionist
            (a/tell (receptionist/register-service stats-service-key service)))
        :empty)

      (.hasRole self-member "client")
      (let [service-router (a/spawn (dsl/group-router stats-service-key) "ServiceRouter")]
        (a/spawn (stats-service-client service-router) "Client")
        :empty)

      :else :empty)))

(def worker-service-key "Worker")

(a/setup routed-stat-service []
  (let [worker-group-behavior
        (-> (dsl/group-router worker-service-key)
            (dsl/group-with-consistent-hashing-routing 1 :word))

        workers-router
        (a/spawn worker-group-behavior "WorkersRouter")]
    (stats-service workers-router)))

;;; corresponds to original example's AppOneMaster.java
(a/setup root-behavior-one-master []
  (let [system (a/system)
        cluster (a/cluster)
        service-singleton (->> {:stop-message :Stop
                                :settings (a/create-cluster-singleton-setting
                                           {:role "compute"})}
                               (cluster-singleton/singleton-actor-of
                                (routed-stat-service) "StatsService"))
        service-proxy (-> (a/cluster-singleton)
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
                (a/tell (receptionist/register-service worker-service-key worker)))))
        :empty)

      (.hasRole self-member "client")
      (do (a/spawn (stats-service-client (.narrow service-proxy)) "Client")
          :empty)

      :else :empty)))

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
