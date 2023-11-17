(ns org.lotuc.examples.cluster-transformation
  (:require
   [clojure.string :as s]
   [org.lotuc.akka.behaviors :as behaviors]
   [org.lotuc.akka.cluster :as cluster]
   [org.lotuc.akka.receptionist :as receptionist]
   [org.lotuc.akka.system :refer [create-system-from-config]])
  (:import
   (akka.actor.typed.receptionist Receptionist$Listing)
   (java.time Duration)))

;;; https://developer.lightbend.com/start/?group=akka&project=akka-samples-cluster-java
;;; transformation

(defmacro info [ctx msg & args]
  `(.info (.getLog ~ctx) ~msg (into-array Object [~@args])))

(defmacro warn [ctx msg & args]
  `(.warn (.getLog ~ctx) ~msg (into-array Object [~@args])))

(def worker-secret-key (receptionist/create-service-key "Worker"))

(defn frontend*
  [!workers !job-counter {:keys [timers context] :as i}]
  (.. context getSystem receptionist
      (tell (receptionist/subscribe worker-secret-key (.getSelf context))))
  (.. timers
      (startTimerWithFixedDelay :Tick :Tick (Duration/ofSeconds 2)))
  (behaviors/receive-message
   (fn [{:keys [action] :as m}]
     (cond
       (= m :Tick)
       (if-some [workers (let [ws @!workers] (when (seq ws) ws))]
         (let [timeout (Duration/ofSeconds 5)
               selected-worker (workers (mod @!job-counter (count workers)))
               text (str "hello-" @!job-counter)]
           (.ask context Object selected-worker timeout
                 (reify akka.japi.function.Function
                   (apply [_ reply-to]
                     {:action :TransformText
                      :text text
                      :reply-to reply-to}))
                 (reify akka.japi.function.Function2
                   (apply [_ r _t]
                     (if r
                       {:action :TransformCompleted :original-text text :transformed-text (:text r)}
                       {:action :JobFailed :why "Processing time out" :text text}))))
           (swap! !job-counter inc)
           :same)
         (warn context "Got tick request but no workers available, not sending any work"))

       (keyword? action)
       (case action
         :WorkersUpdated
         (do (reset! !workers (into [] (:new-workers m)))
             (info context "List of services registered with the receptionist changed: {}"
                   (:new-workers m)))

         :TransformCompleted
         (info context "Got completed transform of {}: {}"
               (:original-text m) (:transformed-text m))

         :JobFailed
         (warn context "Transformation of text {} failed. Because: {}"
               (:text m) (:why m))

         (warn context "Unkown action type: {} {}" action m))

       (instance? Receptionist$Listing m)
       (let [workers (into [] (.getServiceInstances m worker-secret-key))]
         (reset! !workers workers)
         (info context "List of services registered with the receptionist changed: {}"
               workers)
         :same)))))

(defn frontend []
  (let [!workers (atom []) !job-counter (atom 0)]
    (-> (partial frontend* !workers !job-counter)
        (behaviors/setup {:with-timer true}))))

(defn worker []
  (behaviors/setup
   (fn [ctx]
     (info ctx "Registering myself with receptionist")
     (.. ctx getSystem receptionist
         (tell (receptionist/register worker-secret-key (.narrow (.getSelf ctx)))))
     (behaviors/receive-message
      (fn [{:keys [action text reply-to] :as m}]
        (.tell reply-to {:action :TextTransformed :text (s/upper-case text)}))))))

(defn worker-test []
  (behaviors/setup
   (fn [ctx]
     (let [w (.spawn ctx (worker) "worker0")]
       (let [reply-to (.getSelf ctx)]
         (.tell w {:action :TransformText :text "hello world"
                   :reply-to reply-to}))
       (behaviors/receive-message
        (fn [m] (println "recv:" m)))))))

(comment
  (def s (create-system-from-config
          (worker-test)
          "ClusterSystem"
          "cluster-application"
          {"akka.actor.serialize-messages" "on"}))
  (.terminate s))

(defn root-behavior []
  (behaviors/setup
   (fn [ctx]
     (let [cluster (cluster/get-cluster (.getSystem ctx))
           self-member (.selfMember cluster)]
       (info ctx "starting: backend={} frontend={}"
             (.hasRole self-member "backend")
             (.hasRole self-member "frontend"))
       (cond
         (.hasRole self-member "backend")
         (let [workers-per-node (.. ctx getSystem settings config
                                    (getInt "transformation.workers-per-node"))]
           (doseq [i (range workers-per-node)]
             (.spawn ctx (worker) (str "Worker" i))))

         (.hasRole self-member "frontend")
         (.spawn ctx (frontend) "Frontend"))
       :empty))))

(defn startup [role port]
  (create-system-from-config
   (root-behavior)
   "ClusterSystem"
   "cluster-transformation"
   {"akka.remote.artery.canonical.port" port
    "akka.cluster.roles" [role]}))

(comment
  (do (def s0 (startup "backend" 25251))
      (def s1 (startup "backend" 25252))
      (def s2 (startup "frontend" 0))
      (def s3 (startup "frontend" 0))
      (def s4 (startup "frontend" 0)))

  (do (.terminate s0)
      (.terminate s1)
      (.terminate s2)
      (.terminate s3)
      (.terminate s4)))
