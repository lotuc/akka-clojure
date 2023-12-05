(ns lotuc.examples.akka.cluster-transformation
  (:require
   [clojure.string :as s]
   [lotuc.akka.actor.typed.actor-ref :as actor-ref]
   [lotuc.akka.actor.typed.actor-system :as actor-system]
   [lotuc.akka.actor.typed.receptionist :as receptionist]
   [lotuc.akka.actor.scaladsl :as dsl]
   [lotuc.akka.cluster.typed.cluster :as cluster]
   [lotuc.akka.cluster.member :as cluster.member]
   [lotuc.akka.common.slf4j :refer [slf4j-log]]
   [lotuc.akka.cnv :as cnv]))

(set! *warn-on-reflection* true)

;;; https://developer.lightbend.com/start/?group=akka&project=akka-samples-cluster-java
;;; transformation

(defmacro info [ctx msg & args]
  `(slf4j-log (dsl/log ~ctx) info ~msg ~@args))

(defmacro warn [ctx msg & args]
  `(slf4j-log (dsl/log ~ctx) warn ~msg ~@args))

(def worker-service-key "Worker")

(defn frontend*
  [!workers !job-counter
   {:keys [timers context] :as i}]
  (let [system (dsl/system context)
        self (dsl/self context)]
    (actor-ref/tell (.receptionist system)
                    (receptionist/subscribe-service worker-service-key self)))
  (dsl/start-timer timers :Tick {:timer-key :Tick
                                 :timer-type :fix-rate
                                 :interval "2.sec"})
  (dsl/receive-message
   (fn [{:keys [action] :as m}]
     (or (cond
           (= m :Tick)
           (if-some [workers (let [ws @!workers] (when (seq ws) ws))]
             (let [selected-worker (workers (mod @!job-counter (count workers)))
                   text (str "hello-" @!job-counter)]
               (dsl/ask context selected-worker
                        (fn [reply-to] {:action :TransformText
                                        :text text
                                        :reply-to reply-to})
                        (fn [r t]
                          (if t
                            {:action :JobFailed :why "Processing time out" :text text}
                            {:action :TransformCompleted :original-text text :transformed-text (:text r)}))
                        "5.sec")
               (swap! !job-counter inc)
               :same)
             (warn context "Got tick request but no workers available, not sending any work"))

           (keyword? action)
           (case action
             :WorkersUpdated
             (do (reset! !workers (into [] (:new-workers m)))
                 (info context "List of services registered with the receptionist changed: {}" (:new-workers m)))

             :TransformCompleted
             (info context "Got completed transform of {}: {}"
                   (:original-text m) (:transformed-text m))

             :JobFailed
             (warn context "Transformation of text {} failed. Because: {}"
                   (:text m) (:why m))

             (warn context "Unkown action type: {} {}" action m))

           (receptionist/receptionist-listing? m)
           (let [{:keys [get-service-instances]} (cnv/->clj m)
                 workers (into [] (get-service-instances worker-service-key))]
             (reset! !workers workers)
             (info context "List of services registered with the receptionist changed: {}" workers)
             :same))
         :same))))

(defn frontend []
  (let [!workers (atom []) !job-counter (atom 0)]
    (dsl/setup
     (fn [ctx]
       (dsl/with-timers
         (fn [timers]
           (frontend* !workers !job-counter {:context ctx :timers timers})))))))

(defn worker []
  (dsl/setup
   (fn [ctx]
     (info ctx "Registering myself with receptionist")
     (let [system (dsl/system ctx)
           self (dsl/self ctx)]
       (actor-ref/tell (.receptionist system)
                       (receptionist/register-service worker-service-key self)))
     (dsl/receive-message
      (fn [{:keys [action text reply-to] :as m}]
        (actor-ref/tell reply-to {:action :TextTransformed :text (s/upper-case text)})
        :same)))))

(defn worker-test []
  (dsl/setup
   (fn [ctx]
     (let [w (dsl/spawn ctx (worker) "worker0")]
       (let [reply-to (dsl/self ctx)]
         (actor-ref/tell w {:action :TransformText :text "hello world"
                            :reply-to reply-to}))
       (dsl/receive-message
        (fn [m] (println "recv:" m) :same))))))

(comment
  (def s (actor-system/create-system-from-config
          (worker-test)
          "ClusterSystem"
          "cluster-application"
          {"akka.actor.serialize-messages" "on"}))
  (actor-system/terminate s))

(defn root-behavior []
  (dsl/setup
   (fn [ctx]
     (let [system (dsl/system ctx)
           cluster (cluster/get-cluster system)
           self-member (.selfMember cluster)]
       (info ctx "starting: backend={} frontend={}"
             (cluster.member/has-role self-member "backend")
             (cluster.member/has-role self-member "frontend"))
       (cond
         (cluster.member/has-role self-member "backend")
         (let [workers-per-node (.. system settings config
                                    (getInt "transformation.workers-per-node"))]
           (doseq [i (range workers-per-node)]
             (dsl/spawn ctx (worker) (str "Worker" i))))

         (cluster.member/has-role self-member "frontend")
         (dsl/spawn ctx (frontend) "Frontend"))
       :empty))))

(defn startup [role port]
  (actor-system/create-system-from-config
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
