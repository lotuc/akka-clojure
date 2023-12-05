(ns lotuc.examples.akka-clojure.cluster-transformation
  (:require
   [clojure.string :as s]
   [lotuc.akka-clojure :as a]
   [lotuc.akka.actor.typed.receptionist :as receptionist]
   [lotuc.akka.cnv :as cnv]
   [lotuc.akka.actor.typed.actor-system :as actor-system]))

(set! *warn-on-reflection* true)

;;; https://developer.lightbend.com/start/?group=akka&project=akka-samples-cluster-java
;;; transformation

(def worker-service-key "Worker")

(a/setup frontend [] {:with-timer true}
  (let [!workers (atom []) !job-counter (atom 0)]
    (a/subscribe-to-receptionist worker-service-key)
    (a/start-timer :Tick {:timer-key :Tick
                          :timer-type :fix-delay
                          :delay "2.sec"})
    (a/receive-message
     (fn [{:keys [action] :as m}]
       (case (if (keyword? m) m action)
         :Tick
         (if-some [workers (let [ws @!workers] (when (seq ws) ws))]
           (let [selected-worker (workers (mod @!job-counter (count workers)))
                 text (str "hello-" @!job-counter)]
             (a/ask selected-worker {:action :TransformText :text text}
                    (fn [r _t]
                      (if r
                        {:action :TransformCompleted :original-text text :transformed-text (:text r)}
                        {:action :JobFailed :why "Processing time out" :text text}))
                    "5.sec")
             (swap! !job-counter inc)
             :same)
           (do (a/warn "Got tick request but no workers available, not sending any work")
               :same))

         :WorkersUpdated
         (do (reset! !workers (into [] (:new-workers m)))
             (a/info "List of services registered with the receptionist changed: {}"
                     (:new-workers m))
             :same)

         :TransformCompleted
         (do (a/info "Got completed transform of {}: {}" (:original-text m) (:transformed-text m))
             :same)

         :JobFailed
         (do (a/warn "Transformation of text {} failed. Because: {}" (:text m) (:why m))
             :same)

         (if-some [{:keys [get-service-instances]}
                   (when (receptionist/receptionist-listing? m)
                     (cnv/->clj m))]
           (let [workers (into [] (get-service-instances worker-service-key))]
             (reset! !workers workers)
             (a/info "List of services registered with the receptionist changed: {}" workers)
             :same)
           :unhandled))))))

(a/setup worker []
  (a/info "Registering myself with receptionist")
  (a/register-with-receptionist worker-service-key)
  (a/receive-message
    (fn [{:keys [action text reply-to] :as m}]
      (a/tell reply-to {:action :TextTransformed :text (s/upper-case text)})
      :same)))

(a/setup worker-test []
  (let [w (a/spawn (worker) "worker0")]
    (a/tell w {:action :TransformText :text "hello world"
               :reply-to (a/self)})
    (a/receive-message
     (fn [m]
       (println "recv:" m)
       :same))))

(comment
  (def s (create-system-from-config
          (worker-test)
          "ClusterSystem"
          "cluster-application"
          {"akka.actor.serialize-messages" "on"}))
  (.terminate s))

(a/setup root-behavior []
  (let [cluster (a/cluster)
        self-member (.selfMember cluster)]
    (a/info "starting: backend={} frontend={}"
            (.hasRole self-member "backend")
            (.hasRole self-member "frontend"))
    (cond
      (.hasRole self-member "backend")
      (let [workers-per-node (.. (a/system) settings config
                                 (getInt "transformation.workers-per-node"))]
        (doseq [i (range workers-per-node)]
          (a/spawn (worker) (str "Worker" i))))

      (.hasRole self-member "frontend")
      (a/spawn (frontend) "Frontend"))
    :empty))

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
