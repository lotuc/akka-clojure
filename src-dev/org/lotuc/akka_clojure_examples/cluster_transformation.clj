(ns org.lotuc.akka-clojure-examples.cluster-transformation
  (:require
   [clojure.string :as s]
   [org.lotuc.akka-clojure :as a]
   [org.lotuc.akka.system :refer [create-system-from-config]])
  (:import
   (akka.actor.typed.receptionist Receptionist Receptionist$Listing ServiceKey)
   (java.time Duration)))

;;; https://developer.lightbend.com/start/?group=akka&project=akka-samples-cluster-java
;;; transformation

(def worker-secret-key (ServiceKey/create Object "Worker"))

(a/setup frontend [] {:with-timer true}
  (let [!workers (atom []) !job-counter (atom 0)]
    (.. (a/system) receptionist
        (a/tell (Receptionist/subscribe worker-secret-key (a/self))))
    (a/start-timer :Tick {:timer-key :Tick
                          :timer-type :fix-delay
                          :delay (Duration/ofSeconds 2)})
    (a/receive-message
     (fn [{:keys [action] :as m}]
       (cond
         (= m :Tick)
         (if-some [workers (let [ws @!workers] (when (seq ws) ws))]
           (let [timeout (Duration/ofSeconds 5)
                 selected-worker (workers (mod @!job-counter (count workers)))
                 text (str "hello-" @!job-counter)]
             (->> (fn [r _t]
                    (if r
                      {:action :TransformCompleted :original-text text :transformed-text (:text r)}
                      {:action :JobFailed :why "Processing time out" :text text}))
                  (a/ask selected-worker {:action :TransformText :text text} timeout))
             (swap! !job-counter inc)
             :same)
           (a/warn "Got tick request but no workers available, not sending any work"))

         (keyword? action)
         (case action
           :WorkersUpdated
           (do (reset! !workers (into [] (:new-workers m)))
               (a/info "List of services registered with the receptionist changed: {}"
                       (:new-workers m)))

           :TransformCompleted
           (a/info "Got completed transform of {}: {}" (:original-text m) (:transformed-text m))

           :JobFailed
           (a/warn "Transformation of text {} failed. Because: {}" (:text m) (:why m))

           (a/warn "Unkown action type: {} {}" action m))

         (instance? Receptionist$Listing m)
         (let [workers (into [] (.getServiceInstances m worker-secret-key))]
           (reset! !workers workers)
           (a/info "List of services registered with the receptionist changed: {}" workers)
           :same))))))

(a/setup worker []
  (a/info "Registering myself with receptionist")
  (.. (a/system) receptionist
    (a/tell (Receptionist/register worker-secret-key (a/self))))
  (a/receive-message
    (fn [{:keys [action text reply-to] :as m}]
      (a/tell reply-to {:action :TextTransformed :text (s/upper-case text)}))))

(a/setup worker-test []
  (let [w (a/spawn (worker) "worker0")]
    (a/tell w {:action :TransformText :text "hello world"
               :reply-to (a/self)})
    (a/receive-message
     (fn [m] (println "recv:" m)))))

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
