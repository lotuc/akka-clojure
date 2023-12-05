(ns lotuc.akka-clojure
  (:require
   [lotuc.akka.actor.scaladsl :as dsl]
   [lotuc.akka.actor.typed.actor-ref :as actor-ref]
   [lotuc.akka.actor.typed.receptionist :as receptionist]
   [lotuc.akka.actor.typed.actor-system :as actor-system]
   [lotuc.akka.actor.typed.scaladsl.ask-pattern :as scaladsl.ask-pattern]
   [lotuc.akka.cluster.typed.cluster :as cluster]
   [lotuc.akka.cluster.typed.cluster-singleton :as cluster-singleton]
   [lotuc.akka.common.slf4j :refer [slf4j-log]])
  (:import
   (akka.actor.typed ActorRef ActorSystem)
   (akka.actor.typed.scaladsl ActorContext)))

(set! *warn-on-reflection* true)

;;; Dyanmic bindings for message & signal handlers
(def ^:dynamic *local-context* nil)

(defn local-context []
  (when-not *local-context*
    (throw (RuntimeException. "local context is empty, call under behavior context")))
  *local-context*)

(defn actor-context
  "Actor context of local context."
  ^ActorContext []
  (or (:context (local-context))
      (throw (RuntimeException. "no actor context found in local context"))))

(defn timers
  "TimerScheduler of local context."
  []
  (or (:timers (local-context))
      (throw (RuntimeException. "no timers found in local context"))))

(defn stash-buffer
  "StashBuffer of local context."
  []
  (or (:stash-buffer (local-context))
      (throw (RuntimeException. "no stash-buffer found in local context"))))

(defn self
  "ActorRef for local context actor."
  ^ActorRef []
  (dsl/self (actor-context)))

(defn system
  "ActorSystem of local context."
  ^ActorSystem []
  (dsl/system (actor-context)))

(defn cluster ^akka.cluster.typed.Cluster []
  (cluster/get-cluster (system)))

(defn cluster-singleton ^akka.cluster.typed.ClusterSingleton []
  (cluster-singleton/get-cluster-singleton (system)))

(defn receptionist ^akka.actor.typed.ActorRef []
  (actor-system/receptionist (system)))

(defn tell
  "Send message to target. Send to self if not target given"
  ([target message] (actor-ref/tell target message))
  ([message] (actor-ref/tell (self) message)))

(defn !
  "Same as tell."
  ([target message] (actor-ref/tell target message))
  ([message] (actor-ref/tell (self) message)))

(defn ask
  "Ask pattern.

  `msg` should be a map, and `reply-to` key will be overriten by our tmp
  ActorRef for receiving the calling result."
  ([target msg apply-to-response timeout]
   (dsl/ask (actor-context)
            target
            #(assoc msg :reply-to %)
            apply-to-response
            timeout))
  ([^ActorSystem system msg timeout]
   (scaladsl.ask-pattern/ask
    system
    (fn [reply-to] (assoc msg :reply-to reply-to))
    timeout
    (actor-system/scheduler system))))

(defn schedule-once
  ([target duration message]
   (dsl/schedule-once (actor-context) duration target message))
  ([duration message]
   (dsl/schedule-once (actor-context) duration (self) message)))

(defn spawn
  ([behavior name]
   (dsl/spawn (actor-context) behavior name))
  ([behavior]
   (dsl/spawn-anonymous (actor-context) behavior)))

(defn- bound-fn**
  ([f] (bound-fn** f nil))
  ([f updated-ctx] (bound-fn** f *local-context* updated-ctx))
  ([f local-ctx updated-ctx]
   (let [ctx (merge local-ctx updated-ctx)]
     (fn [& args]
       (binding [*local-context* ctx]
         (apply f args))))))

(defn setup*
  ([factory]
   (dsl/setup
    (fn [ctx]
      (binding [*local-context* {:context ctx}]
        (factory ctx)))))
  ([factory {timer? :with-timer stash-opts :with-stash :as opts}]
   (cond
     timer?
     (dsl/with-timers
       #(setup* factory (-> (dissoc opts :with-timer)
                            (assoc-in [::ctx :timers] %))))

     stash-opts
     (dsl/with-stash (:capacity stash-opts)
       #(setup* factory (-> (dissoc opts :with-stash)
                            (assoc-in [::ctx :stash-buffer] %))))

     :else
     (setup*
      #(let [ctx (assoc (::ctx opts) :context %)]
         (binding [*local-context* ctx]
           (factory ctx)))))))

(defmacro setup
  "setup wrapper.

  ```Clojure
  (setup a-behavior [a0 a1]
    ...actor context available...
    returns a guardian behavior)

  ;; additional setup
  (setup a-behavior [a0 a1] {:with-timer true :with-stash {:capacity 2}}
    ...actor context & timers & stash buffer available...
    returns a guardian behavior)

  (a-behavior) ; -> guardian behavior
  ```"
  [n args & [opts-or-body & more-body]]
  (if (map? opts-or-body)
    `(defn ~n ~args
       (setup* (fn [_#] ~@more-body) ~opts-or-body))
    `(defn ~n ~args
       (setup* (fn [_#] ~opts-or-body ~@more-body)))))

(defn receive-message
  ([on-message]
   (let [local-ctx (local-context)]
     (dsl/receive
      (fn [ctx message]
        (let [on-message' (bound-fn** on-message local-ctx {:context ctx})]
          (on-message' message)))))))

(defn receive-signal
  ([on-signal]
   (let [local-ctx (local-context)]
     (dsl/receive
      (fn [_ctx _msg] ::unhandled)
      (fn [ctx signal]
        (let [on-signal' (bound-fn** on-signal local-ctx {:context ctx})]
          (on-signal' signal)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; TimerScheduler
;;; https://doc.akka.io/japi/akka/current/akka/actor/typed/javadsl/TimerScheduler.html

(defn cancel-timer
  ([]
   (dsl/cancel-all-timer (timers)))
  ([timer-key]
   (dsl/cancel-timer (timers) timer-key)))

(defn active-timer? [timer-key]
  (dsl/active-timer? (timers) timer-key))

(defn start-timer
  "Schedule a message to be sent.

  `timer-key` is optional. When given, can be checked or cancelled by
  `active-timer?` and `cancel-timer`.

  ```Clojure
  (start-timer msg {:timer-type :single :delay a-duration})

  ;; for fix-delay timer, initial-delay is optional
  (start-timer msg {:timer-type :fix-delay :delay a-duration})
  (start-timer msg {:timer-type :fix-delay :delay a-duration :initial-delay a-duration'})

  ;; for fix-rate timer, initial-delay is optional
  (start-timer msg {:timer-type :fix-rate :interval a-duration})
  (start-timer msg {:timer-type :fix-rate :interval a-duration :initial-delay a-duration'})
  ```"
  [msg {:keys [timer-key timer-type interval initial-delay delay]
        :as opts}]
  (dsl/start-timer (timers) msg opts))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Cluster & receptionist

(defn create-cluster-singleton-setting
  ([] (cluster-singleton/create-cluster-singleton-setting (system)))
  ([{:keys [buffer-size
            data-center
            hand-over-retry-interval
            lease-settings
            role
            removal-margin]
     :as opts}]
   (cluster-singleton/create-cluster-singleton-setting (system) opts)))

(defn register-with-receptionist
  ([worker-service-key]
   (actor-ref/tell (receptionist) (receptionist/register-service worker-service-key (self))))
  ([worker-service-key reply-to]
   (actor-ref/tell (receptionist) (receptionist/register-service worker-service-key reply-to))))

(defn subscribe-to-receptionist
  ([worker-service-key]
   (actor-ref/tell (receptionist) (receptionist/subscribe-service worker-service-key (self))))
  ([worker-service-key reply-to]
   (actor-ref/tell (receptionist) (receptionist/subscribe-service worker-service-key reply-to))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Log
;;; https://doc.akka.io/japi/akka/2.8/akka/actor/typed/javadsl/ActorContext.html#getLog()
;;; https://www.slf4j.org/api/org/slf4j/Logger.html

(defmacro trace [format-string & args] `(slf4j-log (.log (actor-context)) trace ~format-string ~@args))
(defmacro debug [format-string & args] `(slf4j-log (.log (actor-context)) debug ~format-string ~@args))
(defmacro info  [format-string & args] `(slf4j-log (.log (actor-context)) info ~format-string ~@args))
(defmacro warn  [format-string & args] `(slf4j-log (.log (actor-context)) warn ~format-string ~@args))
(defmacro error [format-string & args] `(slf4j-log (.log (actor-context)) error ~format-string ~@args))

(comment
  (macroexpand '(info "hello {} and {}" "42" "lotuc")))
