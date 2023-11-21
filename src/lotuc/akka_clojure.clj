(ns lotuc.akka-clojure
  (:require
   [lotuc.akka.common.log :refer [slf4j-log]]
   [lotuc.akka.javadsl.actor.behaviors :as behaviors]
   [lotuc.akka.cluster :as cluster]
   [lotuc.akka.javadsl.actor :as javadsl.actor]
   [lotuc.akka.actor.receptionist :as actor.receptionist]))

(set! *warn-on-reflection* true)

;;; Dyanmic bindings for message & signal handlers
(def ^:dynamic *local-context* nil)

(defn local-context []
  (when-not *local-context*
    (throw (RuntimeException. "local context is empty, call under behavior context")))
  *local-context*)

(defn actor-context
  "Actor context of local context."
  ^akka.actor.typed.javadsl.ActorContext []
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
  ^akka.actor.typed.ActorRef []
  (.getSelf (actor-context)))

(defn system
  "ActorSystem of local context."
  ^akka.actor.typed.ActorSystem []
  (.getSystem (actor-context)))

(defn cluster ^akka.cluster.typed.Cluster []
  (cluster/get-cluster (system)))

(defn cluster-singleton ^akka.cluster.typed.ClusterSingleton []
  (cluster/get-cluster-singleton (system)))

(defn receptionist ^akka.actor.typed.ActorRef []
  (.receptionist (system)))

(defn tell
  "Send message to target. Send to self if not target given"
  ([^akka.actor.typed.ActorRef target message] (.tell target message))
  ([message] (.tell (self) message)))

(defn !
  "Same as tell."
  ([^akka.actor.typed.ActorRef target message] (.tell target message))
  ([message] (.tell (self) message)))

(defn ask
  "Ask pattern.

  `msg` should be a map, and `reply-to` key will be overriten by our tmp
  ActorRef for receiving the calling result."
  ([target msg timeout apply-to-response]
   (.ask (actor-context) Object target timeout
         (reify akka.japi.function.Function
           (apply [_ reply-to] (assoc msg :reply-to reply-to)))
         (reify akka.japi.function.Function2
           (apply [_ res throwable]
             (apply-to-response res throwable)))))
  ([^akka.actor.typed.ActorSystem system msg timeout]
   (future (.get (javadsl.actor/ask system (fn [reply-to] (assoc msg :reply-to reply-to))
                                    timeout (.scheduler system))))))

(defn schedule-once
  ([target duration message]
   (.scheduleOnce (actor-context) duration target message))
  ([duration message]
   (.scheduleOnce (actor-context) duration (self) message)))

(defn spawn
  (^akka.actor.typed.ActorRef [behavior name]
   (.spawn (actor-context) behavior name))
  (^akka.actor.typed.ActorRef [behavior]
   (.spawnAnonymous (actor-context) behavior)))

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
   (behaviors/setup
    (fn [ctx]
      (binding [*local-context* {:context ctx}]
        (factory ctx)))))
  ([factory {:keys [with-timer with-stash] :as opts}]
   (-> (fn [v]
         (binding [*local-context* (select-keys v [:context :timers :stash-buffer])]
           (factory v)))
       (behaviors/setup opts))))

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
     (behaviors/receive
      (fn [ctx message]
        (let [on-message' (bound-fn** on-message local-ctx {:context ctx})]
          (on-message' message)))))))

(defn receive-signal
  ([on-signal]
   (let [local-ctx (local-context)]
     (behaviors/receive
      (fn [_ctx _msg] ::unhandled)
      (fn [ctx signal]
        (let [on-signal' (bound-fn** on-signal local-ctx {:context ctx})]
          (on-signal' signal)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; TimerScheduler
;;; https://doc.akka.io/japi/akka/current/akka/actor/typed/javadsl/TimerScheduler.html

(defn cancel-timer
  ([]
   (javadsl.actor/cancel-all-timer (timers)))
  ([timer-key]
   (javadsl.actor/cancel-timer (timers) timer-key)))

(defn active-timer? [timer-key]
  (javadsl.actor/active-timer? (timers) timer-key))

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
  (javadsl.actor/start-timer (timers) msg opts))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Cluster & receptionist

(defn create-cluster-singleton-setting
  ([] (cluster/create-cluster-singleton-setting (system)))
  ([{:keys [buffer-size
            data-center
            hand-over-retry-interval
            lease-settings
            role
            removal-margin]
     :as opts}]
   (cluster/create-cluster-singleton-setting (system) opts)))

(defn register-with-receptionist
  ([worker-service-key]
   (.tell (receptionist) (actor.receptionist/register worker-service-key (self))))
  ([worker-service-key reply-to]
   (.tell (receptionist) (actor.receptionist/register worker-service-key reply-to))))

(defn subscribe-to-receptionist
  ([worker-service-key]
   (.tell (receptionist) (actor.receptionist/subscribe worker-service-key (self))))
  ([worker-service-key reply-to]
   (.tell (receptionist) (actor.receptionist/subscribe worker-service-key reply-to))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Log
;;; https://doc.akka.io/japi/akka/2.8/akka/actor/typed/javadsl/ActorContext.html#getLog()
;;; https://www.slf4j.org/api/org/slf4j/Logger.html

(defmacro trace [format-string & args] `(slf4j-log (.getLog (actor-context)) trace ~format-string ~@args))
(defmacro debug [format-string & args] `(slf4j-log (.getLog (actor-context)) debug ~format-string ~@args))
(defmacro info  [format-string & args] `(slf4j-log (.getLog (actor-context)) info ~format-string ~@args))
(defmacro warn  [format-string & args] `(slf4j-log (.getLog (actor-context)) warn ~format-string ~@args))
(defmacro error [format-string & args] `(slf4j-log (.getLog (actor-context)) error ~format-string ~@args))

(comment
  (macroexpand '(info "hello {} and {}" "42" "lotuc")))
