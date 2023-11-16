(ns org.lotuc.akka.behaviors
  (:require
   [clojure.string :as s])
  (:import
   (akka.actor.typed Behavior LogOptions)
   (akka.actor.typed.javadsl Behaviors)
   (org.slf4j.event Level)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Behaviors API
;; https://doc.akka.io/japi/akka/current/akka/actor/typed/javadsl/Behaviors$.html
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;; Behavior factories
(declare setup)
(declare with-timers)
(declare with-stash)

;;; Build behaviors
(declare receive)
(declare receive-message)
(declare receive-signal)

;;; Work with behaviors
(declare with-mdc)
(declare intercept)
(declare log-messages)
(declare monitor)

;;; implementation utilities
(declare ->behavior)
(declare ->LogOptions)

(defn setup
  "Build behavior with factory function.

  ```Clojure
  (setup (fn [ctx] returns behavior))

  (setup (fn [{:keys [context timers stash-buffer]}] returns behavior)
         {:with-timer true :with-stash {:capacity 2}})
  ```"
  ([factory]
   (Behaviors/setup
    (reify akka.japi.function.Function
      (apply [_ ctx]
        (->behavior (factory ctx) :nil-behavior (Behaviors/empty))))))
  ([factory {timer? :with-timer stash-opts :with-stash :as opts}]
   (cond
     timer?
     (with-timers
       (fn [timers]
         (setup factory (-> (dissoc opts :with-timer)
                            (assoc-in [:factory-input :timers] timers)))))

     stash-opts
     (with-stash (:capacity stash-opts)
       (fn [stash-buffer]
         (setup factory (-> (dissoc opts :with-stash)
                            (assoc-in [:factory-input :stash-buffer] stash-buffer)))))

     :else
     (setup
      (fn [ctx]
        (factory (-> (:factory-input opts)
                     (assoc :context ctx))))))))

(defn with-timers [factory]
  (Behaviors/withTimers
   (reify akka.japi.function.Function
     (apply [_ timers]
       (->behavior (factory timers) :nil-behavior (Behaviors/empty))))))

(defn with-stash [capacity factory]
  (Behaviors/withStash
   capacity
   (reify java.util.function.Function
     (apply [_ stash-buffer]
       (->behavior (factory stash-buffer) :nil-behavior (Behaviors/empty))))))

(defn receive
  "Construct actor behavior that handles message or signals.

  ```Clojure
  (receive (fn [ctx msg] returns behavior))

  (receive (fn [ctx msg] returns behavior)
           (fn [ctx signal] returns behavior))
  ```"
  ([on-ctx-message]
   (Behaviors/receive
    (reify akka.japi.function.Function2
      (apply [_ ctx msg] (->behavior (on-ctx-message ctx msg))))))
  ([on-ctx-message on-ctx-signal]
   (Behaviors/receive
    (reify akka.japi.function.Function2
      (apply [_ ctx msg] (->behavior (on-ctx-message ctx msg))))
    (reify akka.japi.function.Function2
      (apply [_ ctx signal] (->behavior (on-ctx-signal ctx signal)))))))

(defn receive-message
  "Construct actor behavior that handles message.

  ```Clojure
  (receive (fn [msg] returns behavior))
  ```"
  [on-message]
  (Behaviors/receiveMessage
   (reify akka.japi.Function
     (apply [_ msg] (->behavior (on-message msg))))))

(defn receive-signal
  "Construct actor behavior that handles signal.

  ```Clojure
  (receive (fn [signal] returns behavior))
  ```"
  [on-signal]
  (Behaviors/receiveSignal
   (reify akka.japi.Function
     (apply [_ signal] (->behavior (on-signal signal))))))

(defn with-mdc
  "Mapped Diagnostic Context.

  ```Clojure
  (with-mdc some-behavior MessageClass
     {:static-mdc {\"key\" \"value\"}
      :mdc-for-message (fn [a-message] {\"key1\" \"value1\"})})

  ;; MessageClass defaults to be Object
  (with-mdc some-behavior
     {:static-mdc {\"key\" \"value\"}
      :mdc-for-message (fn [a-message] {\"key1\" \"value1\"})})
  ```
  "
  ([^Behavior behavior
    ^Class intercept-message-class
    {:keys [static-mdc mdc-for-message]}]
   (cond
     (and static-mdc mdc-for-message)
     (Behaviors/withMdc intercept-message-class static-mdc
                        (reify akka.japi.function.Function
                          (apply [_ msg] (mdc-for-message msg)))
                        behavior)

     static-mdc
     (Behaviors/withMdc intercept-message-class static-mdc
                        behavior)

     mdc-for-message
     (Behaviors/withMdc intercept-message-class
                        (reify akka.japi.function.Function
                          (apply [_ msg] (mdc-for-message msg)))
                        behavior)

     :else
     behavior))
  ([^Behavior behavior
    {:keys [static-mdc mdc-for-message] :as mdc}]
   (with-mdc behavior Object mdc)))

(defn intercept
  "Intercept messages and signals for a behavior by first passing them to
  a [BehaviorInterceptor](https://doc.akka.io/japi/akka/current/akka/actor/typed/BehaviorInterceptor.html).

  ```Clojure
  (intercept a-behavior (fn [] returns a behavior interceptor))
  ```"
  [behavior behavior-interceptor]
  (Behaviors/intercept
   (reify java.util.function.Supplier
     (get [_] (behavior-interceptor)))
   behavior))

(defn log-messages
  "Behavior decorator that logs all messages to the Behavior.

  ```Clojure
  (log-messages a-behavior)
  (log-messages a-behavior {:enabled true :level :info :logger some-logger})
  (log-messages a-behavior a-log-option-instance)
  ```"
  ([behavior]
   (Behaviors/logMessages behavior))
  ([behavior log-options]
   (Behaviors/logMessages (->LogOptions log-options) behavior)))

(defn monitor
  "Behavior decorator that copies all received message to the designated monitor
  ActorRef before invoking the wrapped behavior."
  ([behavior monitor intercept-message-class]
   (Behaviors/monitor intercept-message-class monitor behavior))
  ([behavior monitor]
   (Behaviors/monitor Object monitor behavior)))

(defn- ->behavior
  "Convert some common value to behavior."
  [behavior-like & {:keys [nil-behavior]
                    :or {nil-behavior (Behaviors/same)}}]
  (let [v (or behavior-like nil-behavior)]
    (or (and (keyword? v)
             (v {:same    (Behaviors/same)
                 :stopped (Behaviors/stopped)
                 :empty   (Behaviors/empty)
                 :ignore  (Behaviors/ignore)}))
        (when (instance? Behavior v) v)
        (throw (ex-info (str "invalid behavior: " (class v)) {:value v})))))

(defn- ->LogLevel [level]
  (cond
    (instance? Level level) level
    (keyword? level) (Level/valueOf (s/upper-case (name level)))
    :else (throw (RuntimeException. (str "illegal log level: " level)))))

(defn- ->LogOptions
  [& [{:keys [^boolean enabled level ^org.slf4j.Logger logger] :as v}]]
  (if (instance? LogOptions v)
    v
    (cond-> (LogOptions/apply)
      (some? enabled) (.withEnabled enabled)
      (some? level) (.withLevel (->LogLevel level))
      (some? logger) (.withLogger logger))))
