(ns lotuc.akka.actor.typed.scaladsl.behaviors
  (:require
   [lotuc.akka.cnv :as cnv]
   [lotuc.akka.common.slf4j :as common.log]
   [lotuc.akka.common.scala :as scala])
  (:import
   (akka.actor.typed LogOptions SupervisorStrategy)
   (akka.actor.typed.scaladsl Behaviors)))

(defn ->behavior [behavior-like]
  (or ({:same      (Behaviors/same)
        :stopped   (Behaviors/stopped)
        :empty     (Behaviors/empty)
        :ignore    (Behaviors/ignore)
        :unhandled (Behaviors/unhandled)}
       behavior-like)
      (when (instance? akka.actor.typed.Behavior behavior-like) behavior-like)
      (throw (ex-info (str "invalid behavior: " (type behavior-like)) {:value behavior-like}))))

(defn ->LogOptions
  ([] (LogOptions/apply))
  ([{:keys [^boolean enabled level ^org.slf4j.Logger logger] :as v}]
   (cond-> (LogOptions/apply)
     (some? enabled) (.withEnabled enabled)
     (some? level)   (.withLevel (common.log/->LogLevel level))
     (some? logger)  (.withLogger logger))))

(defn ->SupervisorStrategy [{:keys [strategy] :as v}]
  (or (and (instance? SupervisorStrategy v) v)
      (and (keyword? v) (cnv/->akka {:dtype :SupervisorStrategy :strategy v}))
      (and (map? v) (cnv/->akka (assoc v :dtype :SupervisorStrategy)))
      (throw (ex-info (str "illegal SupervisorStrategy: " (type v)) {:value v}))))

(comment
  (->LogOptions)
  (->LogOptions {:enabled false})
  (->LogOptions {:level :info})

  (->SupervisorStrategy :resume)
  (->SupervisorStrategy {:strategy :backoff
                         :min "1.sec"
                         :max "10.sec"
                         :random-factor 0.2}))

(defn setup [factory]
  (->> (reify scala.Function1
         (apply [_ ctx] (->behavior (factory ctx))))
       Behaviors/setup))

(defn with-stash [capacity factory]
  (->> (reify scala.Function1
         (apply [_ stash] (->behavior (factory stash))))
       (Behaviors/withStash capacity)))

(defn stopped
  ([] (Behaviors/stopped))
  ([post-stop] (Behaviors/stopped (reify scala.Function0 (apply [_] (post-stop))))))

(defn receive
  ([on-ctx-msg]
   (-> (reify scala.Function2
         (apply [_ ctx msg]
           (->behavior (on-ctx-msg ctx msg))))
       Behaviors/receive))
  ([on-ctx-msg on-ctx-signal]
   (-> (receive on-ctx-msg)
       (.receiveSignal (reify scala.PartialFunction
                         (isDefinedAt [_ _v] true)
                         (apply [_ v]
                           (->behavior (on-ctx-signal (._1 v) (._2 v)))))))))

(defn receive-message [on-msg]
  (-> (reify scala.Function1
        (apply [_ msg]
          (->behavior (on-msg msg))))
      Behaviors/receiveMessage))

(defn receive-partial
  ([on-ctx-msg] (receive-partial on-ctx-msg (constantly true)))
  ([on-ctx-msg defined-at?]
   (-> (reify scala.PartialFunction
         (isDefinedAt [_ v] (defined-at? v))
         (apply [_ v] (->behavior (on-ctx-msg v))))
       Behaviors/receivePartial)))

(defn receive-message-partial
  ([on-ctx-msg]
   (receive-message-partial on-ctx-msg (constantly true)))
  ([on-ctx-msg defined-at?]
   (-> (reify scala.PartialFunction
         (isDefinedAt [_ v] (defined-at? (._1 v) (._2 v)))
         (apply [_ v] (->behavior (on-ctx-msg (._1 v) (._2 v)))))
       Behaviors/receivePartial)))

(defn receive-signal
  ([on-signal]
   (receive-signal on-signal (constantly true)))
  ([on-signal defined-at?]
   (-> (reify scala.PartialFunction
         (isDefinedAt [_ v] (defined-at? v))
         (apply [_ v] (->behavior (on-signal v))))
       Behaviors/receiveSignal)))

(defn intercept [behavior interceptor-fn]
  (-> (reify scala.Function0
        (apply [_] (interceptor-fn)))
      (Behaviors/intercept behavior)))

(defn monitor
  ([behavior monitor-actor-ref]
   (Behaviors/monitor monitor-actor-ref behavior (scala/->scala.reflect.ClassTag Object)))
  ([behavior monitor-actor-ref class-tag-like]
   (Behaviors/monitor monitor-actor-ref behavior (scala/->scala.reflect.ClassTag class-tag-like))))

(defn log-messages
  ([behavior] (Behaviors/logMessages behavior))
  ([behavior log-options-like] (Behaviors/logMessages behavior (->LogOptions log-options-like))))

(defn supervise [behavior strategy]
  (-> (Behaviors/supervise behavior)
      (.withFailure (->SupervisorStrategy strategy))))

(defn with-timers [factory]
  (->> (reify scala.Function1
         (apply [_ timers] (->behavior (factory timers))))
       (Behaviors/withTimers)))

(defn with-dynamic-mdc
  ([behavior mdc-for-message]
   (Behaviors/withMdc (reify scala.Function1 (apply [_ msg] (mdc-for-message msg)))
                      behavior
                      (scala.reflect.ClassTag/Any)))
  ([behavior mdc-for-message class-tag-like]
   (Behaviors/withMdc (reify scala.Function1 (apply [_ msg] (mdc-for-message msg)))
                      behavior
                      (scala/->scala.reflect.ClassTag class-tag-like))))

(defn with-static-mdc
  ([behavior static-mdc]
   (Behaviors/withMdc (scala/->scala.collection.immutable.Map static-mdc)
                      behavior
                      (scala.reflect.ClassTag/Any)))
  ([behavior static-mdc class-tag-like]
   (Behaviors/withMdc (scala/->scala.collection.immutable.Map static-mdc)
                      behavior
                      (scala/->scala.reflect.ClassTag class-tag-like))))

(defn with-mdc
  ([behavior static-mdc mdc-for-message]
   (with-mdc behavior static-mdc mdc-for-message (scala.reflect.ClassTag/Any)))
  ([behavior static-mdc mdc-for-message class-tag-like]
   (cond
     (and static-mdc mdc-for-message)
     (Behaviors/withMdc (scala/->scala.collection.immutable.Map static-mdc)
                        (reify scala.Function1 (apply [_ msg] (mdc-for-message msg)))
                        behavior
                        (scala/->scala.reflect.ClassTag class-tag-like))

     static-mdc
     (with-static-mdc behavior static-mdc class-tag-like)

     mdc-for-message
     (with-dynamic-mdc behavior mdc-for-message class-tag-like)

     :else
     behavior)))
