(ns lotuc.akka.actor.typed.scaladsl.timer-scheduler
  (:import
   (akka.actor.typed.scaladsl TimerScheduler))
  (:require
   [lotuc.akka.common.scala :as scala]))

(set! *warn-on-reflection* true)

(defn cancel-timer [^TimerScheduler timer-scheduler timer-key]
  (.cancel timer-scheduler timer-key))

(defn cancel-all-timer [^TimerScheduler timer-scheduler]
  (.cancelAll timer-scheduler))

(defn active-timer? [^TimerScheduler timer-scheduler timer-key]
  (.isTimerActive timer-scheduler timer-key))

(defn start-timer
  "```clojure
  ;; single
  {:msg :v0 :single \"10.ms\"}
  {:msg :v0 :single {:delay \"10.ms\"}}

  ;; fix-delay
  {:msg :v0 :fix-delay \"10.ms\"}
  {:msg :v0 :fix-delay {:delay \"10.ms\"}}
  {:msg :v0 :fix-delay {:initial-delay \"10.ms\" :delay \"10.ms\"}}

  ;; fix-interval
  {:msg :v0 :fix-interval \"10.ms\"}
  {:msg :v0 :fix-interval {:interval \"10.ms\"}}
  {:msg :v0 :fix-interval {:initial-delay \"10.ms\" :interval \"10.ms\"}}
  ```"
  [^TimerScheduler timer-scheduler
   {:keys [msg single fix-delay fix-rate timer-key]}]
  {:pre [(or single fix-delay fix-rate)]}
  (cond
    single
    (let [delay (-> (if (map? single) (:delay single) single)
                    scala/->scala.concurrent.duration.FiniteDuration)]
      (if timer-key
        (.startSingleTimer timer-scheduler timer-key msg delay)
        (.startSingleTimer timer-scheduler msg delay)))
    fix-delay
    (let [delay
          (-> (cond-> fix-delay (map? fix-delay) :delay)
              scala/->scala.concurrent.duration.FiniteDuration)
          initial-delay
          (some-> (cond-> fix-delay (map? fix-delay) :initial-delay)
                  scala/->scala.concurrent.duration.FiniteDuration)]
      (if timer-key
        (if initial-delay
          (.startTimerWithFixedDelay timer-scheduler timer-key msg initial-delay delay)
          (.startTimerWithFixedDelay timer-scheduler timer-key msg delay))
        (if initial-delay
          (.startTimerWithFixedDelay timer-scheduler msg initial-delay delay)
          (.startTimerWithFixedDelay timer-scheduler msg delay))))
    fix-rate
    (let [interval
          (-> (cond-> fix-rate (map? fix-rate) :interval)
              scala/->scala.concurrent.duration.FiniteDuration)
          initial-delay
          (some-> (cond-> fix-rate (map? fix-rate) :initial-delay)
                  scala/->scala.concurrent.duration.FiniteDuration)]
      (if timer-key
        (if initial-delay
          (.startTimerAtFixedRate timer-scheduler timer-key msg initial-delay interval)
          (.startTimerAtFixedRate timer-scheduler timer-key msg interval))
        (if initial-delay
          (.startTimerAtFixedRate timer-scheduler msg initial-delay interval)
          (.startTimerAtFixedRate timer-scheduler msg interval))))))
