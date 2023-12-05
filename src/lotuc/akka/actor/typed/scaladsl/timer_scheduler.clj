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
  [^TimerScheduler timer-scheduler
   ^Object msg
   {:keys [timer-key
           timer-type
           initial-delay
           interval
           delay]
    :as opts}]
  (let [initial-delay (some-> initial-delay scala/->scala.concurrent.duration.FiniteDuration)
        interval (some-> interval scala/->scala.concurrent.duration.FiniteDuration)
        delay (some-> delay scala/->scala.concurrent.duration.FiniteDuration)]
    (case timer-type
      :single
      (if timer-key
        (.startSingleTimer timer-scheduler timer-key msg delay)
        (.startSingleTimer timer-scheduler msg delay))
      :fix-delay
      (if timer-key
        (if initial-delay
          (.startTimerWithFixedDelay timer-scheduler timer-key msg initial-delay delay)
          (.startTimerWithFixedDelay timer-scheduler timer-key msg delay))
        (if initial-delay
          (.startTimerWithFixedDelay timer-scheduler msg initial-delay delay)
          (.startTimerWithFixedDelay timer-scheduler msg delay)))
      :fix-rate
      (if timer-key
        (if initial-delay
          (.startTimerAtFixedRate timer-scheduler timer-key msg initial-delay interval)
          (.startTimerAtFixedRate timer-scheduler timer-key msg interval))
        (if initial-delay
          (.startTimerAtFixedRate timer-scheduler msg initial-delay interval)
          (.startTimerAtFixedRate timer-scheduler msg interval))))))
