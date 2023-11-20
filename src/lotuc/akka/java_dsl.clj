(ns lotuc.akka.java-dsl
  (:import
   [akka.actor.typed.javadsl AskPattern]))

(set! *warn-on-reflection* true)

(defprotocol TimerScheduler
  (cancel-timer [_ timer-key])
  (cancel-all-timer [_])
  (active-timer? [_ timer-key])
  (start-timer [_ msg {:keys [timer-key timer-type interval initial-delay delay]
                       :as opts}]))

;;; https://doc.akka.io/japi/akka/current/akka/actor/typed/javadsl/TimerScheduler.html
(extend-type akka.actor.typed.javadsl.TimerScheduler
  TimerScheduler
  (cancel-timer [this timer-key] (.cancel this timer-key))
  (active-timer? [this timer-key] (.isTimerActive this timer-key))
  (start-timer [this ^Object msg
                {:keys [^Object timer-key
                        timer-type
                        ^java.time.Duration interval
                        ^java.time.Duration initial-delay
                        ^java.time.Duration delay]
                 :as opts}]
    (case timer-type
      :single    (if timer-key
                   (.startSingleTimer this timer-key msg delay)
                   (.startSingleTimer this msg delay))
      :fix-delay (if timer-key
                   (if initial-delay
                     (.startTimerWithFixedDelay this timer-key msg initial-delay delay)
                     (.startTimerWithFixedDelay this timer-key msg delay))
                   (if initial-delay
                     (.startTimerWithFixedDelay this msg initial-delay delay)
                     (.startTimerWithFixedDelay this msg delay)))
      :fix-rate  (if timer-key
                   (if initial-delay
                     (.startTimerAtFixedRate this timer-key msg initial-delay interval)
                     (.startTimerAtFixedRate this timer-key msg interval))
                   (if initial-delay
                     (.startTimerAtFixedRate this msg initial-delay interval)
                     (.startTimerAtFixedRate this msg interval))))))

(defn ask
  ^java.util.concurrent.CompletableFuture [actor build-msg timeout scheduler]
  (-> (AskPattern/ask actor
                      (reify akka.japi.function.Function
                        (apply [_ reply-to] (build-msg reply-to)))
                      timeout
                      scheduler)
      (.toCompletableFuture)))
