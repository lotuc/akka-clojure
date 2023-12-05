(ns lotuc.akka.actor.typed.scaladsl.ask-pattern
  (:import
   (akka.actor.typed.scaladsl AskPattern AskPattern$Askable)
   (akka.actor.typed ActorSystem Scheduler RecipientRef))
  (:require
   [lotuc.akka.common.scala :as scala]))

(set! *warn-on-reflection* true)

(defn scheduler-from-actor-system [^ActorSystem system]
  (AskPattern/schedulerFromActorSystem system))

(defn- await-ask-response
  [^scala.concurrent.Future f]
  (let [r (promise)

        on-complete
        (reify scala.Function1
          (apply [_ v]
            (->> (try (let [^scala.util.Try r v]
                        {:success (.get r)})
                      (catch Throwable t
                        {:failure t}))
                 (deliver r))))]
    (.onComplete f on-complete (scala.concurrent.ExecutionContext/global))
    (future (let [{:keys [success failure]} @r]
              (when failure (throw failure))
              success))))

(defn ask [^RecipientRef target
           create-request
           timeout
           ^Scheduler scheduler]
  (await-ask-response
   (.ask
    (AskPattern$Askable. target)
    (scala/->scala.function 1 create-request)
    (akka.util.Timeout/apply (scala/->scala.concurrent.duration.FiniteDuration timeout))
    scheduler)))

(defn ask-with-status [^RecipientRef target
                       create-request
                       timeout
                       ^Scheduler scheduler]
  (await-ask-response
   (.askWithStatus
    (AskPattern$Askable. target)
    (scala/->scala.function 1 create-request)
    (akka.util.Timeout/apply (scala/->scala.concurrent.duration.FiniteDuration timeout))
    scheduler)))
