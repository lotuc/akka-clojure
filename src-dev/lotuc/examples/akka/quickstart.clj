(ns lotuc.examples.akka.quickstart
  (:require
   [lotuc.akka.common.log :refer [slf4j-log]]
   [lotuc.akka.javadsl.actor.behaviors :as behaviors]
   [lotuc.akka.system :refer [create-system]]))

(set! *warn-on-reflection* true)

;;; https://developer.lightbend.com/guides/akka-quickstart-java/

(defmacro info [ctx msg & args]
  `(slf4j-log (.getLog ~ctx) info ~msg ~@args))

(defn greeter-behavior []
  (behaviors/receive
   (fn [^akka.actor.typed.javadsl.ActorContext ctx
        {:keys [whom ^akka.actor.typed.ActorRef reply-to]}]
     (when whom
       (info ctx "Hello {}!" whom)
       (.tell reply-to {:whom whom :reply-to (.getSelf ctx)})))))

(defn greeter-bot-behavior [max]
  (let [greeter-counter (atom 0)]
    (behaviors/receive
     (fn [^akka.actor.typed.javadsl.ActorContext ctx
          {:keys [whom ^akka.actor.typed.ActorRef reply-to]}]
       (when whom
         (swap! greeter-counter inc)
         (info ctx "Greeting {} for {}" @greeter-counter whom)
         (if (= @greeter-counter max)
           :stopped
           (.tell reply-to {:whom whom :reply-to (.getSelf ctx)})))))))

(defn greeter-main []
  (behaviors/setup
   (fn [^akka.actor.typed.javadsl.ActorContext ctx]
     (let [greeter (.spawn ctx (greeter-behavior) "greeter")]
       (behaviors/receive-message
        (fn [{:keys [whom] :as m}]
          (when whom
            (let [bot (.spawn ctx (greeter-bot-behavior 3) whom)]
              (.tell greeter {:whom whom :reply-to bot})))))))))

(comment
  (def s (create-system (greeter-main) "helloakka"))
  (.terminate s)
  (.tell s {:whom "42"}))
