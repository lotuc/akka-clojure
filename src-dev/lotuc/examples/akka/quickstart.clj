(ns lotuc.examples.akka.quickstart
  (:require
   [lotuc.akka.common.slf4j :refer [slf4j-log]]
   [lotuc.akka.actor.scaladsl :as dsl]
   [lotuc.akka.actor.typed.actor-ref :as actor-ref]
   [lotuc.akka.actor.typed.actor-system :as actor-system]))

(set! *warn-on-reflection* true)

;;; https://developer.lightbend.com/guides/akka-quickstart-java/

(defmacro info [ctx msg & args]
  `(slf4j-log (dsl/log ~ctx) info ~msg ~@args))

(defn greeter-behavior []
  (dsl/receive
   (fn [ctx {:keys [whom reply-to]}]
     (when whom
       (info ctx "Hello {}!" whom)
       (actor-ref/tell reply-to {:whom whom :reply-to (dsl/self ctx)}))
     :same)))

(defn greeter-bot-behavior [max]
  (let [greeter-counter (atom 0)]
    (dsl/receive-message-partial
     (fn [ctx {:keys [whom reply-to]}]
       (or (when whom
             (swap! greeter-counter inc)
             (info ctx "Greeting {} for {}" @greeter-counter whom)
             (if (= @greeter-counter max)
               :stopped
               (actor-ref/tell reply-to {:whom whom :reply-to (dsl/self ctx)})))
           :same)))))

(defn greeter-main []
  (dsl/setup
   (fn [ctx]
     (let [greeter (dsl/spawn ctx (greeter-behavior) "greeter")]
       (dsl/receive-message
        (fn [{:keys [whom] :as m}]
          (when whom
            (let [bot (dsl/spawn ctx (greeter-bot-behavior 3) whom)]
              (actor-ref/tell greeter {:whom whom :reply-to bot})))
          :same))))))

(comment
  (def s (actor-system/create-system (greeter-main) "helloakka"))
  (actor-system/terminate s)
  (actor-ref/tell s {:whom "42"}))
