(ns org.lotuc.examples.quickstart
  (:require
   [org.lotuc.akka-clojure :as a])
  (:import
   (akka.actor.typed ActorSystem)))

;;; https://developer.lightbend.com/guides/akka-quickstart-java/

(defmacro info [ctx msg & args]
  `(.info (.getLog ~ctx) ~msg (into-array Object [~@args])))

(defn greeter-behavior []
  (a/receive
   (fn [ctx {:keys [whom reply-to]}]
     (when whom
       (info ctx "Hello {}!" whom)
       (.tell reply-to {:whom whom :reply-to (.getSelf ctx)})))))

(defn greeter-bot-behavior [max]
  (let [greeter-counter (atom 0)]
    (a/receive
     (fn [ctx {:keys [whom reply-to]}]
       (when whom
         (swap! greeter-counter inc)
         (info ctx "Greeting {} for {}" @greeter-counter whom)
         (if (= @greeter-counter max)
           :stopped
           (.tell reply-to {:whom whom :reply-to (.getSelf ctx)})))))))

(defn greeter-main []
  (a/setup
   (fn [ctx]
     (let [greeter (.spawn ctx (greeter-behavior) "greeter")]
       (a/receive-message
        (fn [{:keys [whom] :as m}]
          (when whom
            (let [bot (.spawn ctx (greeter-bot-behavior 3) whom)]
              (.tell greeter {:whom whom :reply-to bot})))))))))

(comment
  (def s (ActorSystem/create (greeter-main) "helloakka"))
  (.terminate s)
  (.tell s {:whom "42"}))
