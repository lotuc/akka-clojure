(ns org.lotuc.akka-clojure-examples.quickstart
  (:require
   [org.lotuc.akka-clojure :as a])
  (:import
   (akka.actor.typed ActorSystem)))

;;; https://developer.lightbend.com/guides/akka-quickstart-java/

(defn greeter-behavior []
  (a/receive-message
   (fn [{:keys [whom reply-to]}]
     (when whom
       (a/info "Hello {}!" whom)
       (a/! reply-to {:whom whom :reply-to (a/self)})))))

(defn greeter-bot-behavior [max]
  (let [greeter-counter (atom 0)]
    (a/receive-message
     (fn [{:keys [whom reply-to]}]
       (when whom
         (swap! greeter-counter inc)
         (a/info "Greeting {} for {}" @greeter-counter whom)
         (if (= @greeter-counter max)
           :stopped
           (a/! reply-to {:whom whom :reply-to (a/self)})))))))

(defn greeter-main []
  (a/setup*
   (fn [_ignored-ctx]
     (let [greeter (a/spawn (greeter-behavior) "greeter")]
       (a/receive-message
        (fn [{:keys [whom] :as m}]
          (when whom
            (let [bot (a/spawn (greeter-bot-behavior 3) whom)]
              (a/! greeter {:whom whom :reply-to bot})))))))))

;;; simplify the above pattern with setup macro
(a/setup greeter-main-1 []
  (let [greeter (a/spawn (greeter-behavior) "greeter")]
    (a/receive-message
     (fn [{:keys [whom] :as m}]
       (when whom
         (let [bot (a/spawn (greeter-bot-behavior 3) whom)]
           (a/! greeter {:whom whom :reply-to bot})))))))

(comment
  (def s (ActorSystem/create (greeter-main) "helloakka"))
  (def s (ActorSystem/create (greeter-main-1) "helloakka"))
  (.terminate s)
  (a/tell s {:whom "42"}))
