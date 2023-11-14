(ns org.lotuc.akka-clojure-test
  (:require [clojure.test :refer :all]
            [org.lotuc.akka-clojure :refer [! actor-system ask make-behavior spawn tell]])
  (:import
   (java.time Duration)
   (akka.actor.typed.javadsl Behaviors)))

(defn greeter []
  (make-behavior
   (fn [{:keys [whom]}]
     (when whom
       (println "greeter:" whom)
       (! {:whom whom})))))

(defn greeter-bot [max]
  (let [greeter-counter (atom 0)]
    (make-behavior
     (fn [{:keys [whom]}]
       (when whom
         (swap! greeter-counter inc)
         (println "bot: greeting" @greeter-counter "for" whom "!")
         (if (= @greeter-counter max)
           (Behaviors/stopped)
           (! {:whom whom})))))))

(defn greeter-main []
  (let [!greeter (atom nil)]
    (make-behavior
     (fn [{:keys [whom exp]}]
       (cond
         ;; start a bot for greeter
         whom
         (let [reply-to (spawn (greeter-bot 3) whom)]
           (! @!greeter reply-to {:whom whom}))

         ;; simple expression evaluation for ask test
         exp
         (! (apply (case (keyword (first exp))
                     :+ + :- - :* * :/ /)
                   (rest exp)))))
     (fn []
       ;; initialize greeter
       (reset! !greeter (spawn (greeter) "greeter"))))))

(comment
  (def hello-system (actor-system (greeter-main) "helloakka"))
  (tell hello-system {:whom "42"})

  @(ask hello-system
        {:exp '(+ 40 2)}
        (Duration/ofSeconds 1)
        (.scheduler hello-system))

  (.terminate hello-system))
