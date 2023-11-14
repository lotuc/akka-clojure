(ns org.lotuc.akka-clojure-test
  (:require [clojure.test :refer :all]
            [org.lotuc.akka-clojure :refer [! actor-system ask make-behavior spawn tell self]])
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
     (fn [{:keys [whom exp ask-self] :as m}]
       (cond
         ;; start a bot for greeter
         whom
         (let [reply-to (spawn (greeter-bot 3) whom)]
           (! @!greeter reply-to {:whom whom}))

         ;; simple expression evaluation for ask test
         exp
         (! (apply (case (keyword (first exp))
                     :+ + :- - :* * :/ /)
                   (rest exp)))

         ask-self
         (let [f (bound-fn* #(deref (ask (self) {:exp '(* 6 7)} (Duration/ofSeconds 1))))]
           (future (println "ask within behavior: 6 * 7 =" (f)))
           nil)))
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

  (tell hello-system {:ask-self true})

  (.terminate hello-system))
