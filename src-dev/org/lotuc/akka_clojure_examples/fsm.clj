(ns org.lotuc.akka-clojure-examples.fsm
  (:require
   [org.lotuc.akka-clojure :as a])
  (:import
   (akka.actor.typed ActorSystem)
   (java.time Duration)))

;;; https://developer.lightbend.com/start/?group=akka&project=akka-samples-fsm-java

(a/setup chopstck-behavior []
  (letfn [(available []
            (a/receive-message
             (fn [{:keys [action hakker]}]
               (when (= action :Take)
                 (a/! hakker {:chopstick (a/self) :taken? true})
                 (taken-by hakker)))))
          (taken-by [hakker-holder]
            (a/receive-message
             (fn [{:keys [action hakker]}]
               (cond
                 (= action :Take)
                 (a/! hakker {:chopstick (a/self) :taken? false})

                 (and (= action :Put) (= hakker hakker-holder))
                 (available)))))]
    (available)))

(a/setup hakker-behavior [name left right]
  (letfn [(thinking []
            (a/receive-message
             (fn [{:keys [action]}]
               (when (= action :Eat)
                 (a/! left {:action :Take :hakker (a/self)})
                 (a/! right {:action :Take :hakker (a/self)})
                 (hungry)))))
          (hungry []
            (a/receive-message
             (fn [{:keys [chopstick taken?]}]
               (if taken?
                 (cond
                   (= chopstick left) (wait-for-other-chopstick right left)
                   (= chopstick right) (wait-for-other-chopstick left right))
                 (first-chopstick-denied)))))
          (wait-for-other-chopstick [chopstick-to-wait-for taken-chopstick]
            (a/receive-message
             (fn [{:keys [chopstick taken?]}]
               (when (= chopstick chopstick-to-wait-for)
                 (if taken?
                   (do (a/info "{} picked up {} and {} and starts to eat"
                               name (.. left path name) (.. right path name))
                       (start-eating (Duration/ofSeconds 5)))
                   (do (a/! taken-chopstick {:action :Put :hakker (a/self)})
                       (start-thinking (Duration/ofMillis 10))))))))
          (eating []
            (a/receive-message
             (fn [{:keys [action]}]
               (when (= action :Think)
                 (a/info "{} puts down his chopsticks and starts to think" name)
                 (a/! left {:action :Put :hakker (a/self)})
                 (a/! right {:action :Put :hakker (a/self)})
                 (start-thinking (Duration/ofSeconds 5))))))
          (first-chopstick-denied []
            (a/receive-message
             (fn [{:keys [chopstick taken?]}]
               (if taken?
                 (do (a/! chopstick {:action :Put :hakker (a/self)})
                     (start-thinking (Duration/ofMillis 10)))
                 (start-thinking (Duration/ofMillis 10))))))
          (start-thinking [duration]
            (a/schedule-once duration {:action :Eat})
            (thinking))
          (start-eating [duration]
            (a/schedule-once duration {:action :Think})
            (eating))]
    (a/receive-message
     (fn [{:keys [action]}]
       (when (= action :Think)
         (a/info "{} starts to think" name)
         (start-thinking (Duration/ofSeconds 3)))))))

(a/setup dining-behavior []
  (let [hakker-names ["Ghosh" "Boner" "Klang" "Krasser" "Manie"]
        chopsticks (->> hakker-names
                        (map-indexed (fn [i _] (a/spawn (chopstck-behavior) (str "Chopstick" i))))
                        (into []))
        hakkers (for [[i n] (map-indexed (fn [i n] [i n]) hakker-names)
                      :let [left (get chopsticks i)
                            right (get chopsticks (mod (inc i) (count hakker-names)))]]
                  (a/spawn (hakker-behavior n left right) n))]
    (doseq [hakker hakkers]
      (a/! hakker {:action :Think}))))

(comment
  (def s (ActorSystem/create (dining-behavior) "helloakka"))
  (.terminate s))
