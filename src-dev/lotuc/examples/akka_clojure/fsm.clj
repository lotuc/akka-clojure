(ns lotuc.examples.akka-clojure.fsm
  (:require
   [lotuc.akka-clojure :as a]
   [lotuc.akka.actor.typed.actor-system :as actor-system]))

(set! *warn-on-reflection* true)

;;; https://developer.lightbend.com/start/?group=akka&project=akka-samples-fsm-java

(a/setup chopstck-behavior []
  (letfn [(available []
            (a/receive-message
             (fn [{:keys [action hakker]}]
               (if (= action :Take)
                 (do (a/! hakker {:chopstick (a/self) :taken? true})
                     (taken-by hakker))
                 :same))))
          (taken-by [hakker-holder]
            (a/receive-message
             (fn [{:keys [action hakker]}]
               (cond
                 (= action :Take)
                 (do (a/! hakker {:chopstick (a/self) :taken? false})
                     :same)

                 (and (= action :Put) (= hakker hakker-holder))
                 (available)

                 :else :same))))]
    (available)))

(a/setup hakker-behavior [hakker-name
                          ^akka.actor.typed.ActorRef left-chopstick
                          ^akka.actor.typed.ActorRef right-chopstick]
  (letfn [(thinking []
            (a/receive-message
             (fn [{:keys [action]}]
               (if (= action :Eat)
                 (do (a/! left-chopstick {:action :Take :hakker (a/self)})
                     (a/! right-chopstick {:action :Take :hakker (a/self)})
                     (hungry))
                 :same))))
          (hungry []
            (a/receive-message
             (fn [{:keys [chopstick taken?]}]
               (if taken?
                 (cond
                   (= chopstick left-chopstick) (wait-for-other-chopstick right-chopstick left-chopstick)
                   (= chopstick right-chopstick) (wait-for-other-chopstick left-chopstick right-chopstick)
                   :else :same)
                 (first-chopstick-denied)))))
          (wait-for-other-chopstick [chopstick-to-wait-for taken-chopstick]
            (a/receive-message
             (fn [{:keys [chopstick taken?]}]
               (if (= chopstick chopstick-to-wait-for)
                 (if taken?
                   (do (a/info "{} picked up {} and {} and starts to eat"
                               hakker-name (.. left-chopstick path name) (.. right-chopstick path name))
                       (start-eating "5.sec"))
                   (do (a/! taken-chopstick {:action :Put :hakker (a/self)})
                       (start-thinking "10.millis")))
                 :same))))
          (eating []
            (a/receive-message
             (fn [{:keys [action]}]
               (if (= action :Think)
                 (do (a/info "{} puts down his chopsticks and starts to think" hakker-name)
                     (a/! left-chopstick {:action :Put :hakker (a/self)})
                     (a/! right-chopstick {:action :Put :hakker (a/self)})
                     (start-thinking "5.sec"))
                 :same))))
          (first-chopstick-denied []
            (a/receive-message
             (fn [{:keys [chopstick taken?]}]
               (if taken?
                 (do (a/! chopstick {:action :Put :hakker (a/self)})
                     (start-thinking "10.millis"))
                 (start-thinking "10.millis")))))
          (start-thinking [duration]
            (a/schedule-once duration {:action :Eat})
            (thinking))
          (start-eating [duration]
            (a/schedule-once duration {:action :Think})
            (eating))]
    (a/receive-message
     (fn [{:keys [action]}]
       (if (= action :Think)
         (do (a/info "{} starts to think" hakker-name)
             (start-thinking "3.sec"))
         :same)))))

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
      (a/! hakker {:action :Think}))
    :empty))

(comment
  (def s (actor-system/create-system (dining-behavior) "helloakka"))
  (.terminate s))
