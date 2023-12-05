(ns lotuc.examples.akka.fsm
  (:require
   [lotuc.akka.actor.typed.actor-ref :as actor-ref]
   [lotuc.akka.actor.typed.actor-system :as actor-system]
   [lotuc.akka.actor.scaladsl :as dsl]
   [lotuc.akka.common.slf4j :refer [slf4j-log]]))

(set! *warn-on-reflection* true)

;;; https://developer.lightbend.com/start/?group=akka&project=akka-samples-fsm-java

(defmacro info [ctx msg & args]
  `(slf4j-log (dsl/log ~ctx) info ~msg ~@args))

(def chopstck-behavior
  (dsl/setup
   (fn [ctx]
     (letfn [(available []
               (dsl/receive-message
                (fn [{:keys [action hakker]}]
                  (or (when (= action :Take)
                        (actor-ref/tell hakker {:chopstick (dsl/self ctx) :taken? true})
                        (taken-by hakker))
                      :same))))
             (taken-by [hakker-holder]
               (dsl/receive-message
                (fn [{:keys [action ^akka.actor.typed.ActorRef hakker]}]
                  (or (cond
                        (= action :Take)
                        (actor-ref/tell hakker {:chopstick (dsl/self ctx) :taken? false})

                        (and (= action :Put) (= hakker hakker-holder))
                        (available))
                      :same))))]
       (available)))))

(defn hakker-behavior [hakker-name left right]
  (dsl/setup
   (fn [ctx]
     (letfn [(thinking []
               (dsl/receive-message
                (fn [{:keys [action]}]
                  (or (when (= action :Eat)
                        (actor-ref/tell left {:action :Take :hakker (dsl/self ctx)})
                        (actor-ref/tell right {:action :Take :hakker (dsl/self ctx)})
                        (hungry))
                      :unhandled))))
             (hungry []
               (dsl/receive-message
                (fn [{:keys [chopstick taken?]}]
                  (if taken?
                    (cond
                      (= chopstick left) (wait-for-other-chopstick right left)
                      (= chopstick right) (wait-for-other-chopstick left right))
                    (first-chopstick-denied)))))
             (wait-for-other-chopstick [chopstick-to-wait-for
                                        ^akka.actor.typed.ActorRef taken-chopstick]
               (dsl/receive-message
                (fn [{:keys [chopstick taken?]}]
                  (if (= chopstick chopstick-to-wait-for)
                    (if taken?
                      (do (info ctx "{} picked up {} and {} and starts to eat"
                                hakker-name
                                (.name (actor-ref/path left))
                                (.name (actor-ref/path right)))
                          (start-eating "5.sec"))
                      (do (.tell taken-chopstick {:action :Put :hakker (dsl/self ctx)})
                          (start-thinking "10.sec")))
                    :unhandled))))
             (eating []
               (dsl/receive-message
                (fn [{:keys [action]}]
                  (or (when (= action :Think)
                        (info ctx "{} puts down his chopsticks and starts to think" hakker-name)
                        (actor-ref/tell left {:action :Put :hakker (dsl/self ctx)})
                        (actor-ref/tell right {:action :Put :hakker (dsl/self ctx)})
                        (start-thinking "5.sec"))
                      :unhandled))))
             (first-chopstick-denied []
               (dsl/receive-message
                (fn [{:keys [chopstick taken?]}]
                  (if taken?
                    (do (actor-ref/tell chopstick {:action :Put :hakker (dsl/self ctx)})
                        (start-thinking "10.millis"))
                    (start-thinking "10.millis")))))
             (start-thinking [duration]
               (dsl/schedule-once ctx duration (dsl/self ctx) {:action :Eat})
               (thinking))
             (start-eating [duration]
               (dsl/schedule-once ctx duration (dsl/self ctx) {:action :Think})
               (eating))]
       (dsl/receive-message
        (fn [{:keys [action]}]
          (or (when (= action :Think)
                (info ctx "{} starts to think" hakker-name)
                (start-thinking "3.sec"))
              :unhandled)))))))

(def dining-behavior
  (dsl/setup
   (fn [ctx]
     (let [hakker-names ["Ghosh" "Boner" "Klang" "Krasser" "Manie"]
           chopsticks (->> hakker-names
                           (map-indexed (fn [i _] (dsl/spawn ctx chopstck-behavior (str "Chopstick" i))))
                           (into []))
           hakkers (for [[i n] (map-indexed (fn [i n] [i n]) hakker-names)
                         :let [left (get chopsticks i)
                               right (get chopsticks (mod (inc i) (count hakker-names)))]]
                     (dsl/spawn ctx (hakker-behavior n left right) n))]
       (doseq [^akka.actor.typed.ActorRef hakker hakkers]
         (actor-ref/tell hakker {:action :Think})))
     :empty)))

(comment
  (def s (actor-system/create-system dining-behavior "helloakka"))
  (actor-system/terminate s))
