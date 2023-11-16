(ns org.lotuc.examples.fsm
  (:require
   [org.lotuc.akka.behaviors :as behaviors])
  (:import
   (akka.actor.typed ActorSystem)
   (akka.actor.typed.javadsl Behaviors)
   (java.time Duration)))

;;; https://developer.lightbend.com/start/?group=akka&project=akka-samples-fsm-java

(defmacro info [ctx msg & args]
  `(.info (.getLog ~ctx) ~msg (into-array [~@args])))

(def chopstck-behavior
  (behaviors/setup
   (fn [ctx]
     (letfn [(available []
               (behaviors/receive-message
                (fn [{:keys [action hakker]}]
                  (when (= action :Take)
                    (.tell hakker {:chopstick (.getSelf ctx) :taken? true})
                    (taken-by hakker)))))
             (taken-by [hakker-holder]
               (behaviors/receive-message
                (fn [{:keys [action hakker]}]
                  (cond
                    (= action :Take)
                    (.tell hakker {:chopstick (.getSelf ctx) :taken? false})

                    (and (= action :Put) (= hakker hakker-holder))
                    (available)))))]
       (available)))))

(defn hakker-behavior [name left right]
  (behaviors/setup
   (fn [ctx]
     (letfn [(thinking []
               (behaviors/receive-message
                (fn [{:keys [action]}]
                  (when (= action :Eat)
                    (.tell left {:action :Take :hakker (.getSelf ctx)})
                    (.tell right {:action :Take :hakker (.getSelf ctx)})
                    (hungry)))))
             (hungry []
               (behaviors/receive-message
                (fn [{:keys [chopstick taken?]}]
                  (if taken?
                    (cond
                      (= chopstick left) (wait-for-other-chopstick right left)
                      (= chopstick right) (wait-for-other-chopstick left right))
                    (first-chopstick-denied)))))
             (wait-for-other-chopstick [chopstick-to-wait-for taken-chopstick]
               (behaviors/receive-message
                (fn [{:keys [chopstick taken?]}]
                  (when (= chopstick chopstick-to-wait-for)
                    (if taken?
                      (do (info ctx "{} picked up {} and {} and starts to eat"
                                name (.. left path name) (.. right path name))
                          (start-eating (Duration/ofSeconds 5)))
                      (do (.tell taken-chopstick {:action :Put :hakker (.getSelf ctx)})
                          (start-thinking (Duration/ofMillis 10))))))))
             (eating []
               (behaviors/receive-message
                (fn [{:keys [action]}]
                  (when (= action :Think)
                    (info ctx "{} puts down his chopsticks and starts to think" name)
                    (.tell left {:action :Put :hakker (.getSelf ctx)})
                    (.tell right {:action :Put :hakker (.getSelf ctx)})
                    (start-thinking (Duration/ofSeconds 5))))))
             (first-chopstick-denied []
               (behaviors/receive-message
                (fn [{:keys [chopstick taken?]}]
                  (if taken?
                    (do (.tell chopstick {:action :Put :hakker (.getSelf ctx)})
                        (start-thinking (Duration/ofMillis 10)))
                    (start-thinking (Duration/ofMillis 10))))))
             (start-thinking [duration]
               (.scheduleOnce ctx duration (.getSelf ctx) {:action :Eat})
               (thinking))
             (start-eating [duration]
               (.scheduleOnce ctx duration (.getSelf ctx) {:action :Think})
               (eating))]
       (Behaviors/receiveMessage
        (reify akka.japi.Function
          (apply [_ {:keys [action]}]
            (when (= action :Think)
              (info ctx "{} starts to think" name)
              (start-thinking (Duration/ofSeconds 3))))))))))

(def dining-behavior
  (behaviors/setup
   (fn [ctx]
     (let [hakker-names ["Ghosh" "Boner" "Klang" "Krasser" "Manie"]
           chopsticks (->> hakker-names
                           (map-indexed (fn [i _] (.spawn ctx chopstck-behavior (str "Chopstick" i))))
                           (into []))
           hakkers (for [[i n] (map-indexed (fn [i n] [i n]) hakker-names)
                         :let [left (get chopsticks i)
                               right (get chopsticks (mod (inc i) (count hakker-names)))]]
                     (.spawn ctx (hakker-behavior n left right) n))]
       (doseq [hakker hakkers]
         (.tell hakker {:action :Think}))))))

(comment
  (def s (ActorSystem/create dining-behavior "helloakka"))
  (.terminate s))
