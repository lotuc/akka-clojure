(ns lotuc.akka.actor.typed.event-stream
  (:import
   (akka.actor.typed.eventstream EventStream$Publish
                                 EventStream$Subscribe
                                 EventStream$Unsubscribe))
  (:require
   [lotuc.akka.common.scala :as scala]))

(defn publish-command
  ^EventStream$Publish [ev]
  (EventStream$Publish. ev))

(defn subscribe-command
  (^EventStream$Subscribe [subscriber]
   (subscribe-command subscriber Object))
  (^EventStream$Subscribe [subscriber class-tag-like]
   (EventStream$Subscribe. subscriber (scala/->scala.reflect.ClassTag class-tag-like))))

(defn unsubscribe-command
  ^EventStream$Unsubscribe [subscriber]
  (EventStream$Unsubscribe. subscriber))
