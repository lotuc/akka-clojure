(ns lotuc.akka.actor.typed.actor-ref
  (:import
   (akka.actor.typed ActorRef)))

(set! *warn-on-reflection* true)

(defn tell [^ActorRef actor-ref msg]
  (.tell actor-ref msg))

(defn ! [^ActorRef actor-ref msg]
  (.tell actor-ref msg))

(defn narrow ^ActorRef [^ActorRef actor-ref]
  (.narrow actor-ref))

(defn path ^akka.actor.ActorPath [^ActorRef actor-ref]
  (.path actor-ref))
