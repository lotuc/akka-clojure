(ns lotuc.akka.actor.typed.message-and-signals
  (:import
   (akka.actor.typed Signal
                     PreRestart
                     PostStop
                     Terminated
                     ChildFailed
                     MessageAdaptionFailure))
  (:require
   [lotuc.akka.cnv :as cnv]))

(defn signal? [d] (instance? Signal d))

(defmethod cnv/->clj PostStop [_]
  {:dtype :signal :signal :post-stop})

(defmethod cnv/->clj PreRestart [_]
  {:dtype :signal :signal :pre-restart})

(defmethod cnv/->clj ChildFailed [^ChildFailed d]
  {:dtype :signal :signal :terminated :ref (.getRef d) :cause (.getCause d)})

(defmethod cnv/->clj Terminated [^Terminated d]
  {:dtype :signal :signal :terminated :ref (.getRef d)})

(defmethod cnv/->clj MessageAdaptionFailure [^MessageAdaptionFailure d]
  {:dtype :signal :signal :terminated :exception (.getException d)})

(defmethod cnv/->clj Signal [^Signal d]
  {:dtype :signal :signal d :signal-type (type d)})
