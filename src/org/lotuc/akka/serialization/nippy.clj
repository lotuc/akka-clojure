(ns org.lotuc.akka.serialization.nippy
  (:require
   [taoensso.nippy :as nippy])
  (:import
   (akka.actor.typed ActorRef)))

;; akka.actor.ExtendedActorSystem
#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(def ^:dynamic *extended-actor-system* nil);

;; akka.actor.typed.ActorSystem
#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(def ^:dynamic *actor-system* nil)

;; akka.actor.typed.ActorRefResolver
#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(def ^:dynamic *actor-ref-resolver* nil)

;;; https://doc.akka.io/docs/akka/current/serialization.html#serializing-actorrefs

#_{:clj-kondo/ignore [:unresolved-symbol]}
(nippy/extend-freeze
 ActorRef :akka.actor.typed/ActorRef
 [x data-output]
 (->> (.toSerializationFormat *actor-ref-resolver* x)
      (.writeUTF data-output)))

#_{:clj-kondo/ignore [:unresolved-symbol]}
(nippy/extend-thaw
 :akka.actor.typed/ActorRef
 [data-input]
 (->> (.readUTF data-input)
      (.resolveActorRef *actor-ref-resolver*)))
