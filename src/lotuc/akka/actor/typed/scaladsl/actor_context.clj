(ns lotuc.akka.actor.typed.scaladsl.actor-context
  (:require
   [lotuc.akka.common.scala :as scala])
  (:import
   (akka.actor.typed
    ActorRef
    ActorSystem
    Behavior
    RecipientRef)
   (akka.actor.typed.scaladsl ActorContext)))

(set! *warn-on-reflection* true)

(defn self [^ActorContext ctx]
  (.self ctx))

(defn system ^ActorSystem [^ActorContext ctx]
  (.system ctx))

(defn log [^ActorContext ctx]
  (.log ctx))

(defn set-logger-name [^ActorContext ctx ^Class clazz]
  (.setLoggerName ctx clazz))

(defn children [^ActorContext ctx]
  (iterator-seq (.children ctx)))

(defn child [^ActorContext ctx ^String child-name]
  (.getOrElse (.child ctx child-name) nil))

(defn spawn-anonymous [^ActorContext ctx ^Behavior behavior]
  (.spawnAnonymous ctx behavior (akka.actor.typed.Props/empty)))

(defn spawn [^ActorContext ctx ^Behavior behavior ^String actor-name]
  (.spawn ctx behavior actor-name (akka.actor.typed.Props/empty)))

(defn delegate [^ActorContext ctx ^Behavior delegator ^Object msg]
  (.delegate ctx delegator msg))

(defn stop [^ActorContext ctx ^ActorRef children]
  (.stop ctx children))

(defn watch-with [^ActorContext ctx ^ActorRef other ^Object msg]
  (.watchWith ctx other msg))

(defn watch
  ([^ActorContext ctx ^ActorRef other ^Object msg]
   (.watchWith ctx other msg))
  ([^ActorContext ctx ^ActorRef other]
   (.watch ctx other)))

(defn unwatch
  ([^ActorContext ctx ^ActorRef other]
   (.unwatch ctx other)))

(defn set-receive-timeout [^ActorContext ctx timeout ^Object msg]
  (let [timeout (scala/->scala.concurrent.duration.FiniteDuration timeout)]
    (.setReceiveTimeout ctx timeout msg)))

(defn cancel-receive-timeout [^ActorContext ctx]
  (.cancelReceiveTimeout ctx))

(defn schedule-once [^ActorContext ctx delay ^ActorRef target ^Object msg]
  (let [delay (scala/->scala.concurrent.duration.FiniteDuration delay)]
    (.scheduleOnce ctx delay target msg)))

(defn execution-context [^ActorContext ctx]
  (.executionContext ctx))

(defn message-adapter
  ([^ActorContext ctx adapter-fn]
   (.messageAdapter ctx
                    (reify scala.Function1 (apply [_ v] (adapter-fn v)))
                    (scala.reflect.ClassTag/Any)))
  ([^ActorContext ctx adapter-fn class-tag-like]
   (.messageAdapter ctx
                    (reify scala.Function1 (apply [_ v] (adapter-fn v)))
                    (scala/->scala.reflect.ClassTag class-tag-like))))

(defn ask
  ([^ActorContext ctx
    ^RecipientRef target
    create-request-fn
    map-response
    timeout
    class-tag-like]
   (.ask ctx
         target
         (reify scala.Function1 (apply [_ reply-to]
                                  (create-request-fn reply-to)))
         (reify scala.Function1 (apply [_ try-res]
                                  (let [^scala.util.Try try-res try-res]
                                    (try (map-response (.get try-res) nil)
                                         (catch Throwable t
                                           (map-response nil t))))))
         (akka.util.Timeout/apply (scala/->scala.concurrent.duration.FiniteDuration timeout))
         (scala/->scala.reflect.ClassTag class-tag-like)))
  ([^ActorContext ctx
    ^RecipientRef target
    create-request-fn
    map-response
    timeout]
   (ask ctx
        target
        create-request-fn
        map-response
        timeout
        (scala.reflect.ClassTag/Any))))

(defn ask-with-status
  ([^ActorContext ctx
    ^RecipientRef target
    create-request-fn
    map-response
    timeout
    class-tag-like]
   (.askWithStatus ctx
                   target
                   (reify scala.Function1 (apply [_ reply-to] (create-request-fn reply-to)))
                   (reify scala.Function1 (apply [_ response] (map-response response)))
                   (akka.util.Timeout/apply (scala/->scala.concurrent.duration.FiniteDuration timeout))
                   (scala/->scala.reflect.ClassTag class-tag-like)))
  ([^ActorContext ctx
    ^RecipientRef target
    create-request-fn
    map-response
    timeout]
   (.askWithStatus ctx
                   target
                   (reify scala.Function1 (apply [_ reply-to] (create-request-fn reply-to)))
                   (reify scala.Function1 (apply [_ response] (map-response response)))
                   (akka.util.Timeout/apply (scala/->scala.concurrent.duration.FiniteDuration timeout))
                   (scala.reflect.ClassTag/Any))))

(defn pipe-to-self [^ActorContext ctx
                    ^scala.concurrent.Future future-val
                    map-result]
  (.pipeToSelf ctx future-val (reify scala.Function1 (apply [_ v] (map-result v)))))
