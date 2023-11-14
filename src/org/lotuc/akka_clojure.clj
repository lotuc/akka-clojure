(ns org.lotuc.akka-clojure
  (:import
   (akka.actor.typed ActorSystem)
   (org.lotuc ClojureBehavior)
   (org.lotuc ClojureBehavior$Message)
   (akka.actor.typed.javadsl AskPattern)))

;;; available in sync-setup & message-handler
(def ^:dynamic *context* nil)
;;; available in message-handler
(def ^:dynamic *reply-to* nil)

(defmacro check-context []
  `(when-not *context*
     (throw (RuntimeException. "not behavior context - scheduler required"))))

(defmacro !
  ([msg]
   `(when *reply-to* (! *reply-to* ~msg)))
  ([target msg]
   `(! ~target (when *context* (.self *context*)) ~msg))
  ([target reply-to msg]
   `(.tell ~target (ClojureBehavior$Message. ~msg ~reply-to))))

(defn self []
  (check-context)
  (.self *context*))

(defn scheduler []
  (check-context)
  (.. *context* getSystem scheduler))

(defn tell [actor msg]
  (! actor msg))

(defmacro spawn
  [behavior n]
  `(do (check-context)
       (.spawn *context* ~behavior ~n)))

(defmacro make-behavior
  ([message-handler] `(ClojureBehavior/create ~message-handler))
  ([message-handler sync-setup] `(ClojureBehavior/create ~message-handler ~sync-setup)))

(defn actor-system [guardian-behavior name]
  (ActorSystem/create guardian-behavior name))

(defn ask
  ([actor msg duration scheduler]
   (future
     (-> (AskPattern/ask
          actor
          (reify akka.japi.function.Function
            (apply [_ reply]
              (ClojureBehavior$Message. msg reply)))
          duration
          scheduler)
         (.toCompletableFuture)
         (.get)
         (.content))))
  ([actor msg duration]
   (let [s (scheduler)]
     (ask actor msg duration s))))
