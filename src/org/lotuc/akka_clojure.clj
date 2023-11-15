(ns org.lotuc.akka-clojure
  (:import
   (akka.actor.typed.javadsl Behaviors)
   (akka.actor.typed Behavior)))

(defn- ->behavior
  [v & {:keys [nil-behavior]
        :or {nil-behavior (Behaviors/same)}}]
  (cond
    (nil? v)
    nil-behavior

    (keyword? v)
    (case v
      :same    (Behaviors/same)
      :stopped (Behaviors/stopped)
      :empty   (Behaviors/empty)
      (throw (ex-info (str "unkown behavior: " v) {:value v})))

    (instance? Behavior v)
    v

    :else
    (throw (ex-info (str "invalid behavior: " (class v)) {:value v}))))

(defn setup [factory]
  (Behaviors/setup
   (reify akka.japi.function.Function
     (apply [_ ctx] (->behavior (factory ctx) :nil-behavior (Behaviors/empty))))))

(defn receive [on-message]
  (Behaviors/receive
   (reify akka.japi.function.Function2
     (apply [_ ctx msg] (->behavior (on-message ctx msg))))))

(defn receive-message [on-message]
  (Behaviors/receiveMessage
   (reify akka.japi.Function
     (apply [_ msg] (->behavior (on-message msg))))))

