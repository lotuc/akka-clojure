(ns lotuc.akka.common.scala)

(defmacro ->scala.function [arity f]
  (let [scalaf (symbol (str "scala.Function" arity))
        args (map (fn [i] (gensym (str "arg" i "-"))) (range arity))]
    `(reify ~scalaf (apply [_ ~@args] (~f ~@args)))))

(defmacro ->scala.reflect.ClassTag [v]
  (cond
    (= v Object)
    `scala.reflect.ClassTag/Object

    :else
    `(let [v# ~v]
       (cond
         (instance? scala.reflect.ClassTag v#) v#
         (= java.lang.Class (type v#)) (scala.reflect.ClassTag/apply v#)))))

(defmacro ->scala.collection.immutable.Map [v]
  `(reduce
    (fn [r# [k# v#]] (.updated r# k# v#))
    (scala.collection.immutable.HashMap.)
    ~v))

(defn ->scala.concurrent.duration.FiniteDuration
  ^scala.concurrent.duration.FiniteDuration [duration]
  (cond
    (instance? java.time.Duration duration)
    (scala.concurrent.duration.FiniteDuration/apply
     (.toNanos ^java.time.Duration duration) java.util.concurrent.TimeUnit/NANOSECONDS)

    (string? duration)
    (scala.concurrent.duration.FiniteDuration/create ^String duration)

    (or (keyword? duration) (symbol? duration))
    (scala.concurrent.duration.FiniteDuration/create ^String (name duration))

    (instance? scala.concurrent.duration.FiniteDuration duration)
    duration

    :else
    (throw (ex-info (str "illegal FiniteDuration: " (type duration))
                    {:value duration}))))

(comment
  (.apply (->scala.function 1 (fn [x] (+ x x))) 1)
  (->scala.reflect.ClassTag Object)

  (->scala.collection.immutable.Map {:a 2})
  (->scala.collection.immutable.Map [[:a 2]])

  (->scala.concurrent.duration.FiniteDuration "1.sec")
  (->scala.concurrent.duration.FiniteDuration (java.time.Duration/ofSeconds 1)))
