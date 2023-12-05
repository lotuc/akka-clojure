(ns lotuc.examples.akka.cluster-sharding-killrweather
  (:require
   [lotuc.akka.actor.scaladsl :as dsl]
   [lotuc.akka.actor.typed.actor-ref :as actor-ref]
   [lotuc.akka.actor.typed.actor-system :as actor-system]
   [lotuc.akka.actor.typed.scaladsl.ask-pattern :as scaladsl.ask-pattern]
   [lotuc.akka.cluster.sharding.typed.scaladsl.cluster-sharding :as dsl.cluster-sharding]
   [lotuc.akka.cnv :as cnv]
   [lotuc.akka.common.slf4j :refer [slf4j-log]]))

;;; https://developer.lightbend.com/start/?group=akka&project=akka-samples-cluster-sharding-java

(set! *warn-on-reflection* true)

(defmacro info [ctx msg & args]
  `(slf4j-log (dsl/log ~ctx) info ~msg ~@args))

(defn- calc-result [data func]
  (when (seq data)
    (case func
      :average
      (let [start (-> data first :event-time)
            end (-> data last :event-time)
            v (/ (->> data (map :value) (reduce +)) (double (count data)))]
        [{:action :TimeWindow :start start :end end :value v}])

      :high-low
      (let [min-d (reduce (fn [a b] (if (< (:value a) (:value b)) a b)) data)
            max-d (reduce (fn [a b] (if (> (:value a) (:value b)) a b)) data)]
        [{:action :TimeWindow :start (:event-time min-d) :end (:event-time min-d) :value (:value min-d)}
         {:action :TimeWindow :start (:event-time max-d) :end (:event-time max-d) :value (:value max-d)}])

      :current
      (let [v (last data)]
        [{:action :TimeWindow :start (:event-time v) :end (:event-time v) :value (:value v)}])

      (throw (IllegalArgumentException. (str "Unkown operation: " func))))))

(defn- on-weather-station-message
  [{:keys [context wsid !values]} {:keys [action] :as m}]
  (letfn [(on-record [{:keys [reply-to data processing-timestamp]}]
            (swap! !values conj data)
            (let [data-for-same-type (filter #(= (:data-type %) (:data-type data)) @!values)
                  avg (/ (->> data-for-same-type (map :value) (reduce +)) (double (count data-for-same-type)))]
              (info context "{} total readings from station {}, type {}, average {}, diff: processingTime - eventTime: {} ms"
                    (count @!values) wsid (:data-type data) avg
                    (- processing-timestamp (:event-time data))))
            (actor-ref/tell reply-to {:action :DataRecorded :wsid wsid})
            :same)
          (on-query [{:keys [reply-to func data-type]}]
            (let [data-for-type (filter #(= (:data-type %) data-type) @!values)
                  query-result (or (calc-result data-for-type func) [])]
              (actor-ref/tell reply-to {:action :QueryResult
                                        :wsid wsid
                                        :data-type data-type
                                        :function func
                                        :readings (count query-result)
                                        :value query-result}))
            :same)]
    (cond
      (= action :Record) (on-record m)
      (= action :Query) (on-query m))))

(defn weather-station [wsid]
  (let [!values (atom [])]
    (dsl/receive
     (fn [ctx {:keys [action] :as m}]
       (on-weather-station-message {:context ctx :wsid wsid :!values !values} m))
     (fn [ctx signal-object]
       (let [{:keys [signal] :as v} (cnv/->clj signal-object)]
         (if (= signal :post-stop)
           (info ctx "Stopping, losing all recorded state for station {}" wsid)
           (info ctx "{}: recv signal {} {}" wsid signal signal-object v)))
       :same))))

(def type-key "WeatherStation")

(defn weather-station-init-sharding [system]
  (.init (dsl.cluster-sharding/get-cluster-sharding system)
         (dsl.cluster-sharding/create-entity
          type-key (fn [{:keys [entity-id]}]
                     (weather-station entity-id)))))

(defn killr-weather-guardian []
  (dsl/setup
   (fn [ctx]
     (weather-station-init-sharding (dsl/system ctx))
     :empty)))

(defn startup [port]
  (actor-system/create-system-from-config
   (killr-weather-guardian)
   "KillrWeather"
   "killr-weather.conf"
   {"akka.remote.artery.canonical.port" port}))

(defn- ask* [^akka.actor.typed.ActorSystem system wsid msg]
  (let [sharding (dsl.cluster-sharding/get-cluster-sharding system)
        timeout (.. system settings config
                    (getDuration "killrweather.routes.ask-timeout"))
        ref (dsl.cluster-sharding/entity-ref-for sharding type-key wsid)]
    (scaladsl.ask-pattern/ask ref
                              (fn [reply-to] (assoc msg :reply-to reply-to))
                              timeout
                              (.scheduler system))))

(defn record-data
  [system wsid data]
  (ask* system wsid {:action :Record
                     :data data
                     :processing-timestamp (System/currentTimeMillis)}))

(defn query [system wsid data-type func]
  (ask* system wsid {:action :Query
                     :data-type data-type
                     :func func}))

(comment
  (do (def s0 (startup 2553))
      (def s1 (startup 2554)))

  @(record-data s0 "abc" {:event-time (System/currentTimeMillis)
                          :data-type "temperature"
                          :value 30.0})
  @(record-data s0 "abc" {:event-time (System/currentTimeMillis)
                          :data-type "temperature"
                          :value 20.0})
  @(query s0 "abc" "temperature" :average)
  @(query s0 "abc" "temperature" :current)
  @(query s0 "abc" "temperature" :high-low)
  @(query s1 "def" "temperature" :high-low)

  (do (.terminate s0)
      (.terminate s1)))
