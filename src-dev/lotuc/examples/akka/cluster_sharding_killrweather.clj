(ns lotuc.examples.akka.cluster-sharding-killrweather
  (:require
   [lotuc.akka.common.log :refer [slf4j-log]]
   [lotuc.akka.javadsl.actor :as javadsl.actor]
   [lotuc.akka.javadsl.actor.behaviors :as behaviors]
   [lotuc.akka.system :refer [create-system-from-config]])
  (:import
   (akka.actor.typed PostStop)
   (akka.cluster.sharding.typed.javadsl ClusterSharding Entity EntityTypeKey)))

;;; https://developer.lightbend.com/start/?group=akka&project=akka-samples-cluster-sharding-java

(defmacro info [ctx msg & args]
  `(slf4j-log (.getLog ~ctx) info ~msg ~@args))

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
            (.tell reply-to {:action :DataRecorded :wsid wsid})
            :same)
          (on-query [{:keys [reply-to func data-type]}]
            (let [data-for-type (filter #(= (:data-type %) data-type) @!values)
                  query-result (or (calc-result data-for-type func) [])]
              (.tell reply-to {:action :QueryResult
                               :wsid wsid
                               :data-type data-type
                               :function func
                               :readings (count query-result)
                               :value query-result})))]
    (cond
      (= action :Record) (on-record m)
      (= action :Query) (on-query m))))

(defn weather-station [wsid]
  (let [!values (atom [])]
    (behaviors/receive
     (fn [ctx {:keys [action] :as m}]
       (on-weather-station-message {:context ctx :wsid wsid :!values !values} m))
     (fn [ctx signal]
       (when (instance? PostStop signal)
         (info ctx "Stopping, losing all recorded state for station {}" wsid))
       :same))))

(def type-key (EntityTypeKey/create Object "WeatherStation"))

(defn weather-station-init-sharding [system]
  (.init (ClusterSharding/get system)
         (Entity/of type-key
                    (reify akka.japi.function.Function
                      (apply [_ entity-context]
                        (weather-station (.getEntityId entity-context)))))))

(defn killr-weather-guardian []
  (behaviors/setup
   (fn [ctx]
     (weather-station-init-sharding (.getSystem ctx))
     :empty)))

(defn startup [port]
  (create-system-from-config
   (killr-weather-guardian)
   "KillrWeather"
   "killr-weather.conf"
   {"akka.remote.artery.canonical.port" port}))

(defn- ask* [^akka.actor.typed.ActorSystem system wsid msg]
  (let [sharding (ClusterSharding/get system)
        timeout (.. system settings config
                    (getDuration "killrweather.routes.ask-timeout"))
        ref (.entityRefFor sharding type-key wsid)]
    (-> (javadsl.actor/ask ref
                           (fn [reply-to] (assoc msg :reply-to reply-to))
                           timeout
                           (.scheduler system))
        (.get))))

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

  (record-data s0 "abc" {:event-time (System/currentTimeMillis)
                         :data-type "temperature"
                         :value 30.0})
  (record-data s0 "abc" {:event-time (System/currentTimeMillis)
                         :data-type "temperature"
                         :value 20.0})
  (query s0 "abc" "temperature" :average)
  (query s0 "abc" "temperature" :current)
  (query s0 "abc" "temperature" :high-low)

  (query s1 "def" "temperature" :high-low)

  (do (.terminate s0)
      (.terminate s1)))
