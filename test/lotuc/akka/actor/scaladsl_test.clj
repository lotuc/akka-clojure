(ns lotuc.akka.actor.scaladsl-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [lotuc.akka.actor.scaladsl :as dsl]
   [lotuc.akka.actor.typed.supervisor-strategy]
   [lotuc.akka.cnv :as cnv]
   [lotuc.akka.common.scala :as scala])
  (:import
   (akka.actor.testkit.typed.scaladsl ActorTestKit)
   (akka.actor.typed Behavior)))

(def ^:dynamic *test-kit* nil)
(def ^:dynamic *probe* nil)
(def ^:dynamic *self* nil)

(defn setup-test-kit [f]
  (let [test-kit (ActorTestKit/apply)
        probe (.createTestProbe test-kit)
        self (.ref probe)]
    (binding [*test-kit* test-kit *probe* probe *self* self]
      (f)
      (.shutdownTestKit test-kit))))

(use-fixtures :each setup-test-kit)

(defn- duration [v]
  (scala/->scala.concurrent.duration.FiniteDuration v))

(defn- echo-behavior-fn [{:keys [reply-to msg]}]
  (.tell reply-to msg)
  :same)

(defn- echo-behavior []
  (dsl/receive-message echo-behavior-fn))

(defn- logged-echo-behavior []
  (dsl/receive (fn [ctx {:keys [reply-to msg]}]
                 (.info (.log ctx) "echo: {}" msg)
                 (.tell reply-to msg)
                 :same)))

(defn- constantly-stop-behavior []
  (dsl/receive-message (fn [_] :stopped)))

(defn behavior-on-message-factory-test [factory-fn]
  (let [behavior (factory-fn echo-behavior-fn)
        actor (.spawn *test-kit* behavior)]
    (doseq [i (range 3)
            :let [msg (str "msg-" i)]]
      (.tell actor {:reply-to *self* :msg msg})
      (is (.expectMessage *probe* msg)))))

(defn behavior-on-signal-factory-test [factory-fn]
  (let [!signal (promise)]
    (.spawn
     *test-kit*
     (dsl/setup
      (fn [ctx]
        (let [a (.spawnAnonymous ctx (constantly-stop-behavior))]
          (.watch ctx a)
          (.tell a :anything))
        (factory-fn
         (fn [signal]
           (deliver !signal (:signal (cnv/->clj signal)))
           :same)))))
    (is (= :terminated (deref !signal 1000 ::timeout)))))

(deftest ->behavior-test
  (testing "convert common keyword behavior"
    (doseq [n [:same :stopped :empty :ignore :unhandled]]
      (testing (str "keyword: " n)
        (let [actual (dsl/->behavior n)]
          (is (str/includes? (str/lower-case (str actual)) (name n))
              (str "actual string repr: " (str actual))))))
    (testing "invalid keyword behavior"
      (is (thrown-with-msg? Exception #":invalid-keyword"
                            (dsl/->behavior :invalid-keyword))))
    (testing "return behavior object as is"
      (is (= (akka.actor.typed.scaladsl.Behaviors/same)
             (dsl/->behavior (akka.actor.typed.scaladsl.Behaviors/same)))))

    (testing "stopped behavior"
      (testing "with no post-stop action"
        (testing (= (dsl/stopped) (dsl/->behavior :stopped))))
      (testing "with post-stop action"
        (is (instance? Behavior (dsl/stopped (fn [] "some action"))))))))

(deftest behavior-factories-test
  (testing "message handlers"
    (testing "receive"
      (behavior-on-message-factory-test (fn [on-msg] (dsl/receive (fn [_ctx msg] (on-msg msg))))))
    (testing "receive-message"
      (behavior-on-message-factory-test dsl/receive-message))
    (testing "receive-partial"
      (behavior-on-message-factory-test (fn [on-msg] (dsl/receive-partial (fn [_ctx msg] (on-msg msg))))))
    (testing "receive-message-partial"
      (behavior-on-message-factory-test dsl/receive-message-partial)))

  (testing "signal handlers"
    (testing "receive"
      (behavior-on-signal-factory-test
       (fn [on-signal] (dsl/receive (fn [_ctx _] :same) (fn [_ctx signal] (on-signal signal))))))
    (testing "receive-partial"
      (behavior-on-signal-factory-test
       (fn [on-signal] (dsl/receive-signal (fn [_ signal] (on-signal signal))))))))

(deftest log-messages-test
  (testing "log messages"
    (let [echo (dsl/log-messages (echo-behavior) {:enabled true :level :info})
          echo-actor (.spawn *test-kit* echo)
          msg "hello world"]
      (.tell echo-actor {:reply-to *self* :msg msg})
      (is (.expectMessage *probe* msg)))))

(deftest with-mdc-test
  (let [static-mdc {"mdc.v0" "static"}
        dynamic-mdc (fn [{:keys [msg]}] {"mdc.v1" (str "!dynamic-<" (subs msg 0 10) "...>!")})]
    (let [msg "with-static-mdc"]
      (testing msg
        (let [echo (dsl/with-static-mdc (logged-echo-behavior) static-mdc)
              echo-actor (.spawn *test-kit* echo)]
          (.tell echo-actor {:reply-to *self* :msg msg})
          (is (.expectMessage *probe* msg)))))
    (let [msg "with-dynamic-mdc"]
      (testing msg
        (let [echo (dsl/with-dynamic-mdc (logged-echo-behavior) dynamic-mdc)
              echo-actor (.spawn *test-kit* echo)]
          (.tell echo-actor {:reply-to *self* :msg msg})
          (is (.expectMessage *probe* msg)))))
    (testing "with-mdc"
      (doseq [[static-mdc' dynamic-mdc']
              [[nil nil]
               [static-mdc nil]
               [nil dynamic-mdc]
               [static-mdc dynamic-mdc]]]
        (let [msg (format "with-mdc - [static-mdc %s] [dynamic-mdc %s]"
                          (if (nil? static-mdc') "no" "yes")
                          (if (nil? dynamic-mdc') "no" "yes"))]
          (testing msg
            (let [echo (dsl/with-mdc (logged-echo-behavior) static-mdc' dynamic-mdc')
                  echo-actor (.spawn *test-kit* echo)]
              (.tell echo-actor {:reply-to *self* :msg msg})
              (is (.expectMessage *probe* msg)))))))))

(deftest with-timers-test
  (testing "with-timer-single"
    (doseq [single ["50.ms" {:delay "50.ms"}]]
      (testing (str "single delay representation - " (pr-str single))
        (let [self *self*]
          (.spawn *test-kit*
                  (dsl/with-timers
                    (fn [timers]
                      (dsl/start-timer timers {:msg :v0 :single single})
                      (dsl/receive-message (fn [m] (.tell self m) :same)))))
          (.expectNoMessage *probe* (duration "50.ms"))
          (is (.expectMessage *probe* :v0))
          (.expectNoMessage *probe* (duration "50.ms"))))))

  (testing "with-single-fix-delay"
    (testing "fix-delay (no explicit initial-delay)"
      (doseq [fix-delay ["50.ms" {:delay "50.ms"}]]
        (testing (str "representation - " (pr-str fix-delay))
          (let [self *self* c (atom 0)]
            (.spawn *test-kit*
                    (dsl/with-timers
                      (fn [timers]
                        (dsl/start-timer timers {:msg :v0 :fix-delay fix-delay})
                        (dsl/receive-message (fn [m]
                                               (when (= (swap! c inc) 2)
                                                 (dsl/cancel-all-timer timers))
                                               (.tell self m)
                                               :same)))))
            ;; the default initial delay
            (.expectNoMessage *probe* (duration "40.ms"))
            (is (.expectMessage *probe* (duration "80.ms") :v0))
            (is (.expectMessage *probe* (duration "80.ms") :v0))
            (.expectNoMessage *probe* (duration "80.ms"))))))
    (testing "fix-delay (with explicit initial-delay)"
      (let [self *self* c (atom 0)]
        (.spawn *test-kit*
                (dsl/with-timers
                  (fn [timers]
                    (dsl/start-timer timers {:msg :v0 :fix-delay {:delay "50.ms"
                                                                  :initial-delay "10.ms"}})
                    (dsl/receive-message (fn [m]
                                           (when (= (swap! c inc) 3)
                                             (dsl/cancel-all-timer timers))
                                           (.tell self m)
                                           :same)))))
        ;; initial-delay is 10.ms & delay is 50.ms
        ;; should receive first message between 10ms-60ms
        (is (.expectMessage *probe* (duration "50.ms") :v0))
        (is (.expectMessage *probe* (duration "80.ms") :v0))
        (is (.expectMessage *probe* (duration "80.ms") :v0))
        (.expectNoMessage *probe* (duration "80.ms")))))

  (testing "with-single-fix-rate"
    (testing "fix-rate (no explicit initial-delay)"
      (doseq [fix-rate ["50.ms" {:interval "50.ms"}]]
        (testing (str "representation - " (pr-str fix-rate))
          (let [self *self* c (atom 0)]
            (.spawn *test-kit*
                    (dsl/with-timers
                      (fn [timers]
                        (dsl/start-timer timers {:msg :v0 :fix-rate fix-rate})
                        (dsl/receive-message (fn [m]
                                               (when (= (swap! c inc) 2)
                                                 (dsl/cancel-all-timer timers))
                                               (.tell self m)
                                               :same)))))
            ;; the default initial delay
            (.expectNoMessage *probe* (duration "40.ms"))
            (is (.expectMessage *probe* (duration "80.ms") :v0))
            (is (.expectMessage *probe* (duration "80.ms") :v0))
            (.expectNoMessage *probe* (duration "80.ms"))))))
    (testing "fix-rate (with explicit initial-delay)"
      (let [self *self* c (atom 0)]
        (.spawn *test-kit*
                (dsl/with-timers
                  (fn [timers]
                    (dsl/start-timer timers {:msg :v0 :fix-rate {:interval "50.ms"
                                                                 :initial-delay "10.ms"}})
                    (dsl/receive-message (fn [m]
                                           (when (= (swap! c inc) 3)
                                             (dsl/cancel-all-timer timers))
                                           (.tell self m)
                                           :same)))))
        ;; initial-delay is 10.ms & delay is 50.ms
        ;; should receive first message between 10ms-60ms
        (is (.expectMessage *probe* (duration "50.ms") :v0))
        (is (.expectMessage *probe* (duration "80.ms") :v0))
        (is (.expectMessage *probe* (duration "80.ms") :v0))
        (.expectNoMessage *probe* (duration "80.ms"))))))

(deftest with-stash-test
  (testing "with-statsh"
    (let [self *self*
          actor (.spawn *test-kit*
                        (letfn [(echo-42 [stash]
                                  (let [!count (atom 0)]
                                    (dsl/receive-message
                                     (fn [[from msg :as m]]
                                       (if (= from 42)
                                         (do (.tell self [:echo-42 msg])
                                             (if (< (swap! !count inc) 2)
                                               :same
                                               (.unstashAll stash (echo-others))))
                                         (do (.stash stash m) :same))))))
                                (echo-others []
                                  (dsl/receive-message
                                   (fn [[from msg]]
                                     (.tell self [:echo-others from msg])
                                     :same)))]
                          (dsl/with-stash 100 echo-42)))]
      (.tell actor [42 "hello"])
      (is (= [:echo-42 "hello"] (.receiveMessage *probe*)))

      (.tell actor [41 "hello"])
      (.tell actor [43 "world"])

      (.tell actor [42 "world"])
      (is (= [:echo-42 "world"] (.receiveMessage *probe*)))

      (is (= [:echo-others 41 "hello"] (.receiveMessage *probe*)))
      (is (= [:echo-others 43 "world"] (.receiveMessage *probe*))))))

(deftest monitor-test
  (let [self *self*]
    (letfn [(ping-behavior []
              (dsl/receive-message
               (fn [m] (.tell self [:from-ping m]) (pong-behavior))))
            (pong-behavior []
              (dsl/receive-message
               (fn [m] (.tell self [:from-pong m]) (ping-behavior))))
            (monitor-behavior []
              (dsl/receive-message
               (fn [m] (.tell self [:from-monitor m]) :same)))]
      (let [monitor (.spawn *test-kit* (monitor-behavior))
            ping-pong (.spawn *test-kit* (dsl/monitor (ping-behavior) monitor))]
        (.tell ping-pong "hello")
        (let [m0 (.receiveMessage *probe*)
              m1 (.receiveMessage *probe*)]
          (is (= #{[:from-monitor "hello"] [:from-ping "hello"]} #{m0 m1})
              "from monitor & ping"))

        (.tell ping-pong "world")
        (let [m0 (.receiveMessage *probe*)
              m1 (.receiveMessage *probe*)]
          (is (= #{[:from-monitor "world"] [:from-pong "world"]} #{m0 m1})
              "from monitor & pong"))))))

(deftest supervise-test
  (testing "supervise"
    (doseq [strategy [nil :restart :resume :stop
                      {:strategy :restart}
                      {:strategy :resume}
                      {:strategy :stop}
                      {:strategy :backoff
                       :min "50.ms"
                       :max "60.ms"
                       :random-factor 0.2}]]
      (testing (str "restart=" strategy)
        (let [a0 (.spawn *test-kit* (cond-> (dsl/receive-message
                                             (fn [{:keys [reply-to n]}]
                                               (.tell reply-to (/ 42 n))
                                               :same))
                                      strategy (dsl/supervise strategy)))]
          (.tell a0 {:reply-to *self* :n 2})
          (is (= 21 (.receiveMessage *probe*)))

         ;; stopped
          (.tell a0 {:reply-to *self* :n 0})
          (.tell a0 {:reply-to *self* :n 2})
          (cond
            (#{:restart :resume} (cond-> strategy (not (keyword? strategy)) :strategy))
            (is (= 21 (.receiveMessage *probe*)))

            (or (nil? strategy) (= strategy :stop))
            (.expectNoMessage *probe*)

            (= :backoff (:strategy strategy))
            (do
              (.expectNoMessage *probe* (duration "30.ms"))
              (is (= 21 (.receiveMessage *probe*))))))))))
