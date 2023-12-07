(ns lotuc.akka.actor.scaladsl-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [lotuc.akka.actor.scaladsl :as dsl]
   [lotuc.akka.actor.typed.supervisor-strategy]
   [lotuc.akka.cnv :as cnv])
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

(defn- echo-behavior-fn [{:keys [reply-to msg]}]
  (.tell reply-to msg)
  :same)

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
