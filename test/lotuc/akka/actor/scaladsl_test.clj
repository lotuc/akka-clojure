(ns lotuc.akka.actor.scaladsl-test
  (:require [clojure.test :refer :all]
            [lotuc.akka.actor.scaladsl :as dsl]
            [lotuc.akka.actor.typed.supervisor-strategy]
            [lotuc.akka.cnv :as cnv])
  (:import (akka.actor.testkit.typed.scaladsl ActorTestKit)))

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

(defn behavior-on-message-factory-test [factory-fn]
  (let [behavior (factory-fn
                  (fn [{:keys [action reply-to n]}]
                    (case action
                      :inc (.tell reply-to (inc n))
                      :dec (.tell reply-to (dec n))
                      (.tell reply-to n))
                    :same))
        actor (.spawn *test-kit* behavior)]
    (.tell actor {:reply-to *self* :action :inc :n 41})
    (is (.expectMessage *probe* 42))
    (.tell actor {:reply-to *self* :action :dec :n 43})
    (is (.expectMessage *probe* 42))
    (.tell actor {:reply-to *self* :action :unmatched :n 42})
    (is (.expectMessage *probe* 42))))

(defn behavior-on-signal-factory-test [factory-fn]
  (let [!signal (promise)
        signal-behavior (factory-fn
                         (fn [signal]
                           (deliver !signal (:signal (cnv/->clj signal)))
                           :same))]
    (.spawn
     *test-kit*
     (dsl/setup
      (fn [ctx]
        (let [a (.spawnAnonymous ctx (dsl/receive-message (fn [_] (/ 1 0))))]
          (.watch ctx a)
          (future (.tell a :throw)))
        signal-behavior)))
    (is (= :terminated (deref !signal 1000 ::timeout)))))

(deftest behavior-factories-test
  (testing "message handlers"
    (testing "receive"
      (behavior-on-message-factory-test (fn [on-msg] (dsl/receive (fn [_ctx msg] (on-msg msg))))))
    (testing "receive-message"
      (behavior-on-message-factory-test dsl/receive-message))
    (testing "receive-partial"
      (behavior-on-message-factory-test (fn [on-msg] (dsl/receive-partial (fn [_ctx msg] (on-msg msg))))))
    (testing "receive-message-partial"
      (behavior-on-message-factory-test (fn [on-msg] (dsl/receive-message-partial (fn [_ctx msg] (on-msg msg)))))))

  (testing "signal handlers"
    (testing "receive"
      (behavior-on-signal-factory-test
       (fn [on-signal] (dsl/receive (fn [_ctx _] :same) (fn [_ctx signal] (on-signal signal))))))
    (testing "receive-partial"
      (behavior-on-signal-factory-test
       (fn [on-signal] (dsl/receive-signal (fn [_ signal] (on-signal signal))))))))
