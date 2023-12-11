(ns lotuc.akka.actor.typed.receptionist-test
  (:require
   [clojure.test :refer :all]
   [lotuc.akka.actor.scaladsl :as dsl]
   [lotuc.akka.actor.typed.receptionist :as receptionist])
  (:import
   (akka.actor.testkit.typed.scaladsl ActorTestKit)))

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

(deftest receptionist-test
  (let [service-key "echo-service"
        probe-ref *self*
        echo-service
        (dsl/receive
         (fn [ctx {:keys [action] :as m}]
           (case action
             :register-self
             (.. ctx system receptionist
                 (tell (receptionist/register-service service-key (.. ctx self))))

             :echo
             (.tell (:reply-to m) {:action :echo :msg (:msg m)}))
           :same))
        echo-service-hub
        (dsl/setup
         (let [!services (atom [])]
           (fn [context]
             (.. context system receptionist
                 (tell (receptionist/subscribe-service service-key (.. context self))))

             (dsl/receive-message
              (fn [m]
                (if (receptionist/receptionist-listing? m)
                  (let [s (into [] (receptionist/receptionist-listing-get-service-instances m service-key))]
                    (reset! !services s)
                    ;; tell probe-ref for testing
                    (.tell probe-ref {:msg-type :subscribe-result :service-instances s}))
                  (let [{:keys [action]} m]
                    (case action
                      ;; ask-for-listing -> reply-ask-for-listing
                      :ask-for-listing
                      (dsl/ask context
                               (.. context system receptionist)
                               (fn [reply-to]
                                 (receptionist/find-service service-key reply-to))
                               (fn [listing _]
                                 {:action :reply-ask-for-listing
                                  :reply-to (:reply-to m)
                                  :service-instances (into [] (receptionist/receptionist-listing-get-service-instances listing service-key))})
                               "100.ms")
                      :reply-ask-for-listing
                      (let [s (:service-instances m)]
                        (reset! !services s)
                        (.tell (:reply-to m) {:msg-type :ask-result :service-instances s}))

                      ;; ask-echo -> echo
                      :ask-echo
                      (let [target (first @!services)]
                        (.tell target {:action :echo :reply-to (.. context self) :msg (:msg m)}))
                      :echo
                      ;; tell probe-ref for testing
                      (.tell probe-ref [:echo (:msg m)]))))

                :same)))))

        echo-service-hub-actor (.spawn *test-kit* echo-service-hub)]

    (is (= {:msg-type :subscribe-result :service-instances []}
           (.receiveMessage *probe*)))

    (.tell echo-service-hub-actor {:action :ask-for-listing :reply-to *self*})
    (is (= {:msg-type :ask-result :service-instances []}
           (.receiveMessage *probe*)))

    (let [echo-actor (.spawn *test-kit* echo-service)]
      (.tell echo-actor {:action :register-self})
      (let [{:keys [msg-type service-instances]} (.receiveMessage *probe*)]
        (is (= msg-type :subscribe-result))
        (is (= 1 (count service-instances))))

      (.tell echo-service-hub-actor {:action :ask-for-listing :reply-to *self*})
      (let [{:keys [msg-type service-instances]} (.receiveMessage *probe*)]
        (is (= msg-type :ask-result))
        (is (= 1 (count service-instances))))

      (.tell echo-service-hub-actor {:action :ask-echo :msg "hello world"})
      (is (= [:echo "hello world"] (.receiveMessage *probe*))))))
