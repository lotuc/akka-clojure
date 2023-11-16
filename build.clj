(ns build
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.build.api :as b]
            [org.corfield.build :as bb]))

(def lib 'org.lotuc/akka-clojure)
(def version (format "1.0.%s" (b/git-count-revs nil)))
(def java-build-dir "target/build/src-java/classes")

(defn test "Run the tests." [opts]
  (bb/run-tests opts))

(defn compile-java [{:keys [basis]}]
  (println "Compiling Java to" java-build-dir "...")
  (let [basis (b/create-basis {:project "deps.edn"})]
    (->> {:src-dirs ["src-java"]
          :class-dir java-build-dir
          :basis basis}
         b/javac)))

(defn ci "Run the CI pipeline of tests (and build the JAR)." [opts]
  (-> opts
      (assoc :lib lib :version version)
      (bb/run-tests)
      (bb/clean)
      (bb/jar)))

(defn install "Install the JAR locally." [opts]
  (-> opts
      (assoc :lib lib :version version)
      (bb/install)))

(defn deploy "Deploy the JAR to Clojars." [opts]
  (-> opts
      (assoc :lib lib :version version)
      (bb/deploy)))
