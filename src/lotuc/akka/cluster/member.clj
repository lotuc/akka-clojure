(ns lotuc.akka.cluster.member
  (:import
   (akka.cluster Member)))

(set! *warn-on-reflection* true)

(defn has-role [^Member member ^String role]
  (.hasRole member role))

(defn address [^Member member]
  (.address member))
