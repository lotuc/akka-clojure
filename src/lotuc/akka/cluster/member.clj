(ns lotuc.akka.cluster.member
  (:import
   (akka.actor Address)
   (akka.cluster Member)))

(set! *warn-on-reflection* true)

(defn has-role [^Member member ^String role]
  (.hasRole member role))

(defn address ^Address [^Member member]
  (.address member))
