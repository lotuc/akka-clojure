(ns lotuc.akka.cluster.cluster-event
  (:require
   [lotuc.akka.cnv :as cnv])
  (:import
   (akka.cluster ClusterEvent$ClusterDomainEvent

                 ClusterEvent$LeaderChanged
                 ClusterEvent$RoleLeaderChanged
                 ClusterEvent$ClusterShuttingDown$

                 ClusterEvent$MemberEvent
                 ClusterEvent$MemberJoined
                 ClusterEvent$MemberWeaklyUp
                 ClusterEvent$MemberUp
                 ClusterEvent$MemberLeft
                 ClusterEvent$MemberPreparingForShutdown
                 ClusterEvent$MemberReadyForShutdown
                 ClusterEvent$MemberExited
                 ClusterEvent$MemberDowned
                 ClusterEvent$MemberRemoved

                 ClusterEvent$ReachabilityEvent
                 ClusterEvent$UnreachableMember
                 ClusterEvent$ReachableMember

                 ClusterEvent$DataCenterReachabilityEvent
                 ClusterEvent$UnreachableDataCenter
                 ClusterEvent$ReachableDataCenter)))

(set! *warn-on-reflection* true)

(defn cluster-event? [ev] (instance? ClusterEvent$ClusterDomainEvent ev))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; ClusterEvent

(defmethod cnv/->clj ClusterEvent$LeaderChanged [^ClusterEvent$LeaderChanged v]
  {:dtype :cluster-event :ev-type :leader-changed :leader (.leader v)})

(defmethod cnv/->clj ClusterEvent$RoleLeaderChanged [^ClusterEvent$RoleLeaderChanged v]
  {:dtype :cluster-event :ev-type :role-leader-changed :role (.role v) :leader (.leader v)})

(defmethod cnv/->clj ClusterEvent$ClusterShuttingDown$ [^ClusterEvent$ClusterShuttingDown$ _]
  {:dtype :cluster-event :ev-type :shutting-down})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; ReachabilityEvent

(defn member-reachability-event? [ev] (instance? ClusterEvent$ReachabilityEvent ev))

(defmethod cnv/->clj ClusterEvent$UnreachableMember [^ClusterEvent$UnreachableMember v]
  {:dtype :cluster-event :ev-type :member-unreachable :member (.member v)})

(defmethod cnv/->clj ClusterEvent$ReachableMember [^ClusterEvent$ReachableMember v]
  {:dtype :cluster-event :ev-type :member-reachable :member (.member v)})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; DataCenterReachabilityEvent

(defn dc-reachability-event? [ev] (instance? ClusterEvent$DataCenterReachabilityEvent ev))

(defmethod cnv/->clj ClusterEvent$UnreachableDataCenter [^ClusterEvent$UnreachableDataCenter v]
  {:dtype :cluster-event :ev-type :dc-unreachable :data-center (.dataCenter v)})

(defmethod cnv/->clj ClusterEvent$ReachableDataCenter [^ClusterEvent$ReachableDataCenter v]
  {:dtype :cluster-event :ev-type :dc-reachable :data-center (.dataCenter v)})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; MemberEvent

(defn member-event? [ev] (instance? ClusterEvent$MemberEvent ev))

(defmethod cnv/->clj ClusterEvent$MemberJoined [^ClusterEvent$MemberJoined v]
  {:dtype :cluster-event :ev-type :member-joined :member (.member v)})

(defmethod cnv/->clj ClusterEvent$MemberWeaklyUp [^ClusterEvent$MemberWeaklyUp v]
  {:dtype :cluster-event :ev-type :member-weakly-up :member (.member v)})

(defmethod cnv/->clj ClusterEvent$MemberUp [^ClusterEvent$MemberUp v]
  {:dtype :cluster-event :ev-type :member-up :member (.member v)})

(defmethod cnv/->clj ClusterEvent$MemberLeft [^ClusterEvent$MemberLeft v]
  {:dtype :cluster-event :ev-type :member-left :member (.member v)})

(defmethod cnv/->clj ClusterEvent$MemberPreparingForShutdown [^ClusterEvent$MemberPreparingForShutdown v]
  {:dtype :cluster-event :ev-type :member-preparing-for-shutdown :member (.member v)})

(defmethod cnv/->clj ClusterEvent$MemberReadyForShutdown [^ClusterEvent$MemberReadyForShutdown v]
  {:dtype :cluster-event :ev-type :member-ready-for-shutdown :member (.member v)})

(defmethod cnv/->clj ClusterEvent$MemberExited [^ClusterEvent$MemberExited v]
  {:dtype :cluster-event :ev-type :member-exited :member (.member v)})

(defmethod cnv/->clj ClusterEvent$MemberDowned [^ClusterEvent$MemberDowned v]
  {:dtype :cluster-event :ev-type :member-downed :member (.member v)})

(defmethod cnv/->clj ClusterEvent$MemberRemoved [^ClusterEvent$MemberRemoved v]
  {:dtype :cluster-event :ev-type :member-removed :member (.member v)
   :previous-status (.previousStatus v)})
