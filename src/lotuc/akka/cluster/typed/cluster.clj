(ns lotuc.akka.cluster.typed.cluster
  (:import
   (akka.cluster Member)
   (akka.cluster.typed Cluster Subscribe)
   (akka.cluster ClusterEvent$LeaderChanged
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

(defn get-cluster ^Cluster [system]
  (Cluster/get system))

(defn subscriptions [^Cluster cluster]
  (.subscriptions cluster))

(defn self-member ^Member [^Cluster cluster]
  (.selfMember cluster))

(def ^:private event-classes
  {:leader-changed ClusterEvent$LeaderChanged
   :role-leader-changed ClusterEvent$RoleLeaderChanged
   :shutting-down ClusterEvent$ClusterShuttingDown$

   :member-event ClusterEvent$MemberEvent
   :member-joined ClusterEvent$MemberJoined
   :member-weakly-up ClusterEvent$MemberWeaklyUp
   :member-up ClusterEvent$MemberUp
   :member-left ClusterEvent$MemberLeft
   :member-preparing-for-shutdown ClusterEvent$MemberPreparingForShutdown
   :member-ready-for-shutdown ClusterEvent$MemberReadyForShutdown
   :member-exited ClusterEvent$MemberExited
   :member-downed ClusterEvent$MemberDowned
   :member-removed ClusterEvent$MemberRemoved

   :member-rechability-event ClusterEvent$ReachabilityEvent
   :member-reachable ClusterEvent$ReachableMember
   :member-unreachable ClusterEvent$UnreachableMember

   :dc-rechability-event ClusterEvent$DataCenterReachabilityEvent
   :dc-reachable ClusterEvent$ReachableDataCenter
   :dc-unreachable ClusterEvent$UnreachableDataCenter})

(defn create-subscribe
  [subscriber-actor event-class]
  (if-some [clazz (event-classes event-class)]
    (Subscribe/create subscriber-actor clazz)
    (Subscribe/create subscriber-actor event-class)))
