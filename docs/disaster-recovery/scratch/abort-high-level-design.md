# Abort: high-level design

Abort is the customer's escape hatch from an in-progress switchover. The switchover has been initiated, the former primary is fenced, the secondary is draining, and the customer decides — for any reason — not to complete the swap.

The central design choice: **abort tears down the replication relationship entirely.** Post-abort, the two clusters are independent. Recovery is re-establishment of the relationship from scratch, using the same path the customer used to set it up originally.

## Why this framing

Alternatives considered — "unfence and resume the former primary while keeping the relationship intact," or "escalate to failover in the same code path" — all require a live cross-cluster state machine that has to reason about whether the other side has been promoted yet, whether workers are still running, whether retention leases can be reused. Tearing the relationship down sidesteps all of that.

The tradeoff: the customer cannot quickly back out of a slow-draining switchover and resume writing as if nothing happened. Post-abort, the former primary is fenced until the control plane releases it, and the relationship must be rebuilt (in v1, that means re-bootstrapping the new follower). For v1 this is an acceptable cost — abort is an infrequent, human-initiated operation, and the operational simplicity is worth more than fast recovery from a rare path.

## What abort does

**On the former primary side:**
- Replication metadata (peer, epoch, role) is cleared from cluster state.
- The cluster-state write block installed during switchover *stays in place* until the control plane clears it. The per-shard fence markers persisted in Lucene commit user data stay with them.
- Replication workers (none were running in the leader direction in a pull-based model, but any serving-side state) are torn down.
- The cluster is now standalone, but not accepting writes. It waits for the control plane.

**On the former secondary side:**
- Pull workers stop.
- Replication metadata is cleared.
- Retention leases on the former primary are released.
- The cluster is now standalone. It has whatever data it drained up to the abort point. It can accept writes, though in practice the customer will likely wait for the relationship to be re-established.

**Neither side severs transport connections.** Remote cluster aliases may remain configured. Replication-layer teardown is independent of transport infrastructure.

## Split-brain handling after abort

The abort is a control-plane-coordinated operation, but it does not require both clusters to be reachable at abort time. The control plane records the intent and ensures the former primary is fenced whenever it next becomes reachable.

- **Former primary healthy during abort.** Control plane notifies it, confirms fence stays in place, clears replication metadata.
- **Former primary unhealthy/unreachable during abort.** Control plane records abort intent. Shard-level fence markers remain in Lucene commit data, so the former primary does not accept writes during the unreachable window. When it recovers, the control plane installs the cluster-state write block before any other action.

This is an extension of the mechanism already required by the product definition for failover ("once failover is triggered, the old primary will be made read-only when it becomes available") — not a new capability.

## Recovery from abort

The customer re-establishes the replication relationship when they are ready. In v1 this is the same path as initial setup: choose primary and secondary, trigger enablement, re-bootstrap the secondary from the primary. Re-bootstrap time scales with data volume and may exceed the RTO.

There is no "resume the aborted switchover" path. Once aborted, the switchover is gone. If the customer still wants to swap directions after re-establishment, they initiate a new switchover.

## Escalation from abort to failover

Abort and failover are distinct operations with distinct intent:

- **Abort:** "I don't want to complete this switchover. Tear it down. I'll deal with re-establishment later." No promotion, no data-loss acceptance.
- **Failover:** "The primary is unhealthy. Promote the secondary. I accept data loss up to the RPO."

A customer who aborts a switchover and then decides they need the secondary to be the new primary does so by initiating failover as a separate action. The protocol does not implicitly escalate abort to failover — that would collapse two different risk acceptances into one gesture.

## What abort does not do

- Does not bump the replication epoch (no role change occurred).
- Does not promote either side.
- Does not reconcile any partial drain state — whatever was drained is drained, whatever wasn't isn't, and none of it matters because the relationship is being torn down.
- Does not clear the former primary's fence. The control plane does that, gated on confirmation that no promotion happened on the other side.

## Open questions

- **Timing of cluster-state write-block release on the former primary.** Probably tied to the control plane confirming both sides have acknowledged the abort, but the exact handshake should be specified before implementation.
- **Customer experience during the release window.** If the former primary is fenced for some time post-abort while the control plane completes teardown, the customer sees a cluster that rejects writes for a period. Whether this is surfaced as a distinct state ("aborting") or folded into existing states needs a UX decision.
- **Interaction with in-flight re-establishment.** If the customer initiates re-establishment before abort has fully completed on both sides, the control plane needs to serialize these operations — abort first, then re-establish — rather than allowing them to race.
