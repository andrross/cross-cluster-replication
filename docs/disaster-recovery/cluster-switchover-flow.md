<!-- @formatter:off -->
# Cluster switchover: API contract with the control plane

How the external AOS control plane drives a cluster-scoped switchover through the replication REST APIs on both clusters.

The focus is the cluster-level contract: what the CP calls, when, and what the cluster guarantees in response. Underlying shard mechanics (fence markers, drain detection, engine flip) are assumed to exist.

## Model

A replication relationship has a stable, caller-chosen **relationship ID**. Both clusters use this ID in their URL path; both record identical labels for the two endpoints in their intent document. The only thing that differs between the two clusters' intents is `role`.

Replication intent is a state machine. The fields:

- `relationship_id` — customer-chosen identifier, stable across the life of the relationship.
- `role` — `PRIMARY` | `SECONDARY`. Flips on switchover or failover.
- `local_alias` — this cluster's label (usually its region / deployment name).
- `remote_alias` — the peer cluster's label. On the secondary, this must match an existing `cluster.remote.<alias>` setting; on the primary it is cosmetic.
- `status` — `STEADY` | `SWITCHING` | `FAILED_OVER`.
- `epoch` — relationship-generation counter. Bumps exactly once per role flip (completed switchover, or failover).
- `switchover_attempt_id` — UUID; present only while `status = SWITCHING`. Distinguishes the current attempt from any prior aborted one.

A switchover is a sequence of CP-driven transitions. Each transition is CAS-protected on `(expected_epoch, expected_status)`. Readiness is a live observation computed by fan-out to data nodes, not something persisted in the intent.

## Transitions table

Cluster-side enforced. Any pair not listed is 409 Conflict. DELETE is intentionally not a transition — see the escape hatch.

| From | To | Role change | Epoch bump | Applies on |
|---|---|---|---|---|
| `STEADY` | `SWITCHING` | — | no | primary |
| `SWITCHING` | `STEADY` | — | no | primary (abort) |
| `SECONDARY` + `STEADY` | `PRIMARY` + `STEADY` | yes | **yes** | secondary (promote; readiness-gated) |
| `SWITCHING` | `STEADY` | `PRIMARY → SECONDARY` | no (matches peer) | old primary (demote) |
| `STEADY` | `FAILED_OVER` | `SECONDARY → PRIMARY` | **yes** | new primary (failover) |
| `FAILED_OVER` | `STEADY` | — | no | new primary (after failback) |

Epoch bumps exactly once per role flip. In switchover, that's on the secondary's promote; the primary's demote converges to the already-bumped epoch.

## API surface

Three new endpoints on top of the existing `PUT / GET / DELETE /_replication/cluster/<relationship-id>`.

### `POST /_replication/cluster/<relationship-id>/_transition`

Advance status (and optionally role). CAS on `(expected_epoch, expected_status)`. On 412, the current intent is returned so the CP can recognize "already where I wanted to be."

**Request**

```
{
  "expected_epoch": 1,
  "expected_status": "STEADY",
  "target_status": "SWITCHING"
}
```

For role-changing transitions, include `expected_role` and `target_role`. The cluster computes the new epoch itself.

**Responses**

- `200` with `{ "intent": {...} }` on success.
- `412` with `{ "error": "precondition_failed", "intent": {...} }` on CAS mismatch.
- `409` with `{ "error": "illegal_transition", "intent": {...} }` for disallowed (from, to) pairs.
- `409` with `{ "error": "not_ready", "intent": {...}, "readiness": {...} }` if the secondary's promote fails the readiness gate.

### `GET /_replication/cluster/<relationship-id>/_switchover/validate`

Read-only. Fan-outs to data nodes: "have all follower shards received the fence marker for the current `switchover_attempt_id`?"

```
{
  "intent": {...},
  "ready": true | false,
  "details": { "total_shards": N, "shards_caught_up": M, "shards_pending": [...] }
}
```

Readiness is **monotonic** within an attempt: once the primary's write block + fence marker are in place, follower shards can only advance toward the marker.

## The protocol

Starting state: clusters A and B, relationship ID `us-west-east-dr`. A is PRIMARY (`local_alias=us-west-2`, `remote_alias=us-east-1`), B is SECONDARY (`local_alias=us-east-1`, `remote_alias=us-west-2`). Both `STEADY`, `epoch=1`.

1. **CP → A `_transition`: `STEADY → SWITCHING`.** Atomic: generate `switchover_attempt_id`, install write block, write fence marker to each replicated primary shard.
2. **CP → B `_switchover/validate` (poll)** until `ready: true`.
3. **CP → B `_transition`: `SECONDARY + STEADY → PRIMARY + STEADY`.** Bumps epoch to 2. Re-verifies readiness server-side; 409 if not ready. Tears down secondary-side infrastructure, flips engines to writable.
4. **CP → A `_transition`: `SWITCHING + PRIMARY → STEADY + SECONDARY`.** Converges epoch to 2 (no bump). Removes write block, clears `switchover_attempt_id`, starts secondary-side infrastructure.

Between steps 3 and 4, B is the new primary at epoch 2; A is still in `SWITCHING` at epoch 1, rejecting writes. The AOS routing layer is expected to redirect clients to B during this window.

Labels (`local_alias`, `remote_alias`) do not rotate on switchover. They're identity labels for the two clusters; only `role` flips.

## Why this is correct

**CAS preconditions on every state change.** `(expected_epoch, expected_status)` catches stale retries, concurrent CPs, and any mutation that slipped in from a failover.

**Idempotent by content.** Re-issuing an applied transition returns 412 with the current intent. CP recognizes "I'm there" and moves on; no request-ID dedup table needed.

**Readiness is re-verified in the promote transition.** Even if the CP skips validate, the promote on the secondary runs the same fan-out. A not-caught-up secondary cannot be promoted.

**Fence markers ride the data-plane channel.** The readiness signal is the same channel that replicates user data. If replication is stuck, the marker doesn't arrive, and the secondary reports not-ready. No side-channel liveness assumption.

**Attempt ID disambiguates retries.** Stale markers from a prior aborted attempt carry a different ID than the current attempt. Readiness queries are always keyed on the current attempt.

**Epoch bump stops zombie transitions.** A stale request from a prior relationship generation hits 412 on first contact.

**Transitions are atomic on the cluster manager.** Each `_transition` is an `AckedClusterStateUpdateTask` under the single-threaded applier. Precondition check, side effects (install/remove block, write/clear marker, flip engines), and intent write happen atomically.

## Failure modes

**CP crashes mid-switchover.** Intent is durable on both sides. New CP instance reads current intent on each, infers position, resumes. CAS makes the resume safe.

**Cluster-manager failover mid-step.** Intent is cluster-state-backed; new CM has it immediately. In-flight `_transition` may fail transiently; CP retries.

**B unreachable before step 3.** CP aborts (`SWITCHING → STEADY` on A). Writes restored, no data moved, system at epoch 1.

**A unreachable between steps 3 and 4.** B is already the new primary at epoch 2. A stays in `SWITCHING` until it recovers or the CP uses the escape hatch. On recovery the CP can complete step 4 or treat it as a failover and re-bootstrap A.

**B unreachable after step 3.** New primary is unreachable; writes impossible on both sides. Dual-failure; outside the switchover protocol.

**Primary accepted a write between fence and marker.** Cannot happen. Write block and marker are written atomically in the same cluster-state update task.

**Marker was delivered but overwritten before readiness check.** Cannot happen. The marker is the last op in each shard's history for the duration of `SWITCHING`; the write block prevents anything after it.

## Abort

```
POST /_replication/cluster/<relationship-id>/_transition
{ "expected_epoch": 1, "expected_status": "SWITCHING", "target_status": "STEADY" }
```

Removes the write block, clears `switchover_attempt_id`. No role change, no epoch bump. Only legal before step 3 (before the secondary has been promoted). After step 3, there's no switchover left to abort — the role flip is already done, and restoring the prior arrangement is a failback, not an abort.

## Failover

```
POST /_replication/cluster/<relationship-id>/_transition
{
  "expected_epoch": 1,
  "expected_role": "SECONDARY",
  "expected_status": "STEADY",
  "target_role": "PRIMARY",
  "target_status": "FAILED_OVER"
}
```

CP calls this on the surviving cluster; the unreachable old primary is not contacted. Epoch bumps. **Readiness is not checked** — failover explicitly accepts data loss up to RPO. Writes on the old primary during the failover window are discarded on failback via the re-bootstrap path.

## Escape hatch

`DELETE /_replication/cluster/<relationship-id>` is always legal, on either cluster, from any status. It is intentionally outside the transitions table — it is the bailout for unrecoverable state.

**What it does.** Clears the intent. Removes any write block. Stops any in-flight long-poll. Clears `switchover_attempt_id`. Strips the follower marker (`REPLICATED_INDEX_SETTING`) from any formerly-replicated indices so they become ordinary writable indices. No cross-cluster coordination; no peer-reachability required.

**Guarantee: data and configuration are preserved.** DELETE always leaves the cluster in the state of an independent writable cluster with all the data and configuration it currently holds. Indices that were replicated to a secondary stay there with their contents intact, just no longer marked as followers.

**When to use.** In order of preference: abort for recoverable switchover problems, failover for unreachable-peer problems, DELETE only when neither is viable. Recommended to call DELETE on both clusters when bailing out — otherwise the remaining side's intent becomes stale.

**Consequences.** Two independent writable clusters, no replication. Re-establishing requires fresh `PUT` on both sides followed by a full re-bootstrap, which may take hours for large domains. DR coverage is suspended in the interim.

## Deliberate non-features

- No convenience `/_switchover` endpoint; CP drives every transition individually, so resume-after-crash is trivially correct.
- No peer-to-peer coordination beyond the setup-time auto-install (see implementation overview); the CP is the only authority for state transitions.
- No cluster-side auto-abort; stuck state stays stuck until the CP (or DELETE) intervenes.
- No per-index switchover; scope is cluster-wide via `ReplicationScope.isReplicable`.
- No "force" flag on transitions; CPs that want to override preconditions should GET current state or DELETE.

## Full API surface

- `PUT /_replication/cluster/<relationship-id>` — configure relationship on the secondary; auto-installs the primary's intent via a transport action. See `implementation-overview.md`.
- `GET /_replication/cluster/<relationship-id>` — read intent.
- `DELETE /_replication/cluster/<relationship-id>` — escape hatch.
- `POST /_replication/cluster/<relationship-id>/_transition` — state machine advance (this doc).
- `GET /_replication/cluster/<relationship-id>/_switchover/validate` — readiness probe (this doc).
- `GET /_replication/cluster/<relationship-id>/status` — observability, orthogonal.
