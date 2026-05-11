# Reconciler: replacing the persistent-tasks control plane

## Why

The current CCR control plane uses per-shard persistent tasks. Each replicated shard is represented as a persistent task in cluster state, started explicitly by the index-level task, and driven by a long-running coroutine on whichever node the task is assigned to. This shape has concrete failure modes that have been observed in production:

- **Cluster-state bloat proportional to shards.** 10K replicated shards = 10K persistent-task entries. Every task assignment, pause, resume, and completion emits a cluster-state update. A domain-wide event (circuit breaker trip, node restart, retry storm) produces a flood of per-task updates that saturates the cluster manager.
- **Task lifecycle events cascade.** The incident referenced in `project_ccr_incidents.md` — a single `"*"` auto-follow rule over 516 indices triggered a memory circuit breaker; the resulting pause emitted 500+ simultaneous cluster-state updates, which the cluster manager throttled, which left tasks in inconsistent state.
- **No auto-heal.** A task that fails partway leaves stale metadata in cluster state with no component responsible for reconciling it. Subsequent attempts fail on residual state: missing shard tasks, bootstrapping-stuck indices, orphaned retention leases. Recovery is manual.
- **No detection of stuck work.** Tasks can stall without surfacing any health signal. The 516-index incident was diagnosed by hand over multiple weeks.
- **Poor composition with cluster-scoped intent.** The whole model is per-shard: there is no cluster-scoped "replicate this domain to that domain" primitive. Every cross-cluster concept is expressed as a coordinated fan-out over per-shard tasks.

The replacement is not "better persistent tasks." It is a different control-plane shape.

## Shape of the replacement

Two pieces:

1. A **cluster-scoped intent document** in cluster state — the single source of truth for "what should be happening."
2. A **per-node reconciler** running on every data node — each computes the slice of the intent that applies to shards it hosts, and takes corrective actions within its own process.

The intent is declarative and small:

```
replication.intent: {
  peer_cluster: <id>,
  role: primary | secondary,
  replicated_indices: [<name>, ...],
  epoch: <long>,
  status: steady | switching | aborted,
}
```

One document, cluster-scoped. Not per-shard, not per-index. Index granularity lives inside the document.

Each node's reconciler has a simple contract:

1. Observe the intent via the normal cluster-state publication path.
2. Observe actual state for shards assigned to this node — which workers are running, which retention leases are held, current lag.
3. Compute the local diff.
4. Take idempotent corrective actions within this node's process.
5. Repeat on cluster-state change and on a periodic timer.

The corrective actions are specific, small, and local: start a pull worker for shard X, stop one, renew a retention lease on the peer, flip an engine. Each is safe to re-run and individually traceable.

Shard reallocation is free: when a shard moves from node X to node Y, the normal cluster-state publication tells both. X's reconciler sees "I no longer host S" → stops the worker. Y's reconciler sees "I now host S" → starts one. No central coordinator decides this.

## Division of responsibility

**The cluster manager owns:**

- The intent document. Mutations to it (adding/removing an index from replication, switchover phases, epoch bumps) go through the normal cluster-state update path.
- Cluster-scoped side effects that aren't per-shard: installing an index-level write block, adding/removing `REPLICATED_INDEX_SETTING`, deleting a local index on stop. These are cluster-state changes emitted by the higher-level operations (switchover workflow, stop API), not by the reconciler.
- The switchover workflow itself. Switchover is a linear multi-phase state machine with causal dependencies (fence → drain → role swap). Running it on the cluster manager gives it durable backing via cluster state and survives cluster-manager failover naturally. Its output is mutations of the intent document; the per-node reconcilers pick up those mutations and do the local work.

**Data nodes own:**

- The reconciler loop for their local slice of the intent.
- All per-shard worker lifecycle — start, stop, retry, pause. In-memory. Not persisted as cluster-state entries. Not emitted as cluster-state updates.
- Observability of their own slice: "which of my shards are healthy, which are stuck, what's the lag."

**Neither owns:**

- Cross-cluster decisions (role swap authorization at a specific epoch, failover). Those are owned by the external control plane and arrive as updates to the intent.

## How this addresses the current pain

**Cluster-state bloat.** The intent is one entry. Per-shard state is in memory on the node hosting the shard, not persisted as cluster-state tasks. Scaling is bounded by the size of the intent (index count), not by shard count.

**Lifecycle update storms.** Worker lifecycle events don't drive cluster-state updates. A worker dying, retrying, or pausing is observed and handled by the local reconciler; nothing emits a cluster-state update unless the *desired* state changes. The 516-index pause cascade becomes N local reconciler observations happening in parallel, each handled within a node, with zero cluster-state traffic.

**Auto-heal.** Reconciliation is the auto-heal mechanism. A missing worker is a diff; the next local cycle starts it. Stale retention lease is a diff; the next cycle removes it. A partially-failed bootstrap leaves the intent pointing at an incomplete follower; the relevant node's reconciler drives it to completion (or declares it failed, depending on policy).

**Stuck-work detection.** Each node's reconciler knows its own diff. Surfacing "shard S has been stuck at seqno X for 10 minutes" as a metric is a short step from what the reconciler already computes. Aggregating across nodes is a separate concern but has a clean source.

**Cluster-scoped intent.** The intent is cluster-scoped from the start. Switchover becomes a state transition on the intent ("flip role, bump epoch"), driven by the cluster-manager-hosted workflow, consumed by per-node reconcilers.

**Parallelism and blast radius.** N nodes reconcile their own slices in parallel. A bug in the reconciler affects one node's shards at a time, not the whole cluster. Cluster-manager failover is a non-event for the reconcilers — they observe cluster-state publication regardless of which node is publishing.

## What the reconciler is not

- **Not a workflow engine.** Switchover is a linear multi-phase operation with causal dependencies. The switchover workflow (on the cluster manager) owns that sequencing; the reconciler only reacts to intent state. This split decomposes along the grain of the two concerns — steady state versus one-shot transitions.
- **Not a coordinator for cross-cluster decisions.** Decisions requiring both sides to agree are owned by the external control plane.
- **Not a replacement for per-shard data-plane primitives.** The pull protocol, retention leases, translog/seqno machinery all stay the same. The reconciler decides *whether* a worker should be running for a shard; the worker itself is unchanged.

## What replaces what

| Today | Replacement |
|---|---|
| `IndexReplicationTask` persistent task | Entry in the intent's `replicated_indices` |
| `ShardReplicationTask` persistent task | In-memory worker on the node hosting the primary shard, started by that node's reconciler |
| Task state transitions via cluster-state updates | In-memory state on the node, observed by its local reconciler |
| `_pause`, `_resume`, `_stop` emitting multiple CS updates | Single intent-document edit |
| Stale persistent tasks after failure | Diff observed by per-node reconciler, resolved by next cycle |
| Cluster-manager fan-out via transport actions to data nodes | Per-node reconciliation against cluster-state-carried intent |

## Open questions

- **Reconciler interval and batching.** Cluster-state changes fire each node's reconciler; a periodic timer covers drift. How short is the timer, and how does the reconciler order its corrective actions within one cycle?
- **Handling per-node reconciler failure.** A node whose reconciler crashes (bug, exception) would silently stop reconciling while its shards still exist. Needs some form of self-supervision or a heartbeat the rest of the cluster can observe.
- **Switchover workflow on the cluster manager.** Concrete shape of the state machine and its persistence model. How does it interact with the reconcilers — only by mutating intent, or are there phases where it needs stronger synchronization (e.g., "confirm every node has seen intent at version V before advancing")?
- **Health API shape.** Per-node reconcilers know what's wrong locally; there needs to be a cluster-wide view. Aggregation can be pull (ask each node) or push (nodes publish health to a known place). Both have tradeoffs.
- **Migration from the existing model.** An upgrade from persistent tasks to the reconciler model has to handle in-flight replication without data loss. Likely not one-shot; needs a coexistence window where both control planes are live.
