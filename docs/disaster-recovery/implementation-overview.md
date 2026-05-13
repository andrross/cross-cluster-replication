<!-- @formatter:off -->
# Full-cluster replication: implementation overview

Replication intent and replication status are architecturally distinct and should be
stored differently. Intent is what the system has been told to do, changed only on
deliberate action by an operator or an external control plane. Status is what the
system is currently doing and is changed continuously by controllers and workers as
they observe their own behavior. These two kinds of data are authored by different
actors and change at rates separated by orders of magnitude: intent changes infrequently,
while status can change many times per second per shard.

Key tenets of this design are that an intent change produces O(1) cluster-state writes
regardless of how many indices or shards it affects, and that status is never written to
cluster state.

## API surface

The intent document is a `Metadata.Custom` in cluster state.

The URL parameter is a customer-chosen **relationship ID**. It identifies the
replication relationship and is stable across its life. Both clusters record the
same ID.

```
PUT /_replication/cluster/<relationship-id>
{
  "role": "SECONDARY",
  "local_alias": "us-east-1",
  "remote_alias": "us-west-2"
}
```

The PUT is issued against the SECONDARY. It:
1. Verifies `cluster.remote.<remote_alias>.seeds` is configured.
2. Issues an internal transport action to the peer cluster via that connection,
   asking it to install its side of the relationship with `role=PRIMARY` and
   the two aliases swapped.
3. On ack, writes its own intent locally.

The primary-side install is idempotent: re-issuing for the same relationship ID
is a no-op. A different, conflicting relationship ID returns 409.

```
GET /_replication/cluster/<relationship-id>
→ {
  "relationship_id": "us-west-east-dr",
  "role": "PRIMARY" | "SECONDARY",
  "local_alias": "...",
  "remote_alias": "...",
  "epoch": N,
  "phase": "STEADY"
}
```

```
DELETE /_replication/cluster/<relationship-id>
```

DELETE only affects the cluster it's called on. No cross-cluster coordination.
Deleting the relationship always returns the local cluster to the state of an
independent writable cluster. All data is left in place.

Implementation detail: DELETE strips the follower marker
(`REPLICATED_INDEX_SETTING`) from any previously-replicated indices as part of
the cleanup. Without this, the reconciler would keep attempting to replicate
those indices from a peer that no longer has an intent. With the marker
stripped, the indices become ordinary indices under the local cluster's
control.

## Long-poll on leader metadata changes

One loop per secondary, running on the elected cluster manager only. Uses `ClusterStateAction` against the configured remote cluster client — no custom transport action.

```kotlin
val req = ClusterStateRequest()
    .clear()
    .metadata(true)
    .customs(true)
if (lastAppliedMetadataVersion > 0L) {
    req.waitForMetadataVersion(lastAppliedMetadataVersion + 1)
    req.waitForTimeout(pollWaitTimeout)   // 30s
}
val resp = remoteClient.suspendExecute(ClusterStateAction.INSTANCE, req)
```

The primary side is stateless — `ClusterStateAction` is handled by OpenSearch core. On the primary the listener inside the cluster-applier service wakes up whenever `Metadata.version()` advances, which happens on any metadata mutation.

Bootstrap iteration omits `waitForMetadataVersion` so the first poll returns immediately with current state. Subsequent iterations long-poll. If the primary's metadata version regresses (restore from snapshot, etc.) the secondary resets to 0 and falls back to the bootstrap path.

The loop is keyed on `(relationship-id, epoch)`. Intent-change → stop + restart with fresh `lastAppliedMetadataVersion = 0`.

## Handler pipeline

Each returned cluster state flows through a category-dependency-ordered pipeline:

```
component_templates → templates_v2 → templates → stored_scripts →
ingest_pipelines → search_pipelines → persistent_settings →
indices → data_streams
```

Each handler implements a thin interface:

```kotlin
interface CategoryHandler<T> {
  fun category(): String
  fun extract(clusterState: ClusterState): Map<String, T>
  fun equal(local: T, primary: T): Boolean = local == primary
  fun upsert(name: String, primary: T): ApplyResult
  fun delete(name: String): ApplyResult
}
```

The controller diffs the local state with the new primary state. Each handler knows how to apply its changes.

After handlers run, `BootstrapOrchestrator.tryBootstrap(intent, primaryState)` reconciles index presence:

- For each replicable index that is new, kick off the bootstrap process to create it locally
- For each local index that is no longer in the primary's state, delete it

Restore uses the existing CCR `RemoteClusterRepository` path, which transfers Lucene segments directly from the leader.

## Shard workers

Data-plane replication after bootstrap runs as in-memory coroutines on data nodes, not persistent tasks.

**`PerNodeReconciler`** — one per data node, runs on `clusterChanged` events plus a 2-second periodic tick. For each local index marked as a follower whose primary shards are assigned to this node, it computes the desired worker set. Starts missing workers, stops workers for shards no longer assigned here, drops dead workers so the next tick recreates them.

**`ShardWorker`** — one coroutine per follower primary shard. Lifecycle is pure in-memory: `start()` / `stop()` / `stopAndJoin()`. No `PersistentTask`, no cluster-state entry. On start:

1. Resolve the leader index UUID via a one-shot `ClusterStateAction` to the peer.
2. Renew a retention lease on the leader shard.
3. Read this shard's `localCheckpoint` as the starting seqno.
4. Loop: `GetChangesAction(leaderShard, fromSeq, toSeq)` → `ReplayChangesAction(followerShard, changes)` → renew lease → advance `fromSeq`.

`GetChangesAction` is a long-poll: the leader shard holds it until new operations arrive or 1-minute server timeout. `ReplayChangesAction` writes the ops into the follower shard via the normal replicated-write path, including the existing reactive mapping sync on `MAPPING_UPDATE_REQUIRED`.

## Threading

| Component                        | Thread / scope |
|----------------------------------|---|
| `MetadataReplicationController`  | Single coroutine per (peer, epoch) on a `SupervisorJob + Dispatchers.Default` scope. Cluster-manager only. |
| `clusterChanged` callback        | Cluster-applier thread; listener evaluation is synchronous and cheap (intent compare + syncRole). |
| `tryBootstrap`                   | Runs inside the controller's coroutine, one at a time. Restores issued via `client.suspendExecute(RestoreSnapshotAction)`. |
| `PerNodeReconciler`              | Cluster-applier thread on publication events; `ThreadPool.Names.GENERIC` for the periodic tick. Reconcile work itself is O(local shards) and synchronous. |
| `ShardWorker`                    | One coroutine per follower-primary-shard on its own `SupervisorJob + Dispatchers.Default` scope. `CoroutineExceptionHandler` swallows unexpected failures so they don't escape to the node's uncaught handler. |
| `ClusterStateAction` (primary)   | On the primary, long-polls use `ClusterStateObserver` listener registration. No thread parked per outstanding poll — callback-based. |
| `ClusterStateAction` (secondary) | Suspended coroutine, no thread parked. Delivered on Netty I/O; we don't do heavy work inline there. |

## Observability

- cluster replication status: e.g. current intent (relationship_id, role, local/remote aliases, epoch, phase), metadata controller state (running,
  last_applied_metadata_version, peer's current version, lag), shard-worker summary (per node: N running, N dead, N backing-off), bootstrap summary (in-flight
  restores, quarantined indices).
- per-shard state (leader/follower shard IDs, last seqno pulled, last seqno replayed, lag,
  last-error)

## What's deliberately simple

- No persistent tasks anywhere. Shard lifecycle is in-memory and rebuilt by the reconciler on every cluster-state event.
- No cross-cluster consensus. Primary's cluster state is authoritative; the secondary's apply pipeline is a diff-apply.
- No deltas over the wire. Every long-poll returns the full in-scope metadata; secondary computes deltas locally.
- Single-threaded apply pipeline. Parallelism is a later concern if it measures as a bottleneck.
- Metadata replication is independent of the data plane. A handler failure doesn't stall shard workers, and vice versa. This is what the tenet requires.
