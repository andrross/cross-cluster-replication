# Metadata replication: design

How the "what to replicate" decisions from `metadata-replication.md` get implemented. Companion doc — read that first for category boundaries.

## Tenet

**It is better for a new feature (or a forgotten feature) to not work than to break steady-state replication.**

Steady-state replication here means the shard-level pull that keeps user data flowing. Metadata replication is a separate concern, and the metadata stream must not be able to stall or corrupt data flow.

Corollaries that shape everything below:

- Data-plane pull and metadata pull are independent transports with independent health. A pathological failure in the metadata stream — un-applyable object, handler exception, version confusion — does not touch data flow.
- A plugin, category, or field the framework doesn't know about is silently not replicated. It is never a replication error.
- Apply-time failures isolate to the single object. The rest of the metadata state keeps moving.
- "Fail forward" is the default posture. When in doubt, skip + log + continue.

## Shape

Long-poll of the primary's cluster state, gated on `Metadata.version()`.

- `Metadata.version()` on the primary is a monotonic counter that bumps whenever any `Metadata` mutation is published. OpenSearch core bumps it uniformly in `ClusterManagerService.patchVersions()` for every in-scope category. See `metadata-replication.md` for the full category list.
- Secondary calls `ClusterStateAction` on the primary with `waitForMetadataVersion = last_applied + 1`. The primary blocks until its metadata version reaches that value, then returns the cluster state — or returns `waitForTimedOut = true` if nothing changed within the wait window.
- Secondary extracts in-scope categories from the returned state, diffs against local state, applies per-object changes through category handlers.

No new transport action. No new REST API. `ClusterStateAction` with `waitForMetadataVersion` already does what we need, and `RemoteClusterConnection` already invokes it cross-cluster via the system-context bypass.

## Architecture placement

Single component on each cluster manager: `MetadataReplicationController`. Symmetric — both sides run it at all times. Its active role (respond to polls vs. issue polls) is derived from the intent document's `role` field, not hard-coded.

- Primary-role cluster: nothing active. `ClusterStateAction` is served by OpenSearch core; the controller does not run the polling loop.
- Secondary-role cluster: runs the long-poll loop; applies what it receives.

Switchover flips direction via an intent mutation; the controller re-evaluates its role from the intent and starts or stops the loop accordingly.

## The poll

Single call per iteration. Uses `ClusterStateRequest` via the remote cluster client the replication plugin already establishes.

```
ClusterStateRequest req = new ClusterStateRequest()
    .clear()                              // baseline: nothing
    .metadata(true)                       // we want metadata
    .customs(true)                        // and the customs inside it
    .waitForMetadataVersion(lastApplied + 1)
    .waitForTimeout(pollWaitTimeout);     // e.g., 30s

ClusterStateResponse resp = remoteClient.state(req);
```

Response shapes:

- `resp.isWaitForTimedOut() == true`: no change within the wait window. `resp.getState()` is null. Secondary immediately re-polls with the same `lastApplied + 1`.
- `resp.isWaitForTimedOut() == false`: the primary's metadata advanced. `resp.getState()` contains cluster state at some version `v >= lastApplied + 1`. Secondary enters the apply path.

Long-poll replaces fixed-interval polling. Lower latency on change, near-zero traffic when idle.

### Role check (the `not_primary` safety rail)

`ClusterStateAction` itself has no notion of "only answer if I'm the primary for replication." The check has to happen on the secondary side, after the response arrives.

```
ReplicationIntent intent = resp.getState().custom(ReplicationIntent.TYPE);
if (intent == null || intent.role() != PRIMARY) {
    emit alert metric
    back off, retry later
    // do NOT apply
}
```

The replication-intent document is itself in cluster state as a `Metadata.Custom` (or `ClusterState.Custom`), so it rides in the same response. No extra round-trip.

This places the safety rail on the secondary rather than the primary. Weaker than a server-side check, but `ClusterStateAction` is read-only and returns the data for the secondary to decide. Accept it; the secondary has to parse the payload anyway.

### Filtering

`ClusterStateRequest` filtering is coarse-grained:

- `.metadata(true)` / `.customs(true)` — include/exclude entire sections.
- `.indices(...)` — narrow `IndexMetadata` to named indices. Useful if `replicated_indices` is a short list; ordinary for this product.
- No per-custom-type filter. All `Metadata.Custom` entries with `XContentContext.API` come back.

Consequence: the secondary receives some metadata it will drop on the floor (out-of-scope categories, plugin customs without a handler). Cheap. In-scope filtering happens on the secondary as part of the apply pipeline.

If payload size ever becomes a problem, the mitigation is narrowing `.indices(...)` to `replicated_indices` (already a known list from the intent document), not a new API.

## The long-poll loop

One long-poll loop on the secondary, pointed at the primary named in `intent.peer_cluster`. Per the product contract, each cluster has exactly one peer.

1. Read local `last_applied_metadata_version` from secondary-side persistent state (0 if never applied anything).
2. Issue `ClusterStateRequest`. On the bootstrap iteration (`last_applied == 0`), leave `waitForMetadataVersion` unset — the secondary has no baseline, so there's nothing to wait past; the primary returns current state immediately. On subsequent iterations, set `waitForMetadataVersion = last_applied + 1` and `waitForTimeout = pollWaitTimeout` to long-poll.
3. On response:
   - Timed out: back to step 2. No change, no apply.
   - Returned state: extract intent. If `role != primary`, emit alert, back off, retry step 2 after backoff.
   - Returned state, role is primary: enter apply path.
4. Apply path (see next section) runs to completion.
5. On success, persist new `last_applied_metadata_version` from the response state.
6. Back to step 1.

Poll-wait timeout: on the order of 30s. Small enough that `not_primary` states get re-evaluated promptly after an intent change; large enough that idle clusters aren't generating constant traffic.

**Version regression defense.** If `response.metadata().version() < last_applied_metadata_version` — e.g., after a cluster restore from snapshot on the primary — the secondary treats it as a full resync. Reset `last_applied_metadata_version = 0`; the next iteration falls back to the bootstrap path (no `waitForMetadataVersion`), fetching current state immediately. Apply pipeline handles "current state" the same way it handles any other snapshot.

**Network errors / remote client failures.** Retry with backoff. Idempotent by construction — no cursor to get wrong.

## Apply pipeline

Per response with a cluster state on the secondary:

1. Extract the intent. If `role != primary`, abort (see above).
2. For each in-scope category, in dependency order (component templates → composable templates → v1 templates → stored scripts → ingest pipelines → search pipelines → persistent settings → indices → data streams):
   a. Look up the registered category handler. Unknown category → log + increment counter + continue.
   b. `extractPrimary()` — handler pulls its objects from the response state.
   c. `extractLocal()` — handler pulls its objects from the secondary's current cluster state.
   d. Diff:
      - In primary, not in local → upsert.
      - In primary, in local, `handler.equal(primary, local) == false` → upsert.
      - In primary, in local, equal → skip.
      - In local, not in primary → delete.
   e. For each diff entry, invoke `handler.upsert()` or `handler.delete()`. Handlers are idempotent by contract.
3. Persist `last_applied_metadata_version = response.metadata().version()`.

Step 2e handles per-object failures per the tenet:
- **success** — continue with the next diff entry.
- **permanent failure** — the object will never apply on this secondary (malformed payload, invariant violation unrelated to transient state). Quarantine this object, continue with the next diff entry.
- **transient failure** — retry-after-backoff within this apply cycle. If still failing, quarantine and continue. `last_applied_metadata_version` still advances at the end.

Advancing the version even with quarantined objects is deliberate. The alternative — stall the version until every object applies — means one un-applyable object pauses all metadata replication. That's a tenet violation.

Category dependency order handles referential dependencies: pipelines apply before indices that reference them, component templates apply before composable templates, indices apply before data streams.

Single-threaded. Within a category, no ordering guarantee is required.

## Handlers

One handler per category. Thin interface:

```
interface CategoryHandler<T> {
  String category();                                  // e.g., "ingest_pipelines"
  Map<String, T> extractLocal(ClusterState local);
  Map<String, T> extractPrimary(ClusterState primary);
  boolean equal(T local, T primary);
  ApplyResult upsert(String name, T primary);         // idempotent
  ApplyResult delete(String name);                    // idempotent
}
```

Handlers are registered at startup via `MetadataReplicationCategoryRegistry`. Core handlers are registered by the replication plugin; plugin handlers arrive via the stream 3 SPI.

A missing registration is a no-op, not an error. Plugin writes state on the primary but didn't register a handler? That state doesn't replicate. Per the tenet.

`equal()` is a handler concern. Default: deep equality on a stable serialization. Handlers can override — e.g., a template handler might ignore a version sub-field on the template object itself.

## Forward compat and unknown things

Two shapes of unknown, same response:

- **Unknown category** — primary's response contains a `Metadata.Custom` type the secondary doesn't have a handler for. Skip, increment `unknown_category_count`, continue.
- **Unknown field inside a known object** — primary has a newer schema than the secondary. Handler policy. Default: apply recognized fields, ignore unrecognized. Handler can override if it has a stricter compatibility contract.

Never hard-fail on unknown. Load-bearing consequence of the tenet.

The `ClusterState.readFrom` deserialization on the secondary can tolerate unknown `Metadata.Custom` entries because each custom type registers itself; unrecognized types come through as `null` or skipped entirely depending on the named-writeable registry. A custom the secondary's registry doesn't know about is simply absent from `resp.getState().metadata().customs()` as far as the apply pipeline sees it. Worth verifying before implementation — listed as an open question.

## Deletions

Fall out of the diff. No tombstones. Object absent from the primary's state, present locally → handler's `delete()` is called.

Long-disconnected secondary: the primary's response reflects current state, not history. Many create-and-delete cycles in between don't matter. The diff closes the gap on reconnect.

## Noise sources

Known reasons `Metadata.version()` bumps without in-scope changes:

- **Out-of-scope category mutations** — e.g., someone registered a new snapshot repository. Not replicated, but the version bumped. Secondary's long-poll returns; diff finds no in-scope changes; persist version and re-poll.
- **Non-idempotent primary-side updates** — OpenSearch core has asymmetric no-op detection. Mappings have idempotent equality checks; ingest pipelines do not. Re-PUTting an identical pipeline on the primary bumps the version. Secondary returns from long-poll, diffs, finds no actual change (`handler.equal() == true`), persists version, re-polls.

Cost is a wake-up and a diff, not a correctness issue. Acceptable at v1 scales.

If noise becomes a problem: the replication-intent `replicated_indices` list can narrow `.indices(...)` on the request, and `.customs(false)` can be toggled if no customs are handler-registered. Not needed for v1.

## Division of labor with the data plane

Mappings propagate reactively, not through the metadata controller. When `TransportReplayChangesAction` applies an operation that references an unknown field, the engine returns `MAPPING_UPDATE_REQUIRED`; the action calls `GetMappingsAction` against the leader, applies the mapping to the follower via `UpdateMetadataAction`, and retries the operation. This predates v2 and continues to work with the v2 shard worker because the worker uses the same `ReplayChangesAction` path.

Consequence: the `indices` handler in the metadata controller should *not* replicate mappings. Doing so would race with the data plane's `syncRemoteMapping` and do duplicate work.

What the indices handler *is* responsible for:

- Setting deltas on existing indices that aren't carried by ops: `index.default_pipeline`, `index.final_pipeline`, `index.blocks.*` (user-driven), `index.refresh_interval`, and similar user-policy settings. Read traffic on the secondary honors these without a triggering write, and they must be in place before a post-promotion primary serves writes.
- Alias add/remove on existing indices. Same read-side and post-promotion rationale.
- Nothing when the index is absent locally. Initial creation is `V2BootstrapOrchestrator`'s job, via snapshot restore.

What the indices handler is *not* responsible for:

- Mapping propagation (reactive via `ReplayChangesAction`).
- Initial index creation (bootstrap).
- Settings that only matter for writes accepted on the primary (e.g., `index.write.wait_for_active_shards`) — no harm in replicating them but no correctness requirement.

## Interaction with the workflows

### Switchover

Metadata is part of the switchover "caught up" check.

1. Primary fence phase applies a cluster-metadata write block for in-scope categories (rejecting template/pipeline/script edits), in addition to the index write block.
2. Primary records `metadata_version_at_fence` in the intent document. The intent mutation itself bumps `Metadata.version()`, so the fence version is `metadata.version()` immediately after the mutation lands.
3. Workflow waits for `secondary.last_applied_metadata_version >= metadata_version_at_fence` before advancing to role flip.
4. Role flip: intent mutation on both sides. The former primary's controller observes its own `role != primary` via the intent change and stops the active role (which on the primary side was passive anyway). The new secondary's controller observes `role == primary → role == secondary` and starts polling the peer.

The controller reacts to intent changes like any other cluster-state listener; no special-case code.

### Failover

Unplanned. Former primary is unreachable or being fenced.

- New primary continues to serve `ClusterStateAction` from its current state.
- The secondary, still configured to poll the former primary, gets transport errors (unreachable) or `not_primary` responses (if reachable but fenced, once its intent is mutated). Either case: alert, back off, retry. No apply.
- Applied state on the new primary is whatever the secondary last pulled before divergence. Metadata writes on the former primary during the failover window — per the product contract — are discarded on failback.

### Failback

Failback re-bootstraps the former primary as a secondary. Metadata re-bootstrap is a full resync: `last_applied_metadata_version = 0`, next poll returns immediately with primary's current state, apply pipeline processes it. No separate code.

## Observability

Per the observability stream; stubbed here because the tenet relies on this being visible.

- `metadata_replication.last_applied_metadata_version` / `metadata_replication.peer_metadata_version` — gauge pair; lag is the difference.
- `metadata_replication.poll_latency` — histogram. For long-poll, this is time-to-change, not time-to-response.
- `metadata_replication.poll_timeouts` — counter. Normal in idle clusters.
- `metadata_replication.quarantine_count` — per-category counter. Non-zero is a signal, not a halt.
- `metadata_replication.unknown_category_count` — skipped categories, usually version skew.
- `metadata_replication.not_primary_responses` — counter. Non-zero on a healthy secondary is a red flag.
- `metadata_replication.resync_events` — version-regression-triggered full resyncs.

Non-zero quarantine, unknown-category, or not-primary counts are alertable. Data replication continues regardless.

Surfacing *which* objects are quarantined needs a small state store on the secondary — likely a cluster-state entry alongside `last_applied_metadata_version`. Open question below.

## What this design deliberately does not do

- **Define a new transport action.** `ClusterStateAction` with `waitForMetadataVersion` covers it.
- **Define a REST API.** This is internal infrastructure; no user-facing endpoint.
- **Maintain a change log on the primary.** The version counter + on-demand snapshot via cluster-state fetch covers the same ground with less machinery.
- **Subscribe the secondary to the primary.** The primary is stateless with respect to the secondary. The long-poll listener is a per-request object, torn down when the request completes.
- **Couple metadata to data transport.** Separate cluster-state fetch, separate health, separate failure mode. The tenet requires this.
- **Ship deltas.** The response is a full cluster state; the secondary computes deltas locally. Simpler than partial updates; cost is a full fetch on every version bump.
- **Coordinate with the per-shard reconciler.** The controller publishes via the normal cluster-state update path. Reconcilers react to those updates like any other cluster-state change.
- **Cross-cluster consensus.** Primary's metadata is authoritative. Conflicts at apply time are resolved by the tenet (skip + log + continue), not by negotiation.

## Open questions

- **Unknown-`Metadata.Custom` deserialization behavior.** Need to confirm that a primary-side custom without a matching named-writeable on the secondary doesn't cause the whole `ClusterStateResponse` to fail to deserialize. If it does, we need a narrower request or a serialization compatibility layer. Verify experimentally before locking in.
- **Poll-wait timeout.** 30s is a guess. Informed by how promptly we need to react to intent-role changes (switchover timing) more than by RPO.
- **Persistent settings allowlist enforcement point.** Filter on the secondary only, or also build a primary-side filter into the response. Secondary-only is simpler and sufficient if the allowlist is versioned with the secondary.
- **Quarantine visibility.** Counter is trivial; surfacing *which objects* are quarantined needs a small state store on the secondary. Likely a cluster-state entry alongside `last_applied_metadata_version`.
- **Where `last_applied_metadata_version` lives.** Cluster-state entry on the secondary (survives cluster-manager failover, participates in publishing) vs. a system index entry. Cluster-state entry is simpler; system index gives richer querying. Probably cluster-state for v1.
- **`replicated_indices` narrowing.** Should we always call `.indices(intent.replicated_indices)` to narrow IndexMetadata to the subset we care about, or fetch all and filter? Narrowing reduces payload and wake-ups; fetching-all simplifies the edge case of "index added to `replicated_indices` but not yet in local state." Lean toward narrowing, but needs a worked example for the just-added-an-index case.
- **Handler-level `equal()`.** Default deep equality works, but some objects have sub-fields that shouldn't count as meaningful differences (version counters inside the object itself, timestamps). Needs a handler-by-handler review.
- **Permanent vs. transient failure classification.** Needs worked examples before the contract is crisp — particularly for "schema violation on apply" when the secondary's OpenSearch version is older.
