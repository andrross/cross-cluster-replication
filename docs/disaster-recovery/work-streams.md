# Work streams for full-cluster replication

Seven work streams for in-cluster implementation of the AOS full-cluster replication product. External control plane work is out of scope for this document.

Ordered by dependency — earlier streams don't depend on later streams; later streams do depend on earlier streams.

## 1. Reconciler-based control plane

Replace the per-shard persistent-task control plane with the reconciler model.

- Cluster-scoped intent document in cluster state.
- Per-node reconcilers on every data node, each observing the slice of the intent that applies to shards it hosts.
- In-memory pull workers supervised by each node's reconciler. The workers themselves are mostly what `ShardReplicationTask` does today, with their cluster-state representation stripped so lifecycle events don't emit cluster-state updates.

Migration from the existing persistent-tasks model is inside this stream, not a separate one. The two control planes can't coexist cleanly and the migration shape constrains the reconciler design.

Foundation for everything else.

See `reconciler.md`.

## 2. Metadata replication

The "what cluster state to replicate and how" piece. The biggest single stream and the one with the most unknowns.

Three sub-deliverables:

- **Inclusion/exclusion boundary** for user-facing categories: index templates, component templates, aliases, ingest pipelines, ISM policies, script stored definitions, search pipelines, and others. Per-category decision, not a single toggle.
- **Replication primitive** that ships a sequenced stream of metadata changes from one cluster's cluster state to the other, with an ordering model that handles referential dependencies.
- **Per-category apply logic** that understands each category's invariants. Index templates must be applied before indices matching them. Aliases point at indices that must already exist. Pipelines referenced by index defaults must exist before those indices.

See `metadata-replication.md`.

## 3. Plugin state replication SPI

Separate from stream 2 because the boundary is different. Stream 2 covers the OpenSearch-core metadata categories the product owns. This stream defines how plugin-authored state rides on the same primitive.

- SPI for plugins to declare what they replicate (custom `Metadata.Custom` entries, system-index ranges, or both).
- Default behavior for plugins that don't implement the SPI — probably "nothing replicated, surface a warning."
- Compatibility story: new OpenSearch with old plugins, new plugins with old OpenSearch, and a default behavior that's obviously safe.

Gated on stream 2's primitive but otherwise independent.

## 4. Cluster-scoped switchover workflow

Per-index switchover primitives already exist (fence, flush-and-get-handoff-checkpoint, verify-caught-up, promote, demote). This stream lifts them to cluster scope.

- Switchover workflow running on the cluster manager that drives the phases across every replicated index in one coordinated operation.
- Intent-document phase states (`switching:phase-1`, `switching:phase-2`, …) that per-node reconcilers react to.
- Atomic fence of every replicated index via a single cluster-state update.
- Abort path.

Mechanism exists; this stream makes it coherent at the cluster level.

Depends on stream 1. Can land before streams 2 and 3 are complete if the first pass handles user-index replication only.

See `switchover-design.md`, `abort-high-level-design.md`.

## 5. Failover

Distinct from switchover in code path as well as in product semantics. No quiescence, no drain, no cooperation with the former primary.

- Failover workflow that promotes every replicated index unilaterally.
- Best-effort fence-the-former-primary — a hook the control plane can invoke when the former primary becomes reachable.
- The `writes-discarded-on-failback` contract enforced by the re-bootstrap path.

Depends on stream 4 — most of the cluster-scoped workflow machinery is built there.

## 6. Failback with re-bootstrap

The product definition is clear: failback re-bootstraps the former primary.

- Cluster-scoped re-bootstrap that tears down the former primary's replicated indices and starts them fresh from the new primary.
- Progress reporting (time to restore may exceed RTO for large domains); interrupt/resume if practical.
- Cleanup of residual state from the failover-era dual-primary window.

Smaller than streams 4 and 5, but has subtle interactions with metadata replication (stream 2): the re-bootstrapped cluster has to land in a consistent state across user data and all replicated metadata categories.

## 7. Observability and health

Not an afterthought. The 516-index incident in the current CCR was fundamentally a monitoring failure.

- Cluster-scoped health view aggregated from per-node reconciler state.
- Per-index and per-shard lag metrics.
- Detection of stuck work (a shard not advancing for N minutes).
- Status API surfacing what the reconcilers and workflows are doing.

Reads from the other streams' internal state. Can be built in parallel with them once each stream exposes its state in a queryable form. Start early, not at the end.

## Dependency shape

- Stream 1 is the foundation. Nothing else works well without it.
- Streams 2 and 3 are the data-plane expansion beyond user indices. They can run in parallel with each other once the metadata-replication primitive from stream 2 is agreed.
- Stream 4 requires stream 1 and the basic primitive from stream 2. Can land before 2 and 3 are fully complete if the first pass handles user indices only.
- Stream 5 is mostly mechanical once stream 4 exists.
- Stream 6 needs streams 2 and 5 before it can be meaningful.
- Stream 7 is read-only against the other streams. Start early.

## Scheduling risk

Streams 2 and 3 are where the unknowns live. Both involve enumerating every category of state in a real cluster, deciding per-category semantics, and handling referential dependencies. These are the streams most likely to be sized wrong in the first planning pass and to expand during implementation.

Mitigation: scope conservatively. Ship a smaller `replicated_indices` set plus a short list of metadata categories in the first release. Expand the list in later releases once the primitive is proven.
