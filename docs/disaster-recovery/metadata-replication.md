# Metadata replication

What cluster state to replicate, and how.

## Product contract for metadata

The AOS DR product definition commits to "full domain replication: all user data plus the domain configuration required to serve reads and writes on the secondary after failover." It also excludes "domain lifecycle ops" (resize, instance type changes, deletions) from propagation.

Those two clauses are the boundary. A category is in-scope if it is part of "the domain configuration required to serve reads and writes," and is out-of-scope if it is part of cluster shape, sizing, or lifecycle.

The secondary also serves reads during normal operation, so "serve reads" includes the secondary during steady state replication and not just after failover.

## Which indices are replicated (v1 rule)

All user indices, by a single predicate applied to each IndexMetadata:

- Name does not start with `.` (OpenSearch convention for internal indices).
- `IndexMetadata.isSystem()` is false (explicit SystemIndexPlugin registration).
- `index.hidden` is false.

Data-stream backing indices are included: they carry user data and the `data_streams` category propagates the logical stream separately.

There is no replicated-indices list on the intent document. Scope is computed from local cluster state via the `V2Scope.isReplicable()` predicate. A future user-facing exclude-patterns feature would add a field to the intent and one condition to the predicate — callers (bootstrap orchestrator, reconciler, handlers) remain unchanged.

## Decision framework

Per category, apply these tests in order:

1. **Is this state needed to serve reads or writes on the secondary (either during steady-state reads or after promotion)?** If no, exclude.
2. **Is this state inherently region-, node-, or cluster-shape-specific?** If yes, exclude — or define a transform and revisit.
3. **Is this state covered by the "domain lifecycle ops not propagated" carve-out?** Exclude.
4. **Is this state owned by a plugin?** Defer to plugin state replication design.
5. **Does this state have referential dependencies on other categories?** Record them.

Decision vocabulary used in the tables below:

- **Include** — replicate as-is.
- **Exclude** — not replicated.
- **Conditional** — replicate some sub-set with an explicit rule.
- **Transform** — replicate after per-field transformation (e.g., strip region-specific values).
- **Allowlist** — replicate only members of an explicit allowlist maintained per release.

## Core cluster-state metadata sections

Top-level sections of `Metadata` in cluster state.

| Section | Decision  | Rationale |
|---|-----------|---|
| `indices` (IndexMetadata) | Include   | User data. Each IndexMetadata is replicated per the sub-field table below. |
| `templates` (v1 index templates) | Include   | Auto-created indices after promotion must match primary's conventions. Deprecated but still supported. |
| `templates_v2` (composable index templates) | Include   | Same reason. Applies before matching indices. |
| `component_templates` | Include   | Referenced by composable templates. Must apply before the templates that reference them. |
| `data_streams` | Include   | User-visible aggregate over backing indices. Metadata (name → backing indices, generation) ships; backing indices are replicated as indices. |
| `ingest` (ingest pipelines) | Include   | Index settings `default_pipeline` / `final_pipeline` reference them; writes after promotion require them. |
| `search_pipelines` | Include   | Read path on the secondary requires them during steady state. |
| `stored_scripts` (`ScriptMetadata`) | Include   | Queries and script-using writes require them. |
| `repositories` | Exclude   | Region-specific (bucket, credentials, region endpoint). Treated as domain-lifecycle / per-region infrastructure; control plane or customer configures independently per region. Open question below on a logical repository aliasing abstraction. |
| `persistent_settings` | Allowlist | Almost all cluster settings are topology/sizing. See the cluster-settings section below. |
| `transient_settings` | Exclude   | Deprecated. Per-cluster by nature; not a stable contract. |
| Cluster coordination metadata (voting config, last committed/accepted config) | Exclude   | Per-cluster consensus state. Cluster shape, excluded by product contract. |
| `snapshot-in-progress`, `restore-in-progress`, index-graveyard, deletion-in-progress | Exclude   | Transient per-cluster operational state. No meaning across the link. |
| `metadata.custom` (plugin-registered entries) | TBD       | To be covered by plugin state replication design |

## Per-index sub-fields (IndexMetadata)

When an index is replicated, its IndexMetadata is not shipped as an opaque blob. Several sub-fields are cluster-local and must be stripped or regenerated.

| Sub-field | Decision | Rationale |
|---|---|---|
| Settings — user-facing (analyzers, codec, sort, mapping limits, refresh interval, …) | Include | Part of the index definition. |
| Settings — `index.uuid` | Exclude (regenerated locally) | Follower index has its own UUID, generated at bootstrap. |
| Settings — `index.creation_date`, `index.version.created`, `index.version.upgraded` | Include on bootstrap, frozen thereafter | Must match the source for compatibility; never updated post-bootstrap. |
| Settings — `index.routing.allocation.*` (node-attr filters) | Strip | Node attributes are per-cluster. |
| Settings — `index.number_of_replicas` | Conditional | Open question — see below. Primary's replica count may not suit secondary's topology. |
| Settings — `index.number_of_shards` | Include (bootstrap only) | Set at index creation; cannot change without reindex. |
| Settings — `index.blocks.*` user-driven | Include | User intent. |
| Settings — replication-imposed blocks (e.g., follower write block) | Exclude | Managed locally by the reconciler; not user intent. |
| Settings — `index.default_pipeline`, `index.final_pipeline`, `index.search.default_pipeline` | Include | Referential dependency on pipelines. |
| Mappings | Include | Core schema. |
| Aliases | Include | User-facing read/write addressing. Write-alias behavior after promotion — see open questions. |
| `rollover_info` | Include | Needed for data-stream and rollover-alias continuity across promotion. |
| `in_sync_allocations` | Exclude | Per-cluster routing/allocation state. |
| `primary_terms` | Exclude | Per-cluster shard coordination state. |
| Metadata version counter | Exclude | Per-cluster cluster-state version space. |
| Index-level `custom` map entries | TBD       | To be covered by plugin state replication design |

## Cluster settings

Enumerating every setting is not feasible; new settings arrive each release.

**Approach: positive allowlist of settings, maintained per release.**

Candidates for the v1 allowlist (needs review before lock-in):

- `action.auto_create_index`
- `action.destructive_requires_name`
- `search.default_search_timeout`
- Feature-compatibility flags that are user-selected rather than topology-driven.

Explicitly excluded (illustrative, not exhaustive):

- `cluster.routing.allocation.*`
- `cluster.max_shards_per_node`
- `indices.breaker.*`
- `thread_pool.*`
- `discovery.*`, `gateway.*`
- `remote_store.*` (per-cluster infra)
- `cluster.blocks.read_only*` (replicating a read-only block would violate the secondary's read-availability contract)

Contract for the allowlist: a setting qualifies only if (a) it is user-policy, not topology/sizing, and (b) replicating it does not change cluster behavior in a way that violates the "secondary serves reads, primary serves writes" contract.

## Transient / per-cluster operational state — blanket exclude

None of the following is replicated:

- Routing table (shard allocation)
- Discovery nodes
- Cluster blocks (those added/removed by workflows are local; not shipped across the link)
- Snapshot-in-progress, restore-in-progress, deletion-in-progress
- Task list / persistent tasks owned by other features
- Cluster-state version, cluster UUID, cluster name

## Referential dependency summary

Dependencies across replicated categories:

```
component_templates  →  templates_v2
stored_scripts       →  mappings that reference them
ingest pipelines     →  index settings that reference them (default_pipeline, final_pipeline)
search_pipelines     →  index settings that reference them
templates (any)      →  auto-created indices matching them
indices              →  aliases pointing at them
                     →  data_streams whose backing indices they are
```

Applying any of these is idempotent on name, so the constraint is "apply dependency before dependent," not "fail if dependency missing."

## Open questions

- **Replica count.** Replicate `index.number_of_replicas` verbatim, or let the secondary pick based on its own topology? Cluster shape is not propagated, but replica count is per-index.
- **Allocation awareness on indices.** Node attributes differ across regions (I think?). Blanket strip is probably the safe default.
- **Name collisions on the secondary.** Need a pre-replication setup validation check to ensure the secondary cluster is "empty". Cluster blocks should prevent creating anything on the secondary that could conflict.