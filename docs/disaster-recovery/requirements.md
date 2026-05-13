<!-- @formatter:off -->
# Disaster recovery: requirements

This document defines the customer-facing contract for the OpenSearch native disaster
recovery product.

Terminology in this document is the customer-facing vocabulary: **primary**, **secondary**,
**switchover**, **failover**, **failback**.

## Scope

Two pre-existing OpenSearch clusters in different regions become a primary/secondary
pair via a single enable operation. The primary serves writes and reads; the secondary
serves reads and is continuously synchronized from the primary. Customers can swap
roles deliberately (switchover) or recover from a regional failure (failover/failback).

## Functional requirements

### Core replication

1. **Simple enablement.** Domain-wide replication can be enabled between two
   pre-existing domains. No per-index configuration, no cluster recreation.
2. **Full domain replication.** All user data plus the domain configuration required
   to serve reads and writes on the secondary after failover. The exact set of
   configuration categories is enumerated in `metadata-replication.md`; at minimum
   it includes index templates, ingest pipelines, search pipelines, stored scripts,
   and the persistent cluster settings that affect read/write semantics.
3. **Read-capable secondary.** Customers can direct read queries to the secondary
   region during normal operation. Staleness on those reads is bounded by RPO.
4. **Independent regional endpoints.** Only the primary endpoint accepts writes.
   Both endpoints accept reads. The customer is responsible for routing.
5. **RPO < 1 minute.** End-to-end replication lag in steady state stays under one
   minute under normal workload conditions.
6. **RTO < 20 minutes.** From the customer triggering a recovery operation
   (switchover or failover), writes are accepted on the new primary within 20
   minutes.

### Recovery operations

1. **Switchover.** Both regions healthy. Customer-initiated. Swaps primary/
   secondary roles with no data loss. Period of write unavailability up to the RPO.
2. **Failover.** One region unhealthy. Customer-initiated from whichever side is
   healthy. Promotes secondary to primary. Write unavailability up to the RPO.
   Data loss up to the RPO. Once failover is triggered, the old primary is made
   read-only when it becomes available; until that takes effect, the old primary
   continues to accept writes from any clients that can reach it. **Writes
   accepted during this window are discarded on failback.**
3. **Failback.** After a failover, once the unhealthy region recovers, the previous
   primary can become the secondary. The previous primary is **re-bootstrapped**;
   any divergent state on it is discarded. Time to restore may exceed the RTO
   depending on data volume.

### Operations and lifecycle

1. **Monitoring.** Replication health, replication lag, current primary region,
   failover readiness, and lifecycle events are exposed as CloudWatch metrics and
   events. Sufficient to alert on degradation, decide on failover, and audit what
   happened after the fact.
2. **Version upgrades.** Customer-initiated, per-domain, subject to a compatibility
   matrix. The secondary must be upgraded before the primary; the system rejects
   primary upgrades that would put the secondary on an older incompatible version.
3. **Domain lifecycle ops are not propagated.** Resizes, instance-type changes, and
   deletions on one region are NOT mirrored on the other. The customer applies them
   independently.

## Non-functional requirements

1. **Self-healing.** Replication state must converge to the customer's stated
   intent without operator intervention. Transient failures (node restarts,
   cluster-manager failover, network partitions, in-progress operations
   interrupted mid-flight) recover automatically once the underlying condition
   clears. The system does not get stuck in states that require manual repair.

## Non-goals

1. **Active-active replication.** Only the primary accepts writes. The product
   does not attempt to merge concurrent writes from two regions.
2. **Automatic failover.** Failover is always customer-initiated. The cluster
   never decides on its own to flip roles.

## Out of scope

1. **Customer-controlled index inclusion/exclusion policy.** v1 replicates every 
   replicable index. A future version may let customers opt specific indices out of
   replication.
2. **Three-or-more-region topologies.** One primary, one secondary. Future version may
   allow for multiple secondaries.
3. **Compatibility with CCR.** The DR feature is a new API surface. It is not API-compatible
   with CCR, and the two are not supported on the same cluster simultaneously.
