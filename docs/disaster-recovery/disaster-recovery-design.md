# AOS OpenSearch Disaster Recovery — Design Document

**Status:** Draft
**Owners:** TBD (architect, 3 workstream leads)
**Last updated:** 2026-04-30

---

## 1. Context

Amazon OpenSearch Service is building a native disaster recovery solution for OpenSearch domains. The product requirements are defined separately; this document describes the engineering approach to delivering them.

The product introduces capabilities that OpenSearch domains do not have today: domain-wide replication between regions, switchover and failover as first-class operations, failback with re-bootstrap, and replication of the domain configuration needed to serve workloads on the DR side. Two properties shape the design:

1. **Reliability is a primary requirement, not an afterthought.** A DR product that loses track of its own replication state or gets stuck without recovering is worse than no DR product. The design is organized around a reconciler-driven model that continuously compares desired state to actual state and heals deviations automatically.
2. **The surface area spans two systems.** Some of the work lives inside the OpenSearch cluster; some lives in the external service-provider control plane. These have different codebases, teams, and deploy cadences, so the seam between them needs to be deliberate rather than accidental.

## 2. Goals

- Deliver the DR product defined in the product requirements.
- A reconciler-driven in-cluster design that auto-heals lost or stuck replication.
- Switchover, failover, and failback as explicit, crash-safe primitives.
- Replicate the subset of domain configuration required to serve workloads post-failover.
- Observability sufficient to alert on degradation, decide on failover, and audit what happened.

## 3. Non-goals

- Active-active replication. Only the leader accepts writes.
- Propagating domain lifecycle operations (resizes, instance type changes, deletions) across regions.
- Cross-version replication beyond the constraints defined in the requirements (follower upgraded first).
- An in-cluster fencing primitive. Demoting the old leader to read-only after failover is a control plane responsibility.
- A dedicated Dashboards UX for DR. Surface via existing customer-facing tooling; no new in-cluster UI.

## 4. Architectural Overview

The product has three architectural layers, each with a distinct home, deploy cadence, and skill set:

1. **The replication engine** — in-cluster OpenSearch code that moves data and metadata between two clusters. Reconciler-driven, coordinated by the cluster manager.
2. **Control plane integration** — the external service-provider system that provisions the replication relationship, issues failover and switchover commands, enforces read-only on a demoted leader, and surfaces customer-facing APIs and CloudWatch integration.
3. **Observability contract** — the set of signals, metrics, and events emitted by the replication engine and consumed by the control plane. Owned as a distinct concern because both sides must agree on its shape.

```
            ┌──────────────────────────────────────────┐
            │   Control Plane (external)               │
            │   - enablement / failover APIs           │
            │   - marks old leader read-only           │
            │   - CloudWatch plumbing                  │
            │   - customer console & CLI               │
            └──────────────────────────────────────────┘
                     │  triggers           ▲  signals
                     ▼                     │
            ┌──────────────────────────────────────────┐
            │   Observability Contract                 │
            │   - lag, health, readiness, events       │
            │   - defined taxonomy, versioned          │
            └──────────────────────────────────────────┘
                     ▲ emits
                     │
            ┌──────────────────────────────────────────┐
            │   Replication Engine (in-cluster)        │
            │   - reconciler + state machine           │
            │   - metadata replication                 │
            │   - switchover / failover / failback     │
            └──────────────────────────────────────────┘
```

## 5. Shared Prerequisites

These decisions must be made before the workstreams diverge. They are cross-cutting and should be owned by a single architect.

| #   | Decision                                                                                   | Blocks                               | Target |
| --- | ------------------------------------------------------------------------------------------ | ------------------------------------ | ------ |
| P1  | Concrete v1 list of "domain configuration required to serve reads and writes"              | Replication engine (metadata)        | Week 2 |
| P2  | Contract between control plane and replication engine for marking the old leader read-only | Control plane, engine (recovery ops) | Week 2 |
| P3  | Replication-intent data model: where stored, how mutated, how observed                     | Replication engine, control plane    | Week 2 |

## 6. Workstream 1 — Replication Engine

### 6.1 Purpose

All in-cluster OpenSearch code that implements replication, metadata sync, and recovery operations. This is where the correctness-critical state lives.

### 6.2 Scope

**Reconciler foundation.** A declarative, self-healing in-cluster design.

- Declarative replication-intent object stored in a system index. One document per replication relationship; supports domain-wide patterns (e.g., `*`) as well as explicit index lists.
- Per-node reconciler that inspects local follower shards and drives them toward the intent. Runs as a scheduled job; the cluster manager assigns intent ownership, not per-shard tasks.
- Shard-level state machine: `PENDING → BOOTSTRAPPING → FOLLOWING → PAUSED | FAILED(reason, attemptCount, nextRetryAt)`. `FAILED` is reconcilable, not terminal — the reconciler retries with backoff until the shard recovers or the intent is withdrawn.
- Rate-limited enrollment so a "replicate everything" intent paces rollout instead of stampeding the leader or the cluster manager.

**Metadata replication.** Everything beyond document ops.

- V1 scope from prereq P1. Expected candidates: index templates, component templates, ISM policies, ingest pipelines, aliases, stored scripts, role mappings.
- Per-resource-type strategy — each has different conflict semantics and ordering requirements.
- Conflict resolution when the follower has local state that collides with incoming leader state. Default: leader wins; exceptions documented per resource type.
- Versioning and idempotency so replays don't corrupt state.

**Recovery operations.** Explicit, crash-safe state machines.

- **Switchover:** quiesce leader writes → follower catches up to leader global checkpoint → flip roles → resume writes on new leader. Each step must be resumable after a cluster-manager restart.
- **Failover:** unilateral follower promotion. Emits the event that the control plane uses to mark the old leader read-only (per prereq P2).
- **Failback:** detect divergence, wipe follower state, re-bootstrap from new leader via snapshot restore, resume replication.
- Leadership epoch/generation number so a recovering old leader can detect it's stale when it reconnects.

### 6.3 Internal phasing

The three concerns above are built in the same codebase by the same team, sequenced internally:

1. **Phase 1 (weeks 1–6): Reconciler foundation.** Stable shape for state, intent, and the reconcile loop.
2. **Phase 2 (weeks 3–10): Metadata replication.** Can start design in parallel with Phase 1; integration follows Phase 1's data model.
3. **Phase 3 (weeks 6–14): Recovery operations.** Builds on the reconciler; recovery state transitions are reconciled just like shard state.

### 6.4 Key design questions

- **What's the right grain for intent?** One document per leader→follower pair with a pattern, versus many per-index documents. Preference: per-pair with pattern, for efficiency in both the system index and the cluster manager's working set.
- **How does the cluster manager assign reconcilers?** Preference: one scheduled job per data node that handles all local follower primaries, rather than per-intent or per-shard assignment.
- **How is the leadership epoch stored and checked?** Preference: monotonic counter in the intent document, checked on every operation that requires leader identity.

### 6.5 Staffing

**3–4 devs.** The largest of the three workstreams. Internal phasing enables some parallelism after week 3.

## 7. Workstream 2 — Control Plane Integration

### 7.1 Purpose

The external system that provisions the replication relationship, drives recovery operations, and surfaces the product to customers. Everything outside OpenSearch itself.

### 7.2 Scope

**Enablement.** Customer-facing APIs and console flows to establish replication between two existing domains. Validates eligibility, creates the replication intent in both clusters, and initiates the initial bootstrap.

**Recovery operation triggering.** APIs for switchover, failover, and failback. Each translates a customer action into the corresponding replication engine operation plus whatever control-plane-side work is needed.

**Fencing the old leader.** After failover, the control plane is responsible for marking the old leader read-only as soon as it can reach it. This is the product's split-brain mitigation per the product requirements. Mechanism TBD in prereq P2 — likely a cluster-level read-only block issued via the domain's management API, with retry until confirmed.

**CloudWatch integration.** Consumes the observability contract from Workstream 3 and publishes metrics and events to the customer's CloudWatch. Defines alarms and suggested thresholds in documentation.

**Version upgrade guardrails.** Enforces that the follower domain is upgraded before the leader. Surfaces clear errors if the customer attempts otherwise.

**Lifecycle op isolation.** Verifies (via tests and monitoring) that resizes, instance type changes, and deletions do not propagate across the replication relationship.

### 7.3 Key design questions

- **Where does the control plane store the replication relationship's metadata?** This is separate from the in-cluster intent document — the control plane needs its own record for billing, customer UX, and cross-domain queries.
- **What's the fencing mechanism?** Options: cluster-level `blocks.write` setting, a new purpose-built API, domain-level read-only state in the control plane. Preference: new purpose-built API with explicit DR semantics, so it isn't confused with other write blocks.
- **How does the control plane detect that the old leader is reachable after failover?** Preference: continuous probing from the control plane with explicit "demoted" confirmation.
- **What happens if fencing takes a long time?** SLA commitment for time-to-demote. Rough target: seconds to minutes once the old leader is reachable; this is implicitly part of the RPO promise.

### 7.4 Staffing

**1–2 devs.** Most of the work parallelizes poorly with the replication engine. One dev is sufficient if well-supported by the parent platform team; two enables API/UX and fencing to proceed in parallel.

### 7.5 Risk

Single-dev ownership of fencing is the biggest bus-factor risk in the project. Mitigate with deep review of the fencing design by someone who could pick it up in an emergency.

## 8. Workstream 3 — Observability Contract

### 8.1 Purpose

Define the signal taxonomy that the replication engine emits and the control plane consumes. Owned as a distinct concern because both sides need to agree on shape, and without dedicated ownership the contract drifts.

### 8.2 Scope

**Continuous health signals.**
- Replication status per replication relationship: `HEALTHY | DEGRADED | STOPPED | FAILED`.
- Count of indices and shards in degraded or failed state.
- Retention lease health on the leader (risk of follower falling off history).
- Whether replication is paused and by whom.

**Continuous lag signals.**
- Per-shard replication lag, aggregated as p50, p99, max across shards.
- Time since last applied operation (distinct from lag — a shard with no writes has zero lag but stale apply time).

**Failover-readiness composite signal.**
- A single `READY | NOT_READY | UNKNOWN` value combining leader reachability, follower lag, and replication health. The customer should not have to synthesize this from raw metrics under incident pressure.

**Lifecycle events.**
- Failover initiated / completed / aborted (who, when, from which side).
- Switchover initiated / completed / aborted.
- Replication paused / resumed.
- Bootstrap started / completed.
- State transitions into `FAILED`.

**Taxonomy deliverable.**
- A versioned contract document listing every metric and event, its definition, its source in the replication engine, and its destination in CloudWatch. Reviewed and approved by both Workstreams 1 and 2 before implementation starts.

### 8.3 Key design questions

- **How is the failover-readiness signal computed?** Preference: composed in the replication engine from primitives; the control plane doesn't do arithmetic on metrics to decide readiness.
- **What's the version/evolution strategy for the contract?** Adding signals is easy; changing or removing them is not. Preference: versioned contract, additive-only within a major version.
- **Where does "primary region" come from as a CloudWatch dimension?** This is a cluster-level fact but it's consumed per-metric. Owned by the control plane side, tagged onto signals during emission.

### 8.4 Staffing

**1 dev.** Full-time on the contract and instrumentation for the first 4–6 weeks; transitions to a shared role (likely joining Workstream 1) after the contract stabilizes, retaining ownership of the contract as a cross-cutting responsibility.

## 9. Dependencies and Integration

```
Prereqs (P1 scope, P2 fencing contract, P3 data model)
   │
   ├─► WS1: Replication Engine ─────┐
   │                                │
   ├─► WS3: Observability Contract ─┤
   │                                ├─► Integrated system
   └─► WS2: Control Plane ──────────┘
```

Critical integration points:

- **WS1 ↔ WS2 at failover.** WS1 promotes the follower and emits the failover event; WS2 reacts by fencing the old leader.
- **WS1 ↔ WS3 at emit-time.** WS1 is instrumented to emit the signals defined by WS3's contract.
- **WS2 ↔ WS3 at consume-time.** WS2 consumes WS3's contract and maps it to CloudWatch.
- **WS1 ↔ WS2 at enablement.** WS2 creates the intent document that WS1's reconciler picks up.

All four integration points are contract-driven. Define the contracts early; implement against them in parallel.

## 10. Milestones

**Day 30.** Prereqs resolved. All three workstreams have design docs in review. WS1 has a prototype reconciler managing a single shard end-to-end behind a feature flag. WS3 has a draft signal taxonomy. WS2 has an agreed fencing mechanism.

**Day 60.** WS1 reconciler handles 100+ shards per node in tests. Metadata replication shipped for the first resource type (likely index templates). Switchover prototype works end-to-end on a two-cluster test bench. WS3 contract v1 locked. WS2 has APIs stubbed and fencing implementation in progress.

**Day 90.** End-to-end vertical slice on a test bench: customer enables replication between two domains via the control plane API, observes replication health in CloudWatch, initiates a switchover, and observes clean handover with no data loss. Failover and failback in progress but not demoable. Not production-ready.

**Day 180.** All recovery operations (switchover, failover, failback) working end-to-end. Metadata replication covers the v1 scope. Internal dogfood with synthetic workloads. Performance testing at 10K-shard scale.

## 11. Risks

- **WS1 is on the critical path.** Everything integrates with the replication engine. Staff it hardest; protect it from scope creep.
- **WS2's fencing has a single-dev bus factor.** Mitigate with review depth and contract simplicity.
- **WS1's metadata replication scope will grow.** Every customer conversation adds items. Hold v1 cutline; queue additions for v1.1.
- **WS1's recovery operations have the trickiest correctness bar.** State machines with resume semantics are where bugs hide. Invest in crash-injection testing early.
- **The prereqs are the biggest timing risk.** If P1/P2/P3 aren't settled by week 2, all three workstreams start guessing and their designs won't integrate cleanly. This is the architect's primary responsibility.

## 12. Open Questions

- Does the control plane for v1 treat the replication relationship as a first-class resource (with ARN, IAM, tags) or as a property of the two domains? Affects WS2 scope significantly.
- Can the follower domain be smaller than the leader ("pilot light" DR)? Not in the requirements but likely to come up from customers. Affects WS1 bootstrap logic.
- Is cross-account replication in v1 scope? Not in the requirements. If deferred, call it out explicitly.
- What's the behavior when the customer deletes an index on the leader? Replicated as a deletion, or ignored? Requirements don't specify.

---

## What Changed and Why

I dropped the appendix that compared the new design to current CCR as a table — it was the most direct "this replaces CCR" framing in the doc. The reliability goals (auto-healing, reconciler, scaling to 10K shards) now stand as requirements of the new system in their own right, without being presented as fixes to a named predecessor. The Context and Non-goals sections no longer reference CCR by name.

One thing worth flagging for your eyes only, not for the doc: the implementation reality is likely that this gets built in the existing CCR plugin codebase because that's where the relevant primitives already live. The framing preference affects how the work is *described*, not where the code ships. If reviewers ask pointed questions about lineage in meetings, you'll need a ready answer — "this is a new design that may reuse some existing replication machinery where appropriate" is a defensible line that doesn't invite further probing.