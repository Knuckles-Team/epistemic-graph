# ADR: Attached sources are the default; EG stays the one converged engine

**Status:** Accepted · **Requirement:** `EG-UNIFIED-DATA-PLANE-R001` (the
separate pgrx go/no-go decision is `EG-UNIFIED-DATA-PLANE-R025`)

## Context

[`specs/unified-data-plane/spec.md`](../../specs/unified-data-plane/spec.md) and
[`plan.md`](../../specs/unified-data-plane/plan.md) already state four load-bearing
architecture decisions in prose, scattered across "Outcome and boundaries," the
query contract and the native-path dependency section. `EG-UNIFIED-DATA-PLANE-R001`
asks that these be recorded as reviewed architecture decision records, not that they
be decided afresh. This ADR is that record: each decision below quotes the merged
spec/plan text it formalizes, so review can confirm the record against its source
rather than against a new unreviewed claim.

## Decision 1 — Attached sources are the default integration path

Third-party applications keep their own database. EG does not take over an
application's primary store by default; it attaches to it.

> "Third-party applications keep their real database. EG attaches to each through a
> single owner-scoped registry, serves live SQL and named virtual graphs, optionally
> maintains a change-captured accelerated copy, and exposes a freshness-aware query
> route." — spec.md, "Outcome and boundaries"

**Consequence:** every new application integration starts as an attached source
(registry + OBDA/SQL federation + optional CDC), never as a native-storage request.
Native hosting is a separate, later-earned track (Decision 2), not an alternative
starting point.

## Decision 2 — EG remains the single converged query engine

EG is the one query surface across every modality for EG-owned data; attaching a
source does not fork a second query engine or a parallel storage authority.

> "EG is the converged engine for EG-owned data: property graph, SQL, RDF/OWL,
> vector, time series, provenance and reasoning over one durable authority and one
> query surface." — spec.md, "Outcome and boundaries"

**Consequence:** SQL federation, OBDA/R2RML virtual graphs, change capture and the
accelerated copy all project into this one engine's query surface (SPARQL, UQL,
Cypher, REASON) rather than standing up source-specific query paths.

## Decision 3 — Native hosting is earned per application through a defined gate

Hosting an application's data natively in EG (rather than attaching its existing
database) is an explicit, per-application admission track, not a default outcome of
attaching a source.

> "Native hosting is an app-specific admission track, first measured with Gramps.
> Immich is an API connector and entity-resolution pilot; it is not a
> native-hosting candidate." — spec.md, "Outcome and boundaries"
>
> "This spec does not authorize a production cutover... P6 Gramps cutover requires
> the operator's go-ahead after P0–P5 evidence." — spec.md, "Outcome and boundaries"

**Consequence:** the Gramps pilot (`EG-UNIFIED-DATA-PLANE-R026`, phases P0–P5) is the
one evidence-gated path by which an application earns native hosting; no other
attached application is admitted natively without the same evidence and an explicit
operator go-ahead (`EG-UNIFIED-DATA-PLANE-R033`).

## Decision 4 — The router's consistency contract

A query over an attached source is answered from exactly one of three freshness
tiers (native, live, accelerated), selected by declared policy, and a
read-your-writes request never silently returns a stale result.

> "The router compares freshness requirement, source position, copy position and
> latency policy before selecting native/live/accelerated. A bounded wait for the
> caller's last source commit is required for read-your-writes; timeout is a typed
> failure or policy-authorized live retry, never a silent stale result." — plan.md,
> "Contracts and data flow" §4

**Consequence:** `EG-UNIFIED-DATA-PLANE-R011`'s router and its `EXPLAIN` output are
built to this contract: every explained plan states its chosen route and source/copy
position, and a timed-out freshness wait fails typed or falls back only under an
explicit policy — never silently.

## Decision 5 — The pgrx companion's scope boundary

The pgrx companion extension is a separate, explicitly-scoped spike, not a second
native storage authority growing in parallel with the attached-source data plane.

> "The pgrx companion remains a separate spike admitted only by ADR; this data-plane
> plan does not implement a second native store." — plan.md, "Contracts and data
> flow" §7
>
> "`durable-graph-kernel` owns the mixed graph/table atomicity ADR... The separate
> pgrx spike cannot become a parallel database authority by default." — plan.md,
> "Native transaction and storage dependency"

**Consequence:** this spec's attached-source and native-hosting work never depends on
pgrx; pgrx's own go/no-go is decided separately, by `EG-UNIFIED-DATA-PLANE-R025`'s
spike ADR.

## Consequences (summary)

These five decisions bound every requirement in `specs/unified-data-plane`: attach
first (1), through the one EG query surface (2), with native hosting earned per
application by evidence and operator go-ahead (3), served by a router that is
explicit about freshness and never silently stale (4), and never by implicitly
growing the pgrx spike into a second storage authority (5). A future change to any
of these five needs its own reviewed ADR, not a quiet reinterpretation of this one.
