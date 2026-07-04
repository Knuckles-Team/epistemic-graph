# North Star: Seamless

> **Every cross-modal seam is fully implemented at EVERY surface — never merely flagged.**

The epistemic-graph engine unifies OWL/RDF, vectors, graph, timeseries, relational
SQL, and blobs into ONE store. A *cross-modal seam* is any write→read path that
crosses those modalities — e.g. *stage a graph node + its embedding + an OWL axiom in
one transaction, then read them back together*. The engine exposes many surfaces: the
native RPC/MessagePack transport, the SQL wire family (pgwire, mysql-wire, mssql-wire,
…), SPARQL, and GraphQL.

**The principle.** When a cross-modal seam is discovered, it is implemented **fully at
every surface it can reach**, not just at the one surface it was first built for and a
feature-flag/TODO left for the rest. A seam that works over RPC but errors over psql is
not done — it is a leak. "Seamless" means a user reaches the same cross-modal power
through whatever surface they already speak (a Postgres driver, a SPARQL client, an
agent over RPC), and the underlying seam is the SAME committed machinery underneath —
each surface is a thin parser/router onto it, never a re-implementation.

This is the discipline that keeps the "one unified engine" claim honest: the union of
modalities is only real if it is reachable, uniformly, from every door into the engine.

## The seam backlog

The canonical in-transaction cross-modal seam — *stage graph + vector + OWL + CONSTRUCT
+ timeseries in one txn, read your own writes, commit atomically in one redb
`WriteTransaction`* — tracked across every surface:

| Seam × surface | Status | Concept |
|----------------|--------|---------|
| **RPC / native** — `TxnUnifiedQuery{,Text}` overlay + `TxnAddMeasurement`/`TxnAxiom`/`TxnConstruct` staging + one-`WriteTransaction` commit | **done** | EG-359..363 |
| **pgwire (SQL wire)** — `UQL …` / `SET EMBEDDING FOR …` / `INSERT INTO series …` / `SPARQL UPDATE …` / `SPARQL CONSTRUCT …` inside `BEGIN…COMMIT`, RYOW + atomic cross-modal commit, routed onto the committed RPC seam via the shared `WireSession` | **done (this PR)** | EG-372 |
| **mysql-wire / mssql-wire (SQL wire family)** — INHERITED via the shared EG-074 core: both wires pull `query` and hand each statement verbatim to `WireSession::execute`, so the EG-372 cross-modal verbs are already recognized/staged/committed for them. Remaining: a per-wire executable roundtrip **test** confirming each encodes the UQL `RowSet` + cross-modal command tags (pgwire is proven end-to-end; mysql/mssql are structurally covered, test-pending) | **core done; wire test pending** | EG-074 / EG-372 |
| **in-txn tsdb read-your-own-writes** — overlay a txn's staged `measurements` into `Op::TsScan` so an in-txn UQL joins its own uncommitted series. Built: an in-memory `eg_plan::StagedSeries` overlay (`PlanCtx::with_staged_series`) is MERGED into `Op::TsScan` BEFORE the committed `SeriesStore` (RYOW precedence on a ts collision), threaded from `run_unified_overlaid` off the resolved txn's staged `GraphTxnState.measurements`; an off-txn read sees committed only | **done** | EG-374 |
| **`REASON <iri>` mid-plan** — the UQL front-end lexes an angle-bracketed class IRI (`REASON <http://…/Class>`) and it composes mid-pipeline via the `reason_op` FILTER branch, intersecting a ranked candidate set with the reasoned members of that explicit IRI class (was folded into the advanced set below). *Wire-surface note:* the `Op::Reason` executor the UQL clause lowers to is gated by the reasoner-exec build layer (facade `owl-plan` → `eg-plan/owl`); it is compiled in `full` but NOT by the narrower `owl` feature alone | **done** | EG-375 |
| **string-type↔IRI-class bridge for `REASON`** — a node's bare string `type` (`{"type":"Widget"}`) is bridged to the OWL class IRI (base = the `REASON` target IRI's namespace + the string as local name), applied in the reasoner's membership resolution (`asserted_types_*` / `reason_source`), so `REASON <iri>` includes string-typed nodes (was folded into the advanced set below) | **done** | EG-376 |
| **GraphQL cross-modal** — a multi-request `txnId` handle (`beginTransaction` mints an id into an `eg_graphql::CrossModalTxnRegistry`; `stageEmbedding` / `sparqlUpdate` / `sparqlConstruct` / `addMeasurement` / in-txn `unifiedQuery` / `commitTransaction` / `rollbackTransaction` carry it), staging graph + vector (+ tsdb) modalities, RYOW in-txn via `unifiedQuery`, isolated off-txn until commit. `eg-graphql` sits BELOW the facade in the crate DAG, so it routes onto the SAME LOWER primitives the facade's `GraphTxnState`/`run_unified_overlaid` are built on — `GraphView::overlay_*` + `semantic_overlay` + `eg_plan::StagedSeries` + `eg_plan::execute` (the executor `run_unified` wraps) + `eg_rdf` CONSTRUCT/UPDATE lowering — not the facade wrappers (which are ABOVE it). **Remainder (facade reconcile, EG-383):** the in-crate `commitTransaction` lands graph+vector in the `GraphCore` **in-memory** (the engine's "no persistence ⇒ in-memory only" tier); the DURABLE atomic all-modality commit (one redb `WriteTransaction`, incl. tsdb SERIES) is wired at reconcile by handing the staged txn to the facade's `commit_cross_modal_txn` (exactly as pgwire's `commit_txn_state`), and the facade enables `eg-graphql/crossmodal-tsdb` in its tsdb graphql tier so `addMeasurement` rides `full`. Also: an in-txn `unifiedQuery` reads a staged graph node by MATCH-label only when its `type` is a UQL ident (the RDF projection stamps full-IRI types); the graph modality's RYOW is otherwise proven by overlay + commit-isolation. Proven by `eg-graphql/src/crossmodal.rs` roundtrip tests | **done (in-memory tier); durable-commit + tsdb-tier = facade reconcile** | EG-379..383 |
| **advanced cross-modal correctness** — the advanced cross-modal tests in `workspace/plans/unified-txn-seam-plan.md` (bitemporal AsOf × decay, vector⇄reasoning write→read consistency, SPARQL-UPDATE→reasoning visibility, …). The string-type↔IRI-class bridge for `REASON <iri>` split OUT to its own done row above (EG-375/376) | **open** | EG-365..370 |

## What "done" means for a seam

A seam row flips to **done** only when, at that surface:

1. the surface has a real entrypoint for every verb the seam needs (no "not supported"
   error path that silently drops a modality);
2. it routes onto the **committed** shared machinery — no duplicated txn/plan/commit
   logic (the surface is a thin parser/router);
3. read-your-own-writes and atomic commit hold across the modalities the seam spans, and
   are proven by an executable test at that surface.

When a seam cannot yet be expressed cleanly at some surface, that gap is written **here**
as an explicit open row with its concept id — a tracked seam item, never an unowned
`TODO` buried in code.
