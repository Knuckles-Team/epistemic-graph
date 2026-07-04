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
| **GraphQL cross-modal** — a GraphQL mutation/query that spans modalities in one operation | **open** | KG-2.235 |
| **advanced cross-modal correctness** — the advanced cross-modal tests in `workspace/plans/unified-txn-seam-plan.md` (bitemporal AsOf × decay, vector⇄reasoning write→read consistency, SPARQL-UPDATE→reasoning visibility, …). The string-type↔IRI-class bridge for `REASON <iri>` split OUT to its own done row above (EG-375/376) | **open** | EG-365..370 |
| **advanced cross-modal — PlanCtx unit set (6)** — the richest 3+-modality FUSED plans over a hand-built `PlanCtx`: bitemporal `AsOf→Rank→Reason<iri>→Traverse` (384), federation `Scan→Filter(SQL)→ForeignScan→Rank` + fail-closed named source (385), geo×vector×temporal `SpatialScan→DWithin→Rank→AsOf` (386), tensor×graph×vector + CAS dedup (387), CEP×graph×tsdb `Scan→Cep→Traverse→Window` (388), probabilistic×OWL×vector + MMR (389). All GREEN in `crates/eg-plan/src/advanced_crossmodal_tests.rs` | **done** | EG-384..389 |
| **advanced cross-modal — 5-modality in-txn RYOW capstone** — node + embedding + edge + tsdb measurement + OWL axiom staged in ONE txn: RYOW-visible in-txn (Scan\|>Rank / Traverse / TsScan-over-`StagedSeries` / Reason-over-staged-node), isolated off-txn, committed atomically, re-read off-txn incl `REASON` over the committed TBox. GREEN via the native `dispatch` harness (`tests/advanced_crossmodal_roundtrip.rs`) | **done** | EG-390 |
| **advanced cross-modal — concurrent serializable phantom** — a serializable txn with a captured predicate read-set rolls back (`Commit`→false) when a concurrent txn commits a phantom matching the predicate; its staged cross-modal write never lands. GREEN | **done** | EG-392 |
| **RPC extended-staging routing** — LEAK closed (surfaced by the EG-390 capstone): `TxnAddMeasurement`/`TxnAxiom`/`TxnConstruct` handlers existed but `dispatch.rs` never routed them → they worked over pgwire (EG-372) but errored over native RPC. Added three `cfg`-gated dispatch arms → `handlers::txn::try_handle`. The extended cross-modal staging is now reachable uniformly at RPC + pgwire | **done** | EG-398 |
| **advanced cross-modal — RLS on fused Reason→Rank + overlay** — hide per-agent-invisible rows from BOTH the committed and staged-overlay legs of a fused in-txn plan. `filter_view` exists (`isolation.rs`); the deferred lift is an RLS fixture + caller threading in the test harness | **open** | EG-391 |
| **advanced cross-modal — pgwire + /sparql + native snapshot** — three surfaces observe ONE committed cross-modal snapshot; needs the multi-listener test harness (committed machinery exists, EG-372) | **open** | EG-393 |
| **advanced cross-modal — encryption-at-rest wrong-key fail** — a keyed `RedbBackend` reopened with the wrong key must error on the cross-modal read (no silent plaintext); needs the keyed open/reopen roundtrip (backend exists) | **open** | EG-394 |
| **advanced cross-modal — streaming/CDC matview rebuild** — a cross-modal commit's CDC events rebuild a materialized view; needs a `CdcHub` subscription + `MatViewStore` rebuild assertion (both surfaces exist) | **open** | EG-395 |
| **advanced cross-modal — cross-shard Raft 2PC single decision** — kill the coordinator mid-2PC → all-commit-or-all-abort. GENUINE GAP: no in-process multi-node openraft cluster + 2PC-kill test harness exists in `tests/` | **open** | EG-396 |
| **advanced cross-modal — KV-cache warm-fork fan-out** — fork a warm parent holding fused cross-modal context to N children. CROSS-REPO GAP: the warm-fork primitive lives in agent-utilities, not this engine | **open** | EG-397 |

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
