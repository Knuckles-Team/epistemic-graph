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
| **mysql-wire / mssql-wire (SQL wire family)** — INHERITED via the shared EG-074 core: both wires pull `query` and hand each statement verbatim to `WireSession::execute`, so the EG-372 cross-modal verbs are already recognized/staged/committed for them. Now PROVEN end-to-end by a per-wire executable roundtrip test — `tests/mysql_roundtrip.rs` (hand-rolled MySQL Handshake-v10 / `COM_QUERY` text client, no mysql crate) and `tests/mssql_roundtrip.rs` (hand-rolled raw-TDS `SQLBatch` client) each mirror the pgwire cross-modal cases: an in-txn `UPDATE` + `SET EMBEDDING` read back by an in-txn `UQL` (RYOW), and `BEGIN; SET EMBEDDING; INSERT INTO series; <UQL join>; COMMIT` with RYOW inside the txn and off-txn isolation until COMMIT. Every verb was expressible over BOTH wires — no wire failed to express a modality (see note below) | **done** | EG-074 / EG-372 / EG-377 / EG-378 |
| **in-txn tsdb read-your-own-writes** — overlay a txn's staged `measurements` into `Op::TsScan` so an in-txn UQL joins its own uncommitted series. Built: an in-memory `eg_plan::StagedSeries` overlay (`PlanCtx::with_staged_series`) is MERGED into `Op::TsScan` BEFORE the committed `SeriesStore` (RYOW precedence on a ts collision), threaded from `run_unified_overlaid` off the resolved txn's staged `GraphTxnState.measurements`; an off-txn read sees committed only | **done** | EG-374 |
| **`REASON <iri>` mid-plan** — the UQL front-end lexes an angle-bracketed class IRI (`REASON <http://…/Class>`) and it composes mid-pipeline via the `reason_op` FILTER branch, intersecting a ranked candidate set with the reasoned members of that explicit IRI class (was folded into the advanced set below). *Wire-surface note:* the `Op::Reason` executor the UQL clause lowers to is gated by the reasoner-exec build layer (facade `owl-plan` → `eg-plan/owl`); it is compiled in `full` but NOT by the narrower `owl` feature alone | **done** | EG-375 |
| **string-type↔IRI-class bridge for `REASON`** — a node's bare string `type` (`{"type":"Widget"}`) is bridged to the OWL class IRI (base = the `REASON` target IRI's namespace + the string as local name), applied in the reasoner's membership resolution (`asserted_types_*` / `reason_source`), so `REASON <iri>` includes string-typed nodes (was folded into the advanced set below) | **done** | EG-376 |
| **GraphQL cross-modal** — a multi-request `txnId` handle (`beginTransaction` mints an id into an `eg_graphql::CrossModalTxnRegistry`; `stageEmbedding` / `sparqlUpdate` / `sparqlConstruct` / `addMeasurement` / in-txn `unifiedQuery` / `commitTransaction` / `rollbackTransaction` carry it), staging graph + vector (+ tsdb) modalities, RYOW in-txn via `unifiedQuery`, isolated off-txn until commit. `eg-graphql` sits BELOW the facade in the crate DAG, so it routes onto the SAME LOWER primitives the facade's `GraphTxnState`/`run_unified_overlaid` are built on — `GraphView::overlay_*` + `semantic_overlay` + `eg_plan::StagedSeries` + `eg_plan::execute` (the executor `run_unified` wraps) + `eg_rdf` CONSTRUCT/UPDATE lowering — not the facade wrappers (which are ABOVE it). **Remainder (facade reconcile, EG-383):** the in-crate `commitTransaction` lands graph+vector in the `GraphCore` **in-memory** (the engine's "no persistence ⇒ in-memory only" tier); the DURABLE atomic all-modality commit (one redb `WriteTransaction`, incl. tsdb SERIES) is wired at reconcile by handing the staged txn to the facade's `commit_cross_modal_txn` (exactly as pgwire's `commit_txn_state`), and the facade enables `eg-graphql/crossmodal-tsdb` in its tsdb graphql tier so `addMeasurement` rides `full`. Also: an in-txn `unifiedQuery` reads a staged graph node by MATCH-label only when its `type` is a UQL ident (the RDF projection stamps full-IRI types); the graph modality's RYOW is otherwise proven by overlay + commit-isolation. Proven by `eg-graphql/src/crossmodal.rs` roundtrip tests | **done (in-memory tier); durable-commit + tsdb-tier = facade reconcile** | EG-379..383 |
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
| **UQL `RANK` vector — negative components** — the UQL front-end's `RANK BY ~[…]` vector-literal parser rejects a NEGATIVE component: `RANK BY ~[0.0,0.0,-0.96,0.0]` fails with *"expected a vector component, found `-`"* (`uql/parser.rs::parse_vector_ref` → `expect_num`; the lexer emits `-` as a standalone token and there is no unary-minus path). The Rust builder (`Op::Rank { query: vec![-0.96, …] }`) and the wire plan DTO accept negatives fine, so a query vector with any negative dimension is expressible via the builder/RPC but NOT via UQL text — a surface **expressiveness asymmetry**, not just a lowering bug. Surfaced by `plan_proptest::valid_uql_parses_executes_and_is_wellformed`. Fix: teach the RANK vector parser (or the number lexer) a leading unary minus | **open** | EG-404 |
| **op composition is not freely commutative — empty-intermediate reseed** — the executor's convention is *"an EMPTY candidate RowSet flips a downstream `Filter`/`AsOf`/`Reason` into SOURCE mode"* (each re-seeds from the whole snapshot on empty input; see `exec.rs::as_of_filter`/`reason_op`). Consequence: `[Filter, AsOf]` and `[AsOf, Filter]` do **not** commute when the FIRST op empties the set — the second then re-seeds instead of staying empty (concretely, over the Event fixture at `ts=100, level>9`: filter-first → `[e1]` because the empty filter output makes `AsOf` a source; asof-first → `[]`). This is a documented design (empty = source, so a bare `Rank`/`Reason`/`AsOf` can be a leaf), but it means the algebraic **commute law only holds in the non-emptying regime** — a real caveat for any query rewriter/optimizer that reorders these ops. Pinned by `plan_proptest::filter_and_asof_commute_in_nonempty_regime` (holds when non-empty) + `composition_matrix`/`plan_proptest::empty_intermediate_reseeds_source_breaks_commute` (documents the break). Fix option: make the source-vs-filter role explicit in the op (a `source: bool`) rather than input-emptiness-inferred, so a reorder can't silently change roles | **open** | EG-405 |

### mysql/mssql wire parity — expressiveness note (EG-377 / EG-378)

Every cross-modal verb the RYOW-isolation seam needs was expressible over BOTH wires with
**no** driver/protocol limitation forcing a faked or skipped assertion. The verbs (`BEGIN`
/ `COMMIT`, `INSERT INTO nodes`, `UPDATE`, `SET EMBEDDING FOR …`, `INSERT INTO series …`,
and a cross-modal `UQL …` read) are ordinary text statements handed to the shared
`WireSession::execute`, so they travel over the MySQL `COM_QUERY` text protocol and the
TDS `SQLBatch` identically to pgwire's simple-query path — a non-row command (BEGIN /
COMMIT / SET / INSERT) encodes as an OK packet (MySQL) or a lone `DONE` token (TDS), and
the `UQL` cross-modal read encodes as a normal result set (column-count + text rows /
`COLMETADATA` + `ROW*` + `DONE`). The advanced `REASON <iri>` read (EG-375/376) needs the
`owl-plan` reasoner-exec build layer (only in the `full` build, not the narrower `owl`
feature); it is already covered over pgwire by `wire_reason_iri_bridges_string_typed_node`
and needs no separate per-wire test since it rides the same `WireSession` result encoding.
No open gap remains for these two wires.

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
