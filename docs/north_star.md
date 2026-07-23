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

## Verified seam inventory

The canonical in-transaction cross-modal seam — *stage graph + vector + OWL + CONSTRUCT
+ timeseries in one txn, read your own writes, commit atomically in one redb
`WriteTransaction`* — tracked across every surface:

| Seam × surface | Status | Concept |
|----------------|--------|---------|
| **RPC / native** — `TxnUnifiedQuery{,Text}` overlay + `TxnAddMeasurement`/`TxnAxiom`/`TxnConstruct` staging + one-`WriteTransaction` commit | **done** | EG-359..363 |
| **pgwire (SQL wire)** — `UQL …` / `SET EMBEDDING FOR …` / `INSERT INTO series …` / `SPARQL UPDATE …` / `SPARQL CONSTRUCT …` inside `BEGIN…COMMIT`, RYOW + atomic cross-modal commit, routed onto the committed RPC seam via the shared `WireSession` | **done** | EG-KG.txn.isolation-ryow-begin-set |
| **mysql-wire / mssql-wire (SQL wire family)** — INHERITED via the shared EG-KG.compute.subsystems-reference core: both wires pull `query` and hand each statement verbatim to `WireSession::execute`, so the EG-KG.txn.isolation-ryow-begin-set cross-modal verbs are already recognized/staged/committed for them. Now PROVEN end-to-end by a per-wire executable roundtrip test — `tests/mysql_roundtrip.rs` (hand-rolled MySQL Handshake-v10 / `COM_QUERY` text client, no mysql crate) and `tests/mssql_roundtrip.rs` (hand-rolled raw-TDS `SQLBatch` client) each mirror the pgwire cross-modal cases: an in-txn `UPDATE` + `SET EMBEDDING` read back by an in-txn `UQL` (RYOW), and `BEGIN; SET EMBEDDING; INSERT INTO series; <UQL join>; COMMIT` with RYOW inside the txn and off-txn isolation until COMMIT. Every verb was expressible over BOTH wires — no wire failed to express a modality (see note below) | **done** | EG-KG.compute.subsystems-reference / EG-KG.txn.isolation-ryow-begin-set / EG-KG.query.eg-8 / EG-KG.query.per-surface-parity |
| **in-txn tsdb read-your-own-writes** — overlay a txn's staged `measurements` into `Op::TsScan` so an in-txn UQL joins its own uncommitted series. Built: an in-memory `eg_plan::StagedSeries` overlay (`PlanCtx::with_staged_series`) is MERGED into `Op::TsScan` BEFORE the committed `SeriesStore` (RYOW precedence on a ts collision), threaded from `run_unified_overlaid` off the resolved txn's staged `GraphTxnState.measurements`; an off-txn read sees committed only | **done** | EG-KG.query.txn-tsdb-read-your |
| **`REASON <iri>` mid-plan** — the UQL front-end lexes an angle-bracketed class IRI (`REASON <http://…/Class>`) and it composes mid-pipeline via the `reason_op` FILTER branch, intersecting a ranked candidate set with the reasoned members of that explicit IRI class (was folded into the advanced set below). *Wire-surface note:* the `Op::Reason` executor the UQL clause lowers to is gated by the reasoner-exec build layer (facade `owl-plan` → `eg-plan/owl`); it is compiled in `full` but NOT by the narrower `owl` feature alone | **done** | EG-KG.query.reason-iri-parses-angle |
| **string-type↔IRI-class bridge for `REASON`** — a node's bare string `type` (`{"type":"Widget"}`) is bridged to the OWL class IRI (base = the `REASON` target IRI's namespace + the string as local name), applied in the reasoner's membership resolution (`asserted_types_*` / `reason_source`), so `REASON <iri>` includes string-typed nodes (was folded into the advanced set below) | **done** | EG-KG.ontology.string-type-iri-class |
| **GraphQL cross-modal** — `beginTransaction` mints a restart-safe UUID and stores the transaction under `(verified owner scope, txnId)`; every stage/read/rollback/commit lookup requires that same owner scope. Staged reads use an RLS-projected committed snapshot plus only that transaction's overlay. `commitTransaction` must be the mutation's sole root field and is unavailable in the lower crate: the facade takes the owner-bound buffers, authorizes graph and tenant again, and lands graph, vector, OWL/RDF, and timeseries changes through the authoritative durable `MutationBatch` path before publishing RAM state. SPARQL UPDATE and CONSTRUCT share `eg_rdf::mapping::lower_triples`, so every surface preserves the same repeated-literal/multivalue projection. The `full` tier includes the timeseries leg. Proven by lower-crate owner/isolation tests and the durable-reopen facade roundtrip | **done (owner-bound, RLS, durable)** | EG-379..383 / EG-KG.query.facade-reconcile-hook |
| **UQL `RANK BY ~ "text"` — server-side NL→vector** — a quoted rank ref lowers to `Op::RankEmbed` and is resolved at exec time by a `TextEmbedder` bound on the `PlanCtx` (`with_embedder`), turning the text into a query vector kNN-ranked exactly like a literal `~[…]`. With no embedder bound it is a clean typed error (never a panic); the facade injection point (`run_unified` → `with_embedder`) is where a real embedding model binds (deterministic `HashEmbedder` fallback opt-in via `EG_UQL_TEXT_EMBEDDER=hash`). Was: `~"…"` errored at parse — "no server-side embedder resolver" | **done** | EG-KG.compute.no-embedder-bound-op/412 |
| **UQL `WINDOW` — real tumbling time-series aggregate** — `WINDOW <secs>` now CONSUMES a RowSet of `(ts,value)` rows (e.g. from `Op::TsScan`, or graph-node rows with `valid_from`+`value`/`score`) and PRODUCES one row per non-empty tumbling bucket (`id` = aligned bucket start, `score` = the aggregate) via eg-tsdb's `time_bucket`, composing downstream into `Rank`/`Limit`; `WINDOW <secs> <agg>` (`Op::WindowAgg`) selects the aggregate (mean/sum/min/max/count/first/last). Was: a RowSet-preserving passthrough. Wired under `timeseries`; a non-`timeseries` build passes through | **done** | EG-KG.compute.tsscan-series-window-60s/414 |
| **UQL negative vector components** — `RANK BY ~[-0.1, 0.2, -0.3]` parses (a leading `-` negates the component) to the SAME `Op::Rank` the Rust builder / wire DTO always accepted; closes a lexer/parser asymmetry (the builder took negatives, UQL rejected them) | **done** | EG-KG.compute.negative-vector-component-parses |
| **UQL `FUSE` stage dispatch** — the UQL parser now dispatches `FUSE [branch] [branch] …` to the SAME `Op::FuseRrf { branches, k: 0.0 }` the builder/wire construct (RRF hybrid was builder/wire-only before, though the grammar listed it); each bracketed sub-pipeline is a branch, `k=0.0` ⇒ the `RRF_K` default. Feature-gated to `text` | **done** | EG-KG.compute.fuse-stage-now-dispatches |
| **libFuzzer UQL-parser target** (was EG-426, deferred) — a `cargo-fuzz`/libFuzzer target at `crates/eg-plan/fuzz/` (`fuzz_targets/uql_parse.rs`) feeds ARBITRARY BYTES to `eg_plan::uql::parse`, shaking out parser panics/hangs on malformed input — the UNSTRUCTURED-byte complement to the proptest VALID-pipeline harness (EG-KG.query.pipeline-fuzz). The fuzz crate is a detached workspace (`[workspace]`) so it stays OFF the stable `cargo test` gate (libFuzzer needs nightly); run `cargo +nightly fuzz run uql_parse -- -runs=10000`. Documented in `docs/benchmarks.md` | **done** | EG-KG.query.uql-libfuzzer-parse-target |
| **advanced cross-modal correctness** — bitemporal decay, vector↔reasoning consistency, SPARQL-update visibility, and the richer fused-plan cases below are covered by executable plan and served-transport suites | **done** | EG-365..370 / EG-384..389 |
| **advanced cross-modal — PlanCtx unit set (6)** — the richest 3+-modality FUSED plans over a hand-built `PlanCtx`: bitemporal `AsOf→Rank→Reason<iri>→Traverse` (384), federation `Scan→Filter(SQL)→ForeignScan→Rank` + fail-closed named source (385), geo×vector×temporal `SpatialScan→DWithin→Rank→AsOf` (386), tensor×graph×vector + CAS dedup (387), CEP×graph×tsdb `Scan→Cep→Traverse→Window` (388), probabilistic×OWL×vector + MMR (389). All GREEN in `crates/eg-plan/src/advanced_crossmodal_tests.rs` | **done** | EG-384..389 |
| **advanced cross-modal — 5-modality in-txn RYOW capstone** — node + embedding + edge + tsdb measurement + OWL axiom staged in ONE txn: RYOW-visible in-txn (Scan\|>Rank / Traverse / TsScan-over-`StagedSeries` / Reason-over-staged-node), isolated off-txn, committed atomically, re-read off-txn incl `REASON` over the committed TBox. GREEN via the native `dispatch` harness (`tests/advanced_crossmodal_roundtrip.rs`) | **done** | EG-KG.txn.one-transaction-stage-five |
| **advanced cross-modal — concurrent serializable phantom** — a serializable txn with a captured predicate read-set rolls back (`Commit`→false) when a concurrent txn commits a phantom matching the predicate; its staged cross-modal write never lands. GREEN | **done** | EG-KG.txn.serializable-txn-captures-predicate |
| **RPC extended-staging routing** — LEAK closed (surfaced by the EG-KG.txn.one-transaction-stage-five capstone): `TxnAddMeasurement`/`TxnAxiom`/`TxnConstruct` handlers existed but `dispatch.rs` never routed them → they worked over pgwire (EG-372) but errored over native RPC. Added three `cfg`-gated dispatch arms → `handlers::txn::try_handle`. The extended cross-modal staging is now reachable uniformly at RPC + pgwire | **done** | EG-KG.compute.eg-187 |
| **advanced cross-modal — RLS on fused Reason→Rank + overlay** — hide per-agent-invisible rows from BOTH the committed AND the staged-overlay legs of a fused in-txn `Reason→Rank`. GREEN: RLS fixture (identities + `_owner`/`_visibility` node blobs) + caller-threaded `agent_id` (`req_as`) in `tests/advanced_crossmodal_roundtrip.rs`; the seam that made the OVERLAY leg honor RLS is a post-overlay `rls.filter_view` in `run_unified_overlaid` (`src/server/handlers/query.rs`) — the committed base was already filtered, the staged overlay was not | **done** | EG-KG.compute.eg-181, EG-KG.query.overlay-leg-rls-filter |
| **advanced cross-modal — pgwire + /sparql + native snapshot** — three surfaces observe ONE committed cross-modal snapshot. GREEN: pgwire listener + tokio-postgres client, a `/sparql` HTTP listener, and native `dispatch` all bound to the SAME in-process `ServerState`; after ONE mixed commit (node+embedding+node+`subClassOf` edge+axiom) all three see it, and before commit none do (`tests/advanced_crossmodal_roundtrip.rs`) | **done** | EG-KG.compute.eg-182, EG-KG.query.tri-surface-snapshot-harness |
| **advanced cross-modal — encryption-at-rest wrong-key fail** — a keyed `RedbBackend` reopened with the wrong key must error on the cross-modal read (no silent plaintext). GREEN: `EPISTEMIC_GRAPH_ENCRYPTION_KEY`=K1 keyed cross-modal `commit_crossmodal` → reopen K1 decrypts the fused `read_graph_dump` (nodes+edges+semantic) → reopen with WRONG key K2 fails `read_node` (`tests/advanced_crossmodal_roundtrip.rs`); raw `.redb` bytes never hold the plaintext secret | **done** | EG-KG.compute.eg-183, EG-KG.storage.encryption-reopen-roundtrip |
| **advanced cross-modal — streaming/CDC live view rebuild** — a cross-modal write's CDC events rebuild a live materialized view. GREEN: a `CdcHub` continuous query (the streaming-native live view in `full`; the `MatViewStore` variant requires the `cluster` feature's `compute-dist`/`raft` layer) subscribed to `state.cdc` is maintained INCREMENTALLY off the change stream of a cross-modal write, and a subsequent native read reflects the same state (`tests/advanced_crossmodal_roundtrip.rs`) | **done** | EG-KG.compute.eg-184, EG-KG.query.cdc-live-view-rebuild |
| **advanced cross-modal — cross-shard Raft single decision** — typed participant PREPARE/COMMIT/ABORT, durable decision/finalize, placement-epoch and fencing validation, authenticated forwarding, and recovery produce one all-or-nothing result across graph, RDF, timeseries, blob/KV, table/catalog, work, and control-state domains. The in-process multi-group fault harness covers both coordinator crash windows. Cross-host packet-loss, clock-skew, and failover runs are exact-release certification prerequisites executed with the shipped cluster scripts; they are not missing transaction source code | **done; external qualification required for a production release** | EG-KG.compute.eg-185 / EG-KG.txn.crossshard-2pc-modality-harness |
| **advanced cross-modal — KV-cache warm-fork fan-out** — the engine supplies governed KV pages and cross-modal context; Agent Utilities owns and implements the warm-parent/forkserver execution primitive. The cross-repository contract is closed without duplicating process-fork authority inside the database | **done across the two owning projects** | EG-KG.compute.eg-186 |
| **UQL `RANK` negative components** — the text parser accepts optional unary minus per vector component and matches the builder/wire representation; parser and property tests cover the current grammar | **done** | EG-KG.compute.eg-189 / EG-KG.compute.negative-vector-component-parses |
| **op composition is not freely commutative — empty-intermediate reseed (INTENTIONAL semantics)** — the executor's convention is *"an EMPTY candidate RowSet flips a downstream `Filter`/`AsOf`/`Reason` into SOURCE mode"* (each re-seeds from the whole snapshot on empty input; see `exec.rs::as_of_filter`/`reason_op`). Consequence: `[Filter, AsOf]` and `[AsOf, Filter]` do **not** commute when the FIRST op empties the set — the second then re-seeds instead of staying empty (concretely, over the Event fixture at `ts=100, level>9`: filter-first → `[e1]` because the empty filter output makes `AsOf` a source; asof-first → `[]`). DETERMINATION: this is **intended semantics, NOT a bug** — the empty⇒source convention is what lets a BARE `Rank`/`Reason`/`AsOf`/`Filter` be a leaf SOURCE (a plan seeded by a single op with no upstream Scan), the property the whole plan algebra relies on. The commute law holds in the non-emptying regime, and the cost optimizer already GUARDS this boundary: it only reorders an adjacent narrower-vs-`Rank` pair whose input comes from a source and where BOTH intermediates stay ≥ 1 row (never narrower-vs-narrower — the exact EG-405 witness), so no rewrite can silently flip a role (`optimizer.rs` module doc + `reorder_narrower_rank_pairs` guard). Documented in `docs/uql.md` (Composition & commutativity); the witness `plan_proptest::empty_intermediate_reseeds_source_breaks_commute` + `filter_and_asof_commute_in_nonempty_regime` are kept AS THE SPEC (a future change must preserve the behavior or update the spec + this row) | **intentional semantics (documented)** | EG-KG.compute.eg-190, EG-KG.query.empty-set-commutativity |

### mysql/mssql wire parity — expressiveness note (EG-KG.query.eg-8 / EG-KG.query.per-surface-parity)

Every cross-modal verb the RYOW-isolation seam needs was expressible over BOTH wires with
**no** driver/protocol limitation forcing a faked or skipped assertion. The verbs (`BEGIN`
/ `COMMIT`, `INSERT INTO nodes`, `UPDATE`, `SET EMBEDDING FOR …`, `INSERT INTO series …`,
and a cross-modal `UQL …` read) are ordinary text statements handed to the shared
`WireSession::execute`, so they travel over the MySQL `COM_QUERY` text protocol and the
TDS `SQLBatch` identically to pgwire's simple-query path — a non-row command (BEGIN /
COMMIT / SET / INSERT) encodes as an OK packet (MySQL) or a lone `DONE` token (TDS), and
the `UQL` cross-modal read encodes as a normal result set (column-count + text rows /
`COLMETADATA` + `ROW*` + `DONE`). The advanced `REASON <iri>` read (EG-KG.query.reason-iri-parses-angle/376) needs the
`owl-plan` reasoner-exec build layer (only in the `full` build, not the narrower `owl`
feature); it is already covered over pgwire by `wire_reason_iri_bridges_string_typed_node`
and needs no separate per-wire test since it rides the same `WireSession` result encoding.
No open gap remains for these two wires.

## Closed seam findings

- **Confidence time decay is part of the fused plan.** `Op::Reason` receives the
  bound decay context, so one plan can combine bitemporal liveness and decayed
  confidence. The plan tests execute the same reason operation at multiple times.
- **Served lexical ranking binds a real BM25 index.** `run_unified` builds the
  snapshot-consistent text view only for plans that need it, and served ranking/RRF
  tests prove lexical hits contribute to the result.

## High-value use-case validation suites (EG-KG.query.usecase-agent-memory..EG-KG.query.usecase-kg-lifecycle)

Five end-to-end suites (external-review "High-Value Use Cases", handoff-1 track G) that
stress the cross-modal seams by demonstrating unique value — each builds a small
deterministic dataset and runs a hybrid query through the REAL engine (`eg_plan::execute`
over a live `PlanCtx`, and/or the served `dispatch`), asserting the fused result honors
EVERY modality it spans:

| Suite | File | Modalities fused | Concept |
|-------|------|------------------|---------|
| agent-memory retrieval | `tests/usecase_agent_memory.rs` | semantic + lexical(BM25) + graph-expansion + OWL + bi-temporal `AS OF` + RRF (+ decay on the reasoner surface) | EG-KG.query.usecase-agent-memory |
| codebase / repository intelligence | `tests/usecase_codebase_intel.rs` | vector(signature) + call-graph traverse + bi-temporal `AS OF` + OWL(language ontology) | EG-KG.query.usecase-codebase-intel |
| hybrid RAG + in-engine analytics | `tests/usecase_hybrid_rag_analytics.rs` | graph+vector+text fused retrieval → in-engine PCA/kmeans (`DsPca`/`DsKMeans`, compute-near-data) | EG-KG.query.usecase-hybrid-rag-analytics |
| observability / root-cause | `tests/usecase_observability_rca.rs` | service-dependency graph traverse + error-log vectors + native tsdb `TsScan`→`WindowAgg` | EG-KG.query.usecase-observability-rca |
| KG lifecycle w/ validation & inference | `tests/usecase_kg_lifecycle.rs` | SHACL validation + cross-modal ACID txn (graph+vector+OWL axiom) + inference closure + vector re-index under concurrent hybrid read/write | EG-KG.query.usecase-kg-lifecycle |

The two rows above (EG-KG.query.decay-not-foldable-finding, EG-KG.query.served-text-index-unbound-finding) are the REAL seam gaps these suites surfaced — both now **closed** (EG-KG.query.reason-decay-in-plan folds decay into a fused `Reason` plan; EG-KG.query.served-text-index-binding binds a BM25 index into the served `run_unified`).

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
