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
| **UQL `RANK BY ~ "text"` — server-side NL→vector** — a quoted rank ref lowers to `Op::RankEmbed` and is resolved at exec time by a `TextEmbedder` bound on the `PlanCtx` (`with_embedder`), turning the text into a query vector kNN-ranked exactly like a literal `~[…]`. With no embedder bound it is a clean typed error (never a panic); the facade injection point (`run_unified` → `with_embedder`) is where a real embedding model binds (deterministic `HashEmbedder` fallback opt-in via `EG_UQL_TEXT_EMBEDDER=hash`). Was: `~"…"` errored at parse — "no server-side embedder resolver" | **done** | EG-411/412 |
| **UQL `WINDOW` — real tumbling time-series aggregate** — `WINDOW <secs>` now CONSUMES a RowSet of `(ts,value)` rows (e.g. from `Op::TsScan`, or graph-node rows with `valid_from`+`value`/`score`) and PRODUCES one row per non-empty tumbling bucket (`id` = aligned bucket start, `score` = the aggregate) via eg-tsdb's `time_bucket`, composing downstream into `Rank`/`Limit`; `WINDOW <secs> <agg>` (`Op::WindowAgg`) selects the aggregate (mean/sum/min/max/count/first/last). Was: a RowSet-preserving passthrough. Wired under `timeseries`; a non-`timeseries` build passes through | **done** | EG-413/414 |
| **UQL negative vector components** — `RANK BY ~[-0.1, 0.2, -0.3]` parses (a leading `-` negates the component) to the SAME `Op::Rank` the Rust builder / wire DTO always accepted; closes a lexer/parser asymmetry (the builder took negatives, UQL rejected them) | **done** | EG-417 |
| **UQL `FUSE` stage dispatch** — the UQL parser now dispatches `FUSE [branch] [branch] …` to the SAME `Op::FuseRrf { branches, k: 0.0 }` the builder/wire construct (RRF hybrid was builder/wire-only before, though the grammar listed it); each bracketed sub-pipeline is a branch, `k=0.0` ⇒ the `RRF_K` default. Feature-gated to `text` | **done** | EG-418 |
| **GraphQL cross-modal** — a GraphQL mutation/query that spans modalities in one operation | **open** | KG-2.235 |
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
