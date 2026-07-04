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
| **in-txn tsdb read-your-own-writes** — overlay a txn's staged `measurements` into `Op::TsScan`/`Op::Window` so an in-txn UQL joins its own uncommitted series (today a `TsScan`/`WINDOW` reads only committed series; the graph + vector legs already RYOW) | **open** | EG-360 / EG-363 |
| **GraphQL cross-modal** — a GraphQL mutation/query that spans modalities in one operation | **open** | KG-2.235 |
| **advanced cross-modal correctness** — the 14 advanced cross-modal tests in `workspace/plans/unified-txn-seam-plan.md` (bitemporal AsOf × decay, vector⇄reasoning write→read consistency, SPARQL-UPDATE→reasoning visibility, string-type↔IRI-class bridge for `REASON <iri>`, …) | **open** | EG-365..370 |

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
