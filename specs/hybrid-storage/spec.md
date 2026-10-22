# EG-HYBRID-STORAGE — Columnar and row-oriented hybrid storage

Owner: epistemic-graph (`EG`). Sole owner.
Component: Storage
References: [EG-DURABLE-KERNEL](../durable-graph-kernel/spec.md), [EG-UNIFIED-DATA-PLANE](../unified-data-plane/spec.md), [EG-FEDERATED-QUERY](../federated-query-and-reasoning/spec.md)

Cross-repo IDs: `ARBORA-OPS-R858` issues outside-reader roles for the row engine. Platform
deployment satisfies the R020 contract.

## User outcome

The engine keeps one authority: admission, catalog, commit sequence and the committed stream.
Three storage engines sit under it. The embedded transactional store holds the log, sealed
classes, the graph and the control plane. The columnar engine serves analytic reads over Arrow
and Parquet. A row engine, PostgreSQL, holds user tables that need concurrent writers,
maintained indexes or outside SQL tools.

A user table may be `row-resident`. Its rows then live in the row engine with MVCC writers and
secondary indexes. The engine still admits every write and assigns the commit sequence. A fence
limits what the row engine may show. A table may be `row-projected`: a read-only projection of
the committed stream for outside readers.

A plan-level router sends OLTP writes and index probes to the row engine. It sends analytic work
to the columnar engine. It runs mixed plans as one federated plan at one fence position. Each
routed result carries a routing receipt. Python, Go and JavaScript clients see one contract.

## Requirements

Every requirement ID lives in [requirements.md](requirements.md). The IDs are
`EG-HYBRID-STORAGE-R001`–`R020`. Architecture and live wiring live in
[plan.md](plan.md). The test contract lives in [test-spec.md](test-spec.md). Ordered work
lives in [tasks.md](tasks.md). Machine-readable delivery state lives in
[status.json](status.json).

## Acceptance

The spec accepts when every row reaches `VERIFIED` in [status.json](status.json).

- The capability partition is closed and typed (R001). Providers negotiate capabilities (R013).
- One policy table places each record class (R014). One catalog owns placement and DDL (R003).
- One admission authority owns row writes (R004). The fence keeps the engines consistent (R005, R015).
- The router decides per plan with a receipt (R002). Mixed plans push work down (R016).
- Row security equals engine security in a differential test (R017).
- One backup restores both engines to one fence (R018).
- Each placement default cites a measured benchmark run (R019).
- Failure is typed: degradation, refusal or recovery per durability class (R010, R012, R020).

## Why a new spec

The search read the owning specs on the base revision.

- EG-DURABLE-KERNEL owns transactions, durability classes and the workload classifier (R037).
- It also owns the plan cache (R041), cross-store atomicity (R034) and wire parity (R035).
- EG-UNIFIED-DATA-PLANE owns attached-source adapters: outside databases, not in-authority placement.
- EG-FEDERATED-QUERY owns federated query over foreign sources.

No spec owns a row engine as an in-authority placement target for user tables. No spec owns the
partition, the router, the placement catalog or the fence. This spec owns them. Graph and
control-plane data stay engine-only by construction (R001, R014).
