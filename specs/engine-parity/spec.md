# MEDULLA-ENGINE-PARITY — Relational engine parity inside one library

Owner: Medulla (`EG`). Sole owner.
Component: Storage
References: [MEDULLA-DURABLE-KERNEL](../durable-graph-kernel/spec.md), [MEDULLA-UNIFIED-DATA-PLANE](../unified-data-plane/spec.md), [MEDULLA-FEDERATED-QUERY](../federated-query-and-reasoning/spec.md), [MEDULLA-SCM](../source-changesets/spec.md)

Cross-repo IDs: `ARBORA-OPS-R858` issues outside SQL client identities for the wire front end.
The former prefix `MEDULLA-HYBRID-STORAGE-` is an alias of this prefix in `.config/spec-aliases.toml`.

## User outcome

The engine stays one library: the embedded transactional store plus the columnar query engine.
It gains the strengths of a mature relational database inside that library. No second database
engine joins it, and no data syncs into one.

- Writers scale through group commit, optimistic concurrency and partitioned store files.
- Outside SQL and BI tools connect over the PostgreSQL wire protocol under engine principals and row security.
- User tables gain secondary, partial and expression indexes, constraints, triggers, sequences and views.
- The committed log serves change capture, point-in-time recovery and read replicas.
- Hot record classes keep columnar copies for analytic scans.
- Outside PostgreSQL databases stay attached sources. The engine reads and federates them only.

Each capability is built only when a named consumer needs it.

## Requirements

Every requirement ID lives in [requirements.md](requirements.md). The IDs are
`MEDULLA-ENGINE-PARITY-R001`–`R020`. Architecture and live wiring live in [plan.md](plan.md). The
test contract lives in [test-spec.md](test-spec.md). Ordered work lives in [tasks.md](tasks.md).

## Acceptance

The spec accepts when every row reaches `VERIFIED` in [status.json](status.json).

- 64 concurrent writers reach at least 20,000 commits per second on one store file (R001, R005).
- Disjoint transactions never wait on each other (R002, R004). Partitions commit in parallel (R003).
- A standard PostgreSQL client connects, introspects, prepares, copies and reads under row security (R006 to R009).
- Relational features behave per the compatibility matrix (R009 to R014).
- A subscriber, a restore and a replica all read the one committed log (R015 to R017).
- Analytic scans read columnar copies within a stated staleness (R018).
- No engine record reaches an outside database (R019).

## Why a new spec

The search read MEDULLA-DURABLE-KERNEL, MEDULLA-UNIFIED-DATA-PLANE and MEDULLA-FEDERATED-QUERY.

- MEDULLA-DURABLE-KERNEL-R035 owns the wire operational surfaces: dump, restore, statistics views and cancellation.
- MEDULLA-DURABLE-KERNEL-R084 owns version-checked writes. R016 and R040 own batched durability.
- MEDULLA-UNIFIED-DATA-PLANE-R013 owns the outside PostgreSQL adapter as an attached source.

No spec owns write scaling across store files or the wire protocol message flows. No spec owns
catalog emulation, the dialect matrix or the relational features of user tables. This spec owns
them and links each neighbor row by ID.
