---
name: kg-modality-sql
skill_type: skill
description: >-
  Run SQL against the engine over real database wire protocols — Postgres (pgwire), MySQL,
  MSSQL/TDS, and the SQLite NDJSON endpoint — so psql/DBeaver/BI/ORMs connect to the engine as
  if it were their existing database (epistemic-graph owns the wire). Use for SELECT/joins/
  aggregates/window/CTE and INSERT/UPDATE/DELETE over the `nodes` table + arbitrary user
  tables; via the engine_query MCP tool or a psql connection string.
domain: modality
license: MIT
tags: [epistemic-graph, engine, sql, pgwire, postgres, wire, modality]
tier: modality
wraps: [engine_query]
metadata:
  author: Genius
  version: '0.1.0'
---

# kg-modality-sql — SQL over the engine wire

The engine speaks **SQL over the actual wire protocols** (DataFusion-backed `eg-query`,
wire-neutral `classify → dispatch → exec` core). Full `SELECT` (joins, aggregates,
GROUP BY/HAVING, window frames, CTE, subquery, UNION), `INSERT`/`UPDATE`/`DELETE` (with
compound WHERE, `RETURNING`, `INSERT … SELECT`, `ON CONFLICT` upsert), `CREATE/ALTER/DROP`
DDL on arbitrary durable user tables, views, and SQL/plpgsql functions — plus Postgres
extension drop-ins (pgvector `<->`/`<=>`, Apache AGE `cypher()`, TimescaleDB, ParadeDB BM25).
`pg_catalog`/`information_schema` are synthesized so `psql \d`, ORMs and BI tools introspect.
See `docs/capabilities.md` → *SQL* and *Postgres wire*.

## The wire way (epistemic-graph owns it)
Each wire is **opt-in** (build with its feature + set its `_ADDR` env var). Postgres wire
(feature `pgwire`, `EPISTEMIC_GRAPH_PGWIRE_ADDR`, default `127.0.0.1:5433`):

```bash
psql "host=127.0.0.1 port=5433 user=agent dbname=__commons__"
```
```sql
SET graph = 'my_graph';
SELECT id, properties FROM nodes LIMIT 10;
INSERT INTO nodes (id, properties) VALUES ('n1', '{"label":"Doc"}');
```

Simple **and** extended/prepared protocol; SCRAM-SHA-256 auth (the pg `user` becomes the ACL
actor, so RLS applies) or trust in dev. Sibling wires share the same SQL core:
MySQL (`mysql-wire`, `:3306`), MSSQL/TDS (`mssql-wire`, `:1433`), SQLite NDJSON
(`sqlite-wire`). Recipes + ports: `docs/interfaces/connecting.md`.

## The MCP way (through graph-os)
```
load_tools(tools=["engine_query"])   # then run SQL through engine_query
```
or the REST twin graph-os exposes for the query modality.

## Cross-modal seam
The `nodes` table is the **same** store the RDF, vector, and time-series modalities read, so a
SQL statement can JOIN graph ⋈ vector ⋈ timeseries and commits inside the engine's unified
**mixed-store wire transaction** (`BEGIN`/`COMMIT`/`ROLLBACK`, read-your-own-writes) —
cross-modal ACID, not a separate SQL database.

## Related
- `kg-modality-sparql` — RDF/SPARQL over the same nodes; `kg-modality-consensus` — the
  multi-Raft/tenant substrate these wires commit into.
