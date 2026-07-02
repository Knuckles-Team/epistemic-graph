# SQL interface

epistemic-graph speaks SQL two ways:

1. **In-engine** via the `query` feature — DataFusion 43 executes `SELECT` and the engine handles DML
   over the graph node store. This is what the unified planner's `Filter` op and the native client use.
2. **Over the Postgres wire** via the `pgwire` feature (folded into `cluster`) — any `psql` / BI tool /
   ORM connects as if to Postgres.

> Status snapshot: read-path `SELECT` is full DataFusion (incl. window functions, EG-089). Writes are
> `INSERT`/`UPDATE`/`DELETE` on `nodes` and on **arbitrary user tables**, with full DDL
> (`CREATE`/`ALTER TABLE`/`DROP TABLE`/`CREATE VIEW`/`CREATE FUNCTION`, `COPY`) over a durable redb
> catalog — `ALTER TABLE` now covers `DROP`/`RENAME COLUMN`, `RENAME TO`, `ALTER COLUMN TYPE`, `DROP
> CONSTRAINT` (EG-310). Compound-WHERE DML, `INSERT … SELECT` into `nodes`, multi-table DML
> (`UPDATE…FROM`/`DELETE…USING`), `ON CONFLICT` upsert, user-table `RETURNING`, and mixed-store wire
> transactions are **shipped** (EG-045..049, EG-072). Postgres-extension surfaces
> (`pg_catalog`/`information_schema`, arrays/ranges, pgvector with **real ANN pushdown** (EG-313), AGE
> `cypher()`, TimescaleDB, ParadeDB with **real BM25** (EG-311)) light up via `CREATE EXTENSION`. The same
> tables are also readable as an open **Parquet + Delta + Iceberg** lakehouse with zero ETL — see
> [lakehouse-ltap](../architecture/lakehouse-ltap.md) (EG-317). See the
> [capability matrix](../capabilities.md#sql-eg-querysql-pgwire).

## The tables

The SQL surface exposes the graph as two synthetic tables:

| Table | Shape | Backed by |
|-------|-------|-----------|
| `nodes` | `(id, properties JSON, …)` | the graph node store, with a predicate-pushdown provider |
| `edges` | `(src, tgt, type, …)` | a DataFusion `MemTable` over the edge store |

`pg_catalog` and `information_schema` are registered so introspection-driven tools work (see below).

## SELECT — full DataFusion 43

Joins, aggregates, `GROUP BY`/`HAVING`, window functions, CTEs, subqueries, and set operations all run:

```sql
SELECT n.properties->>'type' AS kind, count(*) AS n
FROM nodes n
GROUP BY 1
ORDER BY n DESC;
```

JSON accessors (`json_get*`) and `epistemic_decay` are registered as UDFs; `pagerank` and `betweenness`
are table-valued functions; under the `finance` feature `var`/`cvar` are aggregate UDFs.

### Window functions & columnar scans (EG-089)

Full SQL window frames run over the table/tsdb store, which keeps a columnar (struct-of-arrays) segment
layout for analytical scans:

```sql
SELECT id,
       ROW_NUMBER() OVER (PARTITION BY region ORDER BY score DESC) AS rnk,
       AVG(score)   OVER (PARTITION BY region
                          ORDER BY t ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg
FROM metrics;
```

`ROW_NUMBER`/`RANK`/`DENSE_RANK`/`LAG`/`LEAD`/`FIRST_VALUE`/`LAST_VALUE`/`SUM`/`AVG` `OVER (PARTITION BY …
ORDER BY … ROWS/RANGE BETWEEN …)` are all supported.

**Predicate pushdown** is real: an indexable `col = literal` equality narrows the `nodes` scan through a
bounded per-column equality index before DataFusion re-applies the filter (the pushdown is `Inexact`, so
correctness never depends on it).

## DML — `nodes` table only (KG-2.198)

```sql
INSERT INTO nodes (id, properties) VALUES ('AgentC', '{"type":"worker"}');
UPDATE nodes SET properties = '{"type":"idle"}' WHERE id = 'AgentC';
DELETE FROM nodes WHERE id = 'AgentC';
```

Each statement maps to a native graph write (`add_node` / `compare_and_set_fields` / `remove_node`) and
is replicated/durable like any other mutation. The DML surface is now full (EG-045..049):

- **Compound WHERE** (EG-045): `UPDATE`/`DELETE` accept `AND`/`OR`/`NOT`/`IN`/`BETWEEN`/ranges/`IS NULL`,
  applied through serializable compare-and-set / remove gates (the predicate is re-checked under the write
  guard, so a concurrent write can't slip a row through);
- **`INSERT INTO nodes … SELECT`** (EG-046): populate the node store from a SELECT that may JOIN user
  tables and the graph, with `RETURNING`;
- **Multi-table DML** (EG-047): correlated `UPDATE nodes … FROM …` / `DELETE FROM nodes … USING …`;
- **`ON CONFLICT`** (EG-048): `INSERT … ON CONFLICT (cols) DO NOTHING|DO UPDATE` for `nodes` and user
  tables, plus `RETURNING` on user-table `INSERT`/`UPDATE`/`DELETE`;
- `INSERT INTO edges …` still errors (target must be `nodes`); `id` cannot be reassigned.

### Transactions (mixed-store, over the wire — EG-049)

pgwire `BEGIN`/`COMMIT`/`ROLLBACK` buffer **both** graph-node ops and user-table ops in one transaction;
reads inside the txn see the buffered writes (read-your-own-writes). `COMMIT` applies the node batch (one
`GraphCore::txn()` + one durable group) then the user-table txn. `ReadyForQuery` reports `T`/`E`/`I`, and
an aborted txn rejects statements until `ROLLBACK` (`25P02`). The node↔table commit is best-effort ordered
(a documented non-2PC window).

## DDL, views & user tables

Arbitrary user tables are first-class: `CREATE TABLE`, full `ALTER TABLE`, `DROP TABLE`, `COPY`,
and `CREATE VIEW` / `DROP VIEW` (EG-072 — a referenced view expands to its stored SELECT) persist to a
durable redb catalog (`crates/eg-query/src/tables/`, EG-018/EG-020). User-table DML
(`INSERT`/`UPDATE`/`DELETE`, `INSERT … SELECT`, `RETURNING`) executes against it, and user tables are
JOINable to the graph `nodes`/`edges` in a single query. Reserved names `nodes`/`edges` are rejected for DDL.

### `ALTER TABLE` (EG-310)

Beyond `ADD COLUMN`, the durable user-table catalog supports the full evolving-schema set with data
migration: `DROP COLUMN`, `RENAME COLUMN`, `RENAME TO` (rename the table), `ALTER COLUMN … TYPE` (with a
row-rewrite migration), and `DROP CONSTRAINT`:

```sql
ALTER TABLE metrics ADD COLUMN region text;
ALTER TABLE metrics RENAME COLUMN value TO score;
ALTER TABLE metrics ALTER COLUMN score TYPE double precision;
ALTER TABLE metrics DROP COLUMN region;
ALTER TABLE metrics RENAME TO measurements;
```

### Stored functions (EG-118)

```sql
CREATE FUNCTION active_count() RETURNS int AS $$
  SELECT count(*) FROM nodes WHERE properties->>'state' = 'active'
$$ LANGUAGE sql;

SELECT active_count();
```

`CREATE FUNCTION … LANGUAGE sql` persists scalar + table SQL-language functions in a durable catalog and
invokes them in queries. PL/pgSQL control-flow (`IF`/`LOOP`/`RETURN`) is a documented follow-up.

### Arrays, ranges & scalar functions (EG-104)

Array types (`int[]`/`text[]`: literals, subscript, `ANY`/`ALL`, `unnest`, `array_agg`, `array_length`,
`||` concat, `@>`/`&&` overlap) and range types (`int4range`/`tsrange` with `@>`/`&&`/`<@`) parse and
execute, along with the common scalar functions ORMs/BI emit: `string_agg`, `split_part`, `regexp_replace`,
`to_char`/`to_timestamp`, `date_trunc`, `extract`, `greatest`/`least`, `generate_series`,
`coalesce`/`nullif`.

## Postgres extensions — `CREATE EXTENSION` (EG-102)

An unmodified Postgres client/ORM can `CREATE EXTENSION` to light up a family surface; enabled extensions
are recorded in a durable catalog:

| Extension | Surfaces | Concept |
|-----------|----------|---------|
| `vector` (pgvector) | `vector(n)` column type + `<->` (L2) / `<=>` (cosine) / `<#>` (neg-inner) operators; `CREATE INDEX … USING hnsw/ivfflat` pushes `ORDER BY emb <-> $1 LIMIT k` down to the eg-ann index — a **real ANN top-k** (HNSW/IVF) + exact re-rank, not the brute-force fallback (which stays as the no-index path) | EG-115 / EG-116 / EG-313 |
| `pg_age` (Apache AGE) | `SELECT * FROM cypher('graph', $$ MATCH … RETURN … $$) AS (a agtype)` routes the inner Cypher to the eg-query Cypher engine | EG-114 |
| `timescaledb` | `create_hypertable()`, `time_bucket()` gap-fill, and `CREATE MATERIALIZED VIEW … WITH (timescaledb.continuous)` continuous aggregates over the eg-tsdb store | EG-117 |
| `pg_search` (ParadeDB) | the `@@@` BM25 search operator + `paradedb.*` `score()`/`snippet()` over the eg-text index — **real BM25 relevance scoring + highlighted snippets** (not a placeholder `1.0`) | EG-119 / EG-311 |

```sql
CREATE EXTENSION vector;
CREATE TABLE items (id text, emb vector(384));
CREATE INDEX ON items USING hnsw (emb vector_cosine_ops);
SELECT id FROM items ORDER BY emb <=> $1 LIMIT 10;   -- pushed to eg-ann (EG-116)

CREATE EXTENSION pg_age;
SELECT * FROM cypher('social', $$ MATCH (a)-[:KNOWS]->(b) RETURN a.id, b.id $$) AS (a agtype, b agtype);
```

## System-catalog compatibility — `pg_catalog` / `information_schema` (EG-103)

So `psql` (`\d`, `\dt`, `\l`), ORMs, and BI tools introspect the engine, the system catalogs are
synthesized from the live table/view/function catalogs and answerable as normal `SELECT`s:
`pg_catalog.pg_class`/`pg_namespace`/`pg_attribute`/`pg_type`/`pg_index`/`pg_proc` and
`information_schema.tables`/`columns`/`schemata`/`views`/`routines`, plus common `pg_catalog` functions
(`pg_table_is_visible`, `format_type`, `current_schema`).

## Postgres wire quick-start

```bash
EPISTEMIC_GRAPH_PGWIRE_ADDR=127.0.0.1:5433 \
  epistemic-graph-server --features cluster

psql -h 127.0.0.1 -p 5433 -U agent -d epistemic
```

- **Auth**: SCRAM-SHA-256 when `GRAPH_SERVICE_AUTH_SECRET` is set, else trust (dev). The pg `user`
  becomes the engine ACL actor, so Row-Level Security applies to every wire query.
- **Protocols**: both simple and extended/prepared (`$N` parameters) are implemented.
- **Connection switch**: `SET graph = '<name>'` selects the graph for the session.
</content>
