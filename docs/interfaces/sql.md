# SQL interface

epistemic-graph speaks SQL two ways:

1. **In-engine** via the `query` feature — DataFusion 43 executes `SELECT` and the engine handles DML
   over the graph node store. This is what the unified planner's `Filter` op and the native client use.
2. **Over the Postgres wire** via the `pgwire` feature (folded into `cluster`) — any `psql` / BI tool /
   ORM connects as if to Postgres.

> Status snapshot: read-path `SELECT` is full DataFusion. Writes are `INSERT`/`UPDATE`/`DELETE` on the
> **`nodes` table only** with a single-equality WHERE. Arbitrary user tables and DDL are 🗺 roadmap and
> error today. See the [capability matrix](../capabilities.md#sql-eg-querysql-pgwire).

## The tables

The SQL surface exposes the graph as two synthetic tables:

| Table | Shape | Backed by |
|-------|-------|-----------|
| `nodes` | `(id, properties JSON, …)` | the graph node store, with a predicate-pushdown provider |
| `edges` | `(src, tgt, type, …)` | a DataFusion `MemTable` over the edge store |

`pg_catalog` and `information_schema` are registered so introspection-driven tools work.

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
is replicated/durable like any other mutation. Constraints today:

- target must be `nodes` (`INSERT INTO edges …` errors);
- `INSERT` takes literal `VALUES` rows with an explicit column list including `id` (no `INSERT … SELECT`);
- `WHERE` is a single `<column> = <literal>` equality (no compound predicates, JOIN, `FROM`, `USING`);
- `id` cannot be reassigned.

These are deliberate KG-2.198 follow-ups, not partial bugs — they error with a clear message.

## DDL & user tables — roadmap

`CREATE TABLE`, `ALTER`, `DROP`, and any non-graph user table currently return `unsupported statement`.
A user-table catalog over redb plus DDL handling is **being added now** — see the
[roadmap](../roadmap.md#sql-toward-full-postgressqlite-parity).

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
