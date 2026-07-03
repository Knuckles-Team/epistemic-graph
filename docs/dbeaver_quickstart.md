# DBeaver / psql quickstart — SQL over the Postgres wire

epistemic-graph speaks the **Postgres wire protocol** (`pgwire`, in the `node` and
`full` tiers). Any Postgres client — **DBeaver**, `psql`, DataGrip, a BI tool, or an
ORM driver — connects to it **unmodified** and runs real SQL: `CREATE TABLE`,
`INSERT`, `SELECT`, joins to the graph `nodes`/`edges`, the works. No
`agent-utilities`, no extra service — just the standalone engine (see
[standalone deployment](standalone_deployment.md)).

---

## 1. Start the server with the wire listener on

The Postgres wire listener is opt-in: it starts only when
`EPISTEMIC_GRAPH_PGWIRE_ADDR` is set.

```bash
export GRAPH_SERVICE_AUTH_SECRET="$(openssl rand -hex 32)"   # keep this — clients need it
export EPISTEMIC_GRAPH_PGWIRE_ADDR=127.0.0.1:5433
epistemic-graph-server --persist-dir ~/eg-data --metrics-addr 127.0.0.1:9101
```

(Or `docker compose -f docker/compose.standalone.yml up -d`, which publishes `5433`.)

Port `5433` (not `5432`) is the documented convention so it never clashes with a real
Postgres on the same host.

---

## 2. Pick an auth mode

`EPISTEMIC_GRAPH_PGWIRE_AUTH` selects how clients authenticate. The **default is
`scram`** when `GRAPH_SERVICE_AUTH_SECRET` is set (an authenticated deployment), else
`trust`.

### Option A — `trust` (dev / localhost only, no password)

```bash
export EPISTEMIC_GRAPH_PGWIRE_AUTH=trust
```

Then any user name connects with **no password**. Simplest for a laptop; never expose
a trust listener beyond loopback.

### Option B — `scram` (SCRAM-SHA-256, the modern-driver default)

There is **no password table**. The per-user password is *derived* from the engine
secret, so an authorized operator can compute it and no secret is ever persisted:

```
password(user) = hex( HMAC-SHA256( GRAPH_SERVICE_AUTH_SECRET, "pgwire:" + user ) )
```

Compute it for the user you'll connect as (here `agent`):

```bash
python3 - <<'PY'
import hmac, hashlib, os
secret = os.environ["GRAPH_SERVICE_AUTH_SECRET"]
user   = "agent"
print(hmac.new(secret.encode(), b"pgwire:" + user.encode(), hashlib.sha256).hexdigest())
PY
```

Worked example — with `GRAPH_SERVICE_AUTH_SECRET=example_secret_do_not_use` and
`user=agent`, the password is:

```
f85c378d6bcd02bd1f98b872c9409c2612d57ce7685da97dadc165566b799cd9
```

The pg `user` you log in as becomes the engine's ACL actor (`AgentIdentity`), so
Row-Level Security / access control applies to your session. Any user string works
(e.g. `agent:planner`); its derived password is what you must present.

---

## 3a. Connect with `psql`

```bash
# trust mode — no password
psql "host=127.0.0.1 port=5433 user=agent dbname=__commons__"

# scram mode — supply the derived password
export PGPASSWORD="$(python3 -c 'import hmac,hashlib,os;print(hmac.new(os.environ["GRAPH_SERVICE_AUTH_SECRET"].encode(),b"pgwire:agent",hashlib.sha256).hexdigest())')"
psql "host=127.0.0.1 port=5433 user=agent dbname=__commons__"
```

- `dbname` maps to the **graph** the connection binds. `__commons__` is the default
  (override with `EPISTEMIC_GRAPH_PGWIRE_GRAPH`); you can switch per-session with
  `SET graph = '<name>'`.
- Introspection works: `\l`, `\dt`, `\d <table>` resolve against the synthetic
  `pg_catalog` + `information_schema` the engine serves.

## 3b. Connect with DBeaver

1. **Database → New Database Connection → PostgreSQL.**
2. **Main** tab:
   - **Host**: `127.0.0.1`
   - **Port**: `5433`
   - **Database**: `__commons__` (or another graph name)
   - **Username**: `agent`
   - **Password**: leave blank for `trust`; for `scram` paste the derived password
     from step 2 (Option B). Tick **Save password**.
3. **Driver settings / connection properties** (recommended):
   - DBeaver may probe features some clients don't need. If the initial connection
     test complains, set **Show all databases = off** and keep the single database
     you entered. The engine serves a read-only `pg_catalog`/`information_schema`
     shim, so metadata browsing of the `nodes`/`edges` and your user tables works,
     but it is not a full Postgres catalog.
4. **Test Connection → Finish.** Open a SQL Editor on the connection and run the SQL
   below.

---

## 4. Run real SQL

Paste this into a `psql` session or a DBeaver SQL editor. It creates a user table,
writes graph nodes, joins the two, and reads back — all durable in the redb store.

```sql
-- pick the graph for this session (optional; defaults to __commons__)
SET graph = '__commons__';

-- 1) an arbitrary user table (first-class: full DDL, durable)
CREATE TABLE items (
    id    text PRIMARY KEY,
    name  text NOT NULL,
    price double precision
);

INSERT INTO items (id, name, price) VALUES
    ('sku-1', 'widget', 9.99),
    ('sku-2', 'gadget', 19.50);

SELECT * FROM items ORDER BY price;
--  id    | name   | price
-- -------+--------+-------
--  sku-1 | widget |  9.99
--  sku-2 | gadget | 19.5

-- 2) write into the graph node store (properties is JSON)
INSERT INTO nodes (id, properties) VALUES
    ('item:sku-1', '{"label":"Item","sku":"sku-1"}'),
    ('item:sku-2', '{"label":"Item","sku":"sku-2"}');

-- 3) read nodes back
SELECT id, properties FROM nodes WHERE properties->>'label' = 'Item';

-- 4) join a user table to the graph in ONE query
SELECT i.name, i.price, n.id AS node_id
FROM   items i
JOIN   nodes n ON n.properties->>'sku' = i.id
ORDER  BY i.price;

-- 5) update / delete work too
UPDATE items SET price = 8.99 WHERE id = 'sku-1';
DELETE FROM items WHERE id = 'sku-2';
```

Notes (verified against [`docs/interfaces/sql.md`](interfaces/sql.md)):

- **`nodes`** `(id, properties JSON, …)` and **`edges`** `(src, tgt, type, …)` are
  always present — the graph store as SQL tables, with real predicate pushdown.
- **User tables** support full DDL (`CREATE`/`ALTER`/`DROP TABLE`, `CREATE VIEW`,
  `CREATE FUNCTION`, `COPY`) and DML (`INSERT`/`UPDATE`/`DELETE`,
  `INSERT … SELECT`, `ON CONFLICT`, `RETURNING`), and JOIN to `nodes`/`edges`.
- `INSERT INTO edges …` is not a write target — build edges via the Cypher/graph
  surface; `nodes` is the SQL write target for graph data. The reserved names
  `nodes`/`edges` cannot be used for `CREATE TABLE`.

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `Connection refused` on 5433 | The listener is opt-in — confirm `EPISTEMIC_GRAPH_PGWIRE_ADDR` is set and (Docker) the port is published. Check the server log for `pgwire: serving Postgres wire protocol on …`. |
| `password authentication failed` | You're in `scram` mode. Recompute `hex(HMAC-SHA256(secret, "pgwire:"+user))` with the **exact** `user` you connect as and the **exact** running secret. |
| Want no password for local dev | Set `EPISTEMIC_GRAPH_PGWIRE_AUTH=trust` and restart. |
| DBeaver metadata errors on connect | The `pg_catalog` is a read-only shim, not a full Postgres catalog. Point at a single database and disable "show all databases". |
| `psql` prints `could not interpret result from server: INSERT 2` after an `INSERT` | Cosmetic. The engine's `INSERT` CommandComplete tag omits the always-`0` oid field libpq expects (`INSERT 0 2`); the rows **are** written (verify with a `SELECT`). `UPDATE`/`DELETE`/`SELECT`/`CREATE` tags are unaffected. DBeaver ignores it. |

See also: [SQL & pgwire](interfaces/sql.md) · [Connecting (per-wire guide)](interfaces/connecting.md) · [standalone deployment](standalone_deployment.md).
