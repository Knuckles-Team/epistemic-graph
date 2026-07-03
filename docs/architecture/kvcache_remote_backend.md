# Remote KV-cache HTTP backend (CONCEPT:EG-187)

The `kvcache-server` feature exposes the engine's **shared, content-addressed
KV-cache backend** (`eg_kvcache::SharedKvIndex`, CONCEPT:EG-186) over a small HTTP
surface so that **parallel vLLM / LMCache instances share KV-cache blocks by
token-hash**. An identical KV page produced by two workers is stored **once** (dedup),
and a cold worker can fetch a page a warm worker already computed — the classic
LMCache / prefix-cache win, but pooled across the whole fleet through one engine.

It is a hand-rolled HTTP/1.1 listener (`src/server/kvcache_http/`), the same
dependency-free `tokio::net` idiom as the s3 (`EG-176`) and obs listeners — no
axum/hyper, so no new HTTP dep enters the tree. It is in the one main build (a network listener +
the shared backend crate); default/pi builds link none of it.

## Endpoints

| Method | Path                  | Semantics |
|--------|-----------------------|-----------|
| `GET`  | `/kv/<hash>`          | Block bytes (`200 application/octet-stream`) or `404` if absent. |
| `PUT`  | `/kv/<hash>`          | Store the binary body under `<hash>`. `201 Created` for a new block; `200 OK` on a dedup hit (block already resident, ref-count bumped). |
| `HEAD` | `/kv/<hash>`          | `200` if present, `404` if absent (existence probe, no body). |
| `GET`  | `/kv/<hash>/exists`   | `200` JSON `{"hash":…,"exists":bool}` — a method-agnostic existence probe. |
| `GET`  | `/kv/stats`           | `200` JSON occupancy + dedup stats (see below). |

`/kv/stats` body:

```json
{
  "unique_blocks": 128,
  "total_refs": 512,
  "resident_bytes": 4194304,
  "logical_bytes": 16777216,
  "dedup_savings_bytes": 12582912,
  "dedup_hits": 384,
  "get_hits": 900,
  "get_misses": 40
}
```

## The address is the caller's token-hash (NOT a byte-hash)

`<hash>` is the address the LMCache / vLLM connector computes over the **token ids** of
the KV block (a stable content key for that page), *not* a hash of the KV bytes. The
server therefore stores the body verbatim under `<hash>` via
`SharedKvBackend::put_block` and does **not** re-hash the body. Dedup is by the
connector-supplied address — exactly what `SharedKvIndex` is designed for. Two workers
that computed the same token-prefix send the same `<hash>` and share one resident copy.

## Auth

A bearer-token guard mirroring the s3 SigV4-lite posture:

- Unset `EPISTEMIC_GRAPH_KVCACHE_TOKEN` ⇒ **anonymous** access (loopback default).
- Set ⇒ every request must carry `Authorization: Bearer <token>` or it is refused `401`.

## Configuration

| Env var                          | Effect |
|----------------------------------|--------|
| `EPISTEMIC_GRAPH_KVCACHE_ADDR`   | Bind address. A bare enable token binds the localhost default `127.0.0.1:9130`; a bare port binds `127.0.0.1:<port>`; a full `host:port` is used verbatim. Unset ⇒ no listener. |
| `EPISTEMIC_GRAPH_KVCACHE_TOKEN`  | Arms the bearer-token guard when set. |

Built `--features kvcache-server` (or any tier that folds it in) AND with
`EPISTEMIC_GRAPH_KVCACHE_ADDR` set, the listener spawns from `main.rs`.

## The vLLM / LMCache connector contract (follow-up)

LMCache supports a **remote backend** to which it offloads/loads KV blocks keyed by a
per-block hash. A connector maps that onto these endpoints 1:1:

- **Store (offload):** on evicting/producing a block with token-hash `H` and bytes `B`,
  the connector issues `PUT /kv/H` with body `B`. It may first probe `HEAD /kv/H` (or
  `GET /kv/H/exists`) and skip the upload if the cluster already has it — the
  `201`-vs-`200` status also distinguishes new from deduped.
- **Load (fetch):** on a local miss for token-hash `H`, the connector issues
  `GET /kv/H`; `200` returns the bytes to splice into the paged KV cache, `404` means
  compute-from-scratch.
- **Layering:** a worker wraps this remote backend *behind* a local
  `TieredCache`/`SharedKvIndex` L1, falling through to the network only on a local miss
  — the standard LMCache L1→remote hierarchy.

A **networked eviction / ref-count coordination** protocol across multiple engine nodes
(so a block is freed only when the last node releases it) is a further follow-up — today
the shared index is per-engine-process.

## The shipped Python driver (`epistemic_graph.kvcache`, CONCEPT:EG-337)

The `epistemic-graph` wheel now ships the packaged Python driver that maps LMCache/vLLM
onto the contract above. It lives in the pure-Python client package and imports with
**stdlib only** (`urllib`) — `import epistemic_graph.kvcache` needs nothing beyond the
base install. The optional `httpx` acceleration (pooled keep-alive) is pulled by
`pip install epistemic-graph[lmcache]`.

Two entry points:

- **`RemoteKVConnector`** — the remote-backend client implementing the LMCache
  `get(key) -> bytes | None` / `put(key, bytes) -> bool` / `contains(key) -> bool` /
  `exists(key) -> bool` / `stats() -> KvCacheStats` shape over the endpoints above. Every
  transport/protocol error degrades to a cache **miss** (never raises on the hot path).
  Build it from the engine's EG-187 environment with `RemoteKVConnector.from_env()`
  (reads `EPISTEMIC_GRAPH_KVCACHE_URL` / `_ADDR` / `_TOKEN` / `_TIMEOUT_S` / `_TLS_VERIFY`).

- **`RemoteKVL2Connector`** — the LMCache **`native_plugin`** L2-adapter *native client*
  (`event_fd()` / `submit_batch_set` / `submit_batch_get` / `submit_batch_exists` /
  `drain_completions()` / `close()`), which wraps `RemoteKVConnector` behind an async
  thread-pool + Linux `eventfd` so the decoupled `lmcache server` can offload its L2 tier
  to the **content-addressed** EG-187 surface (so dedup + `/kv/stats` counters apply,
  unlike the generic `resp`/Redis L2 adapter).

Wire it into the decoupled `lmcache server` via its `--l2-adapter` config — the same
`native_plugin` shape used in `services/vllm/compose.kvcache.yml`:

```json
{"type": "native_plugin",
 "module_path": "epistemic_graph.kvcache",
 "class_name": "RemoteKVL2Connector",
 "adapter_params": {"base_url": "http://localhost:9130"}}
```

`adapter_params` are spread as keyword arguments to the constructor (all optional:
`base_url`, `addr`, `token`, `timeout_s`, `num_workers`, `max_connections`, `verify_tls`);
anything omitted falls back to the EG-187 environment via `KvCacheConfig.from_env()`.

Direct use (custom L1→remote hierarchy) is equally supported:

```python
from epistemic_graph.kvcache import RemoteKVConnector

with RemoteKVConnector.from_env() as kv:      # reads EG-187 env
    if not kv.contains(token_hash):           # HEAD /kv/<hash>
        kv.put(token_hash, block_bytes)       # PUT  /kv/<hash>
    block = kv.get(token_hash)                # GET  /kv/<hash>  (None on miss)
    print(kv.stats())                         # GET  /kv/stats
```
