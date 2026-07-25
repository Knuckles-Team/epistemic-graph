# KV-cache interface (vLLM / LMCache shared blocks)

epistemic-graph can act as a **tiered, shared LLM KV-block cache** — an LMCache/vLLM-style remote backend
so parallel-deployed inference instances share KV blocks by token-hash (prefix-cache dedup) and survive
OOM by offloading blocks to RAM/disk. This is a distinct modality from the vector [ANN](vector.md) index:
ANN stores fixed-dimension embedding vectors; the KV-cache stores opaque attention KV **blocks** keyed by
a token-hash.

> Status snapshot: the tiered store (EG-185), the shared multi-instance backend (EG-186), and the HTTP
> server + vLLM/LMCache connector (EG-KG.backend.is-configured-so-co) are shipped. Pure-Rust, in the one main build. See the
> [capability matrix](../capabilities.md).

## Where the engine sits: the caching levels (L0 GPU → L1 CPU → L2 engine)

The engine is the **L2 tier** in the layered KV-cache hierarchy vLLM and LMCache build on top of it —
the widest, slowest, most persistent tier, and the only one that **dedups and survives everything**.
Each level up is smaller/faster/more volatile. (The vLLM- and LMCache-side wiring is documented in
agent-utilities' `docs/guides/kvcache-vllm-lmcache.md` and `services/vllm/AGENTS.md`; this mirrors the
concept engine-side.)

```mermaid
flowchart TD
    req["inference request<br/>(shared prefix)"] --> L0
    subgraph vllm["vLLM (GPU host)"]
      L0["L0 · GPU HBM<br/>native prefix cache<br/>(--enable-prefix-caching)<br/>⟲ lost on vLLM restart"]
    end
    subgraph lm["LMCache (decoupled lmcache server)"]
      L1["L1 · CPU RAM<br/>--l1-size-gb<br/>✓ survives vLLM restart"]
    end
    subgraph eg["epistemic-graph (kvcache-server, this engine)"]
      L2["L2 · durable + dedup<br/>EG-185 hot/warm/cold tiers<br/>EG-186 content-addressed dedup<br/>✓ survives server restart · persists · shared cross-instance"]
    end
    L0 -->|"miss / evict · offload KV (+ Mamba state) via CUDA-IPC"| L1
    L1 -->|"miss / evict · resp or EG-KG.backend.is-configured-so-co native adapter"| L2
    L2 -.->|"retrieve on cold GPU"| L1
    L1 -.->|"load back into HBM"| L0
```

- **L0** = vLLM's in-process GPU prefix cache — fastest, but lost on restart and per-worker.
- **L1** = LMCache's CPU-RAM tier — survives a vLLM restart (the cross-restart win); still per-box.
- **L2** = **this engine** (`kvcache-server`): the durable, content-addressed, deduplicating tier that
  survives everything and is **shared across instances** — two workers that PUT the same token-hash
  store the bytes **once**. LMCache reaches it either via the built-in `resp` Redis wire or the
  engine-native EG-KG.backend.is-configured-so-co HTTP adapter (dedup + live `/kv/stats`).

## The tiered store (EG-185, crate `eg-kvcache`)

A tiered key→block cache with automatic promotion/demotion (paging) on access + capacity pressure:

| Tier | Backing | Policy |
|------|---------|--------|
| **hot** | in-RAM | LRU + importance/recency scoring |
| **warm** | compressed-RAM | demoted from hot under pressure |
| **cold** | redb / blob CAS | evicted-but-durable; re-promotes on access |

`get`/`put`/`pin`/`evict` operate across the tiers — the substrate that lets a KV cache survive OOM by
offloading to RAM/disk instead of dropping blocks.

## Shared multi-instance backend (EG-186)

A `SharedKvBackend` trait + a **content-addressed shared index** so parallel-deployed engine / vLLM
instances share KV blocks: blocks are hash-keyed, deduplicated, and ref-counted, and a lookup/publish API
lets an external vLLM/LMCache connector fetch or store a block by its token-hash. Two workers that PUT the
same token-hash store the bytes **once** — the LMCache dedup / prefix-cache win.

## Networked (durable, fleet-shared) backend — `SharedKvStoreBackend`

The `SharedKvIndex` above is **in-process + ephemeral**. `SharedKvStoreBackend`
(`src/server/kvcache_http/shared_store.rs`) is the SAME `SharedKvBackend` seam over the engine's
**durable, mutation-store-backed KV store** (`kv.redb`, the store the `KvGet`/`KvPut` wire methods own).
Select it with `EPISTEMIC_GRAPH_KVCACHE_BACKEND=durable`. Then:

- **Fleet-shared through the engine** — every serving instance that reaches this engine (over `/kv` or the
  `KvGet`/`KvPut` wire) reads/writes ONE `kv.redb`, so a prefix block instance A PUTs is a HIT for instance B.
- **Survives a restart** — commit-before-ack redb durability; the shared L2 cache is durable.
- **Fleet-wide invalidation** — the derived-context data-version epoch is persisted, so a graph write on one
  node invalidates stale context for all of them (pure content-addressed pages are immune). Stale disk is
  reclaimed out of band via the bounded `retire_stale` sweep (reads gate lazily meanwhile).
- **Graph-topology-aware paging** — `page_in_ranked` pages the most graph-central nodes' KV blocks into RAM
  first, using the resident graph's own PageRank/centrality (`eg_kvcache::graph_importance`) as the locality
  signal, and seeds the LMCacheMPConnector snapshot→branch working layer.
- **Hit-rate metrics** — atomic `get_hits`/`get_misses`/`dedup_hits`/`put_new`/`stale_missed` feed `/kv/stats`
  and a periodic `tracing` hit-rate line (`target: epistemic_graph::kvcache`), in the W3.7 Seam-6 export shape.

## HTTP server + connector (EG-KG.backend.is-configured-so-co, feature `kvcache-server`)

A gated HTTP surface over the `SharedKvBackend`:

```bash
EPISTEMIC_GRAPH_KVCACHE_ADDR=127.0.0.1:9130 \
EPISTEMIC_GRAPH_KVCACHE_TOKEN=$SECRET \
  epistemic-graph-server --persist-dir "${GRAPH_SERVICE_PERSIST_DIR:?}"   # --features "kvcache-server server"
```

| Route | Behaviour |
|-------|-----------|
| `PUT /kv/<hash>` (binary body) | store the block under `<hash>` (`201 Created`; a duplicate stores once) |
| `GET /kv/<hash>` | the block bytes (`200`) or `404` if absent |
| `GET /kv/<hash>/exists` | `200` JSON `{"hash":…,"exists":bool}` (a cheap existence probe) |
| `GET /kv/stats` | `200` JSON occupancy + dedup stats |

```bash
auth_header="Authorization: Bearer ${EPISTEMIC_GRAPH_KVCACHE_TOKEN:?}"
curl -s -H "$auth_header" -XPUT --data-binary @block.bin http://127.0.0.1:9130/kv/<token-hash>
curl -s -H "$auth_header" http://127.0.0.1:9130/kv/<token-hash>
curl -s -H "$auth_header" http://127.0.0.1:9130/kv/<token-hash>/exists
curl -s -H "$auth_header" http://127.0.0.1:9130/kv/stats
```

- **Auth**: mandatory verified JWT or runtime-injected bearer
  `EPISTEMIC_GRAPH_KVCACHE_TOKEN` (`Authorization: Bearer …`). The listener fails
  closed when neither mode is configured.
- **TLS client trust**: plain HTTP is accepted only for explicit loopback URLs.
  Non-loopback endpoints require HTTPS. Configure trust through standard
  `SSL_CERT_FILE`, `REQUESTS_CA_BUNDLE`, or `SSL_CERT_DIR`; the optional
  `EPISTEMIC_GRAPH_KVCACHE_CLIENT_CERT` / `_CLIENT_KEY` pair supplies mTLS
  identity. Peer verification is mandatory and has no runtime bypass.

## LMCache remote-backend contract

The server shapes the **LMCache remote-backend** contract, so an external vLLM/LMCache instance points its
remote KV backend at this endpoint to fetch/store blocks (OOM-offload + cross-instance reuse). For the wire
contract, block key derivation, and the connector adapter, see the architecture note:
[kvcache-remote-backend](../architecture/kvcache_remote_backend.md).

---

**See also:** [Capabilities matrix](../capabilities.md) · [KV-cache remote backend](../architecture/kvcache_remote_backend.md) · [Key-value & Blob](kv_blob.md) · [Client Drivers](clients.md) · [Connecting (per-wire guide)](connecting.md).
