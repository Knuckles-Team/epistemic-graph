# HelixDB comparison (measured, correctness-gated)

> **Status: MEASURED, 2026-09-24. The Epistemic Graph numbers predate two
> pending fixes (EH-533: per-request durable replay-nonce commit; EH-559:
> cold-open materialization) and must be re-measured when they land.** See
> [Re-running](#re-running). This page makes **no superiority claim** either way. It
> reports what one pinned run measured, including every workload that failed its
> correctness gate, so the gaps are visible.

EH-353 asked for a reproducible, correctness-gated comparison against pinned
revisions. It covers the workloads both engines claim: point get, one hop, filtered
hop, transactional batch, text prefilter, vector prefilter and mixed graph/vector. The
design rationale is in the HelixDB comparative-assimilation notes (EH-351/352/353).
Epistemic Graph owns the workload semantics and this evidence. Automating the run is
pipelines' job.

## What is pinned

| input | value |
|---|---|
| HelixDB | `81e1e741e8d3d61e6b09577e1d46137850fc9343`, `cargo build --release --locked -p server` (rustc 1.97.1), driven through the same commit's Python SDK (`sdks/python/src`) |
| Epistemic Graph | engine tree `6ce3516ef` (release `epistemic-graph-server`, default features), driven through this repository's Python client |
| Corpus | seed 20260924: 20,000 `Doc` nodes (16 categories, year 2000–2025, 24-word Zipf text body, 32-dim unit embedding clustered by category), 99,985 `CITES` edges. Every doc cites up to 5 earlier docs, so the graph is a DAG. Corpus sha256 `0c4b9989…51c1` |
| Query set | seeded separately (seed + 1). sha256 `f38f5d9c…2974` |
| Spec | [`benches/helix_compare/spec.json`](https://github.com/Knuckles-Team/epistemic-graph/blob/main/benches/helix_compare/spec.json) holds both digests, workload sizes, the plan (gate 100 ops, warm-up 200, cold 50, concurrency 1 and 8) and CPU pinning (server CPUs 0–3, client CPUs 4–7) |
| Result | [`benches/helix_compare/results/2026-09-24-reference.json.gz`](https://github.com/Knuckles-Team/epistemic-graph/blob/main/benches/helix_compare/results/2026-09-24-reference.json.gz) (the complete, losslessly compressed report) and its rendered `.md` |

The archived report decompresses with `gzip -dc benches/helix_compare/results/2026-09-24-reference.json.gz`. Its uncompressed SHA-256 is `bb0b585d3a73cb8aa89c69d97ba47ae51d4fded0704c22b73f4384dec9dbf913`. The values and JSON structure are byte-for-byte the original measured result.

`tests/test_bench_helix_compare.py` regenerates the corpus and the query set and fails
if either digest drifts.

## How it is measured

- **One host window, one invocation.** Both engines ran back to back from one
  `scripts/bench_helix_compare.py` process, in the same cgroup (`cpu.max` 4 CPUs
  total, shared by server and client) and on the same disk (ext4 on a rotational
  RAID, both data directories under one tree).
  - Reference host: 2 × Xeon X5650 (24 threads), Linux 7.0.
  - Load average per phase, from the report's `timeline`:
    - Epistemic Graph: 6.4 → 12.1;
    - HelixDB: 9.7 → 14.3.
  - The host is a shared k8s node. Both engines saw comparable load, and the run
    was accepted on that basis.
- **Durable writes on both sides.**
  - Epistemic Graph acknowledges after its Immediate-durability redb commit.
  - Every HelixDB write sets `X-Helix-Await-Durable`.
  - Both servers run disk-backed and authenticated or production-shaped. Epistemic
    Graph runs in its documented service mode (HMAC `eg2.` envelopes, at-rest AEAD).
    HelixDB runs its standalone server, which has no authentication.
- **Idiomatic schema per engine.**
  - HelixDB creates a node-equality index on `category`, a text index on `text`
    and a 32-dim cosine vector index on `embedding`, all active before any data is
    written.
  - Epistemic Graph uses its maintained text index and semantic store.
- **Queries per engine:**

  | workload | Epistemic Graph | HelixDB |
  |---|---|---|
  | point get | `GetNodeProperties` | `n(id).value_map` |
  | one hop | `GetSuccessors` | `n(id).out("CITES")` |
  | filtered hop | id-anchored Cypher `MATCH (a {id})-[:CITES]->(b) WHERE b.year > 2012` | `out("CITES").where(gt year)` |
  | text prefilter | UQL `MATCH (:Doc) WHERE category = … \|> TEXT … \|> LIMIT 10` | `n_with_label_where(category).text_search(k=10)` |
  | vector prefilter | UQL `… \|> RANK BY ~[…] \|> LIMIT 10` | `…vector_search(k=10)` |
  | mixed (2-hop citations re-ranked by vector) | UQL `… \|> TRAVERSE CITES {1,2} \|> RANK BY ~[…] \|> LIMIT 10` | `union(out, out.out).dedup().vector_search(k=10)` |
  | transactional batch (10 new docs, each citing an existing doc, one transaction) | `BatchUpdate` | one write batch |

- **Correctness gate first.** A brute-force oracle over the corpus checks the first 100
  inputs of every workload before any timing, and a workload that fails is reported
  and not timed.
  - Exact workloads must match exactly.
  - The text workload must return only prefilter-matching docs containing the term,
    and exactly `min(k, eligible)` of them.
  - Vector workloads fail outright on a hit outside the prefilter or a wrong count.
    Beyond that, both engines use approximate vector indexes, so their ranking is
    scored as recall@10 against the exact top 10 and must average ≥ 0.95.
  - The transactional batch is read back through point get and one hop.
- **What is reported:**
  - p50/p95/p99 latency (nearest-rank) at concurrency 1 and 8;
  - throughput;
  - server CPU per operation, from the server process's `/proc` utime+stime;
  - idle and peak RSS (VmHWM);
  - storage amplification (allocated bytes ÷ the corpus's canonical JSON bytes);
  - load time;
  - time until the first read succeeds, empty and after a restart;
  - every unsuccessful operation, with samples.

  A transport-floor row (Epistemic Graph `Ping`, HelixDB `GET /healthz`, through the
  same client) shows how much of each latency is client and transport.
- **Warm vs process-cold.**
  - "Warm" is measured after a 200-op warm-up on the loaded server.
  - "Process-cold" means after a server restart, once the graph answers reads. The
    OS page cache is **not** dropped, because the harness has no root.

## Results (reference run, 2026-09-24)

### Correctness gate

| workload | Epistemic Graph | HelixDB |
|---|---|---|
| point get, one hop, filtered hop | pass | pass |
| text prefilter | pass (100/100 at this corpus size; see finding 2) | pass |
| vector prefilter | pass, recall 0.991 (min 0.9) | pass, recall 1.0 |
| mixed | **FAIL**: 7/100 returned fewer than 10 hits although ≥ 10 were reachable; recall 0.89 (min 0.5). Not timed | pass, recall 1.0 |
| transactional batch | pass | pass |

### Warm latency (ms) and throughput

| workload | c | EG p50 / p99 | EG ops/s | EG CPU ms/op | Helix p50 / p99 | Helix ops/s | Helix CPU ms/op |
|---|--:|--:|--:|--:|--:|--:|--:|
| transport floor | 1 | 5.4 / 8.2 | 180 | 9.6 | 2.4 / 3.6 | 412 | 0.14 |
| transport floor | 8 | 39.3 / 79.2 | 200 | 9.3 | 16.9 / 45.5 | 355 | 0.15 |
| point get | 1 | 5.7 / 8.0 | 174 | 10.1 | 4.9 / 6.9 | 205 | 2.1 |
| point get | 8 | 39.6 / 76.9 | 200 | 9.4 | 24.1 / 98.4 | 259 | 2.0 |
| one hop | 1 | 6.0 / 8.6 | 162 | 10.8 | 5.6 / 10.4 | 167 | 2.9 |
| one hop | 8 | 43.7 / 86.0 | 181 | 10.4 | 22.4 / 87.4 | 284 | 2.7 |
| filtered hop | 1 | 6.2 / 9.6 | 154 | 11.5 | 6.5 / 8.0 | 156 | 3.4 |
| filtered hop | 8 | 44.5 / 79.9 | 176 | 10.9 | 24.7 / 98.7 | 255 | 2.6 |
| text prefilter | 1 | 449.7 / 722.3 | 2.8 | 708.8 | 17.0 / 33.7 | 54 | 16.7 |
| text prefilter | 8 | 1812.3 / 2638.0 | 4.7 | 631.9 | 40.9 / 91.2 | 180 | 16.0 |
| vector prefilter | 1 | 342.4 / 520.5 | 3.4 | 579.8 | 46.3 / 86.3 | 21 | 45.0 |
| vector prefilter | 8 | 1243.0 / 1477.2 | 6.5 | 463.5 | 89.7 / 170.2 | 86 | 41.7 |
| mixed | 1 | — (gate failed) | | | 8.1 / 13.1 | 119 | 5.4 |
| mixed | 8 | — (gate failed) | | | 23.0 / 61.7 | 292 | 4.6 |
| transactional batch | 1 | 138.0 / 257.4 | 7.0 | 270.6 | 1314.1 / 3239.2 | 0.6 | 1762.9 |
| transactional batch | 8 | 1158.0 / 1392.5 | 6.8 | 285.1 | 8728.2 / 36827.0 (41/150 failed) | 0.3 | 10650.0 |

### Cold start, load and footprint

| | Epistemic Graph | HelixDB |
|---|--:|--:|
| load 20,000 docs + 99,985 edges | 24.9 s (803 docs/s) | 943.6 s (21.2 docs/s; 775 write-conflict retries) |
| storage after load / amplification | 73.7 MB / 4.71× | 167.9 MB / 10.71× |
| storage at end of run | 156.8 MB | 308.0 MB |
| idle / peak RSS | 84 MB / 1,120 MB | 29 MB / 1,071 MB |
| first successful read, empty store | 2.98 s | 2.28 s |
| **first successful read after restart** | **163.2 s** | **0.16 s** |
| process-cold point get p50 / p99 | 8.1 / 799.0 ms | 3.8 / 8.3 ms |

## Findings

1. **Every served Epistemic Graph request pays a durable write (EH-533).** The
   transport floor is `Ping`: about 5.4 ms and 9.6 ms of server CPU, with
   throughput flat near 200 ops/s at concurrency 1 and 8. The server commits
   each request's replay nonce durably, through the same transaction kernel as a
   write, before dispatch (`src/server/request_replay.rs`). That one serialized
   commit per request sets the floor for every read row above. HelixDB's
   floor is 0.14 ms of CPU.
2. **Filtered text and vector ranking in Epistemic Graph cost 350–450 ms and
   500–700 ms of CPU per query (EH-532).** The `MATCH … WHERE category = …`
   stage scans and filters the whole label on every query before ranking. The text
   leg then takes an over-fetched BM25 pool and applies the filter after
   ranking. At 500 documents that post-filter returned 4–8 of 10 eligible hits
   (smoke run). At 20,000 it happened to pass. The mixed pipeline shows the vector
   counterpart: ranking over the 2-hop set returned fewer than `k` hits for 7 of
   100 queries.
3. **A restarted Epistemic Graph is unreadable for 163 s at 20,000 documents
   (EH-559).**
   - The graph is reopened lazily on first touch. Reads are refused with a
     retryable `PARTIAL_MATERIALIZATION` until the maintained indexes are rebuilt
     over the whole image.
   - Local scaling was linear: 22.5 s, 45.3 s and 92.5 s for 750, 1,500 and 3,000
     documents.
   - The stored pages themselves are read in under a second.
   - HelixDB answers 0.16 s after the process starts.
4. **HelixDB writes conflict with its own index workers.**
   - With text and vector indexes present, 9 of 40 strictly sequential write
     batches aborted with `transaction_conflict`; with no indexes, 0 of 40. The
     harness retries such aborts up to 10 times and counts every retry.
   - That produced 775 retries during load and 41 of 150 batches failing outright
     at concurrency 8.
   - HelixDB's bulk load ran at 21 docs/s.
5. **Where Epistemic Graph is ahead in this run:**
   - load throughput: 38×;
   - transactional-batch latency: 9.5× at c=1, with no failed batches;
   - storage amplification: 4.7× vs 10.7×.
6. **Where they are even:** point and hop reads at concurrency 1 are within
   noise of each other (5–6 ms). Epistemic Graph pays its per-request nonce commit
   on each of them (finding 1). HelixDB pulls ahead at concurrency 8, because
   Epistemic Graph's per-request commit serializes the load.

## Re-running

Build both servers, then run on one quiet host:

```bash
# Epistemic Graph (this tree)
cargo build --release --locked --bin epistemic-graph-server
python3 scripts/build_numeric_kernel.py      # the client's eg2. signing codec
# HelixDB at the pinned commit
git clone https://github.com/HelixDB/helix-db helix && git -C helix checkout 81e1e741e8d3d61e6b09577e1d46137850fc9343
(cd helix && cargo build --release --locked -p server)

PYTHONPATH=$PWD:$PWD/scripts python3 scripts/bench_helix_compare.py \
  --eg-server target/release/epistemic-graph-server \
  --helix-server helix/target/release/server \
  --helix-sdk helix/sdks/python/src \
  --eg-commit "$(git rev-parse HEAD)" \
  --workdir ./helix-compare-work \
  --out report.json --markdown report.md
```

- A run refuses to start if the regenerated corpus or query-set digest differs
  from the spec.
- `--smoke 500` is for harness debugging. Its report is marked `UNPINNED` and is
  not evidence.
- **Re-bench hook:** re-run with the command above, with `--engines eg` alone if
  HelixDB is unchanged, after:
  - EH-533 (per-request durable nonce) lands;
  - EH-559 (cold-open materialization) lands;
  - EH-532 (filtered ranking pushdown) lands.

  Replace the Epistemic Graph columns, the result JSON and this status note, and
  keep the old report under `benches/helix_compare/results/` for comparison.
