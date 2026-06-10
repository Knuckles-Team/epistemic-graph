# Spec: Review Remediation — engine slice (doc-truth, decay, traversal, sharding)

> 2026-06-09. Engine-side slice of the cross-repo program owned by
> `agent-utilities/.specify/specs/os-5.5-l1-cache-fidelity/spec.md`. This spec
> covers only the Rust `epistemic-graph` work. Validated against source below.

## Finding (what we validated against source)

An external review of `epistemic-graph` + `agent-utilities` claimed PyO3 usage,
no working tiered/subgraph layer, and unverified scale. Validated:

- **No PyO3.** `Cargo.toml` declares no `pyo3`, `crate-type = ["rlib"]` (not
  `cdylib`); `pyproject.toml` `[tool.maturin] bindings = "bin"` packages a
  **binary** (`epistemic-graph-server`). Python talks over **UDS + msgpack**
  (`epistemic_graph/pool.py:50`, `rmp-serde` in `Cargo.toml`). The reviewer was
  misled by our **own README** — `README.md:22` ("exposed to Python via PyO3
  FFI") and `:41` ("↕ PyO3 FFI") are **false** and must be corrected.
- **VF2 exists** (`src/graph.rs:583 vf2_subgraph_match`, wired `src/server.rs:998`).
- **Ebbinghaus = schema only.** `src/types.rs:62-68` has `confidence`
  (0–1, "for temporal fact decay") + `valid_from/valid_to`, but **no decay
  function**. `README.md:52` claims it's "natively integrated" — overstated.
  *(Decision: implement the decay so the claim is true.)*
- **Engine can traverse** (`src/graph.rs` DFS/BFS/neighbors/edges_directed) — but
  the Python L1 query interpreter doesn't expose it, so `agent-utilities`
  `tiered_backend.py` falls back to L3 for traversal. Expose it natively.
- **Scale unverified.** `scripts/bench_transport.py` + `docs/benchmarks.md` exist
  (transport only); no `criterion` algo bench, no sharding, no multi-host scale
  test. Single-process in-memory `StableDiGraph`. *(Decision: build-to-validate.)*

## User Stories

### T-E1: README tells the truth about the transport (P0)
**As** an evaluator, **I want** the README to describe the real UDS/msgpack
transport, **so that** no one concludes "PyO3" from our own docs.
- [ ] Replace `README.md:22` "exposed to Python via PyO3 FFI" → "exposed to Python out-of-process over a MessagePack UDS/TCP client (`epistemic_graph.client`); no in-process FFI."
- [ ] Replace the `↕ PyO3 FFI` diagram edge (`README.md:41`) → `↕ MessagePack / UDS`.
- [ ] Add a one-line maturity banner to the "100M" framing wherever it appears in engine docs; link the T-E3 benchmark.
- **Accept:** `grep -ri pyo3 README.md docs/` returns nothing (changelog/history excepted).

### T-E2: Ebbinghaus forgetting-curve decay is implemented (P1b)
**As** the temporal graph, **I want** belief `confidence` to decay with elapsed
time and refresh on access, **so that** README:52 is true.
- [ ] Add `last_access` (epoch s) + stability `S` to the node/edge temporal fields (`src/types.rs`), defaulting back-compat (S = sane default, confidence = 1.0).
- [ ] `decay(now)` → `confidence *= exp(-(now - last_access) / S)`; on read, lazily apply; on write/access, refresh `last_access` and optionally bump `S` (spaced-repetition style).
- [ ] New `Method::` variant in `src/protocol.rs` (e.g. `DecaySweep { floor }`) + dispatch arm in `src/server.rs`; expose `client.<...>.decay_sweep(...)` in `epistemic_graph/client.py` (mirror the `datascience` auto-exposed pattern).
- [ ] Optional prune of nodes/edges below a confidence floor during sweep.
- [ ] Tests: inline Rust unit tests (decay math, refresh, floor prune) + a Python round-trip test (`tests/test_*.py`, mirror `tests/test_compute_primitives.py`).
- **Accept:** a node's confidence measurably drops after simulated Δt and resets on access; sweep prunes below floor; tests green.

### T-E3: L1 exposes native traversal over the protocol (P1 support)
**As** the `agent-utilities` L1 backend, **I want** neighbor/DFS/BFS/bounded-path
queries served by the engine, **so that** tiered reads stop falling back to L3.
- [ ] Ensure protocol methods exist for: neighbors (in/out/both), n-hop bounded traversal (`[*1..k]`), and shortest/any path between two ids — backed by the existing `src/graph.rs` / `src/algorithms.rs` ops (add thin `Method::` variants + dispatch only where missing).
- [ ] Return shapes that the Python interpreter can map to Cypher-style `RETURN n, r` rows (so `agent-utilities` `epistemic_graph_backend.py` can satisfy a relationship pattern without "returns every node").
- **Accept:** a bounded-traversal request over UDS returns exactly the k-hop neighborhood (not the whole graph); covered by a Rust + Python test.

### T-E4: Sharding / horizontal scale-out (P3, HARD GATE)
**As** the platform, **I want** many engine instances each owning a partition,
**so that** 100M concurrent agents (each a bounded subgraph) is a measured
projection across N hosts.
- [ ] Confirm/extend the multi-tenant `src/registry.rs` so one process cleanly hosts many independent bounded graphs with per-graph locks (no global contention).
- [ ] Document + support running multiple server instances (multi-proc, then multi-host) over the existing UDS/TCP transport; the shard router lives on the `agent-utilities` side (`pool.py` endpoint pool).
- [ ] `criterion` microbenchmarks for core graph ops (add/traverse/vf2) + a multi-instance scale harness; sweep agent count, record p50/p99 + RSS per bounded subgraph.
- [ ] Publish results + the 100M extrapolation (footprint × concurrency ÷ host capacity) to `docs/benchmarks.md`.
- **Accept (gate):** harness shows **linear** scaling across ≥2 instances/hosts; extrapolation reproducible.

## Non-Functional Requirements
- [ ] Back-compat: new `types.rs` fields use `#[serde(default = ...)]`; default feature set unchanged; existing msgpack round-trips still pass.
- [ ] No PyO3 introduced — transport stays UDS/TCP + msgpack.
- [ ] `cargo test` + `cargo build --features full` green; ruff/mypy clean on the Python client.

## Status
**PLANNED** — 2026-06-09. Coordinated with the master OS-5.5 spec.
