# Contributing to epistemic-graph

This is the **Rust compute engine** for the
[`agent-utilities`](https://github.com/Knuckles-Team/agent-utilities) ecosystem —
a long-running Tokio service exposed to Python **out-of-process** over
length-prefixed MessagePack on UDS/TCP. There is **no PyO3 / in-process FFI**
(enforced by `scripts/check_no_pyo3.sh`); the shipped wheel is the
`epistemic-graph-server` binary plus a pure-Python client.

## Development setup

```bash
cargo build --release --features server     # the server binary
pip install -e .                            # the Python client
```

## Branch / worktree workflow

Take your own git worktree on your own branch (do not edit the canonical checkout
directly — a concurrent session or sync may reset it):

```bash
rm_worktree add epistemic-graph <your-branch>     # repository-manager MCP, or:
git worktree add /home/apps/worktrees/epistemic-graph/<branch> -b <branch> main
```

Commit early and often; merge to `main` locally when done. Push only when asked.

## Before you push

```bash
cargo test --features server --lib          # Rust unit tests
pytest tests/                               # Python round-trip tests
bash scripts/check_no_pyo3.sh               # the no-PyO3 gate
pre-commit run --all-files
```

## Adding an engine capability

Implement it in the relevant Rust module, then expose it across **three layers** —
a `Method` variant in `src/protocol.rs`, a dispatch arm in `src/server.rs`, and a
client method in `epistemic_graph/client.py` — and add a round-trip test in
`tests/`. Compute already resident in the graph should be a **batch** op (one
round-trip over the wire), never a per-row loop: every call crosses a network
boundary (serialize → socket → deserialize), not a function call. See
`docs/RUST_COMPUTE_GUIDE.md` and [AGENTS.md](AGENTS.md).

## Benchmarks

Performance claims are measured, not asserted: `scripts/bench_transport.py`
(latency) and `scripts/bench_scale.py` (multi-shard scaling + per-agent footprint)
— results in `docs/benchmarks.md`.
