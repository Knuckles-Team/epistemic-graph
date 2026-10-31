# Contributing to Medulla

This is the **Rust compute engine** for the
[`agent-utilities`](https://github.com/Knuckles-Team/agent-utilities) ecosystem —
a long-running Tokio service exposed to Python **out-of-process** over
length-prefixed MessagePack on UDS/TCP. There is **no PyO3 / in-process FFI**
(enforced by `scripts/check_no_pyo3.sh`); the shipped wheel is the
`medulla-server` binary plus a pure-Python client.

## Development setup

```bash
scripts/bootstrap.sh                        # Python, test deps, Rust toolchain, git hooks
cargo build --release --features server     # the server binary
```

Pick any spec under [`specs/`](specs/README.md), implement it on a branch and
open a PR; everything needed to run the gates is installed by the bootstrap
(add `--scanners` for the manual-stage scanners).

## Branch / worktree workflow

Take the operator's own git worktree on the operator's own branch (do not edit the canonical checkout
directly — a concurrent session or sync may reset it):

```bash
rm_worktree add medulla <your-branch>     # repository-manager MCP, or:
git worktree add ${WORKTREE_ROOT}/medulla/<branch> -b <branch> main
```

Commit early and often, push the operator's branch, and open a pull request against `main`;
hosted CI runs the same hooks as the operator's local commit plus the full test matrix.

## Before the operator push

```bash
cargo test --features server --lib          # Rust unit tests
pytest tests/                               # Python round-trip tests
bash scripts/check_no_pyo3.sh               # the no-PyO3 gate
uvx pre-commit run --config .config/pre-commit.yaml --all-files
```

Run the storage crate separately when testing the workspace:

```bash
cargo test --locked -p medulla-storage --all-features --no-fail-fast -- --test-threads=1
```

Its crash-stage tests spawn child processes that can briefly inherit another
thread's read-only database lock before exec. Serial test scheduling isolates
that process lifecycle while retaining every storage test and crash stage.
Exclude `medulla-storage` from a separate parallel `cargo test --workspace` invocation;
CI uses these same two steps. Do not retry or accept lock errors in place of the
asserted storage error.

The complexity (cccc) and KISS gates have written, measured terms of acceptance
in [`docs/quality-gate-terms.md`](docs/quality-gate-terms.md) — what they accept,
why, and what the remaining backlog is. Read it before changing a threshold.

## Adding an engine capability

Implement it in the relevant Rust module, then expose it across **three layers** —
a `Method` variant in `src/protocol.rs`, a dispatch arm in `src/server.rs`, and a
client method in `medulla/client.py` — and add a round-trip test in
`tests/`. Compute already resident in the graph should be a **batch** op (one
round-trip over the wire), never a per-row loop: every call crosses a network
boundary (serialize → socket → deserialize), not a function call. See
`docs/RUST_COMPUTE_GUIDE.md` and [AGENTS.md](AGENTS.md).

## Benchmarks

Performance claims are measured, not asserted: `scripts/bench_transport.py`
(latency) and `scripts/bench_scale.py` (multi-shard scaling + per-agent footprint)
— results in `docs/benchmarks.md`.

## Spec-driven contributions

Every change follows one process: constitution, route, specify, clarify, plan, tasks, analyze, implement, land, release.
Each step names its skill and the files it writes, in the
[organization contribution guide](https://github.com/Knuckles-Team/.github/blob/main/CONTRIBUTING.md).
Read the [constitution](https://github.com/Knuckles-Team/pipelines/blob/main/.specify/memory/constitution.md),
the [spec standard](https://github.com/Knuckles-Team/pipelines/blob/main/reference/spec-standard.md) and the
[spec templates](.specify/templates), and pick an open row in
[Canopy](https://knuckles-team.github.io/.github/architecture/). Commit with a `Spec: <ROW-ID>` trailer and a bound test.
