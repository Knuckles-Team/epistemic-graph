# Contributing to epistemic-graph

This is the **Rust compute engine** for the
[`agent-utilities`](https://github.com/Knuckles-Team/agent-utilities) ecosystem —
a long-running Tokio service exposed to Python **out-of-process** over
length-prefixed MessagePack on UDS/TCP. There is **no PyO3 / in-process FFI**
(enforced by `scripts/check_no_pyo3.sh`); the shipped wheel is the
`epistemic-graph-server` binary plus a pure-Python client.

## Development setup

```bash
scripts/bootstrap.sh                        # Python, test deps, Rust toolchain, git hooks
cargo build --release --features server     # the server binary
```

Pick any spec under [`specs/`](specs/README.md), implement it on a branch and
open a PR; everything needed to run the gates is installed by the bootstrap
(add `--scanners` for the manual-stage scanners).

## Branch / worktree workflow

Take your own git worktree on your own branch (do not edit the canonical checkout
directly — a concurrent session or sync may reset it):

```bash
rm_worktree add epistemic-graph <your-branch>     # repository-manager MCP, or:
git worktree add ${WORKTREE_ROOT}/epistemic-graph/<branch> -b <branch> main
```

Commit early and often, push your branch, and open a pull request against `main`;
hosted CI runs the same hooks as your local commit plus the full test matrix.

## Before you push

```bash
cargo test --features server --lib          # Rust unit tests
pytest tests/                               # Python round-trip tests
bash scripts/check_no_pyo3.sh               # the no-PyO3 gate
uvx pre-commit run --config .config/pre-commit.yaml --all-files
```

The complexity (cccc) and KISS gates have written, measured terms of acceptance
in [`docs/quality-gate-terms.md`](docs/quality-gate-terms.md) — what they accept,
why, and what the remaining backlog is. Read it before changing a threshold.

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

## Spec-driven contributions

Start in [`specs/`](specs/README.md) with a stable ID and the repository-owned `spec.md`, `plan.md`,
`test-spec.md`, and `tasks.md`. Use the [universal-skills SDD
workflow](https://github.com/Knuckles-Team/universal-skills/tree/main/universal_skills/development-workflows/sdd-full-lifecycle)
and its
[spec-generator](https://github.com/Knuckles-Team/universal-skills/tree/main/universal_skills/development/spec-generator),
[spec-verifier](https://github.com/Knuckles-Team/universal-skills/tree/main/universal_skills/development/spec-verifier),
and
[task-planner](https://github.com/Knuckles-Team/universal-skills/tree/main/universal_skills/development/task-planner)
skills. The file sequence aligns with [GitHub Spec Kit
v1.0.12](https://github.com/github/spec-kit/releases/tag/v1.0.12); `test-spec.md` makes our test and
quality contract explicit. Link the PR to its spec IDs and include exact test, wiring, CCCC, jscpd,
dupehound, and KISS evidence before proposing a landed status.
