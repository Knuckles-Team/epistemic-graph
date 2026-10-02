---
name: epistemic-graph-development
skill_type: skill
description: >
  Develop, build, test, and gate changes in the epistemic-graph engine repository. Use
  before adding a capability, changing the wire contract, or running the test/gate suite
  in this repo. Covers the Rust-native rule (new behaviour is a Rust crate exposed through
  the generated wire contract; Python/Go/JS are clients, not a second implementation),
  where heavy builds run, how to regenerate and check the contract, and the commit vs.
  manual pre-commit stages. Not for live Kubernetes promotion (see epistemic-graph-deploy).
domain: development
license: MIT
tags: [epistemic-graph, rust, development, build, test, wire-contract, gates]
metadata:
  author: Genius
  version: '1.0.0'
---

# Epistemic Graph (EG) development

`epistemic-graph` is a Rust workspace (durable graph database, query/compute engine,
server protocol) with a Python client package. **All new behaviour is native Rust.** A
capability is implemented in a crate under `crates/` (or the root facade `src/`), declared
in the wire contract, and dispatched by the server — never added only to a client. The
Python package (`epistemic_graph/`) carries the full hand-written client plus
contract-generated type projections (`epistemic_graph/generated/`); the Go and JS packages
under `clients/` are **thin, generated bindings** covering only the methods the contract
emits for them. None of the three client packages re-implements engine logic — if you find
yourself writing business logic in Python/Go/JS here, it belongs in a Rust crate instead.

Do not hand-edit generated API bindings, generated docs, or the contract JSON — change the
Rust descriptor that produces them and regenerate (§ below).

## Building and testing

Heavy Rust builds (a full workspace build, the complete test suite, `cargo clippy
--all-features`) belong on **a build host with enough memory**, never the development
machine — iterate locally with `cargo check -p <crate>` / `cargo test -p <crate>` against
one touched crate instead of the whole `full` feature set.

Each git worktree of this repo already gets its **own isolated build output
directory for free**: `.cargo/config.toml` sets a *relative* `target-dir =
"target-isolated"`, which resolves against the worktree root, not a shared path. Never
export `CARGO_TARGET_DIR` to point two concurrent worktrees at the same directory —
`lane-guard` (a commit-stage hook) refuses a commit made with an overriding
`CARGO_TARGET_DIR` set.

```bash
cargo check -p <crate>
cargo test -p <crate> --lib -- --test-threads=<bounded-to-the-host's-cores>
```

Bound `--test-threads` explicitly on a many-core host — the suite is not tuned for
unbounded parallelism, which is also why the manual-only `constrained-parallelism` gate
(`scripts/constrained_parallelism_gate.sh`) re-runs it under a fixed CPU affinity.

## Changing the wire contract

The wire contract — the `Method` enum, its request/result schemas, and every generated
client artifact — is produced from hand-authored descriptor rows in
`crates/eg-capabilities/src/domains/*.rs` by the generator in
`crates/eg-capabilities/src/bin/gen_contract.rs`. **Never hand-edit a generated file.**
Extend a descriptor row (or add one), then regenerate and verify byte-for-byte:

```bash
cargo run -q --locked -p eg-capabilities --features contract --bin gen_contract            # regenerate
cargo run -q --locked -p eg-capabilities --features contract --bin gen_contract -- --check  # verify no drift
```

This one command regenerates/checks `contract/methods.json`, `contract/schemas/*.json`,
`contract/receipt.json`, `docs/capabilities.generated.md`, the API reference under
`docs/api/` (and its OpenAPI projection), and the Python type projection
`epistemic_graph/generated/*.py`. It is wired as the manual-stage `engine-contract-check`
hook and runs identically in hosted CI.

The Go and JS clients additionally embed a WebAssembly copy of the method codec
(`clients/go/codec.go`, `clients/js/codec.mjs`). Regenerate and verify both committed
copies with:

```bash
python3 scripts/build_method_codec_wasm.py          # rebuild both client copies
python3 scripts/build_method_codec_wasm.py --check  # byte-compare against committed copies
```

**Declaring a method before its handler lands:** `src/server/contract_wave.rs` lets a
contract wave declare a method or op ahead of the handler that serves it — a declared but
unserved surface answers `METHOD_NOT_YET_SERVED` (via the `contract_wave_stub!` macro and
the `PENDING_METHODS` list), so clients never see an undeclared method, but also never see
a false promise of support. The package that lands the real handler removes the method's
stub entry; a caller left referencing a deleted stub then fails to compile, which is the
proof every declared surface is actually served.

## Gates

Hooks run from `.config/pre-commit.yaml` in three stages:

- **commit stage** (the default `pre-commit` stage, runs on every commit): formatting/lint
  (`ruff-check`, `ruff-format`, `mypy`), fast architecture/contract checks
  (`contract-method-reachability`, `pinned-reference-resolution`,
  `durable-table-registration`, `registry-test-ownership`, `orphan-modules`), privacy/secret
  scanners, and `lane-guard`. Run it with:
  ```bash
  uvx pre-commit run --config .config/pre-commit.yaml --all-files
  ```
- **manual stage**: the heavy, slow gates not run automatically — `engine-contract-check`
  (above), `cargo-clippy` / `cargo-clippy-all-features`, `rust-arch-lint`,
  `constrained-parallelism`, `pytest`, `wheel-smoke`, and the complexity/clone census
  (`complexity-staged`, `dupehound-changed-functions`, `kiss-changed-rust`,
  `jscpd-differential`/`jscpd-census`, `cccc-census`, `kiss-census`). Run with:
  ```bash
  uvx pre-commit run --config .config/pre-commit.yaml --all-files --hook-stage manual
  ```
- `bash scripts/ci_parity.sh` runs all three tiers (commit, pre-push, manual) in sequence —
  it is the one local command that answers "would hosted CI pass?".

A hook that needs a missing tool or a sibling checkout prints `SKIPPED (<gate>): <reason>`
locally and fails closed in CI; never work around a skip with a suppression or a baseline.

## Working in a shared worktree

Take a real `git worktree add <path> -b <branch> origin/main` for concurrent work — never
edit the canonical checkout (a background sync can reset it) and never use a harness
worktree-isolation tool against this repo (it corrupts the shared git config). Never
`git stash` (the stash ref is repo-wide) and never `git add -A`/`git add .` — stage an
explicit path allowlist. See this repo's `AGENTS.md` ("Branching & isolation") for the
short version of this rule.

## Related

- `epistemic-graph-deploy` — promoting a built artifact and rolling it out live.
- `epistemic-graph-migrations` / `epistemic-graph-troubleshooting` — post-deploy operations.
