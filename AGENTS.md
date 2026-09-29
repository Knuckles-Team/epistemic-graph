# epistemic-graph

`epistemic-graph` is a standalone Rust database and compute engine for connected
knowledge, evidence, and multimodal data. Applications can use it directly; it
can also serve as the storage and reasoning engine beneath agent-utilities.
Agent orchestration, connectors, and governed ingestion belong to that optional
application layer, not to this database.

## What this repository owns

This repository owns the durable graph database, query and compute engine,
service protocol, and Python client. Application-level orchestration,
connectors, and governed ingestion are optional integrations owned elsewhere.

## Architecture and module map

The Rust workspace provides the durable engine, query planner, wire protocols,
and server. The Python package provides client and interoperability APIs. A
single-node build is the normal deployment; `cluster` enables replicated
operation, while `full-extras` enables optional GPU and robotics integrations.
The architecture index in `docs/architecture/` maps the major engine
subsystems; `docs/capabilities.md` is the operation-level support matrix.

- `src/`, `crates/`: Rust engine, server, and supporting crates.
- `epistemic_graph/`: Python client and package integration.
- `clients/`, `proto/`: client and wire-protocol definitions.
- `docs/`, `architecture/`: user guides and technical references.
- `scripts/`, `tests/`: development gates and regression coverage.
- `.config/`: explicit-path tool configuration, including
  `.config/pre-commit.yaml`.

The engine owns durable records, cross-modal transactions, and query execution.
Clients authenticate requests through the documented request envelope and
policy context. Treat the code and generated API surfaces as authoritative when
documentation differs.

## Setup

From a fresh clone (locally or in a Claude Code cloud session, where
`.claude/hooks/session-start.sh` runs it automatically):

```bash
scripts/bootstrap.sh              # pinned Python + test deps, Rust toolchain, git hooks
scripts/bootstrap.sh --scanners   # also cccc/kiss/dupehound/jscpd/arch-lint/cargo-deny (~15 min cold)
```

Hooks that need a missing tool or the sibling `agent-utilities` checkout print
`SKIPPED (<gate>): <reason>` locally and fail closed in CI. On a 4-CPU container
keep `CARGO_BUILD_JOBS=3` and debuginfo off (the session hook sets both), and
iterate with `cargo check`/`cargo test -p <crate>` rather than the whole `full`
suite.

## Commands

Regenerate the status page after changing its source data with:

```bash
python3 scripts/build_status_page.py --write
```

Hosted CI runs the same commit-stage hooks (`lint-and-architecture` job), so
this is the local equivalent; `ci_parity.sh` adds the pre-push and manual
(scanner, census, full-suite) stages:

```bash
uvx pre-commit run --config .config/pre-commit.yaml --all-files
uvx pre-commit run --config .config/pre-commit.yaml --all-files --hook-stage manual
bash scripts/ci_parity.sh
```

## Quality gates

The automatic pre-push gate is intentionally bounded for a sub-ten-minute
feedback cycle. It checks generated-artifact freshness, formatting and lint,
cheap contract/static checks, and targeted smoke coverage. Exhaustive feature
matrices, full integration suites, wheel reproducibility, and repository-wide
censuses are manual or hosted-CI validations, not automatic push hooks.

Hosted CI remains the exhaustive publication gate. Keep generated documentation,
contract surfaces, lockfiles, and Rust formatting in sync with their sources.

## Development rules

Do not hand-edit generated API bindings or generated documentation; update their
authoritative inputs and use the repository's generator. Preserve wire protocol
and durability compatibility, and add focused regression coverage for behavior
changes. Keep local tool configuration under `.config/` when the tool supports
an explicit config path.

## Documentation

The user guide and operation matrix are maintained in `docs/`; subsystem design
references are indexed from `docs/architecture/`. Update source documentation
with behavior changes and run the status-page freshness check after editing its
inputs.

## Branching & isolation

Use a separate real Git worktree for concurrent repository changes. Keep builds
and generated outputs scoped to that worktree, and do not use shared stash state
to transfer concurrent work. Follow the workspace and repository instructions
for publication and integration.
