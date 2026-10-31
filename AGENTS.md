# Medulla

<!-- agent-contract:start -->
## Agent contract

Tool-neutral rules for any AI agent or human working in this repository. This block is maintained by the fleet standard; edit outside the markers.

- **Owner boundary:** this repository owns facts, records, storage, query, retrieval, ingest, reasoning, decisions and choices. It is the native Rust engine (spec prefix `EG-`). Query covers UQL and natural language. Feature routing across repositories: [ecosystem boundaries](https://github.com/Knuckles-Team/pipelines/blob/main/reference/ecosystem-boundaries.md).
- **Setup:** run `scripts/bootstrap.sh`. It is idempotent and non-interactive. It pins uv and Python, syncs the locked environment and installs the git hooks. It then prints the gate command.
- **Gates:** `uvx pre-commit run --all-files` must pass before any push. Never bypass hooks to get green.
- **Commits:** one logical change per commit with a `Spec: <ID>` trailer (or `Spec: none (<reason>)`), plus a `Co-Authored-By:` trailer naming the assisting agent. Stage explicit paths only; never `git add -A` or `git add .`. Commit under the author's configured Git identity, never a tool or bot name.
- **Org rules:** [organization AGENTS.md](https://github.com/Knuckles-Team/.github/blob/main/AGENTS.md).
<!-- agent-contract:end -->

## Spec-driven development

Every feature starts with a spec, in this order. Read `.specify/memory/constitution.md` first. It inherits the [organization constitution](https://github.com/Knuckles-Team/pipelines/blob/main/.specify/memory/constitution.md). Route the feature with the [ecosystem boundaries](https://github.com/Knuckles-Team/pipelines/blob/main/reference/ecosystem-boundaries.md) and file each part in its owner's `specs/`. Load the `medulla-development` skill, which runs the flow. Rules: [spec standard](https://github.com/Knuckles-Team/pipelines/blob/main/reference/spec-standard.md); templates: `.specify/templates/`, vended by pipelines.

| Step | Skill | Writes | Checked by |
|---|---|---|---|
| Constitution | `speckit-constitution` | `.specify/memory/constitution.md` | spec-standard constitution check |
| Specify | `speckit-specify` | `specs/<id>/spec.md`, `requirements.md` | spec-standard |
| Clarify | `speckit-clarify` | resolves each `[NEEDS CLARIFICATION: ...]` marker | spec-standard clarification check |
| Plan | `speckit-plan` | `plan.md` (upstream sections, Build contract, Production caller chain, Cross-repo seams) | spec-standard plan check |
| Tasks | `speckit-tasks` | `tasks.md`: `- [ ] T001 [P] [US1] <ROW-ID> ...` | spec-standard task-line check |
| Analyze | `speckit-analyze`, `speckit-checklist` | `checklists/requirements.md`, gap fixes | spec-standard analysis and checklist |
| Implement | `speckit-implement` | code and bound tests (`mark.spec("ID")` or `// spec: ID`) | `pipelines-hook spec-trailers` |
| Land | repository development skill | commit with `Spec: <IDs>` directly above `Co-Authored-By` | `spec-status` computes SPECIFIED, LANDED, VERIFIED, RETIRED |
| Release | repository development skill | pipelines `releases/*.yml` bundles (the PIPE-RELEASE release-bundle rows) | `release-standard` |

Never write code for a row that is not SPECIFIED in this repository's `specs/`. If no row owns the behavior, extend the owning spec first. Never write `status.json` or a `Status:` line; the state is computed.

Medulla is a standalone Rust database and compute engine for connected
knowledge, evidence, and multimodal data. Applications can use it directly; it
can also serve as the storage and reasoning engine beneath agent-utilities.
Agent orchestration, connectors, and governed ingestion belong to that optional
application layer, not to this database.

## Boundaries

- **Owns:** facts, records, vocabulary, durable state and storage. It also owns query (UQL and natural language), retrieval, ingest, reasoning, decisions and choices. Code is native Rust, and each spec row sets budgets and names a benchmark.
- **Never builds:** served operations, admission or auth (route them to arbora). Connector contracts and transports go to agent-connector-sdk. Run mechanics go to agent-utilities.
- **Allowed dependencies:** Rust crates, and its own Python client and bindings. It imports no other ecosystem package.

Route every feature with the [ecosystem boundaries standard](https://github.com/Knuckles-Team/pipelines/blob/main/reference/ecosystem-boundaries.md) before any code. A part this repository does not own goes to its owner's specs.

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
- `medulla/`: Python client and package integration.
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

## Release

Bump first, tag last. A tag on a commit with the previous version rebuilds the old version. PyPI then refuses it.

1. Bump first: run `bump2version --config-file .config/bumpversion.cfg --no-tag patch` (or `minor`, `major`, or `--new-version X.Y.Z`). The wheel version is dynamic from `crates/medulla-pyengine/Cargo.toml` `[package].version`. The config also moves root `Cargo.toml`, `Cargo.lock`, `README.md`, `docker/Dockerfile`, `pyproject.toml` and the server package. Refresh the lock (`cargo update --workspace`) and any fixture that embeds the version. Then open the PR.
2. Merge the bump PR to `main`.
3. On the merged `main` commit, confirm `crates/medulla-pyengine/Cargo.toml` and `current_version` in `.config/bumpversion.cfg` equal the intended `X.Y.Z`. PyPI must not have that version: `curl -s -o /dev/null -w '%{http_code}' https://pypi.org/pypi/medulla/X.Y.Z/json` prints `404`. Alternatively run `python3 -I <pipelines>/.github/actions/verify-release-version/verify_release_version.py --root . --ref refs/tags/vX.Y.Z` once it is on pipelines `main`.
4. Only then push the tag `vX.Y.Z` on that commit; `release.yml` triggers on `v*`.
5. Confirm the `medulla` wheels and sdist and the `medulla-server` wheels are on PyPI at `X.Y.Z`.
6. Tag on the wrong commit and nothing published: delete the tag and re-tag. Anything published: cut the next patch, because PyPI never accepts a file twice.

## Branching & isolation

Use a separate real Git worktree for concurrent repository changes. Keep builds
and generated outputs scoped to that worktree, and do not use shared stash state
to transfer concurrent work. Follow the workspace and repository instructions
for publication and integration.

## Specs: extend first

Find the spec row that owns the behavior before any code change. Search with `git grep -n "<term>" -- specs`. Cite the row ID in the commit `Spec:` trailer. Extend the owning spec before any new spec text. Add a child row, a new rollup, or a dated `plan.md` "Amendments" entry. Create a new spec only for a capability that no spec owns. Audits, reviews, and carry-overs land in the owning spec. They never get a spec directory. Reuse an existing function, module, or store before adding one. The rules are the ecosystem [spec standard](https://github.com/Knuckles-Team/pipelines/blob/main/reference/spec-standard.md).
