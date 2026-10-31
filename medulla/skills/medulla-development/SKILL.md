---
name: medulla-development
skill_type: skill
description: >
  Develop, build, test, and gate changes in the medulla engine repository. Use
  before adding a capability, changing the wire contract, or running the test/gate suite
  in this repo. Covers the Rust-native rule (new behaviour is a Rust crate exposed through
  the generated wire contract; Python/Go/JS are clients, not a second implementation),
  where heavy builds run, how to regenerate and check the contract, and the commit vs.
  manual pre-commit stages. Not for live Kubernetes promotion (see medulla-deploy).
domain: development
license: MIT
tags: [medulla, rust, development, build, test, wire-contract, gates]
metadata:
  author: Genius
  version: '1.0.0'
---

# Medulla development

Use this skill from an medulla checkout before changing an engine
capability or its public contract. Start with the repository's
[engineering guide](https://github.com/Knuckles-Team/medulla-engine/blob/main/AGENTS.md)
and the instructions for your assigned lane; they own the current setup,
commands, resource limits, and publication rules.

## Step 0: route the feature

Route the feature before any code.

1. Read the [ecosystem boundaries standard](https://github.com/Knuckles-Team/pipelines/blob/main/reference/ecosystem-boundaries.md).
2. Split the feature into parts with its routing procedure.
3. If a part is not owned by medulla, stop work on that part. Write its row in the owner's `specs/`, then link it by ID.
4. Continue here only with the parts that medulla owns.

## Spec-driven flow

Run this before any code. Full table and gates: the Spec-driven development section of `AGENTS.md` and the [spec standard](https://github.com/Knuckles-Team/pipelines/blob/main/reference/spec-standard.md).

1. Read `.specify/memory/constitution.md`; create it from `.specify/templates/constitution-template.md` if absent (`speckit-constitution`).
2. Specify the feature: `speckit-specify` gathers the inputs and writes `specs/<stable-id>/spec.md` and `requirements.md`.
3. Clarify: resolve every `[NEEDS CLARIFICATION: ...]` marker (`speckit-clarify`).
4. Plan: `plan.md` from `.specify/templates/plan-template.md`, including Build contract, Production caller chain and Cross-repo seams (`speckit-plan`).
5. Tasks: `tasks.md` lines `- [ ] T001 [P] [US1] <ROW-ID> ...` (`speckit-tasks`).
6. Analyze: fix the gaps and fill `checklists/requirements.md` (`speckit-analyze`).
7. Implement and land with the delivery contract in this skill (`speckit-implement`); release through pipelines `releases/*.yml` bundles.

Never write code for a row that is not SPECIFIED. Feature directories use the stable spec ID, never a numbered prefix. The state is computed from rows, `Spec:` trailers and test bindings; never write `status.json` or a `Status:` line.

## Rapid delivery: the 5-minute contract

Specs are already designed. The work is implementation, delivered in very small, continuously landed slices.

**Extend first (anti-sprawl)**
- Before any code change, find the spec row that owns the behavior: `git grep -n "<term>" -- specs`. Cite its ID in the commit `Spec:` trailer.
- Before any new spec text, extend the owning spec: a child row (`R012.4`), a new rollup at the end of that spec, or a dated entry in the `plan.md` section "Amendments". Create a new spec only for a capability that no spec owns, and record the search in "Why a new spec".
- Audits, reviews, gap analyses and carry-overs never get a spec directory. Their findings land in the spec that owns the row; a finding against another repository lands in that repository's spec.
- Before any new function, module, planner, selector or store, search for the existing one and reuse it. A second implementation of an owned capability is a defect, not a delivery.
- The rules are in `specs/README.md`. The `spec-standard` gate enforces the names, file set, title and index.

**Time and size**
- One agent delivers one PR in 5 minutes or less. No agent or sub-agent runs longer than 5 minutes; an orchestrator dispatches the next slice to a fresh agent.
- Self-timer (hard): run `date +%s > /tmp/agent-start-$$` as your FIRST command, and before every later step check elapsed with `echo $(( $(date +%s) - $(cat /tmp/agent-start-*) ))`. At 300 s, stop starting new work. At 480 s, commit what passes, push it, open the PR, report and STOP. Never exceed 600 s. Keep installed commit/push hooks enabled; use the immutable-range publication procedure below and report required gate time separately.
- CI turns around in 5 minutes or less. PR CI runs the fast subset: lint, type check, the spec check, and only the tests and crates the diff touches. Pushes to main run the full suite.
- CI timer (hard): every PR-path job carries `timeout-minutes: ${{ github.event_name == 'pull_request' && 10 || 60 }}`, so a PR gate is killed at 10 minutes and a slow gate shows up as a failure instead of a silent queue.
- Never spawn sub-agents from a delivery agent.

**Investigation budget**
- At most 2 minutes and about 10 tool calls of reading before the first edit, and at most about 25 tool calls per PR.
- If the change is not clear by then, ship the `.1` slice (typed model plus refusal test) or skip the row with a one-line note. Do not write deferral essays.
- Trust the orchestrator's evidence and ID list; do not re-verify it.

**Sizing (deterministic)**
- Split a requirement when its size score is above 6, it names more than 2 code roots, or it is cross-repo. Children are `<ID>.<n>`, producer first.
- Net-new work is sliced, never skipped: `.1` typed model plus validation and refusal tests, `.2` the one entry point that uses it, `.3`+ each further behavior.
- Cross-repo moves split into one child per repo: the destination copies the behavior first, then the source switches importers and deletes.

**Specs update themselves (land VERIFIED, not just LANDED)**
- Every commit that delivers a requirement ends with the trailer `Spec: <ID>[, <ID>...]`. A commit that touches code without delivering one says `Spec: none (ci|deps|refactor|fix-forward)`.
- Every delivered ID gets at least one test bound to it in the same PR. Python: `@pytest.mark.spec("<ID>")`. Rust/TypeScript: a `// spec: <ID>` line directly above the test.
- Never write `status.json`; the state is computed (`pipelines-hook spec-status`). Never write placeholder SHAs. If the work is only part of a requirement, decompose it into `<ID>.<n>` children first and deliver whole children.
- Write the marker with the literal ID, never a variable. Put `Spec:` and `Co-Authored-By:` on consecutive lines in the last paragraph of the commit message, or `Spec:` is not read as a trailer.
- The generator applies the rules. Run `pipelines-hook spec-status` to see each row's state.

**Delivered means the row's acceptance is met**
- Never delete or stub out behaviour whose replacement is not wired. Never make a caller "fail closed" in place of repointing it. Never substitute a scope, weaken an assertion or add an allowlist entry to pass a gate.
- If the replacement lives in another repository, do not deliver: record the blocker in the row, name the missing capability, and stop. The orchestrator adds the row in the owning repository.
- Never re-add a path that another requirement retired; report the conflict.
- Valid outcomes are DELIVERED (with a bound test), SPLIT, BLOCKED and GAP (a "landed" row that is not really delivered). A half-delivery is not an outcome.

**Where progress shows**
- The repository's spec-delivery page computes each state at read time from rows, `Spec:` trailers and test bindings. SPECIFIED is written, LANDED is on main, VERIFIED is on main with a bound test.

**Before every push**
- Work in a real `git worktree add` from `origin/main`. Never edit a shared checkout, never `git stash`, never `git add -A`, never force-push.
- Run the configured commit and push stages against immutable refs as described below. Fix every failure. No `noqa`, `type: ignore`, skip, xfail or whitelist entries to pass a gate.
- Select targeted tests for local feedback, and honor broader checks required by the configured gates. Run the type checker on every changed file, and keep each function at cyclomatic complexity 10 or less.

**Landing**
- Every PR lands within minutes of going green; no PR sits idle. Mechanical conflicts (generated files, Markdown) are resolved automatically by the merge-train tool. Real conflicts are additive in most cases and are resolved, not deferred.
- Many open PRs land together as a merge train: one integration branch and one CI run.
- Full CI on main catches what the fast subset missed; regressions are fixed forward immediately.
- Landing in this repo: delivery agents open the PR and do not run per-PR build-host builds or merge. The orchestrator lands EG in batches about every 20 minutes, validated by one warm integration build (`cargo check --workspace --all-features` plus contract regeneration) on the least-loaded build host.

**Report**: at most 8 lines: PR URL, requirement IDs, test result, and any `ID:<main sha>` proof for rows already on main.

## Choose the owning implementation

Engine behavior belongs in Rust: locate the responsible crate under `crates/`
or the server/facade under `src/` before editing a client. The Python package
contains the client and generated type projections. Go and JavaScript bindings
consume the generated contract. A client-only implementation does not make an
engine capability available.

Keep orchestration and source connectors in their own repositories. Treat
existing descriptors and dispatch code as authoritative when prose has drifted.

## Plan bounded validation

Identify the touched crate and the tests that reach its public entry point.
Coordinate a build-host slot before starting native work; use the assigned CPU,
memory, time, and artifact limits. A narrow local check cannot establish the
full feature matrix or qualify a release wheel.

The relative output directory in `.cargo/config.toml` separates worktree build
artifacts. Do not redirect concurrent worktrees into one shared Cargo target.
Consult the engineering guide's setup section for bootstrap and general
per-crate commands; the scoped `medulla-types` shortcut below is for local feedback.

Record the tested source identity, enabled features, executed checks, and any
unexecuted work. A missing prerequisite is an evidence gap, not a passed gate.

### Fast scoped `medulla-types` validation

For producer-only batches, reuse the assigned warm validation worktree and its
existing `target-isolated` across sequential batches. Integrate published
changes with normal merges, preserve original branches and cached artifacts,
and freeze the tested source before compiling. Keep concurrent worktrees on
their own targets.

Use the package-only override below for fast local feedback. It changes only
`medulla-types` test optimization to 0; preserve assertions, selected tests, dependency
profiles, features, and incremental settings. Do not edit repository Cargo
profiles or change full optimized CI. Keep three jobs and the existing test
profile's debug level 0, incremental compilation, and 256 codegen units.

Select affected filters and build once with `contract-schema`; later filters
reuse that binary. For the harness and guardrail producer batch:

```bash
for filter in harness_config measured_fact decision::guardrail; do
  CARGO_BUILD_JOBS=3 CARGO_PROFILE_DEV_DEBUG=0 \
    cargo --config 'profile.test.package.medulla-types.opt-level=0' \
    test -p medulla-types --lib --offline --no-default-features \
    --features contract-schema "$filter" || exit
done
```

Avoid a second normal-feature build when schema-enabled tests cover the same
producer code. Keep the assigned time bound; report the exact source, selected
filters, terminal results, and Cargo compile time separately from setup and
cached test time. This local shortcut does not establish optimized, facade, or
served acceptance.

## Change a wire capability

Find the capability's descriptor in `crates/medulla-capabilities/src/domains/` and
trace it through the server dispatch. The method catalog and its digest are
computed from the descriptor rows at run time; update the Python wire model in
`medulla/generated/` by hand to match the Rust serde types.

The generator invocation and its configured features are maintained in
`.config/pre-commit.yaml` under `engine-contract-check`. For the Go and
JavaScript codec artifacts, use `scripts/build_method_codec_wasm.py` and its
check mode; changing only one embedded copy leaves clients inconsistent.

If a contract must land before its implementation, inspect
`src/server/contract_wave.rs`. Its pending-method machinery exposes an explicit
`METHOD_NOT_YET_SERVED` result. Keep that declaration until the real handler
lands; a generated client method alone is not serving evidence.

## Run the applicable gates

Use the engineering guide's command section and `.config/pre-commit.yaml` as
the gate definitions. Commit-stage checks cover the fast source boundaries;
manual and hosted checks cover the broader feature, scanner, and wheel work.
`scripts/ci_parity.sh` composes the local tiers when that workload is authorized.

### Publish a frozen, explicit range

Fetch the intended target's remote refs before the first push (for example,
`git fetch origin refs/heads/main:refs/remotes/origin/main`). Finish generators,
including spec status, and commit their outputs before freezing the candidate.
Record the immutable source `HEAD_SHA`, the PR merge-base against the fetched
target, and the actual outgoing push base `BASE_SHA`; these can differ. For an
existing remote branch, the push base is its advertised old SHA. For a new branch,
pre-commit 4.6.2 finds commits absent from local `--remotes=origin` refs and uses
the first outgoing commit's parent; a root commit selects all files. Stale remote
refs can therefore widen the push range beyond the PR diff.

With those recorded SHAs, run both configured stages:

```bash
pre-commit run --config .config/pre-commit.yaml --hook-stage pre-commit \
  --from-ref "$BASE_SHA" --to-ref "$HEAD_SHA"
pre-commit run --config .config/pre-commit.yaml --hook-stage pre-push \
  --from-ref "$BASE_SHA" --to-ref "$HEAD_SHA"
```

Check the PR merge-base range too if it includes files outside the push range.
Record the checked file count and each stage's result. Keep the checkout frozen
through both stages: 4.6.2 compares the entire worktree diff before/after each hook,
so another writer can falsely produce “files modified by this hook.” If a hook or
generator changes files, commit the result and check the new immutable range.
Verify `git status --short` and `git rev-parse HEAD` before ordinary `git push`;
preserve and explicitly account for pre-existing unrelated dirty/untracked files.
Do not disable installed hooks or suppress genuine manifest-triggered full Rust
checks. Setting `PRE_COMMIT_FROM_REF` externally does not fix a push range: the
hook runner overwrites it from its computed arguments.

Record pre-commit's version and the resolved pipelines checkout SHA. Its cache
lookup uses the literal repository/ref key, so `pipelines@main` can reuse an old
checkout. Keep the sanctioned `rev: main`; when refresh is needed, use a separate
refreshed `PRE_COMMIT_HOME` and preserve shared caches. These behaviors were
checked in pre-commit 4.6.2's `commands/hook_impl.py::_pre_push_ns`,
`commands/run.py` (range environment and worktree diff comparison), and
`store.py::_new_repo`; recheck those sources when upgrading the runner.

Investigate each failure at the failing source and revision. Do not hide an
unavailable tool with a skip, weaken a scanner, or call an advisory result a
clean census. Passing focused tests does not waive a red security check.

## Keep shared work isolated

Create a separate checkout or ordinary Git worktree for the assigned change.
Avoid harness-managed worktree setup that can alter shared Git configuration.
The stash belongs to the whole repository, so it is not a lane-local backup.
Stage reviewed paths explicitly and inspect the resulting patch before commit.
Leave other owners' changes, build slots, and publication decisions alone.

## Server package

The server and its subcommands (`restore`, `migrate-shards`, `rf019-observe`,
`lake-fixture-export`) are engine code in this repository, and so is its packaging. The
public `medulla` wheel carries no executables; `pip install "medulla[server]"`
installs the matching `medulla-server` package at the same version.

- `packaging/medulla-server/` holds the package: a maturin `bindings = "bin"` build over
  the root manifest's `medulla-server` binary, `medulla_server.binary_path()`
  and `BinaryNotFoundError`, and its tests. `medulla/server_binary.py` resolves the
  binary through that function. Build the wheel with
  `maturin build --profile release-server` from that directory, on a build host, never on a
  workstation. The `release-server` Cargo profile is size-optimised with `panic = "unwind"`.
- `docker/Dockerfile` builds the server image from the server wheel staged in `docker/wheels`.
- `.github/workflows/release.yml` job `build-server` builds one wheel per Linux architecture
  natively and uploads each to PyPI right away; `server-image` pushes one image per
  architecture by digest and `server-manifest` joins them into one tag. `ci.yml` job
  `server-wheel-cache` warms the compile cache `egs-<arch>` on main pushes.
- To publish the server package of an already tagged version, dispatch `release.yml` with
  `packages=server` and `source_ref=v<version>`. The guard still requires a successful
  `ci.yml` run on that tag's commit.
- The version moves with the engine through `.config/bumpversion.cfg`, which covers the package
  `pyproject.toml`, its `__version__`, the `server` extra and the Dockerfile.

## Release

Summary in `AGENTS.md` "Release". Bump first, tag last. A tag on a commit with the previous version rebuilds the old version. PyPI then refuses it.

1. Bump first: run `bump2version --config-file .config/bumpversion.cfg --no-tag patch` (or `minor`, `major`, or `--new-version X.Y.Z`). The wheel version is dynamic from `crates/medulla-pyengine/Cargo.toml` `[package].version`. The config also moves root `Cargo.toml`, `Cargo.lock`, `README.md`, `docker/Dockerfile`, `pyproject.toml` and the server package. Refresh the lock (`cargo update --workspace`) and any fixture that embeds the version. Then open the PR.
2. Merge the bump PR to `main`.
3. On the merged `main` commit, confirm `crates/medulla-pyengine/Cargo.toml` and `current_version` in `.config/bumpversion.cfg` equal the intended `X.Y.Z`. PyPI must not have that version: `curl -s -o /dev/null -w '%{http_code}' https://pypi.org/pypi/medulla/X.Y.Z/json` prints `404`. Alternatively run `python3 -I <pipelines>/.github/actions/verify-release-version/verify_release_version.py --root . --ref refs/tags/vX.Y.Z` once it is on pipelines `main`.
4. Only then push the tag `vX.Y.Z` on that commit; `release.yml` triggers on `v*`.
5. Confirm the `medulla` wheels and sdist and the `medulla-server` wheels are on PyPI at `X.Y.Z`.
6. Tag on the wrong commit and nothing published: delete the tag and re-tag. Anything published: cut the next patch, because PyPI never accepts a file twice.

## Operations are separate

Use `medulla-deploy` for rollout, `medulla-migrations` for store
changes, and `medulla-troubleshooting` for runtime diagnosis. A source
repair or development wheel is not authorization for production promotion.
