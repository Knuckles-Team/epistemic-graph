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

# Epistemic Graph development

Use this skill from an epistemic-graph checkout before changing an engine
capability or its public contract. Start with the repository's
[engineering guide](https://github.com/Knuckles-Team/epistemic-graph/blob/main/AGENTS.md)
and the instructions for your assigned lane; they own the current setup,
commands, resource limits, and publication rules.

## Rapid delivery: the 5-minute contract

Specs are already designed. The work is implementation, delivered in very small, continuously landed slices.

**Extend first (anti-sprawl)**
- Before any code change, find the spec row that owns the behavior: `git grep -n "<term>" -- specs`. Cite its ID in the commit `Spec:` trailer.
- Before any new spec text, extend the owning spec: a child row (`R012.4`), a new rollup at the end of that spec, or a dated entry in the `plan.md` section "Amendments". Create a new spec only for a capability that no spec owns, and record the search in "Why a new spec".
- Audits, reviews, gap analyses and carry-overs never get a spec directory. Their findings land in the spec that owns the row; a finding against another repository lands in that repository's spec.
- Before any new function, module, planner, selector or store, search for the existing one and reuse it. A second implementation of an owned capability is a defect, not a delivery.
- The rules are the ecosystem [spec standard](https://github.com/Knuckles-Team/pipelines/blob/main/reference/spec-standard.md); the `spec-standard` gate enforces the names, file set, title and index.

**Time and size**
- One agent delivers one PR in 5 minutes or less. No agent or sub-agent runs longer than 5 minutes; an orchestrator dispatches the next slice to a fresh agent.
- Self-timer (hard): run `date +%s > /tmp/agent-start-$$` as your FIRST command, and before every later step check elapsed with `echo $(( $(date +%s) - $(cat /tmp/agent-start-*) ))`. At 300 s, stop starting new work. At 480 s, commit what passes, push it, open the PR, report and STOP. Never exceed 600 s. Never wait on CI or background jobs; use `git commit --no-verify` and `git push --no-verify` after running pre-commit on the changed files yourself.
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
- Never hand-edit `status.json`; it is generated (`spec-status`). Never write placeholder SHAs. If the work is only part of a requirement, decompose it into `<ID>.<n>` children first and deliver whole children.
- Write the marker with the literal ID, never a variable. Put `Spec:` and `Co-Authored-By:` on consecutive lines in the last paragraph of the commit message, or `Spec:` is not read as a trailer.
- Generated status depends on the commit: run `pipelines-hook spec-status --write` again after committing and commit the result.
- The full rules are in `plans/refactor/reconciliation-20261006/SPEC-STATUS-LIFECYCLE.md`.

**Delivered means the row's acceptance is met**
- Never delete or stub out behaviour whose replacement is not wired. Never make a caller "fail closed" in place of repointing it. Never substitute a scope, weaken an assertion or add an allowlist entry to pass a gate.
- If the replacement lives in another repository, do not deliver: record the blocker in the row, name the missing capability, and stop. The orchestrator adds the row in the owning repository.
- Never re-add a path that another requirement retired; report the conflict.
- Valid outcomes are DELIVERED (with a bound test), SPLIT, BLOCKED and GAP (a "landed" row that is not really delivered). A half-delivery is not an outcome.

**Where progress shows**
- `https://knuckles-team.github.io/<repo>/spec-delivery/` is rebuilt from `specs/*/status.json` on each push to main. SPECIFIED is written, LANDED is on main, VERIFIED is on main with a bound test, RETIRED is withdrawn.
- Orchestrating a session (resuming, running waves, landing trains): `plans/refactor/reconciliation-20261006/SPEC-DELIVERY-PLAYBOOK.md` and the prompt templates in `lane-prompts/` beside it.

**Before every push**
- Work in a real `git worktree add` from `origin/main`. Never edit a shared checkout, never `git stash`, never `git add -A`, never force-push.
- Run the repo's own pre-commit on the changed files: `uvx --from pre-commit==4.6.0 pre-commit run --files $(git diff --name-only origin/main...HEAD)`. Fix every failure. No `noqa`, `type: ignore`, skip, xfail or whitelist entries to pass a gate.
- Run the targeted tests only, never a full suite. Run the type checker on every changed file, and keep each function at cyclomatic complexity 10 or less.

**Landing**
- Every PR lands within minutes of going green; no PR sits idle. Mechanical conflicts (generated files, Markdown, `status.json`) are resolved automatically by the merge-train tool. Real conflicts are additive in most cases and are resolved, not deferred.
- Many open PRs land together as a merge train (one integration branch, one CI run), built with `plans/tools/l9/eg-bulk-merge.sh`.
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
Consult the engineering guide's setup section for the current bootstrap and
per-crate commands instead of copying command sequences into this skill.

Record the tested source identity, enabled features, executed checks, and any
unexecuted work. A missing prerequisite is an evidence gap, not a passed gate.

## Change a wire capability

Find the capability's descriptor in `crates/eg-capabilities/src/domains/` and
trace it through the server dispatch. After changing that authority, use
`crates/eg-capabilities/src/bin/gen_contract.rs` to regenerate its projections
and run the generator's check mode. The generated method catalogs, schemas,
receipts, client types, and API documentation must agree with that source.
Do not repair a mismatch by editing generated output directly.

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

Investigate each failure at the failing source and revision. Do not hide an
unavailable tool with a skip, weaken a scanner, or call an advisory result a
clean census. Passing focused tests does not waive a red security check.

## Keep shared work isolated

Create a separate checkout or ordinary Git worktree for the assigned change.
Avoid harness-managed worktree setup that can alter shared Git configuration.
The stash belongs to the whole repository, so it is not a lane-local backup.
Stage reviewed paths explicitly and inspect the resulting patch before commit.
Leave other owners' changes, build slots, and publication decisions alone.

## Operations are separate

Use `epistemic-graph-deploy` for rollout, `epistemic-graph-migrations` for store
changes, and `epistemic-graph-troubleshooting` for runtime diagnosis. A source
repair or development wheel is not authorization for production promotion.
