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
