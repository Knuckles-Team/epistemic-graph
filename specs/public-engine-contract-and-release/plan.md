# EG-CONTRACT — Architecture and implementation plan

**State:** PROPOSED. This plan is executable from a fresh public checkout.

## Existing implementation to reuse

| Authority | Existing component | Rule for changes |
|---|---|---|
| Method and errors | `crates/eg-capabilities/src/bin/gen_contract.rs`, Rust `ErrorCode` declarations | Extend declarations and generator; never edit generated JSON as the source. |
| Generated catalogs | `contract/methods.json`, `contract/errors.json`, `contract/schemas/`, `contract/fixtures/` | Check digest and fixture parity after regeneration. |
| Python models and package | `epistemic_graph/generated/models.py`, `epistemic_graph/contract/` | Re-export generated wire classes and verify installed wheel contents. |
| Canonical signing | Rust method codec and `scripts/build_method_codec_wasm.py` | Use its body bytes in Go and JS; do not port canonicalization by hand. |
| Go/JS consumers | `clients/go/`, `clients/js/` | Run vector and error tests against embedded WASM and an installed/served engine. |
| Release checks | `scripts/generated_artifacts.py`, `scripts/check_wheel_completeness.py`, `scripts/check_wheel_privacy.py`, `scripts/ci_parity.sh`, `.github/workflows/release.yml` | Make hosted checks hermetic and source-revision-bound; keep live deployment proof a separate gate. |

## Contract data model

Each method record has a stable `id`, `domain`, `request_schema`, `result_schema`, `policy`, `replay_class`, `stability`, `consumer_profiles`, `is_wire_callable` and `error_set`. Each error record has `code`, `class`, `retryable`, `http_status_hint`. The generator fails if a referenced schema or error is absent, a method ID is duplicated, an allowed wire method has no dispatcher, or a stable code changes meaning without a deliberate compatibility change. The receipt hashes every generated file and identifies the source revision.

The request envelope and signed body are distinct: the codec canonicalizes only the specified body fields; the client attaches identity and transport metadata after encoding. Signatures cover the bytes actually sent. A decoder checks schema before use and maps an error code to its generated typed class while retaining an unknown-code compatibility variant for forward compatibility. Unknown fields in strict request/result models fail closed; the unknown-code variant is for a newer server error, not permissive request validation.

## Work sequence

1. **Catalog audit:** enumerate every Rust-dispatched method and error, compare generated descriptors to reachable server paths, and correct missing transaction conflict error mapping.
2. **One Python shape:** replace duplicate hand-written DTO definitions with generated re-exports; preserve import compatibility through aliases only where named and tested.
3. **Codec parity:** build Rust WASM from pinned tool versions, compare both embedded copies, run Go/JS/Python/Rust vectors, and record source and artifact hashes. Reduce size by removing unused decode paths before changing optimization flags.
4. **Package:** include catalogs, schemas and receipt in wheel data. Clean-install into a new virtual environment and verify loader digests and typed operations. Avoid source-tree fallbacks.
5. **CI:** run full root Python tests with the kernel/engine build in hosted CI, static contract reachability, Go/JS tests, all release feature profiles, and differential quality scanners. Tests needing a service start one from checked-in fixtures; deployment/soak has its own documented workflow.
6. **Release:** build the wheel twice in clean environments, compare normalized artifacts, install from the built wheel, run consumer parity and mark a precise commit/tag. Record evidence per requirement.

## Fresh checkout developer path

Clone the public repository; install the versions declared by `rust-toolchain.toml`, `Cargo.lock`, `pyproject.toml`, `clients/go/go.mod` and `clients/js/package-lock.json`. Run the repository's normal generators, followed by:

```bash
cargo run -q --locked -p eg-capabilities --features contract --bin gen_contract -- --check
python3 scripts/build_method_codec_wasm.py --check
go test ./...
npm test
python3 -m pytest -q tests/test_generated_client_contract.py tests/test_contract_method_reachability.py
```

Run Go commands from `clients/go` and npm commands from `clients/js`. If a named Python test is moved, update this plan with its replacement before removing the gate. The complete merge gate is `bash scripts/ci_parity.sh` plus the hosted release workflow. Any integration service used by tests must start from a checked-in container/fixture recipe in CI and be torn down by the job.

## Design decisions

- Catalog metadata is generated from Rust code because server dispatch and stable errors are EG authority.
- The contract receipt binds all language projections to one revision; a package containing current JSON but stale WASM is invalid.
- Hosted CI may split bounded fast checks from long soak. A missing external environment cannot convert a required public gate into a permanent skip.
- Quality scans are differential over changed production code with explicit tracked configuration, and manual scanners remain observable release evidence.

## Amendments

### 2026-10-11: reconcile the native suite contract census

EG-CONTRACT-R009 and EG-CONTRACT-R015 retain one current method authority: the
domain registry, generated method catalog, capability ledger and both packaged
receipts must agree, including CapabilityCoverage. Receipt regression checks
derive their census from the declared catalog instead of pinning historical
totals. The scanner wiring regression recognizes the blocking conditional
pre-commit step for PR-diff and full-publication checks without changing the
intentional pipelines@main workflow policy or pinned scanner binaries.

### 2026-10-11: cache server compilation without bypassing source builds

EG-CONTRACT-R009 retains the unconditional locked, full-featured server build
and same-run revision/digest-verified artifact handoff. The server preparation
job installs a pinned sccache before enabling its compiler wrapper only for
that build step. This addresses the prior missing-wrapper CI incident without
changing local Cargo configuration, wheel builds, or the other validation jobs.
GitHub's built-in cache backend needs no repository secret; only main writes,
and other refs read. Cargo still checks the current source and sccache keys
cacheable compiler outputs by compiler inputs; final binary linking still runs.
Cache statistics make warm reuse observable. The ten-minute target remains
unproven until a main cache seed and a subsequent hosted run are measured.

### 2026-10-11: register the bounded cluster harness tests

EG-CONTRACT-R023.1 owns the completeness regression from EG-DURABLE-KERNEL-R014.2.
Add the exact cluster-harness test path to the existing binary-composition component.
Keep all existing ownership boundaries unchanged. Run the real completeness checker
and pin that exact component assignment in its existing regression module.
