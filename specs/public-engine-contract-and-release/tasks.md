# MEDULLA-CONTRACT — Delivery tasks and evidence

**State:** PARTIAL source baseline; ACCEPTED evidence pending. A checked box means only its named task is proven. The evidence table below records its exact commit and test result.

- [x] **C-01 — MEDULLA-CONTRACT-R001, MEDULLA-CONTRACT-R005:** Audit and converge all Python method DTOs on generated strict models; verify with MEDULLA-T-04.
- [x] **C-02 — MEDULLA-CONTRACT-R014, MEDULLA-CONTRACT-R015:** Complete catalog error sets, conflict mapping, scope projection and wheel contents; verify with MEDULLA-T-01/MEDULLA-T-02/MEDULLA-T-03.
- [x] **C-03 — MEDULLA-CONTRACT-R004, MEDULLA-CONTRACT-R006:** Rebuild and measure both WASM codec copies and replay cross-language vectors; verify with MEDULLA-T-05/MEDULLA-T-06.
- [x] **C-04 — MEDULLA-CONTRACT-R007, MEDULLA-CONTRACT-R008, MEDULLA-CONTRACT-R016, MEDULLA-CONTRACT-R017, MEDULLA-CONTRACT-R019, MEDULLA-CONTRACT-R020:** Make reachability and current-tree static gates follow real code paths. Verify with mutation fixtures MEDULLA-T-09.
- [x] **C-05 — MEDULLA-CONTRACT-R009, MEDULLA-CONTRACT-R018, MEDULLA-CONTRACT-R021:** Run the root Python suite, lifecycle cases and supported feature matrix in hosted CI. Verify with MEDULLA-T-07/MEDULLA-T-10.
- [x] **C-06 — MEDULLA-CONTRACT-R003, MEDULLA-CONTRACT-R010, MEDULLA-CONTRACT-R011, MEDULLA-CONTRACT-R012, MEDULLA-CONTRACT-R013, MEDULLA-CONTRACT-R022, MEDULLA-CONTRACT-R023:** Prove wheel/privacy and exact-tree quality gates without new clones or live-host dependencies; verify with MEDULLA-T-08.
- [x] **C-07 — MEDULLA-CONTRACT-R024, MEDULLA-CONTRACT-R038, MEDULLA-CONTRACT-R039:** Lock the final pre-commit/codespell configuration, regenerate and check the contract receipt on every source change that affects it, and run the duplication gate against the full combined diff compared to `origin/main`.
- [x] **C-08 — MEDULLA-CONTRACT-R002:** Add a served Python Agent Library and Decide journey in hosted CI using a disposable engine and the installed wheel; verify with MEDULLA-T-11.
- [x] **C-09 — MEDULLA-CONTRACT-R025, MEDULLA-CONTRACT-R029, MEDULLA-CONTRACT-R030:** Fix environment-dependent build and test failures: give the UDS bind-error test and the build runner's `TMPDIR` short, fixed-length paths under the 108-byte `sun_path` limit, and give the gate's unconstrained build step full CPU availability instead of inheriting the constrained-run CPU-affinity restriction.
- [x] **C-10 — MEDULLA-CONTRACT-R026, MEDULLA-CONTRACT-R031, MEDULLA-CONTRACT-R034:** Add the combined staged quality-gate script (CCCC, KISS, Dupehound, jscpd, privacy) and drive the complexity, duplication and architecture censuses to zero actionable backlog with clean clippy and formatting; make the dispatch-complexity test fixture derive its residual from the compiler-visible arm count.
- [x] **C-11 — MEDULLA-CONTRACT-R027, MEDULLA-CONTRACT-R033, MEDULLA-CONTRACT-R036, MEDULLA-CONTRACT-R044, MEDULLA-CONTRACT-R045:** Pass the complexity gate for exhaustive dispatch matches via per-arm extraction, keep wire-method feature acceptance aligned with the compiled classifier/dispatch support for each feature profile, scope the pinned-reference gate to each layer's own admission step, and keep the contract gate's structural `Method`-variant discovery and pinned enum-size assertion correct as variants are added.
- [x] **C-12 — MEDULLA-CONTRACT-R032, MEDULLA-CONTRACT-R042, MEDULLA-CONTRACT-R046:** Make the slim-server build, the no-quantum-hardware-feature build, and the broker-disabled slim-server build compile clean under `-D warnings` by fixing the underlying write-classifier return values and feature gates, not by suppressing the warning.
- [x] **C-13 — MEDULLA-CONTRACT-R028, MEDULLA-CONTRACT-R035, MEDULLA-CONTRACT-R037:** Wire the SQLite-format and speech-recognition differential tests and the `medulla-compute` solve test into the release workflow's gates job, run clippy warning-free across every declared release feature profile, and audit declared-versus-executed test targets for the full gates job.
- [x] **C-14 — MEDULLA-CONTRACT-R040, MEDULLA-CONTRACT-R041, MEDULLA-CONTRACT-R043:** Keep the privacy and secret-history scanners accurate: no false positive on their own documented example patterns, host-path detection across the full tracked tree including the files named in MEDULLA-CONTRACT-R041, and zero credential-shaped literals in test fixtures across the full commit range.
- [x] **C-01 — MEDULLA-CONTRACT-R001, MEDULLA-CONTRACT-R005:** Audit and converge all Python method DTOs on generated strict models; verify with MEDULLA-T-04.
- [x] **C-02 — MEDULLA-CONTRACT-R014, MEDULLA-CONTRACT-R015:** Complete catalog error sets, conflict mapping, scope projection and wheel contents; verify with MEDULLA-T-01/MEDULLA-T-02/MEDULLA-T-03.
- [x] **C-03 — MEDULLA-CONTRACT-R004, MEDULLA-CONTRACT-R006:** Rebuild and measure both WASM codec copies and replay cross-language vectors; verify with MEDULLA-T-05/MEDULLA-T-06.
- [x] **C-04 — MEDULLA-CONTRACT-R007, MEDULLA-CONTRACT-R008, MEDULLA-CONTRACT-R016, MEDULLA-CONTRACT-R017, MEDULLA-CONTRACT-R019, MEDULLA-CONTRACT-R020:** Make reachability and current-tree static gates follow real code paths. Verify with mutation fixtures MEDULLA-T-09.
- [x] **C-05 — MEDULLA-CONTRACT-R009, MEDULLA-CONTRACT-R018, MEDULLA-CONTRACT-R021:** Run the root Python suite, lifecycle cases and supported feature matrix in hosted CI. Verify with MEDULLA-T-07/MEDULLA-T-10.
- [x] **C-06 — MEDULLA-CONTRACT-R003, MEDULLA-CONTRACT-R010, MEDULLA-CONTRACT-R011, MEDULLA-CONTRACT-R012, MEDULLA-CONTRACT-R013, MEDULLA-CONTRACT-R022, MEDULLA-CONTRACT-R023:** Prove wheel/privacy and exact-tree quality gates without new clones or live-host dependencies; verify with MEDULLA-T-08.
- [x] **C-07 — MEDULLA-CONTRACT-R024, MEDULLA-CONTRACT-R038, MEDULLA-CONTRACT-R039:** Lock the final pre-commit/codespell configuration, regenerate and check the contract receipt on every source change that affects it, and run the duplication gate against the full combined diff compared to `origin/main`.
- [x] **C-08.1 — MEDULLA-CONTRACT-R002.1:** Add the served `Decide` tenant auth-boundary refusal test (`tests/test_decide_auth_boundary.py`) against the suite's disposable-engine session fixture.
- [x] **C-08.2 — MEDULLA-CONTRACT-R002.2:** Add the served Decide authorized-tenant-path test and the served Agent Library pack-import journey (`tests/test_decide_auth_boundary.py`), reusing `tests/test_connector_pack_planted.py`'s served helpers.
- [x] **C-08.3 — MEDULLA-CONTRACT-R002.3:** Add the installed-wheel/no-source-tree-import check (`tests/test_installed_wheel_decision_client_boundary.py`) as a release-workflow gates-job step against `.gates-wheel-venv`.
- [x] **C-09 — MEDULLA-CONTRACT-R025, MEDULLA-CONTRACT-R029, MEDULLA-CONTRACT-R030:** Fix environment-dependent build and test failures: give the UDS bind-error test and the build runner's `TMPDIR` short, fixed-length paths under the 108-byte `sun_path` limit, and give the gate's unconstrained build step full CPU availability instead of inheriting the constrained-run CPU-affinity restriction.
- [x] **C-10 — MEDULLA-CONTRACT-R026, MEDULLA-CONTRACT-R031, MEDULLA-CONTRACT-R034:** Add the combined staged quality-gate script (CCCC, KISS, Dupehound, jscpd, privacy) and drive the complexity, duplication and architecture censuses to zero actionable backlog with clean clippy and formatting; make the dispatch-complexity test fixture derive its residual from the compiler-visible arm count.
- [x] **C-11 — MEDULLA-CONTRACT-R027, MEDULLA-CONTRACT-R033, MEDULLA-CONTRACT-R036, MEDULLA-CONTRACT-R044, MEDULLA-CONTRACT-R045:** Pass the complexity gate for exhaustive dispatch matches via per-arm extraction, keep wire-method feature acceptance aligned with the compiled classifier/dispatch support for each feature profile, scope the pinned-reference gate to each layer's own admission step, and keep the contract gate's structural `Method`-variant discovery and pinned enum-size assertion correct as variants are added.
- [x] **C-12 — MEDULLA-CONTRACT-R032, MEDULLA-CONTRACT-R042, MEDULLA-CONTRACT-R046:** Make the slim-server build, the no-quantum-hardware-feature build, and the broker-disabled slim-server build compile clean under `-D warnings` by fixing the underlying write-classifier return values and feature gates, not by suppressing the warning.
- [x] **C-13 — MEDULLA-CONTRACT-R028, MEDULLA-CONTRACT-R035, MEDULLA-CONTRACT-R037:** Wire the SQLite-format and speech-recognition differential tests and the `medulla-compute` solve test into the release workflow's gates job, run clippy warning-free across every declared release feature profile, and audit declared-versus-executed test targets for the full gates job.
- [x] **C-14 — MEDULLA-CONTRACT-R040, MEDULLA-CONTRACT-R041, MEDULLA-CONTRACT-R043:** Keep the privacy and secret-history scanners accurate: no false positive on their own documented example patterns, host-path detection across the full tracked tree including the files named in MEDULLA-CONTRACT-R041, and zero credential-shaped literals in test fixtures across the full commit range.
- [ ] **C-15 — All IDs:** Build the wheel twice in clean environments, install the published candidate outside the checkout, run every consumer check against one commit/digest, run the combined quality-gate script plus the full hosted release workflow (CCCC, KISS, Dupehound, jscpd, clippy, formatting, the complete Rust and Python test suites), and publish the evidence record below before setting any requirement to `ACCEPTED`.
- [x] **C-16 — MEDULLA-CONTRACT-R015:** Ship the installed contract package. Project the canonical methods, errors, schemas and receipt into `medulla/contract/` with the generator only. Include that data in the `tool.maturin` wheel contents. Register one importable `contract_errors` module with shared typed exceptions. Authenticate the receipt aggregate against the caller's trusted pin before the loader reads any installed contract file. Reject a missing file, a digest mismatch, a duplicate key and a path escape with typed errors. Prove the loader outside the checkout with the built wheel (MEDULLA-T-03). Recovered from the never-started agent claim `codex-medulla-contract-r015-packaging-01a0fe5f`.
- [x] **C-17 — MEDULLA-CONTRACT-R047:** Publish the main Linux x86_64 wheel to the rolling `engine-main` prerelease. Record the first main run and asset SHA-256. Consumer CI in Meristem, Arbora and Graft SDK installs from it.

## Evidence record to fill for each task

Record a public commit SHA and PR URL, commands and tool versions, pass/fail outputs or hosted run links, wheel/codec/catalog hashes, target platform, and remaining failed or skipped checks. A source commit can move a task to LANDED; it cannot close C-15. Avoid checking a task solely because a file or an earlier branch exists.

## Reconciliation rule

At every merge, compare the source SHA in this table to the public default branch. If an implementation is present and the focused test passes, label that requirement LANDED. If the installed artifact, consumers, hosted quality and release matrix pass on the same SHA, label it ACCEPTED. Otherwise leave the specific failed or unrun gate open.
- [x] **C-18 — MEDULLA-CONTRACT-R048:** Declare the work-claim and evidence DTO bounds in the Rust schema source. Emit strict booleans and unique-item validators from the generator. Merge the PR and record the merged commit.
- [ ] **C-19 — MEDULLA-CONTRACT-R050:** Generated client ingests OTLP logs
- [ ] **C-20 — MEDULLA-CONTRACT-R051:** Generated client ingests RunTrace
- [ ] **C-20.1 — MEDULLA-CONTRACT-R051.1:** Client ingest carries harness fields

## 2026-10-11 lint formatter provisioning

- [x] MEDULLA-CONTRACT-R031.1: Install manifest-selected Rust and rustfmt before lint formatting; 38 focused Python regressions pass. Existing-toolchain execution confirms manifest resolution, not a clean-host download.
- [ ] MEDULLA-CONTRACT-R031.2: Publish complete same-tree zero-backlog census and independent clippy/formatting results. Earlier C-10 implementation checkmarks do not establish this complete acceptance.

## Current architecture documentation units

Each child is a separate documentation review unit of at most five minutes.

- [ ] **MEDULLA-CONTRACT-R052:** Complete all four child checks without replacing existing acceptance or receipts.
- [ ] **MEDULLA-CONTRACT-R052.1:** Compare the module inventory and dependency claims with source using MEDULLA-T-12.1.
- [ ] **MEDULLA-CONTRACT-R052.2:** Compare build, package, and runtime boundaries using MEDULLA-T-12.2.
- [ ] **MEDULLA-CONTRACT-R052.3:** Resolve shared-contract ownership and call sites using MEDULLA-T-12.3.
- [ ] **MEDULLA-CONTRACT-R052.4:** Check evidence boundaries and the diagram coverage map using MEDULLA-T-12.4.

<!-- spec-definition-patch:owner-batch-20261012-public-engine-contract-and-release -->
## Reviewed residual definition tasks — 2026-10-12

The linked requirement rows retain the full contract, hashes, acceptance, and owner dependencies.

- [ ] `MEDULLA-CONTRACT-R001.1`: Extend the Medulla generator and manifest. Wire exports into Meristem `HarnessRequest` and `run_agent`. Keep Meristem lifecycle ownership.
- [ ] `MEDULLA-CONTRACT-R001.2`: Use R094 export/import and governed filesystem effects. Pin the canonical number encoder. Do not add a Medulla filesystem writer.
- [ ] Keep both definitions SPECIFIED until implementation and served evidence receive separate review.
- [ ] **MEDULLA-CONTRACT-R053:** Complete the twelve children without replacing R015 or R047 acceptance.
- [ ] **MEDULLA-CONTRACT-R053.1:** Fold the four offline tools into `medulla-server`.
- [ ] **MEDULLA-CONTRACT-R053.2:** Strip executables in `build_backend.py` and gate the wheel.
- [ ] **MEDULLA-CONTRACT-R053.3:** Add the `server` extra and its bump entry.
- [ ] **MEDULLA-CONTRACT-R053.4:** Resolve the server binary through `medulla.server_binary`.
- [ ] **MEDULLA-CONTRACT-R053.5:** Add `[profile.release-server]` and record measured sizes.
- [ ] **MEDULLA-CONTRACT-R053.6:** Update callers of the retired binary names.
- [ ] **MEDULLA-CONTRACT-R053.7:** Split `ci.yml` (all gates) from `release.yml` (guard, build, check, publish) and guard tags on a passing main CI run.
- [ ] **MEDULLA-CONTRACT-R053.8:** Build the public wheel from the extension crates only, publish each platform as it finishes, and warm the compile cache from main.
- [ ] **MEDULLA-CONTRACT-R053.9:** Build and publish the server wheel per Linux architecture from `packaging/medulla-server`.
- [ ] **MEDULLA-CONTRACT-R053.10:** Keep `binary_path()` and `BinaryNotFoundError` with their tests in the package.
- [ ] **MEDULLA-CONTRACT-R053.11:** Build the server image from the server wheel and publish one multi-architecture tag.
- [ ] **MEDULLA-CONTRACT-R053.12:** Bump the package with the engine and add the tagged-source dispatch.

- [ ] **MEDULLA-CONTRACT-R006.3:** Deliver and qualify bounded hosted canonical contract export with source provenance.

- [ ] **MEDULLA-CONTRACT-R053.13:** Retain and verify the qualified wheel for isolated installed-artifact acceptance.

- [ ] **MEDULLA-CONTRACT-R006.1:** Qualify fresh WASM exports without inferring native reproducibility.
- [ ] **MEDULLA-CONTRACT-R006.2:** Complete independent native builds, measured size and cross-language acceptance.
