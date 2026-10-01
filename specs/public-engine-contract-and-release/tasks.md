# EG-CONTRACT-001 — Delivery tasks and evidence

**State:** PARTIAL source baseline; ACCEPTED evidence pending. A checked box means only its named task is proven, with an exact commit and test result recorded in the evidence table below.

- [ ] **C-01 — EG-CONTRACT-R001, EG-CONTRACT-R005:** Audit and converge all Python method DTOs on generated strict models; verify with EG-T-04.
- [ ] **C-02 — EG-CONTRACT-R014, EG-CONTRACT-R015:** Complete catalog error sets, conflict mapping, scope projection and wheel contents; verify with EG-T-01/EG-T-02/EG-T-03.
- [ ] **C-03 — EG-CONTRACT-R004, EG-CONTRACT-R006:** Rebuild and measure both WASM codec copies and replay cross-language vectors; verify with EG-T-05/EG-T-06.
- [ ] **C-04 — EG-CONTRACT-R007, EG-CONTRACT-R008, EG-CONTRACT-R016, EG-CONTRACT-R017, EG-CONTRACT-R019, EG-CONTRACT-R020:** Make reachability and current-tree static gates follow real code paths; verify with mutation fixtures EG-T-09.
- [ ] **C-05 — EG-CONTRACT-R009, EG-CONTRACT-R018, EG-CONTRACT-R021:** Run the root Python suite, lifecycle cases and supported feature matrix in hosted CI; verify with EG-T-07/EG-T-10.
- [ ] **C-06 — EG-CONTRACT-R003, EG-CONTRACT-R010, EG-CONTRACT-R011, EG-CONTRACT-R012, EG-CONTRACT-R013, EG-CONTRACT-R022, EG-CONTRACT-R023:** Prove wheel/privacy and exact-tree quality gates without new clones or live-host dependencies; verify with EG-T-08.
- [ ] **C-07 — EG-CONTRACT-R024, EG-CONTRACT-R038, EG-CONTRACT-R039:** Lock the final pre-commit/codespell configuration, regenerate and check the contract receipt on every source change that affects it, and run the duplication gate against the full combined diff compared to `origin/main`.
- [ ] **C-08 — EG-CONTRACT-R002:** Add a served Python Agent Library and Decide journey in hosted CI using a disposable engine and the installed wheel; verify with EG-T-11.
- [ ] **C-09 — EG-CONTRACT-R025, EG-CONTRACT-R029, EG-CONTRACT-R030:** Fix environment-dependent build and test failures: give the UDS bind-error test and the build runner's `TMPDIR` short, fixed-length paths under the 108-byte `sun_path` limit, and give the gate's unconstrained build step full CPU availability instead of inheriting the constrained-run CPU-affinity restriction.
- [ ] **C-10 — EG-CONTRACT-R026, EG-CONTRACT-R031, EG-CONTRACT-R034:** Add the combined staged quality-gate script (CCCC, KISS, Dupehound, jscpd, privacy) and drive the complexity, duplication and architecture censuses to zero actionable backlog with clean clippy and formatting; make the dispatch-complexity test fixture derive its residual from the compiler-visible arm count.
- [ ] **C-11 — EG-CONTRACT-R027, EG-CONTRACT-R033, EG-CONTRACT-R036, EG-CONTRACT-R044, EG-CONTRACT-R045:** Pass the complexity gate for exhaustive dispatch matches via per-arm extraction, keep wire-method feature acceptance aligned with the compiled classifier/dispatch support for each feature profile, scope the pinned-reference gate to each layer's own admission step, and keep the contract gate's structural `Method`-variant discovery and pinned enum-size assertion correct as variants are added.
- [ ] **C-12 — EG-CONTRACT-R032, EG-CONTRACT-R042, EG-CONTRACT-R046:** Make the slim-server build, the no-quantum-hardware-feature build, and the broker-disabled slim-server build compile clean under `-D warnings` by fixing the underlying write-classifier return values and feature gates, not by suppressing the warning.
- [ ] **C-13 — EG-CONTRACT-R028, EG-CONTRACT-R035, EG-CONTRACT-R037:** Wire the SQLite-format and speech-recognition differential tests and the `eg-compute` solve test into the release workflow's gates job, run clippy warning-free across every declared release feature profile, and audit declared-versus-executed test targets for the full gates job.
- [ ] **C-14 — EG-CONTRACT-R040, EG-CONTRACT-R041, EG-CONTRACT-R043:** Keep the privacy and secret-history scanners accurate: no false positive on their own documented example patterns, host-path detection across the full tracked tree including the files named in EG-CONTRACT-R041, and zero credential-shaped literals in test fixtures across the full commit range.
- [ ] **C-15 — All IDs:** Build the wheel twice in clean environments, install the published candidate outside the checkout, run every consumer check against one commit/digest, run the combined quality-gate script plus the full hosted release workflow (CCCC, KISS, Dupehound, jscpd, clippy, formatting, the complete Rust and Python test suites), and publish the evidence record below before setting any requirement to `ACCEPTED`.

## Evidence record to fill for each task

Record a public commit SHA and PR URL, commands and tool versions, pass/fail outputs or hosted run links, wheel/codec/catalog hashes, target platform, and remaining failed or skipped checks. A source commit can move a task to LANDED; it cannot close C-15. Avoid checking a task solely because a file or an earlier branch exists.

## Reconciliation rule

At every merge, compare the source SHA in this table to the public default branch. If an implementation is present and the focused test passes, label that requirement LANDED. If the installed artifact, consumers, hosted quality and release matrix pass on the same SHA, label it ACCEPTED. Otherwise leave the specific failed or unrun gate open.
