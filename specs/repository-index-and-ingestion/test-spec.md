# EG-REPO-INGEST test specification

Status: PROPOSED. Tests here are required proof, not a claim that they already pass. Each fixture uses a temporary EG store and public synthetic source; no private repository, inventory, credential or network service is required for the offline tier.

| Test ID | Covers | Fixture and action | Expected result | Tier |
|---|---|---|---|---|
| RI-T01 | RI-01, RI-02 | Submit two immutable refs sharing one blob, a divergent same-path blob, and identical declarations in different files. Permute input order. | One parse per unique submitted blob, distinct occurrence IDs, exact ref/file memberships, stable canonical IDs and write-set digest. | Unit + integration |
| RI-T02 | RI-01, RI-07 | Submit absolute path, traversal path, malformed revision/digest, duplicate conflicting membership, wrong graph grant and cross-principal repository reference. | Each refuses before graph/CAS mutation with declared code; graph version and source marker unchanged. | Contract + integration |
| RI-T03 | RI-03 | Submit supported source with declarations, valid empty source, unsupported extension and parser-error bytes; then upgrade parser capability and retry the latter two. | Ordered one-to-one `success`/`success`/`unsupported`/`error` outcomes, bounded diagnostics, new capability digest, no duplicate successful revision. | Unit + generated client |
| RI-T04 | RI-04 | Two modules with imported callable, receiver method, ambiguous same-name export and external package; repeat with randomized file ordering. | Exact resolved edges and counts for admitted targets; ambiguous/external remain unresolved; no dangling or nondeterministic edges. | Unit + integration |
| RI-T05 | RI-04, RI-10 | Similar functions and source/spec/test/release artifacts with one missing target; change score ties and endpoint relation collision. | Versioned repeatable similarity and provenance; missing target unresolved; multiple legal relations retained per chosen edge model. | Integration |
| RI-T06 | RI-05 | Index, resend identical batch, resend same external key with changed content, kill/restart after CAS admission and after commit. | Replay does not advance graph version; conflict rejects; no partial graph or false receipt; committed snapshot survives restart. | Durable integration |
| RI-T07 | RI-06 | Rename, delete one path, delete a ref, then add the old blob in another ref. | Only named live memberships move/remove; other refs and immutable revision provenance remain queryable. | Durable integration |
| RI-T08 | RI-07 | Source contains harmless `@` and repository-relative `home/` text plus a host email, absolute host path and foreign tenant lookup. | Useful code remains; host identity redacted in materialized values; unauthorized read/write denied without content leak. | Security integration |
| RI-T09 | RI-08 | Generate a batch crossing operation, 48 MiB write-set and 64 MiB durable-record ceilings; split and resend. | Stable `REPOSITORY_BATCH_TOO_LARGE`, zero partial graph writes, split convergence to same final membership. | Boundary integration |
| RI-T10 | RI-09 | Native parse with deliberately unavailable model endpoint; enqueue funded, underfunded and unauthorized enrichment; restart while lease held. | Native commit completes; funded work drains once, underfunded parks visibly, unauthorized refuses, lease resumes without duplicate effects. | Durable integration |
| RI-T11 | RI-09 | Query an unenriched symbol with a small on-demand budget and a larger requested budget. | Small request gets bounded enrichment or explicit abstention; over-budget request refuses; original revision and native facts stay stable. | Query integration |
| RI-T12 | RI-11 | Full/delta/reconcile source snapshots with missing/stale expected checkpoint, empty approved and unapproved reconcile, mapping withdrawal, crash/restart and duplicate request. | Approved mappings only; exact cursor CAS; tombstones and receipt atomic; status after restart equals committed checkpoint; replay returns same receipt identity. | Durable contract |
| RI-T13 | RI-12 | Deliver ordered, duplicate, out-of-order and deleted source changes; query accelerated route before/after consumer watermark. | Idempotent output, explicit deletion, stale/out-of-order refusal, bounded freshness wait and route explanation. | Integration |
| RI-T14 | RI-01–RI-12 | Index a pinned public repository fixture with mixed supported/unsupported files; record file/blob count, bytes, parser distribution, p50/p95 native latency, and exact engine revision. | Reproducible benchmark receipt and a separately approved threshold; no unsupported claim that indexing is “seconds” from fixture-only tests. | Performance/manual |

## Existing test entrypoints to extend

- `crates/eg-compute/src/parser/branch_index_tests.rs` for branch scope, occurrence IDs and deterministic resolution.
- `tests/repository_index_durable.rs` for graph-scoped envelope, replay, tombstone, restart and CAS holder behavior.
- `tests/test_index_repository_result_contract.py` and generated-client tests for typed outcomes and refusal codes.
- `crates/eg-types/src/source_ingestion/tests.rs` and server source-ingestion tests for checkpoint and mapping rules.
- `src/server/dispatch/graph_pipeline/repository_index/enrichment.rs` and repository worker tests for admission, lease and budget.

## Acceptance evidence and reporting

For each test, record exact commit SHA, features, command, fixture digest, pass/fail count and any skipped service dependency in `evidence.md`. A local source test is evidence for implementation only. Acceptance additionally requires generated-client parity, fresh-store and restart proofs, scanner gates, and a served path where the requirement actually crosses a process or replica. Any unrun tier remains **UNVERIFIED**, not a pass. Jscpd, CCCC, Dupehound and KISS use the commands and configured thresholds in [plan.md](plan.md); a finding is resolved in code or recorded as a release blocker.
