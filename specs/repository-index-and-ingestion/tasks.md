# EG-REPO-INGEST implementation tasks

Status: PROPOSED. Task completion requires code and test evidence at an exact commit. The presence of this document does not change feature delivery state.

| Order | Task | Done when | Verification |
|---|---|---|---|
| 1 | Inventory the current `IndexRepository`, `SourceIngest`, parser, generated client, CAS, envelope and worker paths at the target revision. | One owner per effect and an explicit list of real gaps is recorded in the PR. | Code path review; no duplicate writer. |
| 2 | Freeze typed public contract and error vocabulary for scope, parse outcomes, reparse and budget refusal. | Generated Rust/Python round trips agree; invalid fields fail closed. | RI-T02, RI-T03, RI-T09. |
| 3 | Complete immutable identity and branch membership validation. | Shared/diverged blobs, duplicate declarations, rename/delete and input permutations yield exact stable graph identities. | RI-T01, RI-T07. |
| 4 | Complete cross-file resolution and versioned model-free similarity. | Ambiguity and external references are explicit; all emitted endpoints exist and ordering is repeatable. | RI-T04, RI-T05. |
| 5 | Decide and implement the multiple-relations-per-endpoint representation. | `calls`, `depends_on` and semantic bridge relations cannot silently overwrite or drop one another. | RI-T05 collision fixture plus migration check. |
| 6 | Complete source/provenance/semantic-bridge projection through one `ChangeEnvelope`. | One atomic commit, replay and crash recovery include source/spec/test/release lineage. | RI-T05, RI-T06, RI-T08. |
| 7 | Complete durable budgeted enrichment and bounded query demand. | Native commit is independent of models; worker admission, lease, retry and park survive restart. | RI-T10, RI-T11. |
| 8 | Complete approved source-position consumers and freshness signaling. | Mapping, cursor, change projection and accelerated route converge by committed position. | RI-T12, RI-T13. |
| 9 | Run a public mixed-language repository benchmark and set a reviewed performance threshold. | Fixture/revision/host profile and p50/p95 native latency are reproducible. | RI-T14. |
| 10 | Run configured language, contract, scanner and hosted release gates; publish exact evidence and update status. | All required tests and generated surfaces pass at the merge head; any served-only gate is separately evidenced. | `evidence.md`, CI run and review. |

Parallel implementation is allowed only where interfaces and file ownership are explicit. Merge the native engine contract before dependent SDK or application consumers cut over. Retire obsolete graph writers after their callers are migrated and parity tests pass.
