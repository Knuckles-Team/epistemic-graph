# Test specification

All fixtures run against a disposable local data directory and generated credentials. Fault injection is deterministic and records seed, revision, platform, command and artifact path. Tests may use local multi-process nodes and disposable containers; no private cluster, inventory, credentials or always-on service is needed.

| Scenario | Requirements | Positive fixture and expected result | Negative/failure fixture and expected result |
|---|---|---|---|
| T-KG-01 batch terminality | KG-01, KG-02 | Graph+table+KV batch commits once; readback and retry return same terminal result | Inject failure at every commit phase; restart yields all-or-none; lost-ack retry does not duplicate |
| T-KG-02 table-only path | KG-02 | Concurrent table-only writes use current atomic path and match reference corpus | Mixed unsupported member set refuses before any table or graph write |
| T-KG-03 owner lineage | KG-03 | Load each supported golden manifest; one validation per transaction | Unknown predecessor or extra physical table returns typed upgrade/ownership error; stale cached binding never crosses transactions |
| T-KG-04 scrub | KG-04 | Corrupt one persisted node, scrub reports typed finding and resumes cursor after restart | Rate cap prevents writer starvation; unreadable payload is not exposed; no silent repair |
| T-KG-05 outbox | KG-05 | Healthy successors deliver after a failing head is rejected/rewound with ordered receipts | Retry one head 16 times; successors keep their own zero-attempt count; reasoning skip without rebuild is refused |
| T-KG-06 CDC/mirror | KG-06 | Commit 100 ordered graph/table changes, disconnect target, reconnect, replay and match digest | Crash after target apply before cursor; replay is idempotent; empty default target guard and schema drift fail visibly |
| T-KG-07 scope | KG-07 | Owner and explicitly granted principal can access allowed data | Cross-tenant/principal SQL, UQL, TSDB, KV, UDF, foreign-source and cache probes deny; revoked grant denies next use |
| T-KG-07a auth modes | KG-07, KG-09 | `none`, `local` and `external` each resolve to the same internal principal/RBAC evaluator | Spoofed name in `none`, stale local session after mode switch, wrong external issuer/audience/tenant/policy version and generic elevation lease all deny |
| T-KG-08 audit | KG-08 | Reservation precedes governed effect and links truthful outcome | Audit writer unavailable, missing class, wrong tenant, duplicate ID and crash after effect all fail or reconcile without false success |
| T-KG-09 approval | KG-09 | Two distinct principals approve exact digest before expiry | Self-approval, generic lease, altered digest, expiry and replay all deny and audit refusal |
| T-KG-10 Raft | KG-10 | Repeated 3-node multi-group writes/failover preserve independent progress and one committed result | Inject slow peer/leader loss; unaffected groups progress; failed assertion emits causal ring-buffer trace |
| T-KG-11 durability | KG-11, KG-12 | Sync survives kill; async oldest-unflushed age stays ≤100 ms; hot structures rebuild from log | Ephemeral disappears on restart; forced flush lag yields explicit degraded/refusal, never silent class change; concurrent primitive history linearizes |
| T-KG-12 wire batches | KG-13, KG-14 | RESP and pgwire batches return N ordered replies after one commit; point route avoids DataFusion session | Middle operation fails; no earlier reply promises a commit; schema/policy change invalidates cached SQL plan |
| T-KG-13 parity and benchmark | KG-15 | Differential corpus, restore drill and benchmark report include exact config, hardware class, code revision and digest | Accepted deviation is named/reviewed; a regression fails instead of rewriting baseline; missing benchmark row blocks superiority claim |

## Commands and quality evidence

Use focused package tests for `eg-types`, `eg-transaction`, `eg-storage`, `eg-stream`, `eg-tsdb`, and affected server integration tests, followed by the configured full pre-commit and hosted parity commands in `plan.md`. Run `python3 scripts/check_universal_read_rls.py` and `python3 scripts/check_persisted_mutation_contract.py` when their paths are affected. The repository's CCCC changed-function gate rejects new cyclomatic >10 or cognitive >15; run the configured Dupehound changed-function and jscpd differential gates without new suppressions, plus KISS, Rust architecture lint and clippy. Record scanner versions from the checked-in scanner contract. A benchmark is evidence only for its measured hardware/workload/durability class; no live environment is an implicit prerequisite to unit or contract acceptance.

## Evidence record

For each scenario, add a dated table row in the PR or an evidence file with exact main commit, command, CI URL, test count, result, fixture digest and known limitation. A passed local branch test means `BUILT`; merged-head tests and reviewed negative cases are required for `ACCEPTED`. Do not infer acceptance from the presence of these documents.
