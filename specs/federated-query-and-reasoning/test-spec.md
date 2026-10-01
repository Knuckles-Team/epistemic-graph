# Test specification and acceptance evidence

All tests run from the repository root with local fixtures, mock servers, or isolated ephemeral stores by default. Live foreign services are an optional integration profile with explicit provisioning; a cloud contributor can execute parser, optimizer, security, differential and contract gates without private infrastructure. For each accepted slice, record the exact commit, command, exit status, fixture version and relevant assertion count in a reviewed evidence entry.

| Case | Requirements | Fixture and action | Expected result |
|---|---|---|---|
| Q-01 grammar exhaustiveness | FQR-01/02 | Every `OpKind`/`PredKind` plus 512 generated valid programs; parse → canonical print → parse. Run generated EBNF/dispatch check. | Same typed plan; no uncovered production variant. Unsupported feature gives `UQL_FEATURE_NOT_IN_BUILD` or `UQL_UNSUPPORTED`, with span. |
| Q-02 typed bindings | FQR-01/10 | Values with quotes, Unicode, JSONPath, arrays; unbound, mistyped and unused params; injection strings. | One bind result per declared type; no text interpolation; exact typed refusal for each invalid case. |
| Q-03 read-only/snapshot | FQR-02 | All UQL op variants, `LET`/`JOIN`, concurrent write between plan and execution. | No mutation-capable method; single authorized snapshot; budgets refuse at boundary without partial rows. |
| Q-03a reconciliation | EG-FEDERATED-QUERY-R007 | Interrupt a semantic-index source scan before complete-snapshot proof, then replay a complete scan. | The partial scan creates no tombstone; the complete scan tombstones exactly the absent sources. The focused `missing_sources_become_tombstones_only_after_complete_snapshot_proof` regression passes repeatedly. |
| Q-03b graph-free SQL | FQR-02 | DDL, DML, `COPY`, external-table statement and multi-statement payload through direct graph-free SQL and observability search. | Read-only guard refuses each mutation before execution; ordinary SELECT succeeds. |
| Q-04 proof/trace | FQR-02/08/10 | SPARQL/rule derivation, `WITH PROOF`, nested `LET`/`JOIN`, source with secret-like label. | Proof premises verify against generation; channels survive wire; trace has remote/local rows and no secret/URL/DSN. |
| F-01 result equivalence | FQR-03/06 | SQL, HTTP, RemoteEngine, SPARQL and OBDA mocks with duplicate rows, repeated predicate values, null/type edges, filter/order/limit/aggregate/join cases. Run optimized and naive full fetch. | Equal multisets, or equal ordered rows where ordered; no first-page truncation. Mock records lower or equal transfer where pushdown is declared. |
| F-02 budget and key source | FQR-03/04 | Set each request, row, key and wall limit one below needed work; request full scan of `RequiresKeys`. | `FEDERATION_BUDGET_EXCEEDED:<dimension>` or `FEDERATION_SOURCE_REQUIRES_KEYS`; no partial success. |
| F-03 outbound policy | FQR-04 | Local/private/CGNAT/6to4/Teredo, redirect, DNS rebind, host placeholder, SQL injection, ungranted source, cross-owner key. | Refused before request or redirected hop; zero cross-tenant rows, credential bytes or network access. |
| F-04 capability fallback | FQR-03/05 | Source reports `Inexact` filter, lacks top-N, errors on batch, or has unknown stats; retry under rate limit. | Exact local residual; bounded fallback; trace names capability and estimate provenance. |
| F-05 cache/freshness | FQR-05 | Same canonical fragment across owners; watermark advance, grant revoke, missing watermark, schema change. | Only same-owner/current-watermark hit; all other cases miss or refuse. |
| F-06 OBDA/SERVICE/Iceberg | FQR-06 | Two predicate maps sharing rows, nonunique subject, `a`/`aa` template suffix, multivalued RDF predicate, partitioned Iceberg, SERVICE bind variables. | Differential answer equality; unsupported ORDER/aggregation falls back; manifest and request count prove pruning. |
| S-01 filtered search | FQR-07 | BM25 and ANN fixture with 10 permitted matches below unfiltered top-k; two-hop traversal and parallel edges. | Exactly k permitted IDs where available, stable edge identity; otherwise typed shortfall, not silent 7/10. |
| S-02 durable generation | FQR-07/08 | Crash during backfill, restart after activation, delete/replay, skipped reasoning outbox event, revoked tenant. | Atomic previous/new generation, correct scan fallback, rebuild on gap, no forbidden row. |
| S-03 bounded reopen | FQR-07 | Reopen graphs at increasing document counts with persisted index state and a deliberately incomplete state. | Complete state reopens without per-document rebuild; incomplete state returns typed partial-materialization until repair, then correct reads. |
| R-01 ontology corpus | FQR-08 | Shipped core schema plus minimal disjointness and deliberately inconsistent corpus. | Bounded compose with no stack overflow; EL/RL and tableau agree on consistency; unsatisfiable classes named; proof premises verifiable. |
| N-01 series and stats | FQR-09 | Two series at same timestamp, f64 values, constant/alternating/large windows, reference recompute. | No row collision or f32 truncation; flat window variance/z-score exactly zero; O(1) update measured independent of window length. |
| N-02 advanced kernels | FQR-09 | Exact small Shapley oracle, seeded sampled CI, DAG/cycle impact, barrier sigma→0, motif brute-force, high-tail literal. | Exact results where specified; seeded repeatability and CI coverage; high-tail relative error ≤1e-12; budget refusal is typed. |
| C-01 generated client | FQR-10 | Regenerate and check Rust method/error/receipt contracts; invoke UQL through Python generated sender. | Exact shape/code parity and served behavior; generated files show no manual drift. |

## Quality and release gates

### EG-FEDERATED-QUERY-R045 / EG-FEDERATED-QUERY-R052 focused differential tests

| ID | Positive proof | Negative and boundary proof |
|---|---|---|
| EG-FEDERATED-QUERY-R045 | Mock SQL/HTTP/RemoteEngine rows include duplicate names, nulls, numeric/date values and a foreign-only filter. Exact push and inexact push plus local residual equal full-fetch ordered/multiset oracles; projection retains hidden residual columns until evaluated. | Unsupported filter, ambiguous schema, changed type, inexact push missing a matching row and projection dropping an authorization/order column refuse or fall back; never silently return partial rows. |
| EG-FEDERATED-QUERY-R052 | A recording second EG instance receives canonical `Method::Uql` with only authorized keys and a sound LIMIT, returns the same ordered result as no push, and reports lower transferred row count. | Residual filter/top-N interaction, pagination, missing UQL capability, invalid signature, ungranted key, redirect/private target and exhausted request/row budget refuse or use a safe bounded fallback; no secret appears in trace. |


Run changed-file CCCC, Dupehound, KISS and differential jscpd from `.config/pre-commit.yaml`; the configured CCCC threshold is no new function above cyclomatic 10 or cognitive 15 and no regression. Differential jscpd must show zero newly introduced clone pairs. KISS and Dupehound use their checked-in configuration, with zero attributable violations. Run `cargo fmt --all -- --check`, targeted `cargo test --locked` for each changed crate, full-feature Clippy with `-D warnings`, generated-contract checks, and the repository's hosted CI matrix for the final PR head. `bash scripts/ci_parity.sh` is a local CI preview. Record any unavailable live-service profile separately; it cannot be replaced by a fabricated pass. Every new public source kind needs a mock conformance suite before a live environment is considered.

## Evidence record format

For each accepted stable ID: `ID | public merged SHA | test command/profile | expected vs actual | quality gate | client/contract digest | reviewer/date`. A task checkbox, source branch, historical log or local-only build never substitutes for exact-head evidence. If a test is omitted, state the risk and named replacement before the ID can be accepted.
