# Delivery tasks

Check boxes describe work remaining for this combined contract. They do not certify acceptance. Assign one or more stable IDs to each PR, keep implementation and acceptance states separate, and record evidence in the PR and this spec before closing the task.

## UQL and served query

- [x] Audit the public head for the single read-only UQL method, typed parameters and generated Python sender; remove any remaining duplicate query-text route. **EG-FEDERATED-QUERY-R020, EG-FEDERATED-QUERY-R025, EG-DURABLE-KERNEL-R023**
- [x] Close predicate/lexer/source/modality/traversal parity and canonical round-trip exhaustiveness, including generated grammar and feature-gated refusal. **EG-FEDERATED-QUERY-R021 and related rows (see requirements.md), EG-TYPED-PACKS-R080, EG-FEDERATED-QUERY-R061**
- [x] Complete named subplans, score channels, budgets, proof, explain/profile, KnowledgeSet and shared DecideText lexing; prove served wire parity. **EG-FEDERATED-QUERY-R006, EG-FEDERATED-QUERY-R028, EG-FEDERATED-QUERY-R065, EG-FEDERATED-QUERY-R067, EG-FEDERATED-QUERY-R068, EG-FEDERATED-QUERY-R070, EG-DECISION-ENGINE-R104**
- [x] Verify all executable UQL examples and stable error diagnostics against the current grammar. **EG-FEDERATED-QUERY-R064, EG-FEDERATED-QUERY-R026**
- [x] Verify `VALIDATE SHAPE` and graph-free SQL read-only guards with positive and mutation-negative fixtures. **EG-FEDERATED-QUERY-R005, EG-FEDERATED-QUERY-R015**

## Federation and virtual graphs

- [x] Qualify capability model, shared renderer, typed budgets and optimized/full-fetch oracle on the exact public head. **EG-FEDERATED-QUERY-R038, EG-FEDERATED-QUERY-R041–EG-FEDERATED-QUERY-R043**
- [ ] Expose redacted trace and caller-lowered budget in served UQL `EXPLAIN`/`PROFILE`; verify no URL/DSN leakage. **EG-FEDERATED-QUERY-R044**
- [ ] Add typed column-carrying foreign rows and exact/inexact/unsupported residual handling; validate foreign-only predicates, projection dependencies and schema drift against a full-fetch oracle. **EG-FEDERATED-QUERY-R045**
- [x] Execute approved OBDA virtual-graph solutions with typed ORDER/LIMIT/aggregate/join pushdown and sound fallback. **EG-FEDERATED-QUERY-R046, EG-FEDERATED-QUERY-R055**
- [x] Bound source concurrency, learn/persist provenance-tagged stats, and cache only with fresh owner-bound watermarks. **EG-FEDERATED-QUERY-R018, EG-FEDERATED-QUERY-R047–EG-FEDERATED-QUERY-R049**
- [x] Qualify SPARQL SERVICE bind join, Iceberg pruning, RemoteEngine pushdown and the SSRF guard under the one outbound gate. **EG-FEDERATED-QUERY-R050, EG-FEDERATED-QUERY-R051, EG-FEDERATED-QUERY-R052, EG-FEDERATED-QUERY-R054**
- [x] Register Trino, Cypher and Spark as typed `ForeignSourceSpec` kinds with per-kind capability sets; Spark is batch-only. **EG-FEDERATED-QUERY-R053**
- [x] Prove RemoteEngine key/LIMIT UQL pushdown against a recording second engine, including signed owner context, exact residual, pagination and bounded fallback. **EG-FEDERATED-QUERY-R052**
- [ ] Integrate the local [unified-data-plane](../unified-data-plane/spec.md) source identity and approved mapping contract without duplicating its registry or dialect work. **EG-UNIFIED-DATA-PLANE-R002, EG-UNIFIED-DATA-PLANE-R005–EG-UNIFIED-DATA-PLANE-R007 interface only**

## Search, reasoning and numerics

- [x] Validate semantic and edge index lifecycle, filtered BM25/ANN correctness, mixed traversal top-k/shortfall, generation replay and RLS. **EG-FEDERATED-QUERY-R059, EG-FEDERATED-QUERY-R008, EG-FEDERATED-QUERY-R009, EG-FEDERATED-QUERY-R035, EG-FEDERATED-QUERY-R039, EG-FEDERATED-QUERY-R040**
- [x] Restore index state on graph reopen within the bounded performance profile, and refuse reads while materialization is incomplete. **EG-FEDERATED-QUERY-R036**
- [x] Validate current corpus compose, EL/RL/tableau consistency, disjointness, bounded derivation, proof and skipped-event rebuild. **EG-FEDERATED-QUERY-R001, EG-DURABLE-KERNEL-R002, EG-FEDERATED-QUERY-R004, EG-FEDERATED-QUERY-R006, EG-TYPED-PACKS-R071, EG-FEDERATED-QUERY-R010, EG-FEDERATED-QUERY-R011, EG-FEDERATED-QUERY-R012**
- [x] Implement and validate shared series/attribution/impact/barrier/motif kernels with exact or seeded oracles and typed budgets; remove duplicate probability math. **EG-FEDERATED-QUERY-R030–EG-FEDERATED-QUERY-R031, EG-FEDERATED-QUERY-R032, EG-FEDERATED-QUERY-R033, EG-FEDERATED-QUERY-R034, EG-FEDERATED-QUERY-R037, EG-FEDERATED-QUERY-R056**

## Additional coverage

- [x] Close the remaining requirements no task above names: community-detection quality function and naming/connectivity (**EG-FEDERATED-QUERY-R060, EG-FEDERATED-QUERY-R063**), streaming memory test isolation (**EG-FEDERATED-QUERY-R062**), SPARQL evaluator file-size split (**EG-FEDERATED-QUERY-R066**), and SQL/HTTP pushdown batching and pagination (**EG-FEDERATED-QUERY-R069, EG-FEDERATED-QUERY-R071**); see `requirements.md` for each ID's definition.

- [ ] Serve ontology source selection with premises and add operation-bound API, MCP, A2A and GraphQL source kinds; run F-07 and F-08. **EG-FEDERATED-QUERY-R072, EG-FEDERATED-QUERY-R073**

## Acceptance

- [ ] Run all applicable [test cases](test-spec.md) and quality gates on the final public head; record one evidence line per accepted ID.
- [ ] Reconcile the spec state legend and delivery crosswalk against public merged commits and hosted CI. Do not mark a historical `BUILT` row `ACCEPTED` without the stated proof.
- [ ] Keep the SQL provider module within the configured KISS file cap with behavior-preserving extraction. **EG-FEDERATED-QUERY-R057**
- [x] Add the typed `NlQueryResult`/`NlQueryError`/`NlQueryBudget` disclosure model (`plan_only`, `plan_and_execute_typed`) to `eg-plan`, with refusal and plan-only tests. **EG-FEDERATED-QUERY-R058.1**
- [ ] Wire `Method::NlQuery`'s dispatch handler and result contract to the typed disclosure model so the served result carries the executed UQL, and expose a plan-only entry point to callers. **EG-FEDERATED-QUERY-R058.2**
- [ ] Confirm natural-language results are never cached on the NL text and that no mutating/side-effecting operation is reachable from the NL surface. **EG-FEDERATED-QUERY-R058.3**
