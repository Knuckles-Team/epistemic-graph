# Delivery tasks

Check boxes describe work remaining for this combined contract. They do not certify acceptance. Assign one or more stable IDs to each PR, keep implementation and acceptance states separate, and record evidence in the PR and this spec before closing the task.

## UQL and served query

- [ ] Audit the public head for the single read-only UQL method, typed parameters and generated Python sender; remove any remaining duplicate query-text route. **EH-434, EH-441, EH-446**
- [ ] Close predicate/lexer/source/modality/traversal parity and canonical round-trip exhaustiveness, including generated grammar and feature-gated refusal. **EH-435–EH-440, EH-444**
- [ ] Complete named subplans, score channels, budgets, proof, explain/profile, KnowledgeSet and shared DecideText lexing; prove served wire parity. **EH-197, EH-442, EH-443, EH-445, EH-448–EH-450, EH-452**
- [ ] Verify all executable UQL examples and stable error diagnostics against the current grammar. **EH-436, EH-447**
- [ ] Verify `VALIDATE SHAPE` and graph-free SQL read-only guards with positive and mutation-negative fixtures. **EH-196, EH-387**

## Federation and virtual graphs

- [ ] Qualify capability model, shared renderer, typed budgets and optimized/full-fetch oracle on the exact public head. **EH-563, EH-566–EH-570**
- [ ] Expose redacted trace and caller-lowered budget in served UQL `EXPLAIN`/`PROFILE`; verify no URL/DSN leakage. **EH-571**
- [ ] Add column-carrying foreign rows and exact/inexact/unsupported residual handling. **EH-572**
- [ ] Execute approved OBDA virtual-graph solutions with typed ORDER/LIMIT/aggregate/join pushdown and sound fallback. **EH-573, EH-583**
- [ ] Bound source concurrency, learn/persist provenance-tagged stats, and cache only with fresh owner-bound watermarks. **EH-400, EH-574–EH-576**
- [ ] Qualify SPARQL SERVICE bind join, Iceberg pruning, RemoteEngine pushdown and extra source kinds under the one outbound gate. **EH-577–EH-581**
- [ ] Integrate the local [unified-data-plane](../unified-data-plane/spec.md) source identity and approved mapping contract without duplicating its registry or dialect work. **EH-661, EH-664–EH-666 interface only**

## Search, reasoning and numerics

- [ ] Validate semantic and edge index lifecycle, filtered BM25/ANN correctness, mixed traversal top-k/shortfall, generation replay and RLS. **RF-019, EH-351, EH-352, EH-532, EH-564, EH-565**
- [ ] Restore index state on graph reopen within the bounded performance profile, and refuse reads while materialization is incomplete. **EH-559**
- [ ] Validate current corpus compose, EL/RL/tableau consistency, disjointness, bounded derivation, proof and skipped-event rebuild. **EH-119, EH-139, EH-157, EH-197, EH-337, EH-355, EH-356, EH-363**
- [ ] Implement and validate shared series/attribution/impact/barrier/motif kernels with exact or seeded oracles and typed budgets; remove duplicate probability math. **EH-521–EH-523, EH-526, EH-527, EH-529, EH-562, EH-584**

## Acceptance

- [ ] Run all applicable [test cases](test-spec.md) and quality gates on the final public head; record one evidence line per accepted ID.
- [ ] Reconcile the spec state legend and delivery crosswalk against public merged commits and hosted CI. Do not mark a historical `BUILT` row `ACCEPTED` without the stated proof.
- [ ] Keep the SQL provider module within the configured KISS file cap with behavior-preserving extraction. **EH-659**
