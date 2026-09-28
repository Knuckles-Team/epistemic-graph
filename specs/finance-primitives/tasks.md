# Implementable tasks

Tasks are ordered by dependency. Check a task only when its code, tests and exact-head evidence are linked in this directory.

- [ ] **F-01 · EH-698:** Define v2 asset classes and migration aliases; `Account`, `Activity`, `Lot`, `Position`, `PortfolioGroup`, `StrategySpec`, `StrategyRun`, `LeverageTerms` types and SHACL shapes; reject unknown versions and direct writes to derived projections.
- [ ] **F-02 · EH-700:** Implement point-in-time action and exchange-calendar revision resolver; return session/source/as-of for all price paths; add DST, early-close and ticker reuse vectors.
- [ ] **F-03 · EH-699:** Implement canonical activity ordering and checked decimal lot engine for FIFO/LIFO/specific/average basis; add correction replay, transfer and split invariants.
- [ ] **F-04 · EH-699:** Add sourced FX, realized/unrealized P&L, TWR, bounded XIRR and benchmark comparison with partial coverage reports.
- [ ] **F-05 · EH-702:** Define immutable strategy-spec digest and proposal contract; implement DCA, trend and rebalance evaluators with idempotent cadence/missed-cycle evidence.
- [ ] **F-06 · EH-703:** Build common-horizon strategy comparison atop sealed `BacktestRun`; calculate CV, deflated Sharpe and PBO from one verified split matrix with costs.
- [ ] **F-07 · EH-704:** Implement calibration/abstention and outcome scorecard; seal source-citing informational snapshots.
- [ ] **F-08 · EH-706:** Add policy-versioned leverage terms and scenario kernels; prove paper default and no implicit live eligibility.
- [ ] **F-09 · EH-712:** Add stable finance event envelopes and transactional outbox append, preserving existing flip IDs.
- [ ] **F-10 · EH-714:** Implement optional read-only attached-source mapping with dedupe, corrections and schema-drift quarantine using disposable fixtures.
- [ ] **F-11 · EH-715:** Add independent statement, corporate-action, DST/session, daily-reset and futures-roll goldens plus property tests; record provenance for each vector.
- [ ] **F-12 · all IDs:** Regenerate clients; run affected Rust/Python, ontology, authorization, replay and quality gates. Review CCCC, jscpd, Dupehound and KISS results and remove duplicate authority. Update each delivery/acceptance row only with exact published-main evidence.

## Evidence register

| ID | Main SHA | Hosted run / command | Result/date |
|---|---|---|---|
| EH-698 | — | — | NOT_RUN |
| EH-699 | — | — | NOT_RUN |
| EH-700 | — | — | NOT_RUN |
| EH-702 | — | — | NOT_RUN |
| EH-703 | — | — | NOT_RUN |
| EH-704 | — | — | NOT_RUN |
| EH-706 | — | — | NOT_RUN |
| EH-712 | — | — | NOT_RUN |
| EH-714 | — | — | NOT_RUN |
| EH-715 | — | — | NOT_RUN |
