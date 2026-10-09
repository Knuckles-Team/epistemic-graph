# Implementable tasks

Tasks are ordered by dependency. Check a task only when its code, tests and exact-head evidence are linked in this directory.

- [x] **F-00 · EG-FINANCE-PRIMITIVES-R001:** Stabilize `BacktestRun` canonical serialization and digest across two supported hosts; check Pine SuperTrend/ATR parity against checked-in bars and consolidate finance ontology classes under `finance-v1`.
- [ ] **F-01 · EG-FINANCE-PRIMITIVES-R004:** Define v2 asset classes and migration aliases; `Account`, `Activity`, `Lot`, `Position`, `PortfolioGroup`, `StrategySpec`, `StrategyRun`, `LeverageTerms` types and SHACL shapes; reject unknown versions and direct writes to derived projections.
- [ ] **F-02 · EG-FINANCE-PRIMITIVES-R006:** Implement point-in-time action and exchange-calendar revision resolver; return session/source/as-of for all price paths; add DST, early-close and ticker reuse vectors.
- [ ] **F-03 · EG-FINANCE-PRIMITIVES-R005:** Implement canonical activity ordering and checked decimal lot engine for FIFO/LIFO/specific/average basis; add correction replay, transfer and split invariants.
- [ ] **F-04 · EG-FINANCE-PRIMITIVES-R005:** Add sourced FX, realized/unrealized P&L, TWR, bounded XIRR and benchmark comparison with partial coverage reports.
- [ ] **F-05 · EG-FINANCE-PRIMITIVES-R007:** Define immutable strategy-spec digest and proposal contract; implement DCA, trend and rebalance evaluators with idempotent cadence/missed-cycle evidence.
- [ ] **F-06 · EG-FINANCE-PRIMITIVES-R008:** Build common-horizon strategy comparison atop sealed `BacktestRun`; calculate CV, deflated Sharpe and PBO from one verified split matrix with costs.
- [ ] **F-06.1 · EG-FINANCE-PRIMITIVES-R008.1:** Seal a dollar-cost-averaging-by-fixed-amount backtest through the existing `backtest_run::seal` path; refuse a non-positive contribution and an out-of-universe fill. Lump-sum and trend comparisons remain under F-06.
- [ ] **F-07 · EG-FINANCE-PRIMITIVES-R009:** Implement calibration/abstention and outcome scorecard; seal source-citing informational snapshots.
- [ ] **F-08 · EG-FINANCE-PRIMITIVES-R010:** Add policy-versioned leverage terms and scenario kernels; prove paper default and no implicit live eligibility.
- [ ] **F-09 · EG-FINANCE-PRIMITIVES-R011:** Add stable finance event envelopes and transactional outbox append, preserving existing flip IDs.
- [ ] **F-10 · EG-FINANCE-PRIMITIVES-R012:** Implement optional read-only attached-source mapping with dedupe, corrections and schema-drift quarantine using disposable fixtures.
- [ ] **F-11 · EG-FINANCE-PRIMITIVES-R013:** Add independent statement, corporate-action, DST/session, daily-reset and futures-roll goldens plus property tests; record provenance for each vector.
- [ ] **F-12 · all IDs:** Regenerate clients; run affected Rust/Python, ontology, authorization, replay and quality gates. Review CCCC, jscpd, Dupehound and KISS results and remove duplicate authority. Update each delivery/acceptance row only with exact published-main evidence.
- [x] **F-13 · EG-FINANCE-PRIMITIVES-R002:** Audit and consolidate shared evaluation/calibration kernels in `eg-numeric`, convert finance methods to thin aliases or migrate callers, qualify UQL and regenerate method/client counts.
- [ ] **F-14 · EG-FINANCE-PRIMITIVES-R003:** Bind PBO to the sealed CSCV split matrix with deterministic median/tie behavior, verify skilled/overfit fixtures and reject mismatched caller summaries.
- [x] **F-15 · EG-FINANCE-PRIMITIVES-R014, EG-FINANCE-PRIMITIVES-R016:** Align the existing `finance-v1` instrument/signal module (`Instrument`, `Listing`, `Venue`, `BarSeries`, `IndicatorSpec`, `SignalState`, `TrendFlip`, `MacroEvent`) to FIBO and prove incremental, deterministic ATR/SuperTrend/SMA/EMA/200-week-SMA/20-21-band/Heikin-Ashi kernels against Pine parity fixtures.
- [x] **F-16 · EG-FINANCE-PRIMITIVES-R015, EG-FINANCE-PRIMITIVES-R017:** Attach a `share.read` control-lease link and informational-only notice to every sealed `AnalysisSnapshot`, and require every sealed `BacktestRun` to carry fill rule, point-in-time universe and no-look-ahead evidence alongside its mandatory CV/Sharpe/PBO/cost fields.

Requirement IDs not covered by any task above before this line: none remain — EG-FINANCE-PRIMITIVES-R014 through EG-FINANCE-PRIMITIVES-R017 are closed by F-15 and F-16.

## Evidence register

| ID | Main SHA | Hosted run / command | Result/date |
|---|---|---|---|
| EG-FINANCE-PRIMITIVES-R001 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R004 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R005 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R006 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R007 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R008 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R008.1 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R009 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R010 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R011 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R012 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R013 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R002 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R003 | — | — | NOT_RUN |
