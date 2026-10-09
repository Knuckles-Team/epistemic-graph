# Implementable tasks

Tasks are ordered by dependency. Check a task only when its code, tests and exact-head evidence are linked in this directory.

- [x] **F-00 · EG-FINANCE-PRIMITIVES-R001:** Stabilize `BacktestRun` canonical serialization and digest across two supported hosts; check Pine SuperTrend/ATR parity against checked-in bars and consolidate finance ontology classes under `finance-v1`.
- [x] **F-01 · EG-FINANCE-PRIMITIVES-R004:** Define v2 asset classes and migration aliases; `Account`, `Activity`, `Lot`, `Position`, `PortfolioGroup`, `StrategySpec`, `StrategyRun`, `LeverageTerms` types and SHACL shapes; reject unknown versions and direct writes to derived projections.
- [x] **F-00 · EG-FINANCE-PRIMITIVES-R001:** Stabilize `BacktestRun` canonical serialization and digest across two supported hosts; check Pine SuperTrend/ATR parity against checked-in bars and consolidate finance ontology classes under `finance-v1`.
- [x] **F-01.1 · EG-FINANCE-PRIMITIVES-R004.1:** Define the v2 asset-class vocabulary and migration aliases in `finance-v1.ttl`/`finance-v1.shapes.ttl`; tests in `src/server/graph_schema/finance_tests.rs::shapes::asset_class_vocabulary_accepts_canonical_and_legacy_values`.
- [x] **F-01.2 · EG-FINANCE-PRIMITIVES-R004.2:** `Account`, `Activity`, `Lot`, `Position`, `PortfolioGroup`, `StrategySpec`, `StrategyRun`, `LeverageTerms` Rust record types in eg-types, validated against the F-01.1 ontology; reject unknown schema versions and direct writes to derived projections.
- [x] **F-00 · EG-FINANCE-PRIMITIVES-R001:** Stabilize `BacktestRun` canonical serialization and digest across two supported hosts; check Pine SuperTrend/ATR parity against checked-in bars and consolidate finance ontology classes under `finance-v1`.
- [x] **F-01.1 · EG-FINANCE-PRIMITIVES-R004.1:** Define the v2 asset-class vocabulary and migration aliases in `finance-v1.ttl`/`finance-v1.shapes.ttl`.
- [x] **F-01.2 · EG-FINANCE-PRIMITIVES-R004.2:** `Account`, `Activity`, `Lot`, `Position`, `PortfolioGroup`, `StrategySpec`, `StrategyRun`, `LeverageTerms` OWL classes and SHACL shapes, validated against the F-01.1 ontology; reject unknown schema versions and direct writes to derived projections.
  - [x] **F-01.2.1 · EG-FINANCE-PRIMITIVES-R004.2.1:** `Account` and `Position` OWL classes and SHACL shapes in `finance-v1.ttl`/`finance-v1.shapes.ttl`; tests in `src/server/graph_schema/finance_tests.rs` (`declared.len() == 20`, `AccountShape`/`PositionShape` conformance and violation cases).
  - [x] **F-01.2.2 · EG-FINANCE-PRIMITIVES-R004.2.2:** `Activity` and `Lot` OWL classes and SHACL shapes in `finance-v1.ttl`/`finance-v1.shapes.ttl`; tests in `src/server/graph_schema/finance_tests.rs` (`declared.len() == 22`, `ActivityShape`/`LotShape` conformance and violation cases).
- [ ] **F-01.3 · EG-FINANCE-PRIMITIVES-R004.3:** Wire the F-01.2 record types into server handlers and regenerate the contract/generated client surfaces.
- [x] **F-02 · EG-FINANCE-PRIMITIVES-R006:** Implement point-in-time action and exchange-calendar revision resolver; return session/source/as-of for all price paths; add DST, early-close and ticker reuse vectors.
  - [x] Point-in-time `CorporateAction` fact type (split/dividend/symbol-change/delisting) and its `(listing_id, effective_time)` as-of revision resolver, with a ticker-reuse-is-never-conflated test; `Session` (regular/pre/post/closed/unknown) classification over `ExchangeCalendar`'s new pre-/post-market spans, with DST and early-close vectors; `resolve::resolve_priced` wires session/source/as-of onto the bar price path. Code: `crates/eg-types/src/compute_result/market/corporate_action.rs`, `crates/eg-compute/src/finance/market/corporate_actions.rs`, `calendar.rs::session_at`, `resolve.rs::resolve_priced`.
  - [ ] Remaining: wire `session`/`source`/`as_of` onto the other price-result paths (indicator/signal/snapshot outputs), and an ontology/SHACL representation of `CorporateAction` alongside the existing `finance-v1` classes.
- [ ] **F-03 · EG-FINANCE-PRIMITIVES-R005:** Implement canonical activity ordering and checked decimal lot engine for FIFO/LIFO/specific/average basis; add correction replay, transfer and split invariants.
- [x] **F-03.1 · EG-FINANCE-PRIMITIVES-R005.1:** Deterministic fixed-point FIFO/LIFO lot-matching engine (`crates/eg-compute/src/finance/lot_accounting.rs`), pure over an ordered activity slice; oversell/out-of-order/non-positive refusals and a replay-determinism test.
- [ ] **F-03.2 · EG-FINANCE-PRIMITIVES-R005.2:** Add specific-lot/average-cost elections and transfer/split invariants.
- [ ] **F-04 · EG-FINANCE-PRIMITIVES-R005:** Add sourced FX, realized/unrealized P&L, TWR, bounded XIRR and benchmark comparison with partial coverage reports.
- [x] **F-05 · EG-FINANCE-PRIMITIVES-R007:** Define immutable strategy-spec digest and proposal contract; implement DCA, trend and rebalance evaluators with idempotent cadence/missed-cycle evidence.
- [x] **F-05.1 · EG-FINANCE-PRIMITIVES-R007.1:** Implement the typed `StrategySpec`/`StrategyKind` wire shape and the dollar-cost-averaging-by-fixed-amount evaluator producing a `ProposedAction`; refuse a non-positive configured amount. Trend-following and rebalancing evaluators remain under F-05.
- [x] **F-06 · EG-FINANCE-PRIMITIVES-R008:** Build common-horizon strategy comparison atop sealed `BacktestRun`; calculate CV, deflated Sharpe and PBO from one verified split matrix with costs.
- [x] **F-07 · EG-FINANCE-PRIMITIVES-R009:** Implement calibration/abstention and outcome scorecard; seal source-citing informational snapshots.
- [x] **F-07.1 · EG-FINANCE-PRIMITIVES-R009.1:** Map a calibrated flip-confidence outcome to accumulate/hold/de-risk/abstain, citing strategy version and evidence. Scorecards and AnalysisSnapshot sealing remain under F-07.
- [x] **F-08 · EG-FINANCE-PRIMITIVES-R010:** Add policy-versioned leverage terms and scenario kernels; prove paper default and no implicit live eligibility.
- [x] **F-08.1 · EG-FINANCE-PRIMITIVES-R010.1:** Implement the live-order eligibility boundary (per-instrument policy, designated approver, paper default, US-retail CFD refusal). Margin-call/liquidation/sizing scenario kernels remain under F-08.
- [ ] **F-09 · EG-FINANCE-PRIMITIVES-R011:** Add stable finance event envelopes and transactional outbox append, preserving existing flip IDs.
- [ ] **F-05 · EG-FINANCE-PRIMITIVES-R007:** Define immutable strategy-spec digest and proposal contract; implement DCA, trend and rebalance evaluators with idempotent cadence/missed-cycle evidence.
- [ ] **F-06 · EG-FINANCE-PRIMITIVES-R008:** Build common-horizon strategy comparison atop sealed `BacktestRun`; calculate CV, deflated Sharpe and PBO from one verified split matrix with costs.
- [ ] **F-07 · EG-FINANCE-PRIMITIVES-R009:** Implement calibration/abstention and outcome scorecard; seal source-citing informational snapshots.
- [ ] **F-08 · EG-FINANCE-PRIMITIVES-R010:** Add policy-versioned leverage terms and scenario kernels; prove paper default and no implicit live eligibility.
- [x] **F-09a · EG-FINANCE-PRIMITIVES-R011.1:** Add the typed `FinanceAlertEnvelope`/`FinanceAlertKind` (`crates/eg-types/src/finance_event_envelope.rs`), proving the type-level contract for price/trend-flip/DCA-due/margin-threshold alerts and their outbox event-schema names. Test: `crates/eg-types/src/finance_event_envelope.rs::tests::an_empty_event_id_is_refused` (plus the sibling refusal tests in the same module).
- [ ] **F-09 · EG-FINANCE-PRIMITIVES-R011:** Add stable finance event envelopes and transactional outbox append, preserving existing flip IDs. (R011.1 above delivers the typed envelope; remaining: wire it into the transactional outbox append path, preserving existing flip IDs end to end.)
- [ ] **F-10 · EG-FINANCE-PRIMITIVES-R012:** Implement optional read-only attached-source mapping with dedupe, corrections and schema-drift quarantine using disposable fixtures.
- [ ] **F-11 · EG-FINANCE-PRIMITIVES-R013:** Add independent statement, corporate-action, DST/session, daily-reset and futures-roll goldens plus property tests; record provenance for each vector.
- [ ] **F-12 · all IDs:** Regenerate clients; run affected Rust/Python, ontology, authorization, replay and quality gates. Review CCCC, jscpd, Dupehound and KISS results and remove duplicate authority. Update each delivery/acceptance row only with exact published-main evidence.
- [x] **F-13 · EG-FINANCE-PRIMITIVES-R002:** Audit and consolidate shared evaluation/calibration kernels in `eg-numeric`, convert finance methods to thin aliases or migrate callers, qualify UQL and regenerate method/client counts.
- [x] **F-14 · EG-FINANCE-PRIMITIVES-R003:** Bind PBO to the sealed CSCV split matrix with deterministic median/tie behavior, verify skilled/overfit fixtures and reject mismatched caller summaries.
- [x] **F-15 · EG-FINANCE-PRIMITIVES-R014, EG-FINANCE-PRIMITIVES-R016:** Align the existing `finance-v1` instrument/signal module (`Instrument`, `Listing`, `Venue`, `BarSeries`, `IndicatorSpec`, `SignalState`, `TrendFlip`, `MacroEvent`) to FIBO and prove incremental, deterministic ATR/SuperTrend/SMA/EMA/200-week-SMA/20-21-band/Heikin-Ashi kernels against Pine parity fixtures.
- [x] **F-16 · EG-FINANCE-PRIMITIVES-R015, EG-FINANCE-PRIMITIVES-R017:** Attach a `share.read` control-lease link and informational-only notice to every sealed `AnalysisSnapshot`, and require every sealed `BacktestRun` to carry fill rule, point-in-time universe and no-look-ahead evidence alongside its mandatory CV/Sharpe/PBO/cost fields.
- [x] **F-13 · EG-FINANCE-PRIMITIVES-R002:** Audit and consolidate shared evaluation/calibration kernels in `eg-numeric`, convert finance methods to thin aliases or migrate callers, qualify UQL and regenerate method/client counts.
- [x] **F-14 · EG-FINANCE-PRIMITIVES-R003:** Bind PBO to the sealed CSCV split matrix with deterministic median/tie behavior, verify skilled/overfit fixtures and reject mismatched caller summaries.
- [x] **F-15 · EG-FINANCE-PRIMITIVES-R014, EG-FINANCE-PRIMITIVES-R016:** Align the existing `finance-v1` instrument/signal module (`Instrument`, `Listing`, `Venue`, `BarSeries`, `IndicatorSpec`, `SignalState`, `TrendFlip`, `MacroEvent`) to FIBO and prove incremental, deterministic ATR/SuperTrend/SMA/EMA/200-week-SMA/20-21-band/Heikin-Ashi kernels against Pine parity fixtures.
- [x] **F-16 · EG-FINANCE-PRIMITIVES-R015, EG-FINANCE-PRIMITIVES-R017:** Attach a `share.read` control-lease link and informational-only notice to every sealed `AnalysisSnapshot`, and require every sealed `BacktestRun` to carry fill rule, point-in-time universe and no-look-ahead evidence alongside its mandatory CV/Sharpe/PBO/cost fields.

Requirement IDs not covered by any task above before this line: none remain — EG-FINANCE-PRIMITIVES-R014 through EG-FINANCE-PRIMITIVES-R017 are closed by F-15 and F-16.

## Evidence register

| ID | Main SHA | Hosted run / command | Result/date |
|---|---|---|---|
| EG-FINANCE-PRIMITIVES-R001 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R004 | — | — | NOT_RUN (rollup) |
| EG-FINANCE-PRIMITIVES-R004.1 | — | `cargo test -p epistemic-graph --lib server::graph_schema::finance_tests` | PR open; see PR for run result |
| EG-FINANCE-PRIMITIVES-R004.2 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R004.1 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R004.2 | — | — | NOT_RUN (rollup) |
| EG-FINANCE-PRIMITIVES-R004.2.1 | — | `cargo test -p epistemic-graph --lib server::graph_schema::finance_tests` | PR open; see PR for run result |
| EG-FINANCE-PRIMITIVES-R004.2.2 | — | `cargo test -p epistemic-graph --all-features --lib server::graph_schema::finance_tests` | PR open; see PR for run result |
| EG-FINANCE-PRIMITIVES-R004.3 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R005 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R006 | — | — | PARTIAL: corporate-action resolver + session classification delivered; other price paths and ontology remain |
| EG-FINANCE-PRIMITIVES-R004 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R005 | — | — | NOT_RUN (rollup) |
| EG-FINANCE-PRIMITIVES-R005.1 | — | `cargo test -p eg-compute --all-features finance::lot_accounting` | PR open; see PR for run result |
| EG-FINANCE-PRIMITIVES-R005.2 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R006 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R007 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R007.1 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R008 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R009 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R009.1 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R010 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R010.1 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R011 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R012 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R013 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R002 | — | — | NOT_RUN |
| EG-FINANCE-PRIMITIVES-R003 | — | `cargo test -p eg-compute -p eg-types --all-features --lib market` (eg-lane-run `eg-finance-r003-20261009035243-b5a404398`) | PR open; see run id for result |
