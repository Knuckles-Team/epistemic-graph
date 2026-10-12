# Implementable tasks

Tasks are ordered by dependency. Check a task only when its code, tests and exact-head evidence are linked in this directory.

- [x] **F-00 · EG-FINANCE-PRIMITIVES-R001:** Stabilize `BacktestRun` canonical serialization and digest across two supported hosts. Check Pine SuperTrend/ATR parity against checked-in bars and consolidate finance ontology classes under `finance-v1`.
- [x] **F-01 · EG-FINANCE-PRIMITIVES-R004:** Define v2 asset classes and migration aliases. `Account`, `Activity`, `Lot`, `Position`, `PortfolioGroup`, `StrategySpec`, `StrategyRun`, `LeverageTerms` types and SHACL shapes; reject unknown versions and direct writes to derived projections.
- [x] **F-00 · EG-FINANCE-PRIMITIVES-R001:** Stabilize `BacktestRun` canonical serialization and digest across two supported hosts. Check Pine SuperTrend/ATR parity against checked-in bars and consolidate finance ontology classes under `finance-v1`.
- [x] **F-01.1 · EG-FINANCE-PRIMITIVES-R004.1:** Define the v2 asset-class vocabulary and migration aliases in `finance-v1.ttl`/`finance-v1.shapes.ttl`; tests in `src/server/graph_schema/finance_tests.rs::shapes::asset_class_vocabulary_accepts_canonical_and_legacy_values`.
- [ ] **F-01.2 · EG-FINANCE-PRIMITIVES-R004.2:** `Account`, `Activity`, `Lot`, `Position`, `PortfolioGroup`, `StrategySpec`, `StrategyRun`, `LeverageTerms` Rust record types in eg-types, validated against the F-01.1 ontology. Reject unknown schema versions and direct writes to derived projections.
- [x] **F-00 · EG-FINANCE-PRIMITIVES-R001:** Stabilize `BacktestRun` canonical serialization and digest across two supported hosts. Check Pine SuperTrend/ATR parity against checked-in bars and consolidate finance ontology classes under `finance-v1`.
- [x] **F-01.1 · EG-FINANCE-PRIMITIVES-R004.1:** Define the v2 asset-class vocabulary and migration aliases in `finance-v1.ttl`/`finance-v1.shapes.ttl`.
- [ ] **F-01.2 · EG-FINANCE-PRIMITIVES-R004.2:** `Account`, `Activity`, `Lot`, `Position`, `PortfolioGroup`, `StrategySpec`, `StrategyRun`, `LeverageTerms` OWL classes and SHACL shapes, validated against the F-01.1 ontology. Reject unknown schema versions and direct writes to derived projections.
  - [ ] **F-01.2.1 · EG-FINANCE-PRIMITIVES-R004.2.1:** `Account` and `Position` OWL classes and SHACL shapes in `finance-v1.ttl`/`finance-v1.shapes.ttl`; tests in `src/server/graph_schema/finance_tests.rs` (`declared.len() == 20`, `AccountShape`/`PositionShape` conformance and violation cases).
  - [ ] **F-01.2.2 · EG-FINANCE-PRIMITIVES-R004.2.2:** `Activity` and `Lot` OWL classes and SHACL shapes in `finance-v1.ttl`/`finance-v1.shapes.ttl`; tests in `src/server/graph_schema/finance_tests.rs` (`declared.len() == 22`, `ActivityShape`/`LotShape` conformance and violation cases).
- [ ] **F-01.3 · EG-FINANCE-PRIMITIVES-R004.3:** Wire the F-01.2 record types into server handlers and regenerate the contract/generated client surfaces.
- [x] **F-02 · EG-FINANCE-PRIMITIVES-R006:** Implement point-in-time action and exchange-calendar revision resolver. Return session/source/as-of for all price paths. Add DST, early-close and ticker reuse vectors.
  - [x] Point-in-time `CorporateAction` fact type (split/dividend/symbol-change/delisting) and its `(listing_id, effective_time)` as-of revision resolver, with a ticker-reuse-is-never-conflated test; `Session` (regular/pre/post/closed/unknown) classification over `ExchangeCalendar`'s new pre-/post-market spans, with DST and early-close vectors; `resolve::resolve_priced` wires session/source/as-of onto the bar price path. Code: `crates/eg-types/src/compute_result/market/corporate_action.rs`, `crates/eg-compute/src/finance/market/corporate_actions.rs`, `calendar.rs::session_at`, `resolve.rs::resolve_priced`.
  - [ ] Remaining: wire `session`/`source`/`as_of` onto the other price-result paths (indicator/signal/snapshot outputs), and an ontology/SHACL representation of `CorporateAction` alongside the existing `finance-v1` classes.
- [ ] **F-03 · EG-FINANCE-PRIMITIVES-R005:** Implement canonical activity ordering and checked decimal lot engine for FIFO/LIFO/specific/average basis. Add correction replay, transfer and split invariants.
- [x] **F-03.1 · EG-FINANCE-PRIMITIVES-R005.1:** Deterministic fixed-point FIFO/LIFO lot-matching engine (`crates/eg-compute/src/finance/lot_accounting.rs`), pure over an ordered activity slice. Oversell/out-of-order/non-positive refusals and a replay-determinism test.
  - [ ] **F-03.1.1 · EG-FINANCE-PRIMITIVES-R005.1.1 (reopened 2026-10-09, audit: suspect test):** Confirm `fifo_closes_the_oldest_lot_first` and its two siblings actually run and pass, and extend them beyond buy-only activity fixtures to a representative mixed buy/sell sequence. Test: `crates/eg-compute/src/finance/lot_accounting.rs::tests::fifo_and_lifo_lot_matching_tests_run_and_pass_against_a_mixed_buy_sell_sequence`.
- [ ] **F-03.2 · EG-FINANCE-PRIMITIVES-R005.2:** Add specific-lot/average-cost elections and transfer/split invariants.
  - [x] **F-03.2.1 · EG-FINANCE-PRIMITIVES-R005.2.1:** `LotMethod::Specific(opened_by)` closes the named open lot instead of a FIFO/LIFO choice, refusing an absent or insufficient named lot. Tests in `crates/eg-compute/src/finance/lot_accounting.rs`.
  - [ ] Remaining: average-cost election, and transfer/split invariants.
- [ ] **F-04 · EG-FINANCE-PRIMITIVES-R005:** Add sourced FX, realized/unrealized P&L, TWR, bounded XIRR and benchmark comparison with partial coverage reports.
- [x] **F-05 · EG-FINANCE-PRIMITIVES-R007:** Define immutable strategy-spec digest and proposal contract; implement DCA, trend and rebalance evaluators with idempotent cadence/missed-cycle evidence.
- [x] **F-05.1 · EG-FINANCE-PRIMITIVES-R007.1:** Implement the typed `StrategySpec`/`StrategyKind` wire shape and the dollar-cost-averaging-by-fixed-amount evaluator producing a `ProposedAction`; refuse a non-positive configured amount. Trend-following and rebalancing evaluators remain under F-05.
- [x] **F-06 · EG-FINANCE-PRIMITIVES-R008:** Build common-horizon strategy comparison atop sealed `BacktestRun`; calculate CV, deflated Sharpe and PBO from one verified split matrix with costs.
- [x] **F-06.1 · EG-FINANCE-PRIMITIVES-R008.1:** Seal a dollar-cost-averaging-by-fixed-amount backtest through the existing `backtest_run::seal` path; refuse a non-positive contribution and an out-of-universe fill. Lump-sum and trend comparisons remain under F-06.
- [x] **F-07 · EG-FINANCE-PRIMITIVES-R009:** Implement calibration/abstention and outcome scorecard; seal source-citing informational snapshots.
- [x] **F-07.1 · EG-FINANCE-PRIMITIVES-R009.1:** Map a calibrated flip-confidence outcome to accumulate/hold/de-risk/abstain, citing strategy version and evidence. Scorecards and AnalysisSnapshot sealing remain under F-07.
- [x] **F-08 · EG-FINANCE-PRIMITIVES-R010:** Add policy-versioned leverage terms and scenario kernels; prove paper default and no implicit live eligibility.
- [x] **F-08.1 · EG-FINANCE-PRIMITIVES-R010.1:** Implement the live-order eligibility boundary (per-instrument policy, designated approver, paper default, US-retail CFD refusal). Margin-call/liquidation/sizing scenario kernels remain under F-08.
- [ ] **F-09 · EG-FINANCE-PRIMITIVES-R011:** Add stable finance event envelopes and transactional outbox append, preserving existing flip IDs.
- [x] **F-05 · EG-FINANCE-PRIMITIVES-R007:** Define immutable strategy-spec digest and proposal contract; implement DCA, trend and rebalance evaluators with idempotent cadence/missed-cycle evidence.
- [x] **F-06 · EG-FINANCE-PRIMITIVES-R008:** Build common-horizon strategy comparison atop sealed `BacktestRun`. Calculate CV, deflated Sharpe and PBO from one verified split matrix with costs.
- [x] **F-07 · EG-FINANCE-PRIMITIVES-R009:** Implement calibration/abstention and outcome scorecard; seal source-citing informational snapshots.
- [x] **F-08 · EG-FINANCE-PRIMITIVES-R010:** Add policy-versioned leverage terms and scenario kernels; prove paper default and no implicit live eligibility.
- [x] **F-09a · EG-FINANCE-PRIMITIVES-R011.1:** Add the typed `FinanceAlertEnvelope`/`FinanceAlertKind` (`crates/eg-types/src/finance_event_envelope.rs`), proving the type-level contract for price/trend-flip/DCA-due/margin-threshold alerts and their outbox event-schema names. Test: `crates/eg-types/src/finance_event_envelope.rs::tests::an_empty_event_id_is_refused` (plus the sibling refusal tests in the same module).
- [ ] **F-09 · EG-FINANCE-PRIMITIVES-R011:** Add stable finance event envelopes and transactional outbox append, preserving existing flip IDs. (R011.1 above delivers the typed envelope; remaining: wire it into the transactional outbox append path, preserving existing flip IDs end to end.)
- [ ] **F-10 · EG-FINANCE-PRIMITIVES-R012:** Implement optional read-only attached-source mapping with dedupe, corrections and schema-drift quarantine using disposable fixtures.
  - [x] **F-10.2 · EG-FINANCE-PRIMITIVES-R012.2:** `register_ghostfolio_source`/`import_ghostfolio_fixture` attach a Ghostfolio source through the typed source registry and import a disposable fixture idempotently by `ghostfolio_activity_id`. Tests in `crates/eg-compute/src/finance/ghostfolio_import.rs`.
- [ ] **F-11 · EG-FINANCE-PRIMITIVES-R013:** Add independent statement, corporate-action, DST/session, daily-reset and futures-roll goldens plus property tests; record provenance for each vector.
  - [x] **F-11.2.1 · EG-FINANCE-PRIMITIVES-R013.2.1:** `compare_broker_statement_figure` refuses incomplete provenance then compares `value_ticks` against a published reference. Tests in `crates/eg-compute/src/finance/golden_fixture.rs`.
  - [ ] **F-11.2.2 · EG-FINANCE-PRIMITIVES-R013.2.2:** Check in independently sourced split/dividend notice vectors and execute corporate-action reference comparisons and provenance refusals.
  - [ ] **F-11.2.3 · EG-FINANCE-PRIMITIVES-R013.2.3:** Check in published exchange-calendar/DST vectors and execute session/offset, host-time-zone replay and provenance checks.
  - [ ] **F-11.2.4 · EG-FINANCE-PRIMITIVES-R013.2.4:** Check in published multi-session fund return/NAV vectors and execute daily-reset decay comparisons with explicit fee/rounding assumptions and provenance refusals.
  - [ ] **F-11.2.5 · EG-FINANCE-PRIMITIVES-R013.2.5:** Check in published futures contract/settlement vectors and execute roll cash/position comparisons, term/refusal and deterministic replay checks.
  - [ ] Remaining qualification: R013.2.1 currently proves the comparator with synthetic values, not an independent broker-statement dataset. Record sourced statement acceptance before accepting the full R013 fixture family.
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
| EG-FINANCE-PRIMITIVES-R001 | — | — | No execution receipt is recorded here. |
| EG-FINANCE-PRIMITIVES-R004 | — | — | No execution receipt is recorded here. |
| EG-FINANCE-PRIMITIVES-R004.1 | — | `cargo test -p epistemic-graph --lib server::graph_schema::finance_tests` | The recorded command has no linked result here. |
| EG-FINANCE-PRIMITIVES-R004.2 | — | — | No execution receipt is recorded here. |
| EG-FINANCE-PRIMITIVES-R004.1 | — | — | No execution receipt is recorded here. |
| EG-FINANCE-PRIMITIVES-R004.2 | — | — | No execution receipt is recorded here. |
| EG-FINANCE-PRIMITIVES-R004.2.1 | — | `cargo test -p epistemic-graph --lib server::graph_schema::finance_tests` | The recorded command has no linked result here. |
| EG-FINANCE-PRIMITIVES-R004.2.2 | — | `cargo test -p epistemic-graph --all-features --lib server::graph_schema::finance_tests` | The recorded command has no linked result here. |
| EG-FINANCE-PRIMITIVES-R004.3 | — | — | No execution receipt is recorded here. |
| EG-FINANCE-PRIMITIVES-R005 | — | — | No execution receipt is recorded here. |
| EG-FINANCE-PRIMITIVES-R006 | — | — | The corporate-action resolver and session classification exist; other price paths and ontology remain. |
| EG-FINANCE-PRIMITIVES-R004 | — | — | No execution receipt is recorded here. |
| EG-FINANCE-PRIMITIVES-R005 | — | — | No execution receipt is recorded here. |
| EG-FINANCE-PRIMITIVES-R005.1 | — | `cargo test -p eg-compute --all-features finance::lot_accounting` | The recorded command has no linked result here. |
| EG-FINANCE-PRIMITIVES-R005.2 | — | — | No execution receipt is recorded here. |
| EG-FINANCE-PRIMITIVES-R006 | — | — | No execution receipt is recorded here. |
| EG-FINANCE-PRIMITIVES-R007 | — | — | No execution receipt is recorded here. |
| EG-FINANCE-PRIMITIVES-R007.1 | — | — | No execution receipt is recorded here. |
| EG-FINANCE-PRIMITIVES-R008 | — | — | No execution receipt is recorded here. |
| EG-FINANCE-PRIMITIVES-R008.1 | — | — | No execution receipt is recorded here. |
| EG-FINANCE-PRIMITIVES-R009 | — | — | No execution receipt is recorded here. |
| EG-FINANCE-PRIMITIVES-R009.1 | — | — | No execution receipt is recorded here. |
| EG-FINANCE-PRIMITIVES-R010 | — | — | No execution receipt is recorded here. |
| EG-FINANCE-PRIMITIVES-R010.1 | — | — | No execution receipt is recorded here. |
| EG-FINANCE-PRIMITIVES-R011 | — | — | No execution receipt is recorded here. |
| EG-FINANCE-PRIMITIVES-R012 | — | — | No execution receipt is recorded here. |
| EG-FINANCE-PRIMITIVES-R013 | — | — | No execution receipt is recorded here. |
| EG-FINANCE-PRIMITIVES-R002 | — | — | No execution receipt is recorded here. |
| EG-FINANCE-PRIMITIVES-R003 | — | `cargo test -p eg-compute -p eg-types --all-features --lib market` (eg-lane-run `eg-finance-r003-20261009035243-b5a404398`) | The recorded command names its run; no result is linked here. |

## Decomposition children (tracked)

- [x] **EG-FINANCE-PRIMITIVES-R004.3.1:** The wire boundary refuses an unknown finance-v1 schema version
- [x] **EG-FINANCE-PRIMITIVES-R012.1:** A typed gate admits only a one-way Ghostfolio import, never a write-back
- [ ] **EG-FINANCE-PRIMITIVES-R012.2:** Ghostfolio attaches through the typed source registry with disposable-fixture coverage
- [x] **EG-FINANCE-PRIMITIVES-R013.1:** A golden-fixture figure is refused when its provenance is incomplete
- [ ] **EG-FINANCE-PRIMITIVES-R013.2:** Golden fixtures compare computed figures against published reference values

## Figure-provenance verification scope

R013.1's four bound admission/refusal tests were executed directly from the production module at commit `168b52ef630717f5d6789869c46826621bcaa01f`: all seven module tests passed, including three existing comparator regression tests. This is narrow module acceptance; published-main/hosted qualification remains a separate evidence step. R013.2.2 through R013.2.5 remain unimplemented and unverified; the generated R013/R013.2 rollups must remain open.

## Restored runtime acceptance children

These children remain unverified. Intended paths and test names define future acceptance; they do not claim existing test bindings.

- [ ] **EG-FINANCE-PRIMITIVES-R004.3.2:** Served record registration and round trips. Source: `src/server/handlers/finance.rs`. Complete the child rows before accepting this rollup.
- [ ] **EG-FINANCE-PRIMITIVES-R004.3.2.1:** Account served entry point. Source: `src/server/handlers/finance.rs; src/server/handlers/finance/records.rs (intended handler module)`. Intended test: `src/server/handlers/finance/records/tests.rs::account_served_round_trip_and_refusals (intended test)`.
- [ ] **EG-FINANCE-PRIMITIVES-R004.3.2.2:** Activity served entry point. Source: `src/server/handlers/finance.rs; src/server/handlers/finance/records.rs (intended handler module)`. Intended test: `src/server/handlers/finance/records/tests.rs::activity_served_round_trip_and_refusals (intended test)`.
- [ ] **EG-FINANCE-PRIMITIVES-R004.3.2.3:** Lot served entry point. Source: `src/server/handlers/finance.rs; src/server/handlers/finance/records.rs (intended handler module)`. Intended test: `src/server/handlers/finance/records/tests.rs::lot_served_round_trip_and_refusals (intended test)`.
- [ ] **EG-FINANCE-PRIMITIVES-R004.3.2.4:** Position served entry point. Source: `src/server/handlers/finance.rs; src/server/handlers/finance/records.rs (intended handler module)`. Intended test: `src/server/handlers/finance/records/tests.rs::position_served_round_trip_and_refusals (intended test)`.
- [ ] **EG-FINANCE-PRIMITIVES-R004.3.2.5:** PortfolioGroup served entry point. Source: `src/server/handlers/finance.rs; src/server/handlers/finance/records.rs (intended handler module)`. Intended test: `src/server/handlers/finance/records/tests.rs::portfolio_group_served_round_trip_and_refusals (intended test)`.
- [ ] **EG-FINANCE-PRIMITIVES-R004.3.2.6:** StrategySpec served entry point. Source: `src/server/handlers/finance.rs; src/server/handlers/finance/records.rs (intended handler module)`. Intended test: `src/server/handlers/finance/records/tests.rs::strategy_spec_served_round_trip_and_refusals (intended test)`.
- [ ] **EG-FINANCE-PRIMITIVES-R004.3.2.7:** StrategyRun served entry point. Source: `src/server/handlers/finance.rs; src/server/handlers/finance/records.rs (intended handler module)`. Intended test: `src/server/handlers/finance/records/tests.rs::strategy_run_served_round_trip_and_refusals (intended test)`.
- [ ] **EG-FINANCE-PRIMITIVES-R004.3.2.8:** LeverageTerms served entry point. Source: `src/server/handlers/finance.rs; src/server/handlers/finance/records.rs (intended handler module)`. Intended test: `src/server/handlers/finance/records/tests.rs::leverage_terms_served_round_trip_and_refusals (intended test)`.
- [ ] **EG-FINANCE-PRIMITIVES-R004.3.3:** Record contract and generated-client acceptance. Source: `crates/eg-capabilities/src/bin/gen_contract.rs`. Complete the child rows before accepting this rollup.
- [ ] **EG-FINANCE-PRIMITIVES-R004.3.3.1:** Finance record contract regeneration. Source: `crates/eg-capabilities/src/bin/gen_contract.rs; contract/methods.json; contract/schemas/`. Intended test: `tests/test_contract_wave_surface.py::test_finance_record_contract_surface (intended test)`.
- [ ] **EG-FINANCE-PRIMITIVES-R004.3.3.2:** Finance generated-client round trips. Source: `crates/eg-capabilities/src/bin/gen_contract.rs; epistemic_graph/generated/`. Intended test: `tests/test_generated_client_contract.py::test_finance_record_client_round_trips (intended test)`.
- [ ] **EG-FINANCE-PRIMITIVES-R005.2.2:** Average-cost election. Source: `crates/eg-compute/src/finance/lot_accounting.rs`. Intended test: `crates/eg-compute/src/finance/lot_accounting.rs::tests::average_cost_election_preserves_basis (intended test)`.
- [ ] **EG-FINANCE-PRIMITIVES-R005.2.3:** Transfer conservation. Source: `crates/eg-compute/src/finance/lot_accounting.rs`. Intended test: `crates/eg-compute/src/finance/lot_accounting.rs::tests::paired_transfer_conserves_quantity_cash_and_basis (intended test)`.
- [ ] **EG-FINANCE-PRIMITIVES-R005.2.4:** Split basis preservation. Source: `crates/eg-compute/src/finance/lot_accounting.rs`. Intended test: `crates/eg-compute/src/finance/lot_accounting.rs::tests::split_preserves_aggregate_basis (intended test)`.
- [ ] **EG-FINANCE-PRIMITIVES-R011.2:** Finance transactional append. Source: `src/server/mutation_batch/compile/finish_helpers.rs`. Complete the child rows before accepting this rollup.
- [ ] **EG-FINANCE-PRIMITIVES-R011.2.1:** Finance envelope to outbox intent. Source: `src/server/mutation_batch/compile/finish_helpers.rs`. Intended test: `src/server/mutation_batch/compile.rs::tests::finance_alert_intents_preserve_schema_and_identity (intended test)`.
- [ ] **EG-FINANCE-PRIMITIVES-R011.2.2:** Finance event atomic commit. Source: `src/server/mutation_batch.rs; src/server/mutation_batch/compile.rs`. Intended test: `src/server/mutation_batch/tests.rs::finance_alert_and_outbox_commit_atomically (intended test)`.
- [ ] **EG-FINANCE-PRIMITIVES-R011.3:** Finance delivery replay. Source: `src/server/mutation_batch.rs; crates/eg-transaction/src/outbox/mod.rs`. Intended test: `src/server/mutation_batch/tests.rs::finance_alert_replay_preserves_one_outbox_fact (intended test)`.

## Remaining record-schema producers

- [ ] **EG-FINANCE-PRIMITIVES-R004.2.3:** Add `PortfolioGroup` to the finance ontology and SHACL module. Reuse its existing record contract. Intended test: `src/server/graph_schema/finance_tests.rs::shapes::portfolio_group_schema_acceptance (intended test)`. Target: one five-minute code slice; native validation may need more time.
- [ ] **EG-FINANCE-PRIMITIVES-R004.2.4:** Add `StrategySpec` to the finance ontology and SHACL module. Reuse its existing record contract. Intended test: `src/server/graph_schema/finance_tests.rs::shapes::strategy_spec_schema_acceptance (intended test)`. Target: one five-minute code slice; native validation may need more time.
- [ ] **EG-FINANCE-PRIMITIVES-R004.2.5:** Add `StrategyRun` to the finance ontology and SHACL module. Reuse its existing record contract. Intended test: `src/server/graph_schema/finance_tests.rs::shapes::strategy_run_schema_acceptance (intended test)`. Target: one five-minute code slice; native validation may need more time.
- [ ] **EG-FINANCE-PRIMITIVES-R004.2.6:** Add `LeverageTerms` to the finance ontology and SHACL module. Reuse its existing record contract. Intended test: `src/server/graph_schema/finance_tests.rs::shapes::leverage_terms_schema_acceptance (intended test)`. Target: one five-minute code slice; native validation may need more time.

## Complete schema acceptance

- [ ] **EG-FINANCE-PRIMITIVES-R004.2.1.1:** Complete the `Account/Position` omission, invalid-link and vocabulary matrix. Existing sampled tests are partial evidence. Intended test: `src/server/graph_schema/finance_tests.rs::shapes::account_position_complete_schema_matrix (intended test)`. Target: five-minute code slice; native execution may need more time.
- [ ] **EG-FINANCE-PRIMITIVES-R004.2.2.1:** Complete the `Activity/Lot` omission, invalid-link and vocabulary matrix. Existing sampled tests are partial evidence. Intended test: `src/server/graph_schema/finance_tests.rs::shapes::activity_lot_complete_schema_matrix (intended test)`. Target: five-minute code slice; native execution may need more time.
  - [ ] **EG-FINANCE-PRIMITIVES-R018:** Factor research records each factor as a typed node (rollup).
  - [ ] **EG-FINANCE-PRIMITIVES-R018.1:** `FactorRecord` typed model
  - [ ] **EG-FINANCE-PRIMITIVES-R018.2:** Ranking over the shared kernels
  - [ ] **EG-FINANCE-PRIMITIVES-R018.3:** Attach at the finance method route
  - [ ] **EG-FINANCE-PRIMITIVES-R018.4:** Typed abstention on thin input
  - [ ] **EG-FINANCE-PRIMITIVES-R018.5:** Factor provenance edges
  - [ ] **EG-FINANCE-PRIMITIVES-R018.6:** Served factor ranking proof


## R002 implementation sequence

- [ ] **EG-FINANCE-PRIMITIVES-R002.5:** Define versioned state-space inputs and checkpoints. Implement its proposed proving test from requirements.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.6:** Retain the original rollup acceptance and complete every child below.
- [ ] **EG-FINANCE-PRIMITIVES-R002.7:** Detect innovation shifts and retune within fixed bounds. Implement its proposed proving test from requirements.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.8:** Bound nonlinear approximation and robust tail influence. Implement its proposed proving test from requirements.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.1:** Retain the original rollup acceptance and complete every child below.
- [ ] **EG-FINANCE-PRIMITIVES-R002.4:** Bound original regime derivations and streaming work. Implement its proposed proving test from requirements.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2:** Retain the original rollup acceptance and complete every child below.
- [ ] **EG-FINANCE-PRIMITIVES-R002.3:** Reconcile generated methods and client bindings. Implement its proposed proving test from requirements.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.9:** Wire versioned filtering through served series operations. Implement its proposed proving test from requirements.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.10:** Prove filtering on the production hot paths. Implement its proposed proving test from requirements.md.

The new scenarios have no execution receipts. This definition change introduces no implementation or test bindings.

### R002 kernel and model units

- [ ] **EG-FINANCE-PRIMITIVES-R002.1.1:** Rolling z-score shared kernel. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.1.2:** EWMA shared kernel. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.1.3:** Information coefficient shared kernel. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.1.4:** Information ratio shared kernel. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.1.5:** Effective sample size shared kernel. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.1.6:** Diebold-Mariano shared kernel. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.1.7:** Deflated Sharpe shared kernel. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.1.8:** Purged combinatorial cross-validation shared kernel. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.1.9:** Probability of backtest overfitting shared kernel. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.1.10:** Brier shared kernel. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.1.11:** Kalman shared kernel. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.1.12:** Ornstein-Uhlenbeck shared kernel. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.1.13:** ADF shared kernel. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.1.14:** Regime HMM shared kernel. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.1:** Generic caller migration rollup. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.1.1:** Rolling z-score generic caller migration. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.1.2:** EWMA generic caller migration. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.1.3:** Information coefficient generic caller migration. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.1.4:** Information ratio generic caller migration. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.1.5:** Effective sample size generic caller migration. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.1.6:** Diebold-Mariano generic caller migration. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.1.7:** Deflated Sharpe generic caller migration. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.1.8:** Purged combinatorial cross-validation generic caller migration. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.1.9:** Probability of backtest overfitting generic caller migration. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.1.10:** Brier generic caller migration. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.1.11:** Kalman generic caller migration. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.1.12:** Ornstein-Uhlenbeck generic caller migration. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.1.13:** ADF generic caller migration. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.1.14:** Regime HMM generic caller migration. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.2:** Finance alias compatibility rollup. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.2.1:** Rolling z-score finance alias compatibility. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.2.2:** EWMA finance alias compatibility. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.2.3:** Information coefficient finance alias compatibility. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.2.4:** Information ratio finance alias compatibility. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.2.5:** Effective sample size finance alias compatibility. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.2.6:** Diebold-Mariano finance alias compatibility. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.2.7:** Deflated Sharpe finance alias compatibility. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.2.8:** Purged combinatorial cross-validation finance alias compatibility. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.2.9:** Probability of backtest overfitting finance alias compatibility. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.2.10:** Brier finance alias compatibility. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.2.11:** Kalman finance alias compatibility. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.2.12:** Ornstein-Uhlenbeck finance alias compatibility. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.2.13:** ADF finance alias compatibility. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.2.14:** Regime HMM finance alias compatibility. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.3:** UQL time-series exposure rollup. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.3.1:** Rolling z-score uql time-series exposure. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.3.2:** EWMA uql time-series exposure. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.3.3:** Information coefficient uql time-series exposure. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.3.4:** Information ratio uql time-series exposure. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.3.5:** Effective sample size uql time-series exposure. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.3.6:** Diebold-Mariano uql time-series exposure. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.3.7:** Deflated Sharpe uql time-series exposure. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.3.8:** Purged combinatorial cross-validation uql time-series exposure. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.3.9:** Probability of backtest overfitting uql time-series exposure. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.3.10:** Brier uql time-series exposure. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.3.11:** Kalman uql time-series exposure. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.3.12:** Ornstein-Uhlenbeck uql time-series exposure. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.3.13:** ADF uql time-series exposure. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.3.14:** Regime HMM uql time-series exposure. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.2.4:** Complete caller coverage check. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.6.1:** Shared causal linear recurrence. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.6.2:** Volatility regime profile. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.6.3:** Mean-reversion strength profile. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.6.4:** Hedge ratio profile. Execute its positive and negative proof from test-spec.md.
- [ ] **EG-FINANCE-PRIMITIVES-R002.6.5:** Time-varying factor sensitivity profile. Execute its positive and negative proof from test-spec.md.

<!-- spec-definition-patch:owner-batch-20261012-finance-primitives -->
## Reviewed residual definition tasks — owner batch 2026-10-12

- [ ] **EG-FINANCE-PRIMITIVES-R015.1 (EG:43):** Define and wire the EG-owned residual for `EG-FINANCE-PRIMITIVES-R015` using the reviewed producer/consumer route; preserve covered clauses `EG-DECISION-ENGINE-R019`, `EG-DURABLE-KERNEL-R057`, `EG-FINANCE-PRIMITIVES-R015`.
- [ ] **EG-FINANCE-PRIMITIVES-R015.1 acceptance:** Deliver the positive, negative, refusal, boundary, or readback cases explicitly required by: Define order-snapshot lifecycle and promotion admission using existing sealed-record and transactional authorities; add immutable-history, derivation-link and veto/no-effects acceptance. Keep provider order submission in SDK governed writeback and host confirmation in GraphOS.
- [ ] **EG-FINANCE-PRIMITIVES-R015.1 status:** Keep SPECIFIED until separately reviewed implementation and served evidence are available.

- [ ] **EG-FINANCE-PRIMITIVES-R002.11 (EG:67):** Define and wire the EG-owned residual for `EG-FINANCE-PRIMITIVES-R002` using the reviewed producer/consumer route; preserve covered clauses `EG-DECISION-ENGINE-R103`, `EG-FINANCE-PRIMITIVES-R002`, `EG-FINANCE-PRIMITIVES-R010`, `EG-FINANCE-PRIMITIVES-R002.1.14`, `EG-FINANCE-PRIMITIVES-R002.6.2`, `EG-FINANCE-PRIMITIVES-R009.1`.
- [ ] **EG-FINANCE-PRIMITIVES-R002.11 acceptance:** Deliver the positive, negative, refusal, boundary, or readback cases explicitly required by: Extend existing finance owner with transition/state horizon probabilities and their sizing consumer/calibration binding. Use seeded transition-matrix oracle, horizon sums, low-confidence refusal and served sizing dependency tests; preserve R002 original kernels.
- [ ] **EG-FINANCE-PRIMITIVES-R002.11 status:** Keep SPECIFIED until separately reviewed implementation and served evidence are available.
<!-- /spec-definition-patch -->
