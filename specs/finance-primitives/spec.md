# EG-FINANCE-PRIMITIVES — Finance primitives: accounts, strategies, risk and evidence

**Owner:** epistemic-graph (EG) · **Program:** finance asset manager

## Outcome and boundary

EG is the authority for typed finance facts, deterministic portfolio calculations, strategy evaluation, backtest evidence, and informational recommendations. A client can import dated activities, reconstruct a portfolio at any past knowledge time, compare strategies, and inspect every displayed number's source and as-of time. The engine emits proposals and analysis, never an executable order or an approval lease. Connectors own external acquisition; the serving layer owns schedules and delivery; a browser owns presentation. Each can use the public EG contracts specified here without access to any other documentation.

Existing `finance-v1` already models `FinancialInstrument`, `Listing`, `Venue`, `BarSeries`, `IndicatorSpec`, `SignalState`, `TrendFlip`, `BacktestRun` and `AnalysisSnapshot`. Existing `FinanceMarket` operations resolve revisions, compute indicators, seal backtests and snapshots, and publish `finance.flip` events. Extend these types and operations; keep bars in the time-series store rather than copying bars into graph nodes. Reuse the engine's sealed-record guard and graph authorization for persisted account and strategy records.

The `BacktestRun` content digest must be canonical across supported hosts: normalize serialization, floating-point and timestamp inputs before hashing, and compare independent builds on two architectures. The SuperTrend/ATR reference companion in Pine must produce the same signal transitions as EG from the same checked-in bar fixture and parameter version. Fold finance-specific classes into the one `finance-v1` ontology and remove duplicate class definitions; this obligation is tracked as EG-FINANCE-PRIMITIVES-R001.

## Evidence and obligations

The generator owns delivery state in `status.json`.
Record exact commits, commands, hosted runs, dates and scenario outcomes as evidence.
Existing components establish reuse opportunities, not completion of these obligations.

| ID | EG obligation |
|---|---|
| EG-FINANCE-PRIMITIVES-R001 | Cross-host deterministic `BacktestRun` digest, Pine companion parity for SuperTrend/ATR, and one canonical `finance-v1` class authority. |
| EG-FINANCE-PRIMITIVES-R004 | Extend ontology and validated portfolio record types. |
| EG-FINANCE-PRIMITIVES-R005 | Fixed-point lot accounting and performance kernels. |
| EG-FINANCE-PRIMITIVES-R006 | Point-in-time corporate actions and market sessions. |
| EG-FINANCE-PRIMITIVES-R007 | Versioned typed strategy evaluator and proposal records. |
| EG-FINANCE-PRIMITIVES-R008 | Strategy comparisons through sealed backtests. |
| EG-FINANCE-PRIMITIVES-R009 | Calibrated, abstaining informational recommendations. |
| EG-FINANCE-PRIMITIVES-R010 | Leverage risk models and live-action refusal boundary. |
| EG-FINANCE-PRIMITIVES-R011 | Finance event facts for downstream delivery. |
| EG-FINANCE-PRIMITIVES-R012 | Optional one-way portfolio import from an attached source. |
| EG-FINANCE-PRIMITIVES-R013 | Golden accounting/risk fixtures and provenance invariants. |
| EG-FINANCE-PRIMITIVES-R002 | Shared numeric kernel ownership, thin compatibility aliases, UQL and method-count reconciliation. |
| EG-FINANCE-PRIMITIVES-R003 | Split-derived PBO and exact-median convention in sealed backtests. |

## Normative requirements

### F1. Identity and records (EG-FINANCE-PRIMITIVES-R004)

1. Asset class is a validated enum: `equity`, `etf`, `fund`, `index`, `fx_pair`, `commodity_spot`, `commodity_future`, `crypto`, `real_estate`, `cash`; preserve mappings from current `stock`, `forex`, and `commodity` values during migration. A tradable identity remains `(instrument, venue, quote instrument, listing type)`, not a ticker alone. An index and manually valued real estate are observational holdings; they are not order targets.
2. `Account` has tenant, owner, base currency, account kind, source identity and policy scope. `Activity` has stable source ID, account, instrument/listing where relevant, type (`buy`, `sell`, `dividend`, `split`, `fee`, `transfer`, `interest`, `deposit`, `withdrawal`), effective time, known-at time, quantity, currency amounts, source digest and revision. Its identity is `(tenant, account, source, source activity ID, revision)`. Same revision plus different content is a conflict. Corrections append a higher revision.
3. `Lot` is derived from activities and records acquisition time, remaining quantity, native-currency cost and source activity. `Position` is a dated projection with quantity, valuation source, as-of time and currency; neither is an independently writable accounting authority. `PortfolioGroup` associates holdings or watchlist items with named user views; group membership never changes accounting.
4. `StrategySpec`, `StrategyRun`, `LeverageTerms`, corporate actions and valuation records have schema versions, tenant/visibility, source, effective time and known-at time. Unknown schema versions fail closed. OWL classes and SHACL shapes enforce required links, cardinality and controlled vocabularies; public method contracts carry the same invariants.

### F2. Deterministic accounting (EG-FINANCE-PRIMITIVES-R005)

1. Money, quantities, rates and FX conversions use checked integer/decimal arithmetic with explicit scale, rounding mode and currency minor-unit metadata. No binary float can enter a persisted monetary result. Overflow and missing scale are typed errors. Replaying identical ordered inputs yields identical bytes.
2. On an as-of query, include only activity/corporate-action revisions whose `known_at <= as_of`, then order by effective time, source ID and revision. Support FIFO, LIFO, specific-lot and average-cost elections; record the election per account and tax context. A sell that exceeds available lots, invalid specific-lot allocation, duplicate event, unsupported action or unresolved split fails with a typed reason rather than silently creating inventory.
3. Realized P&L derives from disposed cost and proceeds less allocated fees. Unrealized P&L uses a sourced valuation at its own as-of time. Dividends, transfers and corporate actions preserve economic continuity; splits rescale quantity and per-unit basis without changing total basis. Trade-date FX is used for booked cash flows, valuation-date FX for current base-currency value, and the exact rate record is cited.
4. Time-weighted return geometrically chains subperiod returns separated by external cash flows. Money-weighted return solves dated cash flows as XIRR with a bounded deterministic root search and reports no unique root where appropriate. Benchmark comparison uses the same time window and explicit currency/return basis. Missing prices, stale valuations or unknown corporate actions yield partial results with coverage metadata, never a fabricated zero.

### F3. Temporal reference data (EG-FINANCE-PRIMITIVES-R006)

Corporate actions (`split`, `dividend`, `symbol_change`, `delisting`) identify the affected immutable listing/instrument, effective date, announcement/known-at time, source and revision. A renamed ticker remains linked to the same economic identity unless a source explicitly identifies a successor. Calendars are versioned data containing regular, pre-market and post-market spans, holidays, early closes and time-zone offset spans. Every price/valuation result declares `session` (`regular`, `pre`, `post`, `closed`, `unknown`), source revision and as-of/known-at time. Do not query the host clock or ambient time-zone database in replay kernels.

### F4. Strategies and comparisons (EG-FINANCE-PRIMITIVES-R007, EG-FINANCE-PRIMITIVES-R008)

`StrategySpec` is immutable by `(id, version, canonical digest)` and binds universe, currency, calendar, decision cadence, parameters, cost model and risk policy. Evaluators consume point-in-time bars/positions and produce `ProposedAction` with target, direction, size/rationale, input digests, decision time and expiry. No order transport, broker credentials or approval token are present in the result. Built-ins: DCA by fixed cash or shares and value-averaging target; existing SuperTrend flips and 50/200 moving-average trend; calendar and threshold rebalance. A missed cadence emits `MISSED` evidence, and retry idempotency prevents double proposals.

Compare DCA, lump sum and trend on the same point-in-time universe, cash flows, calendar, costs and valuation horizon. Reuse `BacktestRun` sealing and no-look-ahead validation. The engine computes purged combinatorial cross-validation, deflated Sharpe and probability of backtest overfitting from the *same* underlying split results, not caller-supplied headline metrics. Reject short histories, overlapping train/test information, missing transaction costs or incompatible runs. A comparison includes run digests, assumptions and uncertainty; it makes no universal performance claim.

### F5. Recommendation and risk (EG-FINANCE-PRIMITIVES-R009, EG-FINANCE-PRIMITIVES-R010)

For each holding/watchlist item and horizon, a recommendation is `accumulate`, `hold`, `de_risk` or `abstain`. A non-abstaining result cites the strategy ID/version, sealed backtest/evaluation record, calibration cohort, data revision, price source and as-of time. Insufficient history, warmup, drift, ambiguity, stale price, unsupported jurisdiction or missing risk terms must abstain with a typed reason. Scorecards compare prior recommendations to later outcomes using an immutable evaluation window. Seal informational output as `AnalysisSnapshot`; no output authorizes an order.

`LeverageTerms` identifies instrument, product type (`margin`, `future`, `daily_reset_fund`, `fx`), jurisdiction/policy version, multiplier, tick, currency, initial and maintenance margin, financing/fee schedule, expiry/roll and liquidation convention as applicable. Simulate margin calls, liquidation levels, sizing and risk-of-ruin under explicit scenarios. Daily-reset funds compound daily returns, not a horizon return multiplied by leverage. Unknown terms or unavailable product policy refuse any live-action eligibility. EG's default is paper analysis. A separate authorized execution system must enforce per-instrument policy, a valid approval lease, risk guards and authorized approver; EG cannot mint or bypass any of these.

### F6. Events and optional import (EG-FINANCE-PRIMITIVES-R011, EG-FINANCE-PRIMITIVES-R012)

Finance events use a versioned envelope `(tenant, event_id, topic, subject, occurrence_time, known_at, source_digest, payload_version)`. `finance.flip` keeps its existing stable ID. Add `finance.price`, `finance.dca_due` and `finance.margin_threshold` facts as needed; replay and outbox publication are idempotent. Delivery channels and scheduling remain consumers. An event never authorizes a trade.

An optional external portfolio source can attach read-only through EG's typed source registry. A one-way mapper converts source activities into F1 records with original source key, source revision and digest. Re-import is idempotent, deletions become explicit tombstone/correction records, and source schema drift rejects or quarantines records. The external database retains its own schema and migrations. EG never writes into it and never shares its storage ownership. This optional connector is independent of completing F1–F5.

### F7. Quality and provenance (EG-FINANCE-PRIMITIVES-R013)

Every externally displayed monetary, price, return, risk or recommendation field carries source ID/digest, economic as-of, known-at, session where market-derived, currency and precision. The engine must pass broker-statement, corporate-action, DST/session, futures-roll and daily-reset-fund golden cases. A trace can reconstruct each number from input revisions and formula version. CCCC, jscpd, Dupehound and KISS checks apply to changed code; no duplicate accounting or finance decision authority is introduced in another layer.

## Completion contract

### Shared evaluation kernels and split-derived overfit probability (EG-FINANCE-PRIMITIVES-R002, EG-FINANCE-PRIMITIVES-R003)

**EG-FINANCE-PRIMITIVES-R002.** Generic rolling z-score, EWMA, information coefficient/ratio, effective sample size, Diebold–Mariano, deflated Sharpe, purged combinatorial cross-validation, probability of backtest overfitting, Brier, Kalman, Ornstein–Uhlenbeck, ADF and regime HMM calculations have one domain-neutral owner in `eg-numeric` evaluation/calibration. Time-series UQL operators call those kernels through the existing series execution path. Finance methods may remain only as thin, documented compatibility aliases while public callers migrate to the typed generic surface; no copied implementation, silent shim or second precision/rounding convention is allowed. Reconcile generated method counts and client bindings from the canonical method inventory in one change. A regime derivation requiring an unbounded prefix refit must declare a budget or refuse with typed state-budget failure; it cannot report a bounded success it did not compute.

**EG-FINANCE-PRIMITIVES-R003.** A `BacktestRun` computes PBO from its own combinatorial symmetric cross-validation split matrix, not a separate caller-provided summary row array. For each split, select the highest in-sample candidate by a deterministic tie rule, rank its out-of-sample performance against all candidates, and count it as overfit only when strictly below the exact median; equality to median is not overfit. Record split count, selected candidate, rank and convention in verifiable evidence. A missing/degenerate matrix, unequal candidate dimensions, nonfinite values or mismatched reported split count refuses sealing. The same split matrix supplies related validation outputs so a headline PBO cannot contradict the sealed record.

Acceptance requires exact-head hosted checks and deterministic positive and negative fixtures.
It also requires schema generation, authorization, replay and the applicable quality gates in test-spec.md.
Baseline checks use disposable data without private credentials.
Optional external-source integration requires disposable fixtures.
Record deployment qualification separately from source evidence.

Requirement IDs are defined in [requirements.md](requirements.md); delivery state per ID is in `status.json`.


R002 also owns versioned causal state-space filtering, bounded innovation retuning and robust nonlinear approximation.
Its original kernel inventory and acceptance remain mandatory.
The plan defines all four state models, production attachment points and prerequisite contracts.
The test contract requires independent numerical oracles and real served positive and negative sequences.

R002.1, R002.2 and R002.6 retain their acceptance as rollups of kernel-specific and model-specific children.
The linear recurrence requires mutually uncorrelated process and measurement noises; unsupported correlation declarations refuse.

<!-- spec-definition-patch:owner-batch-20261012-finance-primitives -->
## Owner definition extensions — 2026-10-12

### `EG-FINANCE-PRIMITIVES-R015.1` — EG:43

**EG-owned lifecycle.** Define public typed methods `FinanceOrderProposal.StageV1` and `.PromoteV1`; `OrderProposalSnapshotV1` is a sealed, content-addressed owner record with tenant, proposal identity/revision, predecessor digest, decision identity/revision/digest, payload schema id/revision, canonical payload bytes and SHA-256 (hash stored bytes as-is and refuse if they are not canonical for their declared payload schema/version), policy id/revision/digest and evidence refs. Staging is create-only. Revision 1 has no predecessor; every subsequent revision names the previous digest and uses expected-head compare-and-set. A byte-identical retry is idempotent; conflicting content at an existing revision refuses. `PromoteV1` binds expected promoted head, candidate snapshot digest, required-gate manifest digest and gate receipts; it revalidates the staged payload hash, R019 decision verification/approval and the R010 risk/sizing policy (including per-instrument policy/designated approvers when applicable), and in one transaction writes the promoted revision, append-only `derivedFrom` history edge and `OrderAdmissionReceiptV1` binding proposal/revision/snapshot/decision/policy/gate-manifest/gate-receipt digests and transaction revision. Stale head, missing or mismatched evidence, unavailable or failing required gate, and hook veto refuse before any promotion pointer, history or receipt write. The result means admitted for host review only. `AnalysisSnapshot` stays informational-only; the operation never submits an order.

**Exact cross-owner hold.** Existing evidence does not name the SDK-accepted order payload schema/version, the complete admission-hook registry or its completeness rule, or GraphOS's authenticated-confirmation operation/receipt fields. EG can define and store the opaque schema-versioned payload and its digest, but must refuse promotion for host review until the admission set and GraphOS confirmation boundary are defined. SDK governed writeback cannot act until its payload and confirmation dependencies are reviewed. No live-order behavior or external effect is included.

Preserve `EG-DECISION-ENGINE-R019`, `EG-DURABLE-KERNEL-R057`, `EG-FINANCE-PRIMITIVES-R015`. Source record SHA-256 `e3ad9b1e3121eb3278c3e2b52ce948817e317db3de48d5886a624ffe2ba9be47`. Definition only; no implementation, GraphOS receipt or SDK effect is claimed.

### `EG-FINANCE-PRIMITIVES-R002.11` — EG:67

**Forecast contract.** Extend the existing shared regime-HMM kernel R002.1.14 through the typed finance-model route R002.9 with `RegimeForecastV1`. It accepts `{model_id, model_revision, source_series_id, source_revision, as_of, horizons, horizon_unit, calibration_policy_id, calibration_policy_revision, calibration_policy_digest, heldout_dataset_digest}` and resolves ordered state ids, posterior `p0` and transition matrix `T` only from that pinned HMM model/source revision; callers cannot substitute them. Its output binds model/source revisions, `as_of`, ordered state ids, `p0`, transition-matrix digest, horizons, forecast vectors and calibration evidence. For each requested `h`, return `p_h = p0 T^h`, preserving state order and numeric precision; all vectors and each row of `T` must be finite, nonnegative, and sum to one within the pinned model revision’s declared precision. Report `max_s(p_h[s])` as top-state probability. Each held-out row binds sample id, source series, origin, horizon, ordered state ids, predicted probability vector, realized state and target-observation time; require unique samples, matching model/state order and origin before target observation. Calibration uses those held-out realized states and categorical Brier per horizon, `Brier_h = (1/N_h) Σ_n Σ_s (p_{n,h,s} - 1[y_n=s])²`; `N_h` must meet `minimum_heldout_examples_by_horizon`. `calibration_evidence_by_horizon` returns horizon, dataset/model/state-order/scoring revisions, calibration-policy digest, sample count, score, applied thresholds and pass state. Bind the score to dataset digest, model revision, state ordering and scoring revision. Only a forecast passing the versioned per-horizon `max_brier_by_horizon`, `min_top_state_probability_by_horizon` and positive `minimum_heldout_examples_by_horizon` thresholds can become sizing-eligible. `R010` consumes the forecast digest with a `RegimeSizingPolicyV1` with `{policy_id, revision, instrument_id, target_weight_by_state, unit, fixed_point_scale, risk_cap}` using one R010 policy fixed-point unit/scale and applies `expected_target_weight_h = Σ_s p_h[s] × target_weight_s`; missing weights, risk-cap violation or failed calibration yields typed abstention/no sizing value. This is not the Kalman volatility classification or R103 directional flip confidence and cannot submit a live order.

**Exact policy hold.** No reviewed source sets values for `max_brier_by_horizon`, `min_top_state_probability_by_horizon`, `minimum_heldout_examples_by_horizon`, state-to-target weights or risk cap. The caller must provide all of these through a complete versioned calibration/sizing policy; unknown or absent values have no default and refuse sizing. Do not use R009.1 flip calibration for regime-state calibration.

Preserve `EG-DECISION-ENGINE-R103`, `EG-FINANCE-PRIMITIVES-R002`, `R010`, `R002.1.14`, `R002.6.2`, `R009.1`. Source record SHA-256 `037f3edf5fe38f55ac24d6e0cc7bd85f1efc60cadf7c97b66b44978613a7acb6`. Definition only; no implementation or served evidence is claimed.

<!-- /spec-definition-patch -->
