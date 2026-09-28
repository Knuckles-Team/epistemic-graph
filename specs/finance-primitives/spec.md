# EG-FINANCE-PRIMITIVES — Finance primitives: accounts, strategies, risk and evidence

**Owner:** epistemic-graph (EG) · **Program:** finance asset manager · **State:** proposed

## Outcome and boundary

EG is the authority for typed finance facts, deterministic portfolio calculations, strategy evaluation, backtest evidence, and informational recommendations. A client can import dated activities, reconstruct a portfolio at any past knowledge time, compare strategies, and inspect every displayed number's source and as-of time. The engine emits proposals and analysis, never an executable order or an approval lease. Connectors own external acquisition; the serving layer owns schedules and delivery; a browser owns presentation. Each can use the public EG contracts specified here without access to any other documentation.

Existing `finance-v1` already models `FinancialInstrument`, `Listing`, `Venue`, `BarSeries`, `IndicatorSpec`, `SignalState`, `TrendFlip`, `BacktestRun` and `AnalysisSnapshot`. Existing `FinanceMarket` operations resolve revisions, compute indicators, seal backtests and snapshots, and publish `finance.flip` events. Extend these types and operations; keep bars in the time-series store rather than copying bars into graph nodes. Reuse the engine's sealed-record guard and graph authorization for persisted account and strategy records.

The `BacktestRun` content digest must be canonical across supported hosts: normalize serialization, floating-point and timestamp inputs before hashing, and compare independent builds on two architectures. The SuperTrend/ATR reference companion in Pine must produce the same signal transitions as EG from the same checked-in bar fixture and parameter version. Fold finance-specific classes into the one `finance-v1` ontology and remove duplicate class definitions; this obligation is tracked as EH-517.

## State legend and evidence rule

Delivery and acceptance are independent. `WAITING` means no reviewed implementation is linked; `IN_PROGRESS` means a branch is under review; `SOURCE_LANDED` means the exact change is reachable from published `main`; `RETIRED` requires a recorded superseding decision. `NOT_RUN`, `FAILED`, `PARTIAL`, and `ACCEPTED` describe validation. A green branch check never implies source landing; a merged source never implies acceptance. Record an exact commit SHA, hosted run URL or reproducible local command, result, and date per ID before changing its state. The statuses below describe only this new extension; existing market core is a reuse dependency, not a claim that these extension IDs are complete.

| ID | Delivery | Acceptance | EG obligation |
|---|---|---|---|
| EH-517 | IN_PROGRESS | NOT_RUN | Cross-host deterministic `BacktestRun` digest, Pine companion parity for SuperTrend/ATR, and one canonical `finance-v1` class authority. |
| EH-698 | WAITING | NOT_RUN | Extend ontology and validated portfolio record types. |
| EH-699 | WAITING | NOT_RUN | Fixed-point lot accounting and performance kernels. |
| EH-700 | WAITING | NOT_RUN | Point-in-time corporate actions and market sessions. |
| EH-702 | WAITING | NOT_RUN | Versioned typed strategy evaluator and proposal records. |
| EH-703 | WAITING | NOT_RUN | Strategy comparisons through sealed backtests. |
| EH-704 | WAITING | NOT_RUN | Calibrated, abstaining informational recommendations. |
| EH-706 | WAITING | NOT_RUN | Leverage risk models and live-action refusal boundary. |
| EH-712 | WAITING | NOT_RUN | Finance event facts for downstream delivery. |
| EH-714 | WAITING | NOT_RUN | Optional one-way portfolio import from an attached source. |
| EH-715 | WAITING | NOT_RUN | Golden accounting/risk fixtures and provenance invariants. |

## Normative requirements

### F1. Identity and records (EH-698)

1. Asset class is a validated enum: `equity`, `etf`, `fund`, `index`, `fx_pair`, `commodity_spot`, `commodity_future`, `crypto`, `real_estate`, `cash`; preserve mappings from current `stock`, `forex`, and `commodity` values during migration. A tradable identity remains `(instrument, venue, quote instrument, listing type)`, not a ticker alone. An index and manually valued real estate are observational holdings; they are not order targets.
2. `Account` has tenant, owner, base currency, account kind, source identity and policy scope. `Activity` has stable source ID, account, instrument/listing where relevant, type (`buy`, `sell`, `dividend`, `split`, `fee`, `transfer`, `interest`, `deposit`, `withdrawal`), effective time, known-at time, quantity, currency amounts, source digest and revision. Its identity is `(tenant, account, source, source activity ID, revision)`. Same revision plus different content is a conflict. Corrections append a higher revision.
3. `Lot` is derived from activities and records acquisition time, remaining quantity, native-currency cost and source activity. `Position` is a dated projection with quantity, valuation source, as-of time and currency; neither is an independently writable accounting authority. `PortfolioGroup` associates holdings or watchlist items with named user views; group membership never changes accounting.
4. `StrategySpec`, `StrategyRun`, `LeverageTerms`, corporate actions and valuation records have schema versions, tenant/visibility, source, effective time and known-at time. Unknown schema versions fail closed. OWL classes and SHACL shapes enforce required links, cardinality and controlled vocabularies; public method contracts carry the same invariants.

### F2. Deterministic accounting (EH-699)

1. Money, quantities, rates and FX conversions use checked integer/decimal arithmetic with explicit scale, rounding mode and currency minor-unit metadata. No binary float can enter a persisted monetary result. Overflow and missing scale are typed errors. Replaying identical ordered inputs yields identical bytes.
2. On an as-of query, include only activity/corporate-action revisions whose `known_at <= as_of`, then order by effective time, source ID and revision. Support FIFO, LIFO, specific-lot and average-cost elections; record the election per account and tax context. A sell that exceeds available lots, invalid specific-lot allocation, duplicate event, unsupported action or unresolved split fails with a typed reason rather than silently creating inventory.
3. Realized P&L derives from disposed cost and proceeds less allocated fees. Unrealized P&L uses a sourced valuation at its own as-of time. Dividends, transfers and corporate actions preserve economic continuity; splits rescale quantity and per-unit basis without changing total basis. Trade-date FX is used for booked cash flows, valuation-date FX for current base-currency value, and the exact rate record is cited.
4. Time-weighted return geometrically chains subperiod returns separated by external cash flows. Money-weighted return solves dated cash flows as XIRR with a bounded deterministic root search and reports no unique root where appropriate. Benchmark comparison uses the same time window and explicit currency/return basis. Missing prices, stale valuations or unknown corporate actions yield partial results with coverage metadata, never a fabricated zero.

### F3. Temporal reference data (EH-700)

Corporate actions (`split`, `dividend`, `symbol_change`, `delisting`) identify the affected immutable listing/instrument, effective date, announcement/known-at time, source and revision. A renamed ticker remains linked to the same economic identity unless a source explicitly identifies a successor. Calendars are versioned data containing regular, pre-market and post-market spans, holidays, early closes and time-zone offset spans. Every price/valuation result declares `session` (`regular`, `pre`, `post`, `closed`, `unknown`), source revision and as-of/known-at time. Do not query the host clock or ambient time-zone database in replay kernels.

### F4. Strategies and comparisons (EH-702, EH-703)

`StrategySpec` is immutable by `(id, version, canonical digest)` and binds universe, currency, calendar, decision cadence, parameters, cost model and risk policy. Evaluators consume point-in-time bars/positions and produce `ProposedAction` with target, direction, size/rationale, input digests, decision time and expiry. No order transport, broker credentials or approval token are present in the result. Built-ins: DCA by fixed cash or shares and value-averaging target; existing SuperTrend flips and 50/200 moving-average trend; calendar and threshold rebalance. A missed cadence emits `MISSED` evidence, and retry idempotency prevents double proposals.

Compare DCA, lump sum and trend on the same point-in-time universe, cash flows, calendar, costs and valuation horizon. Reuse `BacktestRun` sealing and no-look-ahead validation. The engine computes purged combinatorial cross-validation, deflated Sharpe and probability of backtest overfitting from the *same* underlying split results, not caller-supplied headline metrics. Reject short histories, overlapping train/test information, missing transaction costs or incompatible runs. A comparison includes run digests, assumptions and uncertainty; it makes no universal performance claim.

### F5. Recommendation and risk (EH-704, EH-706)

For each holding/watchlist item and horizon, a recommendation is `accumulate`, `hold`, `de_risk` or `abstain`. A non-abstaining result cites the strategy ID/version, sealed backtest/evaluation record, calibration cohort, data revision, price source and as-of time. Insufficient history, warmup, drift, ambiguity, stale price, unsupported jurisdiction or missing risk terms must abstain with a typed reason. Scorecards compare prior recommendations to later outcomes using an immutable evaluation window. Seal informational output as `AnalysisSnapshot`; no output authorizes an order.

`LeverageTerms` identifies instrument, product type (`margin`, `future`, `daily_reset_fund`, `fx`), jurisdiction/policy version, multiplier, tick, currency, initial and maintenance margin, financing/fee schedule, expiry/roll and liquidation convention as applicable. Simulate margin calls, liquidation levels, sizing and risk-of-ruin under explicit scenarios. Daily-reset funds compound daily returns, not a horizon return multiplied by leverage. Unknown terms or unavailable product policy refuse any live-action eligibility. EG's default is paper analysis. A separate authorized execution system must enforce per-instrument policy, a valid approval lease, risk guards and authorized approver; EG cannot mint or bypass any of these.

### F6. Events and optional import (EH-712, EH-714)

Finance events use a versioned envelope `(tenant, event_id, topic, subject, occurrence_time, known_at, source_digest, payload_version)`. `finance.flip` keeps its existing stable ID. Add `finance.price`, `finance.dca_due` and `finance.margin_threshold` facts as needed; replay and outbox publication are idempotent. Delivery channels and scheduling remain consumers. An event never authorizes a trade.

An optional external portfolio source can attach read-only through EG's typed source registry. A one-way mapper converts source activities into F1 records with original source key, source revision and digest; re-import is idempotent, deletions become explicit tombstone/correction records, and source schema drift rejects or quarantines records. The external database retains its own schema and migrations. EG never writes into it and never shares its storage ownership. This optional connector is independent of completing F1–F5.

### F7. Quality and provenance (EH-715)

Every externally displayed monetary, price, return, risk or recommendation field carries source ID/digest, economic as-of, known-at, session where market-derived, currency and precision. The engine must pass broker-statement, corporate-action, DST/session, futures-roll and daily-reset-fund golden cases. A trace can reconstruct each number from input revisions and formula version. CCCC, jscpd, Dupehound and KISS checks apply to changed code; no duplicate accounting or finance decision authority is introduced in another layer.

## Completion contract

An ID becomes `SOURCE_LANDED` only at a published-main commit containing its contract, implementation and tests. It becomes `ACCEPTED` only when exact-head hosted CI, deterministic positive/negative fixtures, schema/contract generation, authorization and replay checks, and the quality gates in `test-spec.md` pass. Baseline PR checks must provision disposable data and run without a live deployment or private credentials. Optional external-source integration is tested against a disposable fixture. Record deployment qualification separately from source acceptance.
