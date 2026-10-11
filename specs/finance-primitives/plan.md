# Delivery plan

## Sequence

1. **Contract foundation (EG-FINANCE-PRIMITIVES-R004, EG-FINANCE-PRIMITIVES-R006).** Add typed versioned records, ontology and SHACL shapes, temporal revision resolution and session/calendar semantics. Review migration of existing asset-class values. Generate clients and prove old `FinanceMarket` decoding.
2. **Accounting authority (EG-FINANCE-PRIMITIVES-R005, EG-FINANCE-PRIMITIVES-R013).** Implement one fixed-point event-to-lot projection, valuation join and return kernels. Add independent vectors and property tests before exposing a public projection method. Reconcile imported activities against statement fixtures.
3. **Strategy/evidence authority (EG-FINANCE-PRIMITIVES-R007, EG-FINANCE-PRIMITIVES-R008).** Implement immutable strategy specs and deterministic proposal evaluation. Feed run provenance into the existing sealed `BacktestRun`; calculate validation from the same split matrix and compare common horizons/costs.
4. **Advisory and risk (EG-FINANCE-PRIMITIVES-R009, EG-FINANCE-PRIMITIVES-R010).** Add calibrated abstention and scorecards, seal `AnalysisSnapshot`, and implement leverage scenarios with explicit product/policy terms. Audit that no result acts as an order authorization.
5. **Events and optional attached source (EG-FINANCE-PRIMITIVES-R011, EG-FINANCE-PRIMITIVES-R012).** Extend the finance event family with stable IDs and atomic outbox state. Qualify the optional read-only importer against disposable fixtures after account identity and revision contracts are stable.
6. **Final qualification.** Run the matrix in `test-spec.md` at an exact main head and add per-ID evidence. Publish a new contract version only after generated clients and compatibility tests are green.

## Decisions to resolve before implementation

| Decision | Required resolution and evidence |
|---|---|
| Accounting policy | State whether average cost is available for each account/tax context. Persist the election and prevent silent retrospective changes. |
| Monetary primitive | Reuse an existing checked decimal type if its scale and rounding semantics satisfy F2. Otherwise add one in the finance compute boundary. |
| Price gaps | Decide which results become partial versus refused; never substitute another source, session or timeframe silently. |
| Calendar data | Version offset spans and sessions as source facts so replay does not depend on the host time-zone database. |
| Backtest statistics | Specify one split-result matrix contract shared by purged CV, deflated Sharpe and PBO. Reject headline metrics that cannot be recomputed. |
| Leverage policies | Keep jurisdiction and provider-specific availability in versioned policy facts; do not hard-code an assumed live trading permission. |

## Integration contracts

External connectors submit source events and revisions through the typed import boundary. The serving layer schedules recurring DCA intentions and consumes outbox facts. It cannot change EG calculations. The browser reads positions, comparisons, risk and snapshots with provenance and renders partial/abstain states. The execution system receives proposals only through its separate governed approval path. These interfaces are complete at the behavioral level here. Companion repositories may add their own implementation details without changing EG's authority.

## Rollout

Ship read and replay contracts before imports. Add tenant-scoped migrations with a dry-run reconciliation report; preserve old revisions. Gate any projection head switch on statement/vector agreement and authorization tests. Use a schema/contract version bump for incompatible fields. If a stage fails, retain the prior read path and do not publish a new head. A rollback never rewrites activity history.

## R013 fixture completion sequence

R013.1 validates only the figure shape and provenance refusal gate. Preserve its executed unit proof without treating it as independent accounting/risk fixture acceptance. R013.2.1 currently exercises the broker-statement comparator with synthetic values; independent broker-statement dataset qualification remains necessary for the complete fixture family.

Deliver R013.2.2 (corporate actions), R013.2.3 (DST/session boundaries), R013.2.4 (daily-reset ETF decay) and R013.2.5 (futures rolls) as separate, still-open fixture slices. Each first checks in an independently published, redistributable reference and a manifest with source URL, publication/version, content digest, as-of time, session, and applicable currency/scale/rounding assumptions. Then compare the existing Rust authority's output with independently transcribed expected values, execute its negative/provenance cases, and record exact-head evidence. Do not introduce a second accounting implementation or derive expected values from the implementation under test. Only a fully qualified fixture family can close R013/R013.2; a generic comparator does not close the missing children.

## Architecture

### Ownership and reuse

EG owns the `finance-v1` OWL/SHACL model, deterministic Rust calculations, typed protocol, sealed evidence, graph persistence and finance outbox. The existing market implementation is the seam:

| Existing component | Reuse for this extension |
|---|---|
| `crates/eg-core/ontology/finance-v1.ttl` and `.shapes.ttl` | Add account, activity, strategy, reference and risk classes under existing finance IRIs and validation. |
| `crates/eg-compute/src/finance/market/` | Use bar revision resolution, calendars, indicators, event IDs, `BacktestRun` and `AnalysisSnapshot`; do not fork market-history logic. |
| `crates/eg-compute/src/finance/` | Add fixed-point `accounting`, `strategy`, `risk` modules behind the existing compute crate. Reuse existing quantitative validation kernels. |
| `crates/eg-types/src/compute_result/market/` | Extend versioned request/result contracts; regenerate language clients from canonical Rust contracts. |
| `src/server/handlers/finance/` | Dispatch new FinanceMarket suboperations or a cohesive typed finance operation family; keep public method authorization and result mapping centralized. |
| EG graph store, sealed-record guard and outbox | Persist immutable source facts and sealed results through the owner transaction. Prohibit generic-node overwrites of sealed evidence. |

`FinancialInstrument` and `Listing` remain the shared keys. Avoid a second "portfolio database" or a copy of historical bars. A read model may cache a `Position` by `(tenant, account, as_of, input_digest, accounting_policy_version)`; it is discardable and recomputed after a revised activity, price or action. The account activity journal is authoritative for holdings. All keys include tenant and authorization scope; a cache cannot broaden visibility.

### Data model

```text
FinancialInstrument <- Listing -> Venue
        |                 |         |
     asset class        BarSeries  CalendarRevision
        |                 |              |
        +---- Activity ---+---- PriceRevision
                 |                    |
              Account ---- Lot* ---- PositionProjection
                 |                    |
           PortfolioGroup      AnalysisSnapshot

StrategySpec --(point-in-time inputs)--> StrategyRun --> ProposedAction
       |                                |
       +--------------------------> BacktestRun --> Recommendation
LeverageTerms -----------------------> RiskScenario --/
```

Persist an activity as an append-only, versioned source record. Its effective time answers *when the economic event happened*; `known_at` answers *when EG could have known it*. A corrected record supersedes a prior version while replay at an earlier `known_at` sees the prior version. Use a canonical payload digest with domain separation. A source file digest alone is insufficient to distinguish rows: include source row ID and revision. Validate SHACL and the typed request before write; commit record, projection invalidation and outbox in one owner transaction.

Use a `DecimalValue { mantissa: i128, scale: u8 }` style boundary or existing fixed-point primitive if it provides the same checked semantics. Specify each formula's intermediate scale, rounding and overflow behavior in code contracts and golden vectors. Never use `f32`/`f64` for monetary persistence or equality. Statistical metrics may use controlled numeric kernels, but the monetary inputs and outputs remain fixed-point.

### Operations

| Operation | Input | Output/refusal |
|---|---|---|
| `FinanceAccount.AppendActivity` | account, source/revision, activity and expected account head | committed digest/new head; conflict, invalid currency/quantity, unauthorized account |
| `FinanceAccount.Project` | account or group, knowledge as-of, valuation as-of, basis policy, base currency | positions, lots, cash, P&L, returns, provenance and coverage. Typed missing-data result |
| `FinanceReference.Upsert` | source revision, action/calendar/price, expected head | validated revision; conflict, bad chronology, unsupported action |
| `FinanceStrategy.Evaluate` | spec digest, point-in-time universe and read snapshot | proposed actions and deterministic input digest; abstain/refusal |
| `FinanceStrategy.Compare` | comparable strategy runs, common horizon/cost model | sealed `BacktestRun` set and comparison record; insufficient validation |
| `FinanceAnalysis.Recommend` | holding/watchlist key, horizon, strategy/evidence digests | sealed informational snapshot or typed abstention |
| `FinanceRisk.Simulate` | position, leverage terms, scenario grid, policy version | margin/liquidation/risk distribution, assumptions; unknown term/policy refusal |

Names are design targets, not a claim that they exist today. Keep wire variants and generated clients in sync. Requests must carry an explicit `as_of` and an expected head or read snapshot where mutation/replay consistency matters. Result envelopes include protocol version, input digest, source revisions, calculation version, currency and session. An old client gets a typed unsupported-version error instead of silently dropping fields.

### Calculation sequence

1. Resolve caller visibility and tenant before any finance data read or count. Resolve temporal revisions at `known_at <= as_of` and pin a read snapshot/input digest.
2. Normalize activities and actions into an ordered immutable event stream. Validate balanced transfers and source provenance; apply splits and lot allocations with a selected accounting policy.
3. Build cash, lots and positions; join valuations and FX at their specified times. Calculate P&L and returns only over covered periods, attaching missing-data reasons.
4. Evaluate strategies over the same point-in-time inputs; seal runs and compare only compatible cohorts. Calibrate a recommendation or abstain; seal its `AnalysisSnapshot`.
5. Atomically append event facts when thresholds or state transitions occur. Downstream schedulers and notification adapters use stable event IDs and their own durable delivery state.

### Trust and execution boundary

The finance compute path is deterministic and side-effect free except explicitly governed storage/outbox writes. A recommendation and `ProposedAction` are data. The execution adapter must separately obtain authorization, risk approval and a policy-valid lease. The EG protocol contains no generic `execute` conversion for proposed actions. Broker credentials are never accepted by these methods. External-source adapters use registry-scoped read capability and convert into canonical activities. They never receive EG store write credentials through a finance read response.

### Migration and compatibility

Introduce v2 asset-class values with explicit aliases from existing values, then dual-read old records during a bounded migration. Version SHACL, contract and persisted payload together; do not reinterpret prior digests. Backfill account activities from trusted imports only after dedupe, reconcile derived balances against independent statements, and publish the new head only when a tenant's migration validates. Retain prior activity revisions for as-of replay. Existing `FinanceMarket` bars, flip IDs and analysis snapshot versions must keep decoding throughout migration.
## EG-FINANCE-PRIMITIVES-R002 and EG-FINANCE-PRIMITIVES-R003 kernel reuse

`crates/eg-numeric/src/evaluation/` is the shared numerical authority; `crates/eg-compute/src/finance/quant/validation.rs` and finance wire methods are callers. The UQL/series layer uses the same kernels through `eg-plan` and `eg-tsdb` without finance-specific copies. `crates/eg-compute/src/finance/market/backtest_run.rs` seals and verifies the finance record from its draft split matrix, while `eg-types` owns the versioned public record fields. If the generic `EvaluationRun` record is extended, share seal/no-look-ahead code with the decision replay path rather than diverging finance and decision validations. Generated clients derive from Rust descriptors; changing a method requires regenerated count, schema and binding checks.
