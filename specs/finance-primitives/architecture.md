# Architecture and interface design

## Ownership and reuse

EG owns the `finance-v1` OWL/SHACL model, deterministic Rust calculations, typed protocol, sealed evidence, graph persistence and finance outbox. The existing market implementation is the seam:

| Existing component | Reuse for this extension |
|---|---|
| `crates/eg-core/ontology/finance-v1.ttl` and `.shapes.ttl` | Add account, activity, strategy, reference and risk classes under existing finance IRIs and validation. |
| `crates/eg-compute/src/finance/market/` | Use bar revision resolution, calendars, indicators, event IDs, `BacktestRun` and `AnalysisSnapshot`; do not fork market-history logic. |
| `crates/eg-compute/src/finance/` | Add fixed-point `accounting`, `strategy`, `risk` modules behind the existing compute crate. Reuse existing quantitative validation kernels. |
| `crates/eg-types/src/compute_result/market/` | Extend versioned request/result contracts; regenerate language clients from canonical Rust contracts. |
| `src/server/handlers/finance/` | Dispatch new FinanceMarket suboperations or a cohesive typed finance operation family; keep public method authorization and result mapping centralized. |
| EG graph store, sealed-record guard and outbox | Persist immutable source facts and sealed results through the owner transaction; prohibit generic-node overwrites of sealed evidence. |

`FinancialInstrument` and `Listing` remain the shared keys. Avoid a second "portfolio database" or a copy of historical bars. A read model may cache a `Position` by `(tenant, account, as_of, input_digest, accounting_policy_version)`; it is discardable and recomputed after a revised activity, price or action. The account activity journal is authoritative for holdings. All keys include tenant and authorization scope; a cache cannot broaden visibility.

## Data model

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

## Operations

| Operation | Input | Output/refusal |
|---|---|---|
| `FinanceAccount.AppendActivity` | account, source/revision, activity and expected account head | committed digest/new head; conflict, invalid currency/quantity, unauthorized account |
| `FinanceAccount.Project` | account or group, knowledge as-of, valuation as-of, basis policy, base currency | positions, lots, cash, P&L, returns, provenance and coverage; typed missing-data result |
| `FinanceReference.Upsert` | source revision, action/calendar/price, expected head | validated revision; conflict, bad chronology, unsupported action |
| `FinanceStrategy.Evaluate` | spec digest, point-in-time universe and read snapshot | proposed actions and deterministic input digest; abstain/refusal |
| `FinanceStrategy.Compare` | comparable strategy runs, common horizon/cost model | sealed `BacktestRun` set and comparison record; insufficient validation |
| `FinanceAnalysis.Recommend` | holding/watchlist key, horizon, strategy/evidence digests | sealed informational snapshot or typed abstention |
| `FinanceRisk.Simulate` | position, leverage terms, scenario grid, policy version | margin/liquidation/risk distribution, assumptions; unknown term/policy refusal |

Names are design targets, not a claim that they exist today. Keep wire variants and generated clients in sync. Requests must carry an explicit `as_of` and an expected head or read snapshot where mutation/replay consistency matters. Result envelopes include protocol version, input digest, source revisions, calculation version, currency and session. An old client gets a typed unsupported-version error instead of silently dropping fields.

## Calculation sequence

1. Resolve caller visibility and tenant before any finance data read or count. Resolve temporal revisions at `known_at <= as_of` and pin a read snapshot/input digest.
2. Normalize activities and actions into an ordered immutable event stream. Validate balanced transfers and source provenance; apply splits and lot allocations with a selected accounting policy.
3. Build cash, lots and positions; join valuations and FX at their specified times. Calculate P&L and returns only over covered periods, attaching missing-data reasons.
4. Evaluate strategies over the same point-in-time inputs; seal runs and compare only compatible cohorts. Calibrate a recommendation or abstain; seal its `AnalysisSnapshot`.
5. Atomically append event facts when thresholds or state transitions occur; downstream schedulers and notification adapters use stable event IDs and their own durable delivery state.

## Trust and execution boundary

The finance compute path is deterministic and side-effect free except explicitly governed storage/outbox writes. A recommendation and `ProposedAction` are data. The execution adapter must separately obtain authorization, risk approval and a policy-valid lease. The EG protocol contains no generic `execute` conversion for proposed actions. Broker credentials are never accepted by these methods. External-source adapters use registry-scoped read capability and convert into canonical activities; they never receive EG store write credentials through a finance read response.

## Migration and compatibility

Introduce v2 asset-class values with explicit aliases from existing values, then dual-read old records during a bounded migration. Version SHACL, contract and persisted payload together; do not reinterpret prior digests. Backfill account activities from trusted imports only after dedupe, reconcile derived balances against independent statements, and publish the new head only when a tenant's migration validates. Retain prior activity revisions for as-of replay. Existing `FinanceMarket` bars, flip IDs and analysis snapshot versions must keep decoding throughout migration.
