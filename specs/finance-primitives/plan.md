# Delivery plan

## 2026-10-11: reconcile the composed finance corpus census

R004.2.1 and R004.2.2 added 95 triples to the existing finance module:
45 for Account/Position and 50 for Activity/Lot. Update the existing composed
corpus regression to include those declarations. Preserve every other census
assertion. The four new class links raise the named subclass count by four.
The asset-class comment migration replaces one triple without a net increase.
The shipped ontology and its admission policy do not change in this repair.

The native facade run on 43423bcd reported 13,751 composed triples. An independent
RDFLib parse of all 36 registered documents confirms that total. It also confirms
13,565 semantic axioms, 462 named subclass links, and unchanged header/import/property counts.
This cross-parser check is supporting evidence; the changed native census test
still requires execution. It does not establish the remaining served finance work.

The existing native finance tests passed at that same immutable head. Bind the
unchanged catalog placement, valid-record, and malformed-record tests to R004.2.1
and R004.2.2. They exercise Account, Position, Activity, and Lot declarations and
their SHACL records. The failure fixtures cover missing links and invalid vocabulary.
Evidence: [facade job 114434983311](https://github.com/Knuckles-Team/epistemic-graph/actions/runs/38127544759/job/114434983311).
These individual passes do not make the failed overall facade job successful.

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

## Amendments

### 2026-10-11: restore runtime acceptance children

Affected rows: `EG-FINANCE-PRIMITIVES-R004.3`, `EG-FINANCE-PRIMITIVES-R005.2`, and `EG-FINANCE-PRIMITIVES-R011`.
The former rollups counted only their typed children, despite their broader acceptance requirements.
The restored children retain the original parent acceptance and every existing verified-child binding.
The generator determines delivery states; this amendment provides no runtime evidence.

### 2026-10-11: correct R011.1 acceptance scope

Affected row: `EG-FINANCE-PRIMITIVES-R011.1`.
The former verification cell required outbox integration and idempotent delivery, despite the typed-envelope title, task and production-module scope.
The corrected cell requires valid-envelope admission, four field refusals and distinct schemas for four alert kinds.
The unchanged R011 parent retains the original publication and replay obligations. R011.2 and R011.3 retain their runtime acceptance children.
This correction changes acceptance prose; it neither changes test bindings nor assigns delivery state.

## Audit record

### 2026-10-11: finance runtime scope

Inspection base: `7df2685439e98ebc8eaf036279f7f25d8fb9fe33`.
`crates/eg-types/src/wire_finance.rs` defines the version gate. The inspected source contains no served caller.
`crates/eg-compute/src/finance/lot_accounting.rs` supports FIFO, LIFO and specific-lot elections.
Its activity representation has buy/sell quantities, without transfer or split operations.
`crates/eg-types/src/finance_event_envelope.rs` explicitly defers transactional append and topic wiring.
The existing mutation compiler and transaction outbox provide the intended integration authority.
These findings establish unverified runtime acceptance; missing children alone do not prove absent implementation elsewhere.
The intended handler module and test names are work targets, not existing evidence.
Each implementation must inspect existing served paths before extending them.
`gen_contract` remains the generator for contract and client outputs; generated files must not receive manual edits.

### 2026-10-11: R011.1 production-module execution

Tested checkout: `3c1968158ea2b5f0f88fc3fe41b3bd92d7ca2505`.
Source: `crates/eg-types/src/finance_event_envelope.rs`.
Source SHA-256: `456bf7b236da943a516fee246bed80bafb976512184c38104a6f0b6da0b21724`.
Rust compiler: `rustc 1.96.0 (ac68faa20 2026-05-25)`.
The direct module harness enabled `finance` and linked the cached native `serde` dependency.
All six existing tests passed; zero tests failed, skipped or remained filtered.
The harness compiled the unchanged production file, including its existing test module.
This evidence covers module acceptance only; it does not establish crate-wide or hosted runtime acceptance.

Reproduction command uses `RUSTC`, `DEPS` and `SERDE` for the compiler, cached dependency directory and cached serde library.
The cached serde library is `libserde-896ab768b6bd5b93.rlib`.

```sh
mkdir -p target
"$RUSTC" --edition=2021 --test --cfg 'feature="finance"' crates/eg-types/src/finance_event_envelope.rs -L dependency="$DEPS" --extern serde="$SERDE" -o target/finance-envelope-module-tests
target/finance-envelope-module-tests --nocapture
```

## 2026-10-11: preserve the unfinished schema rollup

The generator marks R004.2 complete from only its two existing children.
The ontology still lacks PortfolioGroup, StrategySpec, StrategyRun, and LeverageTerms.
Declare those already-promised producers as R004.2.3 through R004.2.6. Keep them
SPECIFIED and the parent open. Existing served-entry requirements remain separate.
The two executed schema groups do not prove those four absent groups.

## 2026-10-11: keep complete shape qualification open

Independent review found that the passed fixtures sample the shape constraints.
They omit some required-field removals and some accepted vocabulary values.
Record their actual passes as partial evidence only. R004.2.1.1 and R004.2.2.1
track the missing complete matrices. Both schema groups remain open until those
children pass native execution. Do not treat a sample as complete acceptance.

## EG-FINANCE-PRIMITIVES-R018 factor research

Data flow: served finance wire method → route (`src/server/handlers/finance/route_families.rs:211`) → `rank_factors` → `information_coefficient` (`crates/eg-numeric/src/evaluation/skill.rs:16`) → `FactorRecord` node with provenance edges.
Types: `FactorRecord` in `crates/eg-compute/src/finance/factor.rs`.
Attach point: the finance route. Production caller is the served finance method dispatch.
Failure: thin input yields `FACTOR_INSUFFICIENT_DATA` and no node.
Reuses the shared evaluation kernels per R002. New: ranking and the node record.
Decision: reuse the shared kernels. Rejected: a finance copy of the kernels, which R002 forbids.
Cross-repository: none.

References
- No paper is cited.


## Streaming state-space design for R002

R002.1 through R002.4 retain the original numerical, migration, surface and budget obligations.
R002.5 through R002.10 add typed filtering, adaptation, robust approximation, wiring and served proof.
No child removes an original acceptance obligation.

### Original acceptance coverage

| Original parent obligation | Required children | Proof retained |
|---|---|---|
| Every named numerical kernel moves to eg-numeric | R002.1 | Independent positive and negative unit vectors for every kernel |
| UQL time-series exposure for every kernel | R002.2 | Real UQL requests and shared-kernel equivalence |
| Wrapper removal or documented thin aliases; migrated callers | R002.2 | Caller inventory, generic requests, alias equivalence and duplicate detection |
| Generated method counts match client bindings in one change | R002.3 | Identity-set, count, schema and client reconciliation |
| Prefix refits declare bounds or refuse with typed state-budget errors | R002.4 | Exact-bound success, overflow refusal and unchanged prior state |
| Golden precision, invalid aliases, nonfinite/short inputs and stale bindings | R002.1, R002.2, R002.3, R002.4 | Preserve both existing R002 test-spec rows |
| Shared Kalman ownership and no duplicate implementation | R002.1, R002.6, R002.8 | One recurrence for all models and consumers |
| Served integration and production hot paths | R002.9, R002.10 | Wire request, call-site observation, independent oracle and readback |

The original R002 verification cell remains mandatory alongside this matrix.
New filtering tests cannot substitute for the residual kernel and migration children.
Further decomposition must preserve each listed obligation in explicit children.

### Existing implementation and attachment points

Inspection base: `a98f2037d36d0ab6f79a10ba91b9816f61658377`.

| Existing source | Observed behavior and required extension |
|---|---|
| `crates/eg-numeric/src/series/kalman.rs:52` | `Kalman::observe` provides scalar predict/update. It lacks versioned adaptation and robust matrix profiles. |
| `crates/eg-compute/src/finance/statespace.rs:56` | Level and beta wrappers call the shared scalar kernel. Volatility keeps a separate recurrence. |
| `crates/eg-compute/src/finance/statespace.rs:94` | Unspecified volatility initialization uses the first 60 available observations. Streaming initialization must use an explicit prior or completed warmup. |
| `crates/eg-compute/src/finance/statespace.rs:167` and `crates/eg-compute/src/finance/regime.rs` | ADF, OU and regime calculations still require the original ownership migration. |
| `src/server/handlers/finance/route_families.rs:576` | Finance filter requests enter existing compute wrappers. Preserve compatibility while migrating generic callers. |
| `src/server/handlers/timeseries.rs:367` | `handle_append` commits source points, then calls `derived::maintain_after_append`. |
| `src/server/handlers/timeseries/derived.rs:222` | `maintain_one` advances or replays `DerivedState`, then commits derived points and checkpoint state. |
| `crates/eg-tsdb/src/derive/mod.rs:1` | `Program` compiles series expressions to shared numerical kernels. The current kernel revision is `eg-series-kernels/2`. |
| `src/server/handlers/query/sql_read.rs:305` | `handle_unified_query` handles structured plans. The planner's `Op::Derive` dispatch uses the same series evaluator. |

The structured path is `handle_unified_query` -> `run_unified_off_lock` -> `run_unified_off_lock_with`.
The runner binds the served plan context before calling `execute_rows`.
`apply_timeseries` dispatches `Op::Derive` at `crates/eg-plan/src/exec/dispatch.rs:461`.
`derive_op` at `crates/eg-plan/src/exec/derive.rs:21` compiles `Program`, then `derive_row` calls `Program::step`.
Extend these existing boundaries to carry the typed profiles and diagnostics.
The current append path commits source data before derived maintenance.
Maintenance failure can leave the derived series behind its source.
R002.9 must expose that cursor and lag; it must never imply source and derived commits are atomic together.
Existing `TsDefineSeries` parses an expression string. It does not establish a typed matrix-model endpoint.
R002.5 carriers and R002.9 schema/operation registration are prerequisites for serving the new profiles.
Preserve existing operation compatibility and record the new schema and kernel revisions explicitly.
Do not claim new models already exist because scalar method names exist.
The held textual query grammar remains outside this definition change.
R002.2 remains incomplete until every original kernel has its required UQL exposure through its owning implementation lane.
R002.9 must use the existing structured-plan route for new typed profiles without silently extending textual grammar.
Adapters and harnesses consume EG results and decisions. They must not retune parameters or duplicate filter authority.

### Model and numerical behavior

Each checkpoint binds the observation cursor, source revisions, model parameters, transform revision, kernel revision and numeric policy.
Each observation has a stable sequence key and strictly ordered event time within its source revision.
An exact retry returns the prior result. A conflicting retry refuses.
Corrections require explicit bounded replay from a compatible checkpoint, preserving existing as-of visibility.
Missing observations produce an explicit gap outcome unless the model declares a predict-only step.
No profile silently pairs unequal arrays, clamps a transform or reads future observations during initialization.

Use x_minus = F*x and P_minus = F*P*F_transpose + Q.
Then innovation v = z - H*x_minus and S = H*P_minus*H_transpose + R.
Solve S for K = P_minus*H_transpose*S_inverse; do not require an explicit matrix inverse.
Update x = x_minus + K*v.
Let Identity denote the identity matrix.
Use Joseph covariance P = (Identity-K*H)*P_minus*(Identity-K*H)_transpose + K*R*K_transpose.
Validate finite dimensions, symmetric positive-semidefinite P/Q and positive-definite R/S before committing any update.
Pin symmetry tolerance, decomposition tolerance and floating-point policy in the model revision.
An ill-conditioned solve produces a typed numerical refusal; it must not silently replace gain with zero.

The gain minimizes linear mean-square estimation error under the declared linear model and correct second moments.
The linear profile requires zero-mean, temporally uncorrelated noises independent of the initial state.
Process and measurement noises must also be mutually uncorrelated at all time indices.
The current S, K and Joseph formulas support only zero process/measurement cross-covariance.
Model admission must refuse any declaration of nonzero cross-covariance before an update or checkpoint change.
Unknown noise-correlation policy must refuse; model revisions must explicitly declare the supported uncorrelated-noise policy.
Mutually independent Gaussian noises and a Gaussian initial state also permit the conditional-mean interpretation.
Nonlinear approximations, transformed volatility observations and robust weights carry no universal optimality guarantee.

| Profile | State and observation contract |
|---|---|
| Volatility regime | State is log variance with random-walk dynamics. Observe log(return squared) using a declared positive floor and bias correction. Report latent variance and configured variance-band classification. Do not label this classification an HMM posterior. |
| Mean-reversion strength | State is the varying coefficient phi in spread_next = phi*spread_previous + noise. Fixed interval dt and centering revision are mandatory. Report theta = -log(phi)/dt only for 0 < phi < 1. Other phi values produce a typed non-reverting outcome. |
| Hedge ratio | State contains intercept and hedge coefficient. The paired-series observation uses H = [1, hedge_input]. Alignment, units and hedge direction belong to the model revision. |
| Time-varying factor sensitivity | State contains one coefficient per ordered factor. H contains contemporaneously available factor returns. Factor ordering and all source timestamps are pinned. |

Warmup consumes only already-observed data and reports its sample count.
The first emitted estimate follows the completed warmup or explicit provided prior.
No earlier estimate is backfilled from later calibration data.

### Bounded adaptation and robust approximation

The following defaults define reproducible qualification profiles, not universal market parameters.
Model revisions may replace them only with recorded numerical and served evidence.
Retain a fixed baseline window and a recent window, each containing W=32 observations.
The initial baseline uses the first 32 observations; subsequent non-overlapping windows provide the recent statistics.
Whiten each innovation using its own predicted S before inserting it into the windows.
Compute each window's centered sample variance per component, then average across components.
Use a canonical lower-triangular Cholesky solve for whitening and divide centered sums by W-1.
After 64 observations, compare recent variance with max(baseline variance, 1e-12).
Keep that baseline fixed throughout confirmation.
A ratio outside [0.5, 2.0] for three consecutive completed windows triggers a shift.
Advance comparisons every 32 observations. Reset the confirmation count after an in-range window.
After a shift, enforce a 64-observation cooldown.
Use its final 32 observations as the next fixed baseline; resume comparison with the next complete window.
Scale R by clamp(ratio, 0.5, 2.0); constrain each eigenvalue to its revision's positive absolute bounds.
Keep Q unchanged in this profile. Do not pretend innovation variance uniquely identifies Q and R.
Publish old/new parameter digests and the trigger cursor. Apply the revision starting at the next observation.
State and covariance survive retuning; incompatible parameter changes require an explicit reset or bounded replay.
Budget limits cover window storage, eigenvalue work, retuning count and replay.

For nonlinear profiles, use one extended-filter linearization at the prior state per observation.
Registered transition/observation functions provide finite Jacobians and explicit valid domains.
No caller-provided executable code enters the numerical kernel.
Qualification uses f(x)=x and h(x)=x squared on x>0, with derivative 2*x.
This approximation must report its profile revision and must not claim exact nonlinear inference.

For robust gain, let d = sqrt(v_transpose*S_inverse*v).
Set weight w = min(1, c/max(d, 1e-12)), with qualification c=2.5.
Use K_effective = w*K for both the state update and Joseph covariance.
Report w, d and the approximation flag. State corrections then have bounded innovation influence for fixed prior matrices.
Compute shift windows from pre-downweight innovations, with each whitened component clipped to [-8, 8].
Clipping affects detection only; the robust state update uses the original innovation.
This limits isolated tail contamination while retaining persistent-shift detection in the declared profile.
The clipping threshold and warmup/cooldown state belong to the checkpoint revision.
Unbounded errors, invalid covariance and work exhaustion produce typed outcomes without publishing a new derived checkpoint.

### References

R. E. Kalman, "A New Approach to Linear Filtering and Prediction Problems" (1960), sections "Optimal Estimates" and "Orthogonal Projections".
The [primary paper reproduction](https://scispace.com/pdf/a-new-approach-to-linear-filtering-and-prediction-problems-1f1lkk9j5b.pdf) supports the linear estimation assumptions.
The accepted source provides no arXiv identifier; none is assigned here.
The bounded retuning and robust qualification profiles above are engineering requirements, not results attributed to that paper.
The source method requests innovation adaptation and tail downweighting but provides no validated numerical policy for either.


### 2026-10-12: preserve acceptance through smaller R002 units

R002.1, R002.2 and R002.6 are rollups; their original acceptance remains mandatory.
Each new leaf names one kernel or one model at one boundary.
Source paths are intended attachment points, not claims of completed implementations.
R002.2.4 checks the complete caller census; an omitted caller prevents parent completion.
Any additional independent caller requires an explicit child before its migration.
Each leaf retains its own positive and negative proof.
The generic route and generated contract are prerequisites for served caller tests.
R002.3 still requires method counts and client bindings to reconcile in the migration change.
The held query lane owns missing textual admission; these definitions do not change that grammar.

| Retained obligation | Explicit descendants |
|---|---|
| Fourteen shared kernels and independent unit vectors | R002.1.1 through R002.1.14, in original inventory order |
| Generic callers for all fourteen kernels | R002.2.1.1 through R002.2.1.14 |
| Removed or thin documented finance aliases | R002.2.2.1 through R002.2.2.14 |
| Real UQL exposure for all fourteen kernels | R002.2.3.1 through R002.2.3.14 |
| Complete caller coverage, including additional discovered callers | R002.2.4 |
| Causal recurrence and supported noise assumptions | R002.6.1 |
| Volatility, mean-reversion, hedge and factor models | R002.6.2 through R002.6.5, respectively |

These scenarios have no new execution receipts. Existing parent receipts remain unchanged.
