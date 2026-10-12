# Verification and acceptance contract

Every test below runs with generated or checked-in synthetic fixtures. No existing broker account, market subscription, live graph service or private credential is part of the baseline PR gate. Where an external source is relevant, use a disposable local fixture and a separate optional live qualification record.

| ID | Positive proof | Negative / boundary proof |
|---|---|---|
| EG-FINANCE-PRIMITIVES-R001 | Two independent supported hosts compute identical sealed `BacktestRun` digest from one checked-in fixture. Pine and Rust SuperTrend/ATR agree on every transition, warmup and session boundary. Ontology class census has one canonical IRI per finance concept | different source/input version changes digest. Host locale, timezone or iteration order does not. Stale Pine parameters and duplicate class authority fail |
| EG-FINANCE-PRIMITIVES-R004 | OWL/SHACL class and shape validation. Account/activity revision round trip. Multi-venue listing identity | unknown asset class/schema version. Cross-tenant read/write. Same source revision with different payload. Direct write to derived `Position` |
| EG-FINANCE-PRIMITIVES-R005 | FIFO, LIFO, specific and average cost reconciled to independent statement vectors; realized/unrealized P&L, TWR, XIRR, trade/valuation FX, dividends | insufficient lots, zero/multiple XIRR roots, stale price, missing FX, checked overflow, binary-float ingress, unequal replay bytes |
| EG-FINANCE-PRIMITIVES-R006 | split/dividend/symbol-change/delist replay before and after `known_at`; exchange pre/regular/post, early close and DST offset span | impossible action chronology, duplicate revision conflict, host time-zone changes replay result, ticker reuse conflated with identity |
| EG-FINANCE-PRIMITIVES-R007 | fixed-cash, fixed-share and value-averaging DCA. Trend flip and MA crossover. Calendar/threshold rebalance. Exact replay digest | missed cadence recorded, idempotent retry, stale inputs abstain, no transport/approval token in proposed action |
| EG-FINANCE-PRIMITIVES-R008 | same-universe DCA/lump-sum/trend runs with costs; sealed backtest verifies; validation outputs recomputed from the same split matrix | look-ahead fill, mismatch between claimed PBO rows and split count, omitted costs, thin history, incompatible horizons |
| EG-FINANCE-PRIMITIVES-R009 | calibrated per-horizon recommendation cites strategy/evaluation/sources. Scorecard outcomes. Sealed snapshot verifies | thin history, warmup, drift, ambiguous conformal set, stale price, unsourced claim all abstain/refuse |
| EG-FINANCE-PRIMITIVES-R010 | margin and futures tick/multiplier vectors; gold future roll, daily-reset fund path, risk-of-ruin scenario | missing maintenance margin, expired contract, unsupported policy/jurisdiction, assumed live permission, negative equity mishandled |
| EG-FINANCE-PRIMITIVES-R011 | event replay yields one stable ID and one outbox fact. Existing `finance.flip` remains compatible | duplicate source revision, unauthorized tenant, event replay cannot trigger order |
| EG-FINANCE-PRIMITIVES-R012 | disposable attached-source fixture imports an activity once and reconciles; corrected and deleted rows become revision/tombstone | schema drift quarantined, no writes to source, source scope denied, repeated import no duplicate balance |
| EG-FINANCE-PRIMITIVES-R013 | independent broker-statement, corporate action, DST/session, fund decay and futures-roll golden vectors | corrupt fixture provenance, absent as-of/session/currency, second accounting authority or cloned helper detected |
| EG-FINANCE-PRIMITIVES-R002 | generic and finance aliases agree on shared numerical golden vectors and generated method/client counts | duplicate kernel, stale binding or unbounded regime derivation fails |
| EG-FINANCE-PRIMITIVES-R003 | skilled and overfit 15-split PBO fixtures derive from sealed split matrix | three-row headline mismatch, bad matrix or tampered split evidence refuses |

## Cross-cutting property tests

### EG-FINANCE-PRIMITIVES-R002 / EG-FINANCE-PRIMITIVES-R003 focused acceptance

| ID | Positive proof | Negative and boundary proof |
|---|---|---|
| EG-FINANCE-PRIMITIVES-R002 | Golden vectors for every moved kernel match finance alias, generic API and UQL where exposed; generated method schema/count and Python bindings agree; bounded regime derivation reports exact state/work. | Changed precision, duplicated implementation, unknown method alias, nonfinite/short series, insufficient regime state budget and stale generated bindings refuse or fail the build. |
| EG-FINANCE-PRIMITIVES-R003 | Skilled and overfit fixtures use all 15 sealed CSCV splits; skilled produces 0/15 and deliberately overfit 15/15; a tie exactly at median is not counted. Re-sealing identical input yields identical digest and verify result. | Caller supplies three attractive performance rows against 15 contradictory splits, unequal candidate rows, zero splits, nonfinite score or tampered split evidence: seal/verify refuses or recomputes from the true matrix, never reports the supplied headline. |

For any valid activity sequence: activity permutation with the same canonical ordering yields one digest. A higher known-at correction cannot alter an earlier as-of query. A split preserves aggregate basis. A transfer conserves quantity/cash across included accounts after explicit fees. A buy followed by a complete sell has zero remaining units. A valuation currency conversion round trip is bounded by the declared rounding unit. Property generators must shrink to a minimal counterexample and persist a regression vector for failures.

For any strategy: identical spec and input digests yield byte-identical proposals and run evidence; increasing data known-at cannot retroactively change an earlier replay; no strategy result serializes an executable broker order. For any risk scenario, changing a maintenance margin or multiplier must change the documented threshold monotonically where the model's assumptions imply monotonicity; document exceptions.

## Concrete acceptance commands and hosted gates

Run the repository's pinned formatter, Rust Clippy, affected crate tests, ontology/SHACL tests, Python client tests and contract generation freshness check at the exact PR head. Run CCCC, jscpd with zero new clone pairs against main, Dupehound and KISS checks for changed code. The affected-path gate must exercise finance compute, server route, generated client and graph authorization/retention paths; a compile-only check is insufficient. Use the repository's checked-in scripts/configuration as the command authority, and record the exact command/version in evidence when implemented. Hosted CI must create disposable stores and mocks itself and must not require a live finance provider or manually provisioned environment. A live-provider smoke test may be a separate nonblocking qualification.

Each row requires all positive and refusal cases, an exact published-main SHA and the relevant hosted run URL.
Record independent fixture provenance and unresolved correctness defects.
An unresolved severity-one correctness defect prevents completion.
When only a subset passes, describe that subset and its missing proof.
A train-level result does not close unproven scenarios.

## R013 decomposed fixture acceptance

The R013.1 production-module unit gate admits a fully-provenanced figure and refuses each absent source, as-of time and session. It does not establish the broader fixture rows below. R013.2.1 currently has synthetic comparator tests only; independent broker-statement data is not claimed as validated.

| ID | Positive proof | Negative / boundary proof |
|---|---|---|
| EG-FINANCE-PRIMITIVES-R013.2.2 | Published issuer/exchange split and dividend notice vectors reproduce independent share, basis and cash ticks before/after the action. | Changed expected ticks and each absent source/as-of/session fail. |
| EG-FINANCE-PRIMITIVES-R013.2.3 | Published exchange calendar/offset vectors match session boundaries and both DST transitions; host-time-zone replay is identical. | Wrong offset/session label and each absent source/as-of/session fail. |
| EG-FINANCE-PRIMITIVES-R013.2.4 | Published multi-session fund return/NAV vectors match fixed-point daily-reset results with explicit fees, scale and rounding. | A modified return/reference, omitted daily reset and each absent source/as-of/session fail. |
| EG-FINANCE-PRIMITIVES-R013.2.5 | Published contract/settlement vectors match roll cash and position ticks and replay identically. | Incorrect expiry/tick size/multiplier/reference ticks and each absent source/as-of/session fail. |

Every fixture must carry an independently inspectable source URL, publication/version and content digest plus as-of/session and applicable currency. Check in redistributable reference extracts so baseline tests need no provider account or live network. Expected values are independent of the implementation under test. These four scenarios have no execution receipts; their definitions add no test bindings. Record exact published-main SHA and hosted run for each before accepting the parent fixture family.

## Restored runtime acceptance

Existing parent acceptance remains unchanged. Each intended test below must run before its row receives runtime acceptance.

| ID | Positive proof | Negative / boundary proof | Intended test |
|---|---|---|---|
| EG-FINANCE-PRIMITIVES-R004.3.2 | Each record-specific child passes through a served entry point. | A typed schema gate alone does not satisfy handler acceptance. | Child-row tests |
| EG-FINANCE-PRIMITIVES-R004.3.2.1 | A served request round-trips a valid `Account` record. | Unknown versions and cross-tenant access refuse. Derived-record writes refuse where the record policy prohibits them. | `src/server/handlers/finance/records/tests.rs::account_served_round_trip_and_refusals (intended test)` |
| EG-FINANCE-PRIMITIVES-R004.3.2.2 | A served request round-trips a valid `Activity` record. | Unknown versions and cross-tenant access refuse. Derived-record writes refuse where the record policy prohibits them. | `src/server/handlers/finance/records/tests.rs::activity_served_round_trip_and_refusals (intended test)` |
| EG-FINANCE-PRIMITIVES-R004.3.2.3 | A served request round-trips a valid `Lot` record. | Unknown versions and cross-tenant access refuse. Derived-record writes refuse where the record policy prohibits them. | `src/server/handlers/finance/records/tests.rs::lot_served_round_trip_and_refusals (intended test)` |
| EG-FINANCE-PRIMITIVES-R004.3.2.4 | A served request round-trips a valid `Position` record. | Unknown versions and cross-tenant access refuse. Derived-record writes refuse where the record policy prohibits them. | `src/server/handlers/finance/records/tests.rs::position_served_round_trip_and_refusals (intended test)` |
| EG-FINANCE-PRIMITIVES-R004.3.2.5 | A served request round-trips a valid `PortfolioGroup` record. | Unknown versions and cross-tenant access refuse. Derived-record writes refuse where the record policy prohibits them. | `src/server/handlers/finance/records/tests.rs::portfolio_group_served_round_trip_and_refusals (intended test)` |
| EG-FINANCE-PRIMITIVES-R004.3.2.6 | A served request round-trips a valid `StrategySpec` record. | Unknown versions and cross-tenant access refuse. Derived-record writes refuse where the record policy prohibits them. | `src/server/handlers/finance/records/tests.rs::strategy_spec_served_round_trip_and_refusals (intended test)` |
| EG-FINANCE-PRIMITIVES-R004.3.2.7 | A served request round-trips a valid `StrategyRun` record. | Unknown versions and cross-tenant access refuse. Derived-record writes refuse where the record policy prohibits them. | `src/server/handlers/finance/records/tests.rs::strategy_run_served_round_trip_and_refusals (intended test)` |
| EG-FINANCE-PRIMITIVES-R004.3.2.8 | A served request round-trips a valid `LeverageTerms` record. | Unknown versions and cross-tenant access refuse. Derived-record writes refuse where the record policy prohibits them. | `src/server/handlers/finance/records/tests.rs::leverage_terms_served_round_trip_and_refusals (intended test)` |
| EG-FINANCE-PRIMITIVES-R004.3.3 | Both generated-surface children pass. | A schema-version constant alone does not establish generated-client support. | Child-row tests |
| EG-FINANCE-PRIMITIVES-R004.3.3.1 | The generated contract represents each R004.2 record and served entry point. | The contract freshness check rejects stale generated artifacts. | `tests/test_contract_wave_surface.py::test_finance_record_contract_surface (intended test)` |
| EG-FINANCE-PRIMITIVES-R004.3.3.2 | Generated clients preserve all required record fields through served requests. | Unknown versions refuse; omitted required fields fail. The generator check rejects stale clients. | `tests/test_generated_client_contract.py::test_finance_record_client_round_trips (intended test)` |
| EG-FINANCE-PRIMITIVES-R005.2.2 | Mixed-price buys and partial sells reproduce expected basis and realized gains. Identical ordered inputs replay identically. | Oversell and arithmetic overflow refuse. Rounding follows the declared scale without floating-point money. | `crates/eg-compute/src/finance/lot_accounting.rs::tests::average_cost_election_preserves_basis (intended test)` |
| EG-FINANCE-PRIMITIVES-R005.2.3 | A transfer preserves source-lot identity and basis across included accounts. Identical ordered inputs replay identically. | Unbalanced transfers and insufficient source quantity refuse without a partial ledger update. | `crates/eg-compute/src/finance/lot_accounting.rs::tests::paired_transfer_conserves_quantity_cash_and_basis (intended test)` |
| EG-FINANCE-PRIMITIVES-R005.2.4 | Forward and reverse splits preserve aggregate basis. Identical ordered inputs replay identically. | Invalid ratios and arithmetic overflow refuse. Fractional quantities follow the declared rounding policy. | `crates/eg-compute/src/finance/lot_accounting.rs::tests::split_preserves_aggregate_basis (intended test)` |
| EG-FINANCE-PRIMITIVES-R011.2 | Both append children preserve the existing flip identity. | Envelope validation alone does not establish committed delivery. | Child-row tests |
| EG-FINANCE-PRIMITIVES-R011.2.1 | Price, trend-flip, DCA-due and margin-threshold alerts preserve their schema, payload and event identity. | Invalid envelopes refuse. Existing `finance.flip` identifiers remain unchanged. | `src/server/mutation_batch/compile.rs::tests::finance_alert_intents_preserve_schema_and_identity (intended test)` |
| EG-FINANCE-PRIMITIVES-R011.2.2 | Each alert condition commits its typed event and outbox fact together. | Failed transactions publish neither record. Unauthorized tenants and conflicting source revisions refuse. | `src/server/mutation_batch/tests.rs::finance_alert_and_outbox_commit_atomically (intended test)` |
| EG-FINANCE-PRIMITIVES-R011.3 | All four alert kinds retain their event identity across replay and restart. Existing `finance.flip` delivery remains compatible. | Duplicate replay creates no extra event or delivery fact. Alert delivery never authorizes an order. | `src/server/mutation_batch/tests.rs::finance_alert_replay_preserves_one_outbox_fact (intended test)` |

## R011.1 typed-envelope acceptance

| ID | Positive proof | Negative / boundary proof | Proving tests |
|---|---|---|---|
| EG-FINANCE-PRIMITIVES-R011.1 | A valid envelope passes validation. All four alert kinds have distinct outbox schema names. | Empty identity, listing and message fields refuse. A non-positive timestamp refuses. | `crates/eg-types/src/finance_event_envelope.rs::tests`: `a_well_formed_envelope_validates`, `each_alert_kind_has_a_distinct_outbox_schema`, `an_empty_event_id_is_refused`, `an_empty_listing_id_is_refused`, `an_empty_message_is_refused`, `a_non_positive_timestamp_is_refused`. |

These six module tests prove the typed envelope only. R011 retains transactional publication and idempotent delivery acceptance.
R011.2 and R011.3 track that runtime acceptance without claiming these module tests prove it.

## Remaining record-schema producers

| ID | Positive proof | Negative proof | Proving test |
|---|---|---|---|
| EG-FINANCE-PRIMITIVES-R004.2.3 | A valid `PortfolioGroup` record conforms to its declared shape. Its class exists in the composed finance module. | Missing required fields, invalid links, and unknown vocabulary refuse. | `src/server/graph_schema/finance_tests.rs::shapes::portfolio_group_schema_acceptance (intended test)` |
| EG-FINANCE-PRIMITIVES-R004.2.4 | A valid `StrategySpec` record conforms to its declared shape. Its class exists in the composed finance module. | Missing required fields, invalid links, and unknown vocabulary refuse. | `src/server/graph_schema/finance_tests.rs::shapes::strategy_spec_schema_acceptance (intended test)` |
| EG-FINANCE-PRIMITIVES-R004.2.5 | A valid `StrategyRun` record conforms to its declared shape. Its class exists in the composed finance module. | Missing required fields, invalid links, and unknown vocabulary refuse. | `src/server/graph_schema/finance_tests.rs::shapes::strategy_run_schema_acceptance (intended test)` |
| EG-FINANCE-PRIMITIVES-R004.2.6 | A valid `LeverageTerms` record conforms to its declared shape. Its class exists in the composed finance module. | Missing required fields, invalid links, and unknown vocabulary refuse. | `src/server/graph_schema/finance_tests.rs::shapes::leverage_terms_schema_acceptance (intended test)` |

## Complete schema acceptance

| ID | Positive proof | Negative proof | Proving test |
|---|---|---|---|
| EG-FINANCE-PRIMITIVES-R004.2.1.1 | Valid `Account/Position` fixtures accept every declared value of accountKind and costBasisMethod. | Omit each required field separately. Use literal values for IRI links. Wrong datatypes, duplicate singleton fields, and unknown vocabulary refuse. | `src/server/graph_schema/finance_tests.rs::shapes::account_position_complete_schema_matrix (intended test)` |
| EG-FINANCE-PRIMITIVES-R004.2.2.1 | Valid `Activity/Lot` fixtures accept every declared value of activityKind. | Omit each required field separately. Use literal values for IRI links. Wrong datatypes, duplicate singleton fields, and unknown vocabulary refuse. | `src/server/graph_schema/finance_tests.rs::shapes::activity_lot_complete_schema_matrix (intended test)` |

## EG-FINANCE-PRIMITIVES-R018 acceptance

| Scenario | Requirement | Fixture |
|---|---|---|
| Rank three factors | R018.6 | Three factors rank by absolute information ratio and write three nodes. |
| Thin input | R018.4 | Zero and one row return `FACTOR_INSUFFICIENT_DATA`. |


## R002 numerical and served acceptance

Every test below is proposed. This definition change has no runtime scenario evidence.
Keep the original R002 positive and negative rows above as independent obligations.
The proving test names below are implementation targets, not existing test bindings.

| ID | Positive proof | Negative or boundary proof | Proposed proving test |
|---|---|---|---|
| EG-FINANCE-PRIMITIVES-R002.1 | Complete the original shared-kernel inventory; run its requirements.md verification. | Run every refusal stated in that row and the fixtures below. | `tests/test_finance_shared_kernel_inventory.py::test_complete_kernel_inventory` |
| EG-FINANCE-PRIMITIVES-R002.2 | Migrate callers and preserve generic time-series exposure; run its requirements.md verification. | Run every refusal stated in that row and the fixtures below. | `tests/test_finance_shared_kernel_routes.py::test_generic_alias_and_uql_equivalence` |
| EG-FINANCE-PRIMITIVES-R002.3 | Reconcile generated methods and client bindings; run its requirements.md verification. | Run every refusal stated in that row and the fixtures below. | `tests/test_finance_shared_kernel_routes.py::test_generated_method_identity_reconciliation` |
| EG-FINANCE-PRIMITIVES-R002.4 | Bound original regime derivations and streaming work; run its requirements.md verification. | Run every refusal stated in that row and the fixtures below. | `crates/eg-tsdb/src/derive/maintain_tests.rs::state_space_resource_bounds` |
| EG-FINANCE-PRIMITIVES-R002.5 | Define versioned state-space inputs and checkpoints; run its requirements.md verification. | Run every refusal stated in that row and the fixtures below. | `crates/eg-types/src/series_expr.rs::tests::state_space_model_and_refusals` |
| EG-FINANCE-PRIMITIVES-R002.6 | Apply causal predict-update filtering to four state models; run its requirements.md verification. | Run every refusal stated in that row and the fixtures below. | `crates/eg-numeric/src/series/kalman.rs::tests::four_models_match_independent_oracles` |
| EG-FINANCE-PRIMITIVES-R002.7 | Detect innovation shifts and retune within fixed bounds; run its requirements.md verification. | Run every refusal stated in that row and the fixtures below. | `crates/eg-numeric/src/series/kalman.rs::tests::bounded_innovation_retuning` |
| EG-FINANCE-PRIMITIVES-R002.8 | Bound nonlinear approximation and robust tail influence; run its requirements.md verification. | Run every refusal stated in that row and the fixtures below. | `crates/eg-numeric/src/series/kalman.rs::tests::nonlinear_robust_gain` |
| EG-FINANCE-PRIMITIVES-R002.9 | Wire versioned filtering through served series operations; run its requirements.md verification. | Run every refusal stated in that row and the fixtures below. | `tests/test_finance_state_space_served.py::test_served_filter_wiring` |
| EG-FINANCE-PRIMITIVES-R002.10 | Prove filtering on the production hot paths; run its requirements.md verification. | Run every refusal stated in that row and the fixtures below. | `tests/test_finance_state_space_served.py::test_stream_restart_refusals_and_hot_path_receipts` |


### Fixed numerical vectors

Use explicit scalar prior x=0, P=1, F=H=1, Q=0, R=1 and observations [1, 2].
The first update produces innovation=1, S=2, K=1/2, x=1/2 and P=1/2.
The second produces innovation=3/2, S=3/2, K=1/3, x=1 and P=1/3.
Check every intermediate against these independent rational values with absolute error at most 1e-12.
For multidimensional goldens, use abs(error) <= 1e-12 + 1e-10*abs(expected).
Pin input serialization and numeric policy separately from mathematical tolerances.
Same-build replay must produce identical checkpoint bytes. Cross-host comparisons use the declared tolerances and identical revision identities.

For the nonlinear profile, use prior x=2, P=1, Q=0, R=1 and observation z=5.
The Jacobian is 4, innovation is 1, S=17, K=4/17 and posterior x=38/17.
Joseph covariance is 1/17 before robust downweighting; this residual has weight 1.
For the scalar tail case, replace the first observation with 1000.
With c=2.5, the effective update magnitude is 2.5/sqrt(2), below the ordinary update magnitude 500.
Check both signs and the Joseph covariance with the actual effective gain.

Check in independent fixture outputs for every named kernel and all four state profiles.
Fixtures record generator version, seed=44045, input digest and oracle provenance.
Use 4096 observations: stable variance, a persistent fourfold noise-variance change, and isolated signed outliers.
Include a seeded Student-t sequence with five degrees of freedom and explicit generator details.
Freeze generated bytes so library RNG changes cannot change the fixture silently.
The oracle must not import production numerical functions.
Compare every state, covariance, innovation statistic, tail weight, shift cursor and parameter revision.
Pin expected trigger positions from the declared window algorithm, including three-window confirmation and cooldown.
Check windows at 31, 32, 63 and 64 observations and exact threshold equality.
A single outlier must not satisfy the three-window confirmation rule.
Changing an observation after cursor t must not change the result at t.

### Served protocol and hot-path receipts

Launch a real EG server with disposable stores and the required time-series, query and finance feature set.
Record the exact source head, feature list, generated schema revision and kernel revision.
Create source series through its served operation and install a supported versioned model through the typed production route.
Append observations through `TsAppend`; observe derived maintenance at `maintain_after_append` and `maintain_one`.
Read results through `TsRange` and run the equivalent structured `UnifiedQuery` plan.
Exercise existing UQL exposure for every original R002 kernel when its implementation prerequisite is present.
Compare raw wire readback with independent goldens, including diagnostics and revision identities.
Repeat as one sequence and chunks [1, 31, 32, 64, remainder].
Restart the server between chunks and compare restored checkpoint behavior with uninterrupted execution.
Replay a correction within budget, then compare its affected suffix with the independent oracle.
Preserve prior known-at read results.

Send malformed dimensions, nonfinite values, negative covariance, singular innovation covariance and unsupported model/schema revisions.
Also send zero-return transforms without an admitted floor, misaligned factor arrays, conflicting retries and out-of-order observations.
Require typed outcomes, unchanged prior derived checkpoints and no fabricated complete result.
Repeat using another actor and tenant; neither may read or mutate the original series.
At every configured resource limit, test exact admission and limit-plus-one refusal.
If source append succeeds before maintenance refuses, verify source visibility, unchanged derived cursor and explicit lag.
Inject checkpoint revision mismatch and incompatible parameter updates; require refusal or the explicitly requested bounded replay.

Receipts identify public test names, request IDs, production call sites, revisions, consumed budgets and output digests.
Record sanitized assertions rather than raw request logs or credentials.
A helper invocation, mocked dispatcher or unobserved handler cannot establish production integration.
No numerical, route or hot-path proof closes R002 while any original inventory or migration obligation remains uncovered.
