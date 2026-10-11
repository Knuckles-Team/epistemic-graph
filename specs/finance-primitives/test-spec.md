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

`ACCEPTED` for a row requires all its positive and refusal cases, exact published-main SHA, relevant hosted run URL, independent fixture provenance, and no unresolved severity-one finance correctness defect. A row may be `PARTIAL` when a subset passes; document the subset and missing proof. No blanket train-level acceptance closes an unproven row.

## R013 decomposed fixture acceptance

The R013.1 production-module unit gate admits a fully-provenanced figure and refuses each absent source, as-of time and session. It does not establish the broader fixture rows below. R013.2.1 currently has synthetic comparator tests only; independent broker-statement data is not claimed as validated.

| ID | Positive proof | Negative / boundary proof |
|---|---|---|
| EG-FINANCE-PRIMITIVES-R013.2.2 | Published issuer/exchange split and dividend notice vectors reproduce independent share, basis and cash ticks before/after the action. | Changed expected ticks and each absent source/as-of/session fail. |
| EG-FINANCE-PRIMITIVES-R013.2.3 | Published exchange calendar/offset vectors match session boundaries and both DST transitions; host-time-zone replay is identical. | Wrong offset/session label and each absent source/as-of/session fail. |
| EG-FINANCE-PRIMITIVES-R013.2.4 | Published multi-session fund return/NAV vectors match fixed-point daily-reset results with explicit fees, scale and rounding. | A modified return/reference, omitted daily reset and each absent source/as-of/session fail. |
| EG-FINANCE-PRIMITIVES-R013.2.5 | Published contract/settlement vectors match roll cash and position ticks and replay identically. | Incorrect expiry/tick size/multiplier/reference ticks and each absent source/as-of/session fail. |

Every fixture must carry an independently inspectable source URL, publication/version and content digest plus as-of/session and applicable currency. Check in redistributable reference extracts so baseline tests need no provider account or live network. Expected values are independent of the implementation under test. These four rows are NOT_RUN; defining their acceptance adds no test binding or delivery claim. Record exact published-main SHA and hosted run for each before accepting the parent fixture family.

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
