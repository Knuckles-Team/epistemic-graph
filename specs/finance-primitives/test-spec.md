# Verification and acceptance contract

Every test below runs with generated or checked-in synthetic fixtures. No existing broker account, market subscription, live graph service or private credential is part of the baseline PR gate. Where an external source is relevant, use a disposable local fixture and a separate optional live qualification record.

| ID | Positive proof | Negative / boundary proof |
|---|---|---|
| EG-FINANCE-PRIMITIVES-R001 | Two independent supported hosts compute identical sealed `BacktestRun` digest from one checked-in fixture; Pine and Rust SuperTrend/ATR agree on every transition, warmup and session boundary; ontology class census has one canonical IRI per finance concept | different source/input version changes digest; host locale, timezone or iteration order does not; stale Pine parameters and duplicate class authority fail |
| EG-FINANCE-PRIMITIVES-R004 | OWL/SHACL class and shape validation; account/activity revision round trip; multi-venue listing identity | unknown asset class/schema version; cross-tenant read/write; same source revision with different payload; direct write to derived `Position` |
| EG-FINANCE-PRIMITIVES-R005 | FIFO, LIFO, specific and average cost reconciled to independent statement vectors; realized/unrealized P&L, TWR, XIRR, trade/valuation FX, dividends | insufficient lots, zero/multiple XIRR roots, stale price, missing FX, checked overflow, binary-float ingress, unequal replay bytes |
| EG-FINANCE-PRIMITIVES-R006 | split/dividend/symbol-change/delist replay before and after `known_at`; exchange pre/regular/post, early close and DST offset span | impossible action chronology, duplicate revision conflict, host time-zone changes replay result, ticker reuse conflated with identity |
| EG-FINANCE-PRIMITIVES-R007 | fixed-cash, fixed-share and value-averaging DCA; trend flip and MA crossover; calendar/threshold rebalance; exact replay digest | missed cadence recorded, idempotent retry, stale inputs abstain, no transport/approval token in proposed action |
| EG-FINANCE-PRIMITIVES-R008 | same-universe DCA/lump-sum/trend runs with costs; sealed backtest verifies; validation outputs recomputed from the same split matrix | look-ahead fill, mismatch between claimed PBO rows and split count, omitted costs, thin history, incompatible horizons |
| EG-FINANCE-PRIMITIVES-R009 | calibrated per-horizon recommendation cites strategy/evaluation/sources; scorecard outcomes; sealed snapshot verifies | thin history, warmup, drift, ambiguous conformal set, stale price, unsourced claim all abstain/refuse |
| EG-FINANCE-PRIMITIVES-R010 | margin and futures tick/multiplier vectors; gold future roll, daily-reset fund path, risk-of-ruin scenario | missing maintenance margin, expired contract, unsupported policy/jurisdiction, assumed live permission, negative equity mishandled |
| EG-FINANCE-PRIMITIVES-R011 | event replay yields one stable ID and one outbox fact; existing `finance.flip` remains compatible | duplicate source revision, unauthorized tenant, event replay cannot trigger order |
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


For any valid activity sequence: activity permutation with the same canonical ordering yields one digest; a higher known-at correction cannot alter an earlier as-of query; a split preserves aggregate basis; a transfer conserves quantity/cash across included accounts after explicit fees; a buy followed by a complete sell has zero remaining units; a valuation currency conversion round trip is bounded by the declared rounding unit. Property generators must shrink to a minimal counterexample and persist a regression vector for failures.

For any strategy: identical spec and input digests yield byte-identical proposals and run evidence; increasing data known-at cannot retroactively change an earlier replay; no strategy result serializes an executable broker order. For any risk scenario, changing a maintenance margin or multiplier must change the documented threshold monotonically where the model's assumptions imply monotonicity; document exceptions.

## Concrete acceptance commands and hosted gates

Run the repository's pinned formatter, Rust Clippy, affected crate tests, ontology/SHACL tests, Python client tests and contract generation freshness check at the exact PR head. Run CCCC, jscpd with zero new clone pairs against main, Dupehound and KISS checks for changed code. The affected-path gate must exercise finance compute, server route, generated client and graph authorization/retention paths; a compile-only check is insufficient. Use the repository's checked-in scripts/configuration as the command authority, and record the exact command/version in evidence when implemented. Hosted CI must create disposable stores and mocks itself and must not require a live finance provider or manually provisioned environment. A live-provider smoke test may be a separate nonblocking qualification.

`ACCEPTED` for a row requires all its positive and refusal cases, exact published-main SHA, relevant hosted run URL, independent fixture provenance, and no unresolved severity-one finance correctness defect. A row may be `PARTIAL` when a subset passes; document the subset and missing proof. No blanket train-level acceptance closes an unproven row.
