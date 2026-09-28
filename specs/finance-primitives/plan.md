# Delivery plan

## Sequence

1. **Contract foundation (EH-698, EH-700).** Add typed versioned records, ontology and SHACL shapes, temporal revision resolution and session/calendar semantics. Review migration of existing asset-class values. Generate clients and prove old `FinanceMarket` decoding.
2. **Accounting authority (EH-699, EH-715).** Implement one fixed-point event-to-lot projection, valuation join and return kernels. Add independent vectors and property tests before exposing a public projection method. Reconcile imported activities against statement fixtures.
3. **Strategy/evidence authority (EH-702, EH-703).** Implement immutable strategy specs and deterministic proposal evaluation. Feed run provenance into the existing sealed `BacktestRun`; calculate validation from the same split matrix and compare common horizons/costs.
4. **Advisory and risk (EH-704, EH-706).** Add calibrated abstention and scorecards, seal `AnalysisSnapshot`, and implement leverage scenarios with explicit product/policy terms. Audit that no result acts as an order authorization.
5. **Events and optional attached source (EH-712, EH-714).** Extend the finance event family with stable IDs and atomic outbox state. Qualify the optional read-only importer against disposable fixtures after account identity and revision contracts are stable.
6. **Final qualification.** Run the matrix in `test-spec.md` at an exact main head and add per-ID evidence. Publish a new contract version only after generated clients and compatibility tests are green.

## Decisions to resolve before implementation

| Decision | Required resolution and evidence |
|---|---|
| Accounting policy | State whether average cost is available for each account/tax context; persist the election and prevent silent retrospective changes. |
| Monetary primitive | Reuse an existing checked decimal type if its scale and rounding semantics satisfy F2; otherwise add one in the finance compute boundary. |
| Price gaps | Decide which results become partial versus refused; never substitute another source, session or timeframe silently. |
| Calendar data | Version offset spans and sessions as source facts so replay does not depend on the host time-zone database. |
| Backtest statistics | Specify one split-result matrix contract shared by purged CV, deflated Sharpe and PBO; reject headline metrics that cannot be recomputed. |
| Leverage policies | Keep jurisdiction and provider-specific availability in versioned policy facts; do not hard-code an assumed live trading permission. |

## Integration contracts

External connectors submit source events and revisions through the typed import boundary. The serving layer schedules recurring DCA intentions and consumes outbox facts; it cannot change EG calculations. The browser reads positions, comparisons, risk and snapshots with provenance and renders partial/abstain states. The execution system receives proposals only through its separate governed approval path. These interfaces are complete at the behavioral level here; companion repositories may add their own implementation details without changing EG's authority.

## Rollout

Ship read and replay contracts before imports. Add tenant-scoped migrations with a dry-run reconciliation report; preserve old revisions. Gate any projection head switch on statement/vector agreement and authorization tests. Use a schema/contract version bump for incompatible fields. If a stage fails, retain the prior read path and do not publish a new head. A rollback never rewrites activity history.
