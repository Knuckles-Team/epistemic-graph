# EG-PLACEMENT - Test specification

Governing spec: [spec.md](spec.md).

| Test ID | Requirement | Level | Setup and input | Expected observation | Evidence |
|---|---|---|---|---|---|
| T-001 | FR-001 | Integration | Served Decide Placement with HA demand over four targets | Swarm chosen, in_process and compose eliminated with reasons, no lease | PENDING |
| T-002 | FR-001 | Integration | Demand above every cell headroom | Abstained with the lease-named reason; ledger unchanged | PENDING |
| T-003 | FR-002 | Integration | AgentAssemble with the fixture deployment template | Solved, steps ordered, certificate present; missing tool abstains | PENDING |
| T-004 | FR-003 | Integration | Register 16-core host; forged lane in claim | Grant from SchedulerConfig; forged field refused | PENDING |
| T-005 | FR-004 | Integration | 31 and 32 distinct items; 32-item itemset to generate_rules | Accepted, refused, empty rules without panic | PENDING |

## Negative and boundary cases

Duplicate or unknown candidate targets, zero replicas, loosening policy caps, missing capacity ledger, over-subscribed floors, worker ceiling above grant, repeated label counted once.

## Quality and release proof

Focused `cargo test` on the named files per row; `spec-status` and `spec-decomposition` gates; cargo is run by the implementing lane, not by this design.

R002.1 producer evidence: worker_registry::tests::capacity_round_trips_and_heartbeat_extends_expiry preserves cores, memory_bytes and lanes_supported through registration, serialization and expiry. Served policy scenario T-004 remains pending under R074.2 and R075.

R002.2 producer evidence: `scheduler_config::tests` uses real serde to prove defaults 360 and 3 GiB, override round trips, bounds and over-subscribed floor refusal. The FR-003 served integration scenario remains PENDING.

### EG-PLACEMENT-R004 tests

- `EG-PLACEMENT-R004.1`: `crates/eg-types/src/decision/statistical/placement.rs::tests::capability_types_round_trip`.
- `EG-PLACEMENT-R004.2`: `crates/eg-types/src/decision/statistical/placement.rs::tests::route_orders_by_headroom`.
- `EG-PLACEMENT-R004.3`: `tests/jobs/test_capability_claim_served.py::test_claim_uses_declared_capability`.
- `EG-PLACEMENT-R004.4`: `crates/eg-jobs/src/store.rs::tests::no_capable_host_refuses`.
- `EG-PLACEMENT-R004.5`: `crates/eg-jobs/src/store.rs::tests::routing_premises_name_generation`.
- `EG-PLACEMENT-R004.6`: `tests/jobs/test_capability_claim_served.py::test_backtest_job_lands_on_declared_host`.

<!-- spec-definition-patch:owner-batch-20261012-placement-and-worker-policy -->
## Reviewed residual acceptance targets — owner batch 2026-10-12

All targets below are planned acceptance, not executed tests or receipts. Use the real producer/consumer boundary named for each row and retain existing covered behavior.

- **`EG-PLACEMENT-R001.15` / EG:9:** Add the closed-loop producer/GraphOS consumer and applied-state receipt; preserve existing GRAPHOS-HOST references. Producer/consumer path: EG capacity premises/place verdicts feed hosting policy; GraphOS exposes limits/mode and pure AIMD. Engine scaling actuation/reconciliation is an absent definition, not an unlinked throttle. Source review `cb17ea03a7344584fed7a9f22a78c1e97c58919616433e7026481606fd0bdccb`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.
<!-- /spec-definition-patch -->
