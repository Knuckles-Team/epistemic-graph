# EG-PLACEMENT - Test specification

Status: PROPOSED. Governing spec: [spec.md](spec.md).

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
