# M3 cross-node elasticity proposal planner

`raft::cross_node_elasticity` is the policy-only planning layer for cross-node
graph/shard elasticity. It is deliberately separate from the placement catalog and
the online reshard actuator: a planner run produces proposals, while the existing
policy-gated placement/reshard owner remains the only component allowed to mutate
authority.

## Inputs and admission

Each shard contributes a typed, bounded observation containing resident bytes and
nodes, read/write rates, queue depth, fsync p99, CPU, fan-out, delta-stream estimate,
placement state, followers, and placement epoch. Each node contributes observed load,
hard per-axis limits, network throughput, object-tier headroom, and availability.

Pressure and target admission use all eight axes with fixed versioned weights. A node
must fit the projected shard in every axis and must remain inside queue, fsync, and
fan-out SLO limits. A proposal is rejected with a typed `InsufficientCapacity`,
`SloRisk`, `BudgetExceeded`, `NoSafeTarget`, `CooldownActive`, or
`HysteresisNotCrossed` reason when the corresponding bound is not met. The planner
does not infer capacity from resident-node count alone.

The input, proposal count, follower list, graph identifiers, copy window, network
bytes, and budget are explicitly bounded. Controllers with larger inventories should
submit deterministic windows rather than materialize an unbounded per-million-shard
plan.

## Restart and no-loss identity

Every move derives a stable `move_id`, snapshot ID, delta-stream ID, and fenced-cutover
ID from graph, shard, source, target, placement epoch, and move kind. A
`MoveCheckpoint` carries the phase, snapshot epoch, delta cursor, copied bytes, and
applied-delta count. Replanning an active checkpoint resumes the same identity; a
different target or placement epoch cannot overwrite it. The fence ID is surfaced as
the proposal's no-loss/deduplication token. No proposal is a completion receipt.

## Closed-loop handoff

The intended control loop is:

1. Collect bounded observations from engine/node telemetry.
2. Run `CrossNodeElasticityPlanner::plan` on a canonical input ordering.
3. Persist proposals and abort evidence under the existing action policy.
4. Let the placement/reshard owner execute snapshot → delta catch-up → fenced
   cutover, checkpointing progress durably.
5. Re-read placement and telemetry, then plan again after cooldown/hysteresis.

Cold placement is represented explicitly. A read-demanded cold shard may receive a
`Hydrate` proposal whose cost includes object-tier bytes; an already hydrating,
snapshotting, delta-catching-up, or fenced shard is never duplicated by a fresh
proposal.
