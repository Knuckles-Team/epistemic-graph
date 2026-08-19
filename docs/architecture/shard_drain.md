# Bounded shard/Raft drain contract (NE-167)

`src/raft/drain.rs` owns the small, side-effect-free state machine used by a
shard/Raft membership owner before it removes a node. It is a contract, not a
second autoscaler: the caller still decides when to propose a drain and the
live owner still performs the admission and membership operations.

## Safety invariants

- The durable identity is `(operation_id, revision, fence)`. Every event carries
  all three values; an old revision or fence is rejected as a stale identity.
- Admission must be observed closed before draining begins. A drain observation
  must report `outstanding_work == 0`; an issued request is never treated as
  completion.
- A plan names the authoritative writer and retains the original and expected
  voter sets. The plan cannot target its authoritative writer, remove that
  writer implicitly, or commit a voter set below either the Raft quorum floor or
  the configured minimum healthy-voter (PDB-equivalent) floor.
- A committed shrink is followed by an explicit completion event. A
  post-shrink failure enters `RollbackRequired` and can only leave that state
  after the original voter set and authoritative writer are observed restored.
- Restart/replay is fail closed. In-flight pre-shrink phases become
  `RecoveryRequired` and cannot accept old events. A persisted committed shrink
  becomes `RollbackRequired`, because the side effect may already be durable.
  Event replay uses the same phase and identity checks as live acknowledgements;
  truncated or reordered journals are rejected.

## Event sequence

```text
Proposed
  -> AdmissionStopRequested -> AdmissionStopped
  -> DrainObserved(outstanding_work=0)
  -> ShrinkRequested -> ShrinkCommitted -> Completed
                                  \-> PostShrinkFailure -> RollbackRequired
                                                            -> RolledBack
```

The state machine does not spawn tasks, alter Raft membership, or bypass the
authoritative writer. A runtime integration must persist/replay the operation
through the existing durable authority and must supply fresh observations after
restart. The focused unit fixtures in `src/raft/drain.rs` cover happy-path
ordering, stale acknowledgements, writer/quorum/health gates, rollback, and
restart recovery. Root validation should run the feature-gated module tests,
then the existing Raft/cluster test selectors and the repository's normal
source/documentation gates.
