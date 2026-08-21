//! Bounded shard/Raft drain contract (NE-167).
//!
//! This is deliberately a small, side-effect-free state machine.  It does not
//! start workers, move data, or choose a replica count.  The live shard/Raft
//! owner supplies observations and applies the resulting membership change;
//! this module makes the ordering and safety gates explicit so that an
//! orchestrator cannot mistake a request for an observed drain.
//!
//! The durable operation identity is `(operation_id, revision, fence)`.  Every
//! acknowledgement carries that complete identity.  A caller with an old
//! revision or fence receives [`DrainError::StaleIdentity`] and cannot advance
//! the operation.

use std::collections::BTreeSet;
use std::fmt;

use serde::{Deserialize, Serialize};

use super::{GroupId, NodeId};

/// Bound operation ids so a malformed/replayed request cannot grow the durable
/// drain journal without limit.
const MAX_OPERATION_ID_BYTES: usize = 128;

/// The identity that fences one drain attempt from every prior or subsequent
/// attempt for the same shard.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DrainIdentity {
    /// Stable caller-visible id for the drain request.
    pub operation_id: String,
    /// Monotonic revision of the shard/Raft membership view.
    pub revision: u64,
    /// Opaque fencing value issued by the authoritative membership owner.
    pub fence: u64,
}

impl DrainIdentity {
    /// Build a valid identity.  Zero is never a usable revision or fence: it is
    /// reserved for an absent/uninitialized value during restart recovery.
    pub fn new(
        operation_id: impl Into<String>,
        revision: u64,
        fence: u64,
    ) -> Result<Self, DrainError> {
        let operation_id = operation_id.into();
        if operation_id.is_empty()
            || operation_id.len() > MAX_OPERATION_ID_BYTES
            || revision == 0
            || fence == 0
        {
            return Err(DrainError::InvalidIdentity);
        }
        Ok(Self {
            operation_id,
            revision,
            fence,
        })
    }
}

/// The pre-commit membership and safety contract for one node drain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardDrainPlan {
    pub identity: DrainIdentity,
    /// The Raft group whose durable shard is being drained.
    pub group_id: GroupId,
    /// The local durable shard index owned by `group_id`.
    pub shard_id: u32,
    /// The node that must leave this group after the drain observation.
    pub target_node: NodeId,
    /// Voters before the drain.  This is retained for rollback and replay.
    pub original_voters: BTreeSet<NodeId>,
    /// Exact voter set expected after the one planned shrink.
    pub voters_after: BTreeSet<NodeId>,
    /// The authoritative writer that must remain available throughout the
    /// operation.  Leader transfer is a separate, explicit operation.
    pub authoritative_writer: NodeId,
    /// Minimum voters required to retain a Raft quorum after the shrink.
    pub quorum_required: usize,
    /// PDB-equivalent floor: the minimum number of observed healthy voters.
    pub minimum_healthy_voters: usize,
}

impl ShardDrainPlan {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        identity: DrainIdentity,
        group_id: GroupId,
        shard_id: u32,
        target_node: NodeId,
        original_voters: BTreeSet<NodeId>,
        voters_after: BTreeSet<NodeId>,
        authoritative_writer: NodeId,
        quorum_required: usize,
        minimum_healthy_voters: usize,
    ) -> Result<Self, DrainError> {
        let plan = Self {
            identity,
            group_id,
            shard_id,
            target_node,
            original_voters,
            voters_after,
            authoritative_writer,
            quorum_required,
            minimum_healthy_voters,
        };
        plan.validate()?;
        Ok(plan)
    }

    fn validate(&self) -> Result<(), DrainError> {
        if self.original_voters.is_empty()
            || self.voters_after.is_empty()
            || self.quorum_required == 0
            || self.minimum_healthy_voters == 0
            || self.quorum_required > self.original_voters.len()
            || self.voters_after.len() < self.quorum_required
            || self.voters_after.len() < self.minimum_healthy_voters
            || !self.original_voters.contains(&self.authoritative_writer)
            || !self.original_voters.contains(&self.target_node)
            || self.target_node == self.authoritative_writer
            || self.voters_after.contains(&self.target_node)
            || !self
                .voters_after
                .iter()
                .all(|node| self.original_voters.contains(node))
            || !self.voters_after.contains(&self.authoritative_writer)
        {
            return Err(DrainError::InvalidPlan);
        }
        Ok(())
    }

    fn validates_voter_observation(
        &self,
        voters: &BTreeSet<NodeId>,
        authoritative_writer: NodeId,
        healthy_voters: usize,
    ) -> Result<(), DrainError> {
        if voters != &self.voters_after {
            return Err(DrainError::VoterSetMismatch);
        }
        if authoritative_writer != self.authoritative_writer
            || !voters.contains(&self.authoritative_writer)
        {
            return Err(DrainError::AuthoritativeWriterNotPreserved);
        }
        if voters.len() < self.quorum_required {
            return Err(DrainError::QuorumSafety);
        }
        if healthy_voters < self.minimum_healthy_voters {
            return Err(DrainError::HealthyVoterFloor);
        }
        Ok(())
    }
}

/// Durable phase of a drain operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DrainPhase {
    Proposed,
    AdmissionStopRequested,
    AdmissionStopped,
    DrainObserved,
    ShrinkRequested,
    ShrinkCommitted,
    Completed,
    RollbackRequired,
    RolledBack,
    /// A restart saw an in-flight pre-shrink operation.  No old observation or
    /// command may resume it; a new fenced operation must be planned.
    RecoveryRequired,
}

impl DrainPhase {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::RolledBack)
    }
}

/// Bounded failure reasons retained with the operation rather than arbitrary
/// operator text.  This keeps replay deterministic and the journal compact.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DrainFailure {
    PostShrinkApplyFailed,
    AuthoritativeWriterUnavailable,
    QuorumSafetyLost,
    HealthyVoterFloorLost,
    AdmissionStillOpen,
    WorkStillInFlight,
}

/// Events are acknowledgements/observations from the live owner.  They are not
/// commands that perform side effects.  Each variant carries the complete
/// [`DrainIdentity`] so stale events can never be accepted accidentally.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "event")]
pub enum DrainEvent {
    AdmissionStopRequested {
        identity: DrainIdentity,
    },
    AdmissionStopped {
        identity: DrainIdentity,
        /// Must be false.  A request to stop admission is not an observation
        /// until the owner confirms new work is rejected.
        accepting: bool,
        /// Existing work may remain in flight while admission is stopped.
        in_flight: u64,
    },
    DrainObserved {
        identity: DrainIdentity,
        /// Explicitly repeats the admission gate at the drain observation.
        admission_stopped: bool,
        /// Must be zero.  This is the distinction between a drain request and
        /// an observed, safe-to-shrink drain.
        outstanding_work: u64,
        authoritative_writer: NodeId,
        healthy_voters: usize,
    },
    ShrinkRequested {
        identity: DrainIdentity,
    },
    ShrinkCommitted {
        identity: DrainIdentity,
        voters_after: BTreeSet<NodeId>,
        authoritative_writer: NodeId,
        healthy_voters: usize,
    },
    Completed {
        identity: DrainIdentity,
    },
    PostShrinkFailure {
        identity: DrainIdentity,
        reason: DrainFailure,
    },
    RollbackCommitted {
        identity: DrainIdentity,
        restored_voters: BTreeSet<NodeId>,
        authoritative_writer: NodeId,
        healthy_voters: usize,
    },
}

impl DrainEvent {
    fn identity(&self) -> &DrainIdentity {
        match self {
            Self::AdmissionStopRequested { identity }
            | Self::AdmissionStopped { identity, .. }
            | Self::DrainObserved { identity, .. }
            | Self::ShrinkRequested { identity }
            | Self::ShrinkCommitted { identity, .. }
            | Self::Completed { identity }
            | Self::PostShrinkFailure { identity, .. }
            | Self::RollbackCommitted { identity, .. } => identity,
        }
    }
}

/// Typed state machine for a one-node shard/Raft drain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardDrain {
    pub plan: ShardDrainPlan,
    pub phase: DrainPhase,
    pub failure: Option<DrainFailure>,
}

impl ShardDrain {
    pub fn propose(plan: ShardDrainPlan) -> Result<Self, DrainError> {
        plan.validate()?;
        Ok(Self {
            plan,
            phase: DrainPhase::Proposed,
            failure: None,
        })
    }

    pub fn identity(&self) -> &DrainIdentity {
        &self.plan.identity
    }

    /// Replay a bounded event sequence from the durable plan.  Replay uses the
    /// same identity and phase gates as live acknowledgements; a truncated,
    /// reordered, or stale journal therefore fails closed instead of yielding a
    /// plausible but unsafe completed state.
    pub fn replay(
        plan: ShardDrainPlan,
        events: impl IntoIterator<Item = DrainEvent>,
    ) -> Result<Self, DrainError> {
        let mut drain = Self::propose(plan)?;
        for event in events {
            drain.apply(event)?;
        }
        // Replay is the restart path, not a live command-folding helper.  Do
        // not return an in-flight state that a caller could accidentally treat
        // as safe to continue.
        drain.recover_after_restart();
        Ok(drain)
    }

    /// Apply one observed lifecycle event.  Identity is checked before phase,
    /// so an old ack is always reported as stale rather than accidentally being
    /// interpreted as a valid event for the current phase.
    pub fn apply(&mut self, event: DrainEvent) -> Result<(), DrainError> {
        if event.identity() != &self.plan.identity {
            return Err(DrainError::StaleIdentity);
        }

        match event {
            DrainEvent::AdmissionStopRequested { .. } => {
                self.require_phase(DrainPhase::Proposed)?;
                self.phase = DrainPhase::AdmissionStopRequested;
            }
            DrainEvent::AdmissionStopped { accepting, .. } => {
                self.require_phase(DrainPhase::AdmissionStopRequested)?;
                if accepting {
                    return Err(DrainError::AdmissionMustBeStopped);
                }
                self.phase = DrainPhase::AdmissionStopped;
            }
            DrainEvent::DrainObserved {
                admission_stopped,
                outstanding_work,
                authoritative_writer,
                healthy_voters,
                ..
            } => {
                self.require_phase(DrainPhase::AdmissionStopped)?;
                if !admission_stopped {
                    return Err(DrainError::AdmissionMustBeStopped);
                }
                if outstanding_work != 0 {
                    return Err(DrainError::WorkStillInFlight);
                }
                if authoritative_writer != self.plan.authoritative_writer {
                    return Err(DrainError::AuthoritativeWriterNotPreserved);
                }
                if healthy_voters < self.plan.minimum_healthy_voters {
                    return Err(DrainError::HealthyVoterFloor);
                }
                self.phase = DrainPhase::DrainObserved;
            }
            DrainEvent::ShrinkRequested { .. } => {
                self.require_phase(DrainPhase::DrainObserved)?;
                self.phase = DrainPhase::ShrinkRequested;
            }
            DrainEvent::ShrinkCommitted {
                voters_after,
                authoritative_writer,
                healthy_voters,
                ..
            } => {
                self.require_phase(DrainPhase::ShrinkRequested)?;
                self.plan.validates_voter_observation(
                    &voters_after,
                    authoritative_writer,
                    healthy_voters,
                )?;
                self.phase = DrainPhase::ShrinkCommitted;
            }
            DrainEvent::Completed { .. } => {
                self.require_phase(DrainPhase::ShrinkCommitted)?;
                self.phase = DrainPhase::Completed;
            }
            DrainEvent::PostShrinkFailure { reason, .. } => {
                if self.phase != DrainPhase::ShrinkCommitted {
                    return Err(DrainError::UnexpectedPhase);
                }
                self.failure = Some(reason);
                self.phase = DrainPhase::RollbackRequired;
            }
            DrainEvent::RollbackCommitted {
                restored_voters,
                authoritative_writer,
                healthy_voters,
                ..
            } => {
                self.require_phase(DrainPhase::RollbackRequired)?;
                if restored_voters != self.plan.original_voters {
                    return Err(DrainError::VoterSetMismatch);
                }
                if authoritative_writer != self.plan.authoritative_writer
                    || !restored_voters.contains(&self.plan.authoritative_writer)
                {
                    return Err(DrainError::AuthoritativeWriterNotPreserved);
                }
                if restored_voters.len() < self.plan.quorum_required {
                    return Err(DrainError::QuorumSafety);
                }
                if healthy_voters < self.plan.minimum_healthy_voters {
                    return Err(DrainError::HealthyVoterFloor);
                }
                self.phase = DrainPhase::RolledBack;
            }
        }
        Ok(())
    }

    /// Reconcile a deserialized state after process restart.  Any in-flight
    /// pre-shrink phase is made unusable until a new fenced operation is
    /// planned.  A committed shrink is different: the side effect may already
    /// be durable, so recovery requires an explicit rollback observation.
    pub fn recover_after_restart(&mut self) -> DrainPhase {
        self.phase = match self.phase {
            DrainPhase::Proposed
            | DrainPhase::Completed
            | DrainPhase::RolledBack
            | DrainPhase::RecoveryRequired
            | DrainPhase::RollbackRequired => self.phase,
            DrainPhase::ShrinkCommitted => {
                self.failure = Some(DrainFailure::PostShrinkApplyFailed);
                DrainPhase::RollbackRequired
            }
            DrainPhase::AdmissionStopRequested
            | DrainPhase::AdmissionStopped
            | DrainPhase::DrainObserved
            | DrainPhase::ShrinkRequested => DrainPhase::RecoveryRequired,
        };
        self.phase
    }

    fn require_phase(&self, expected: DrainPhase) -> Result<(), DrainError> {
        if self.phase == expected {
            Ok(())
        } else if self.phase == DrainPhase::RecoveryRequired {
            Err(DrainError::RecoveryRequired)
        } else if self.phase == DrainPhase::RollbackRequired {
            Err(DrainError::RollbackRequired)
        } else {
            Err(DrainError::UnexpectedPhase)
        }
    }
}

/// Errors are intentionally small and stable: callers can make a policy
/// decision without parsing operator text, and replay remains deterministic.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DrainError {
    InvalidIdentity,
    InvalidPlan,
    StaleIdentity,
    UnexpectedPhase,
    AdmissionMustBeStopped,
    WorkStillInFlight,
    AuthoritativeWriterNotPreserved,
    QuorumSafety,
    HealthyVoterFloor,
    VoterSetMismatch,
    RecoveryRequired,
    RollbackRequired,
}

impl fmt::Display for DrainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidIdentity => "invalid drain identity",
            Self::InvalidPlan => "invalid shard drain plan",
            Self::StaleIdentity => "stale drain revision or fence",
            Self::UnexpectedPhase => "drain event is invalid for the current phase",
            Self::AdmissionMustBeStopped => "admission must be stopped before draining",
            Self::WorkStillInFlight => "drain still has outstanding work",
            Self::AuthoritativeWriterNotPreserved => {
                "authoritative writer is unavailable or was not preserved"
            }
            Self::QuorumSafety => "shrink would violate the quorum floor",
            Self::HealthyVoterFloor => "observed healthy voters are below the safety floor",
            Self::VoterSetMismatch => "observed voter set does not match the fenced plan",
            Self::RecoveryRequired => "restart recovery requires a new fenced operation",
            Self::RollbackRequired => "rollback must complete before another drain event",
        };
        f.write_str(message)
    }
}

impl std::error::Error for DrainError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn voters(nodes: &[NodeId]) -> BTreeSet<NodeId> {
        nodes.iter().copied().collect()
    }

    fn identity() -> DrainIdentity {
        DrainIdentity::new("drain-1", 7, 99).expect("test identity")
    }

    fn plan() -> ShardDrainPlan {
        ShardDrainPlan::new(
            identity(),
            4,
            4,
            3,
            voters(&[1, 2, 3]),
            voters(&[1, 2]),
            1,
            2,
            2,
        )
        .expect("test plan")
    }

    fn drain_at_shrink_commit() -> ShardDrain {
        let id = identity();
        let mut drain = ShardDrain::propose(plan()).expect("propose");
        drain
            .apply(DrainEvent::AdmissionStopRequested {
                identity: id.clone(),
            })
            .expect("request admission stop");
        drain
            .apply(DrainEvent::AdmissionStopped {
                identity: id.clone(),
                accepting: false,
                in_flight: 2,
            })
            .expect("admission stopped");
        drain
            .apply(DrainEvent::DrainObserved {
                identity: id.clone(),
                admission_stopped: true,
                outstanding_work: 0,
                authoritative_writer: 1,
                healthy_voters: 3,
            })
            .expect("drain observed");
        drain
            .apply(DrainEvent::ShrinkRequested {
                identity: id.clone(),
            })
            .expect("request shrink");
        drain
            .apply(DrainEvent::ShrinkCommitted {
                identity: id,
                voters_after: voters(&[1, 2]),
                authoritative_writer: 1,
                healthy_voters: 2,
            })
            .expect("shrink committed");
        drain
    }

    #[test]
    fn happy_path_requires_observed_zero_work_before_shrink() {
        let id = identity();
        let mut drain = ShardDrain::propose(plan()).expect("propose");
        drain
            .apply(DrainEvent::AdmissionStopRequested {
                identity: id.clone(),
            })
            .expect("request admission stop");
        drain
            .apply(DrainEvent::AdmissionStopped {
                identity: id.clone(),
                accepting: false,
                in_flight: 1,
            })
            .expect("admission stopped");
        drain
            .apply(DrainEvent::DrainObserved {
                identity: id.clone(),
                admission_stopped: true,
                outstanding_work: 0,
                authoritative_writer: 1,
                healthy_voters: 2,
            })
            .expect("zero-work observation");
        drain
            .apply(DrainEvent::ShrinkRequested {
                identity: id.clone(),
            })
            .expect("request shrink");
        drain
            .apply(DrainEvent::ShrinkCommitted {
                identity: id.clone(),
                voters_after: voters(&[1, 2]),
                authoritative_writer: 1,
                healthy_voters: 2,
            })
            .expect("commit shrink");
        drain
            .apply(DrainEvent::Completed { identity: id })
            .expect("complete");
        assert_eq!(drain.phase, DrainPhase::Completed);
    }

    #[test]
    fn stale_ack_cannot_advance_current_revision_or_fence() {
        let mut drain = ShardDrain::propose(plan()).expect("propose");
        let stale = DrainIdentity::new("drain-1", 6, 98).expect("stale identity");
        let error = drain
            .apply(DrainEvent::AdmissionStopRequested { identity: stale })
            .expect_err("stale event denied");
        assert_eq!(error, DrainError::StaleIdentity);
        assert_eq!(drain.phase, DrainPhase::Proposed);
    }

    #[test]
    fn replay_rejects_reordered_or_stale_events() {
        let id = identity();
        let recovered = ShardDrain::replay(
            plan(),
            [DrainEvent::AdmissionStopRequested {
                identity: id.clone(),
            }],
        )
        .expect("partial replay is represented as recovery-required");
        assert_eq!(recovered.phase, DrainPhase::RecoveryRequired);

        let error = ShardDrain::replay(
            plan(),
            [DrainEvent::AdmissionStopped {
                identity: id.clone(),
                accepting: false,
                in_flight: 0,
            }],
        )
        .expect_err("replay cannot skip admission-stop request");
        assert_eq!(error, DrainError::UnexpectedPhase);

        let stale = DrainIdentity::new("drain-1", 6, 99).expect("stale identity");
        let error = ShardDrain::replay(
            plan(),
            [DrainEvent::AdmissionStopRequested { identity: stale }],
        )
        .expect_err("replay cannot accept an old revision");
        assert_eq!(error, DrainError::StaleIdentity);
    }

    #[test]
    fn writer_quorum_and_health_gates_are_fail_closed() {
        let mut drain = drain_at_shrink_commit();
        let id = identity();
        drain
            .apply(DrainEvent::PostShrinkFailure {
                identity: id.clone(),
                reason: DrainFailure::QuorumSafetyLost,
            })
            .expect("failure moves to rollback");
        assert_eq!(drain.phase, DrainPhase::RollbackRequired);

        let error = drain
            .apply(DrainEvent::RollbackCommitted {
                identity: id,
                restored_voters: voters(&[1, 2, 3]),
                authoritative_writer: 2,
                healthy_voters: 3,
            })
            .expect_err("writer replacement is not implicit");
        assert_eq!(error, DrainError::AuthoritativeWriterNotPreserved);
        assert_eq!(drain.phase, DrainPhase::RollbackRequired);
    }

    #[test]
    fn restart_pre_shrink_is_recovery_required_and_committed_shrink_requires_rollback() {
        let id = identity();
        let mut pre_shrink = ShardDrain::propose(plan()).expect("propose");
        pre_shrink
            .apply(DrainEvent::AdmissionStopRequested {
                identity: id.clone(),
            })
            .expect("request admission stop");
        assert_eq!(
            pre_shrink.recover_after_restart(),
            DrainPhase::RecoveryRequired
        );
        let error = pre_shrink
            .apply(DrainEvent::AdmissionStopped {
                identity: id,
                accepting: false,
                in_flight: 0,
            })
            .expect_err("recovery cannot resume old operation");
        assert_eq!(error, DrainError::RecoveryRequired);

        let mut committed = drain_at_shrink_commit();
        assert_eq!(
            committed.recover_after_restart(),
            DrainPhase::RollbackRequired
        );
        let id = identity();
        committed
            .apply(DrainEvent::RollbackCommitted {
                identity: id,
                restored_voters: voters(&[1, 2, 3]),
                authoritative_writer: 1,
                healthy_voters: 3,
            })
            .expect("rollback after restart");
        assert_eq!(committed.phase, DrainPhase::RolledBack);
    }

    #[test]
    fn invalid_plan_cannot_remove_authoritative_writer_or_break_quorum() {
        let writer_target = ShardDrainPlan::new(
            identity(),
            4,
            4,
            1,
            voters(&[1, 2, 3]),
            voters(&[2, 3]),
            1,
            2,
            2,
        )
        .expect_err("leader removal needs an explicit transfer operation");
        assert_eq!(writer_target, DrainError::InvalidPlan);

        let quorum_break = ShardDrainPlan::new(
            identity(),
            4,
            4,
            3,
            voters(&[1, 2, 3]),
            voters(&[1]),
            1,
            2,
            1,
        )
        .expect_err("quorum floor");
        assert_eq!(quorum_break, DrainError::InvalidPlan);
    }
}
