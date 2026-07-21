//! The durable **machine instance** — a running statechart as one small row
//! (CONCEPT:INT-P2-2).
//!
//! A running machine is nothing more than `(state, context)` plus bookkeeping. That
//! is the entire point of requirement 3: a long-running agent that is *waiting days
//! for an event* does not need a live task, a thread, or a parked coroutine — it is
//! just a [`MachineInstance`] row on disk. When the event finally arrives, the row is
//! read back (rehydrated), the pure [`crate::transition::transition`] function is
//! applied, and the new `(state, context)` is written back. Nothing about the machine
//! lives in memory between events.
//!
//! Like the other durable records in this engine (e.g. `eg-jobs`' `AnalyticsJob`), the
//! instance carries an OCC `version` that increments on every FIRING transition, so a
//! caller can do compare-and-set updates and detect a lost-update race.

use serde::{Deserialize, Serialize};

use crate::context::Context;
use crate::model::{DefId, StateId};

/// An instance identifier (`sc-<hex>`).
pub type InstanceId = String;

/// Where an instance is in its lifecycle. Kept intentionally tiny — the discrete
/// STATE lives in `MachineInstance::state`; this is only the terminal/non-terminal
/// distinction the store and callers care about.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstanceStatus {
    /// The instance is in a non-final state and can still transition.
    Active,
    /// The instance's current state is a member of F — it has reached an accepting /
    /// terminal state. Events still arrive as well-defined no-ops (finals have no
    /// outgoing edges), so this is a convenience marker, not a lock.
    Final,
}

/// A durable, rehydratable machine instance: `(state, context)` + OCC + provenance
/// (CONCEPT:INT-P2-2).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MachineInstance {
    /// Server-issued instance id.
    pub instance_id: InstanceId,
    /// The definition this instance runs (content-addressed [`DefId`]).
    pub def_id: DefId,
    /// The current discrete state (a member of the definition's S).
    pub state: StateId,
    /// The current extended state.
    pub context: Context,
    /// OCC version — increments on every FIRING transition (a no-op does not bump it).
    pub version: u64,
    /// Terminal/non-terminal marker (see [`InstanceStatus`]).
    pub status: InstanceStatus,
    /// Opaque tenancy scope (who owns this instance). Set at instantiation.
    pub tenant: String,
    /// Opaque actor scope (who created it). Set at instantiation.
    pub actor: String,
    /// Count of events DELIVERED to this instance, including no-ops — a diagnostic
    /// distinct from `version` (which counts only firing transitions).
    #[serde(default)]
    pub events_seen: u64,
    /// Count of FIRING transitions — always equal to `version` for a fresh instance,
    /// retained explicitly for readability in status responses.
    #[serde(default)]
    pub transitions_fired: u64,
    /// Creation time (Unix ms).
    pub created_at_ms: i64,
    /// Last-update time (Unix ms).
    pub updated_at_ms: i64,
}

impl MachineInstance {
    /// Whether the instance has reached a final state.
    pub fn is_final(&self) -> bool {
        matches!(self.status, InstanceStatus::Final)
    }
}
