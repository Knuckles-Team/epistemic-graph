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

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::context::Context;
use crate::model::{DefId, StateId, StatechartDef};

/// An instance identifier (`sc-<hex>`).
pub type InstanceId = String;

/// The **active configuration** of a running machine (CONCEPT:INT-P2-2, hierarchy
/// phase).
///
/// A flat machine's "state" is a single id; a *hierarchical / parallel* machine is in a
/// SET of states at once — every currently-active state, from the innermost active leaf
/// up through each of its composite ancestors, plus (for a parallel state) one active
/// branch per orthogonal region. `active` is exactly that set.
///
/// A flat chart is the degenerate case: `active` is a one-element set `{leaf}` and
/// `history` is empty, so nothing about the flat interpreter's observable behaviour
/// changes — the turnstile is `{"locked"}` then `{"unlocked"}`.
///
/// `history` is the per-composite memory backing shallow/deep history pseudo-states: it
/// maps a composite state id (one that carries a [`crate::model::HistoryKind`] marker)
/// to the set of its descendant states that were active the last time it was exited, so
/// re-entering that composite can RESUME instead of restarting from its default child.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Configuration {
    /// Every currently-active state id (leaves AND their active composite ancestors).
    /// A `BTreeSet` so the configuration serializes deterministically (the same byte
    /// stability the `context` relies on) and equality is order-independent.
    pub active: BTreeSet<StateId>,
    /// History memory: composite id → the descendant states active at its last exit.
    #[serde(default)]
    pub history: BTreeMap<StateId, BTreeSet<StateId>>,
}

impl Configuration {
    /// The degenerate flat configuration: a single active atomic state, no history.
    pub fn atomic(id: impl Into<String>) -> Self {
        let mut active = BTreeSet::new();
        active.insert(id.into());
        Self {
            active,
            history: BTreeMap::new(),
        }
    }

    /// Whether `id` is currently active.
    pub fn contains(&self, id: &str) -> bool {
        self.active.contains(id)
    }

    /// Whether the configuration is empty (no active state — only a not-yet-entered
    /// machine).
    pub fn is_empty(&self) -> bool {
        self.active.is_empty()
    }

    /// The active ATOMIC (leaf) states — active states that declare no children. For a
    /// flat machine this is the whole configuration; for a hierarchical one it is the
    /// innermost active states, one per parallel branch.
    pub fn leaves<'a>(&'a self, def: &'a StatechartDef) -> Vec<&'a str> {
        self.active
            .iter()
            .filter(|id| def.state(id.as_str()).map_or(true, |s| s.children.is_empty()))
            .map(|s| s.as_str())
            .collect()
    }

    /// Whether the configuration contains a member of the definition's F — i.e. the
    /// machine has reached a final/accepting state.
    pub fn is_final(&self, def: &StatechartDef) -> bool {
        self.active.iter().any(|id| def.is_final(id))
    }
}

/// Where an instance is in its lifecycle. Kept intentionally tiny — the discrete
/// active states live in `MachineInstance::configuration`; this is only the
/// terminal/non-terminal distinction the store and callers care about.
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
    /// The current active CONFIGURATION — the set of active states (plus history
    /// memory). For a flat chart this is a single-element set; for a hierarchical /
    /// parallel chart it is the full active set. Replaces the phase-1 single `state`
    /// string.
    pub configuration: Configuration,
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

    /// Whether `id` is one of the instance's currently-active states. The
    /// configuration-aware replacement for the old `self.state == id` check.
    pub fn in_state(&self, id: &str) -> bool {
        self.configuration.contains(id)
    }
}
