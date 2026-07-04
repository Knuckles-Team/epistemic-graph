//! Recorded operation history (CONCEPT:AU-KG.ontology.emits-database-ontology-entities).
//!
//! The load generator records every mutation it issues as two events — an
//! `Invoke` when the write is dispatched and a `Complete` (ack OR err) when the
//! response returns — each timestamped. The checker replays this history against
//! the cluster's final state to assert the invariants (acked-write-survives,
//! no-loss). A simple append-only log, locked behind a `Mutex`; the volumes here
//! (thousands of ops) make a fancier lock-free structure unnecessary.

use std::sync::Mutex;
use std::time::Instant;

/// The outcome of a single write as the client observed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The write was ACKED by the leader (committed + applied). It MUST survive.
    Acked,
    /// The write returned an error (e.g. ForwardToLeader, partitioned, timeout).
    /// It may or may not have committed — the checker only requires that an error
    /// is NEVER later observed as a phantom extra node beyond what was acked.
    Err(String),
}

/// One completed operation in the history.
#[derive(Debug, Clone)]
pub struct Op {
    /// Monotonic per-op id (also the node_id suffix written: `n{seq}`).
    pub seq: u64,
    /// Which writer task issued it.
    pub writer: usize,
    /// When the write was dispatched.
    pub invoked: Instant,
    /// When the response returned.
    pub completed: Instant,
    /// The observed outcome.
    pub outcome: Outcome,
}

impl Op {
    /// The graph node id this op writes (`n{seq}`) — the checker counts these.
    pub fn node_id(&self) -> String {
        format!("n{}", self.seq)
    }
}

/// An append-only, thread-safe history the writers record into.
#[derive(Default)]
pub struct History {
    ops: Mutex<Vec<Op>>,
}

impl History {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a completed op.
    pub fn record(&self, op: Op) {
        self.ops.lock().unwrap().push(op);
    }

    /// A snapshot copy of every recorded op (for the checker).
    pub fn snapshot(&self) -> Vec<Op> {
        self.ops.lock().unwrap().clone()
    }

    /// The set of `seq` values whose write was ACKED — these MUST survive.
    pub fn acked_seqs(&self) -> Vec<u64> {
        self.ops
            .lock()
            .unwrap()
            .iter()
            .filter(|o| o.outcome == Outcome::Acked)
            .map(|o| o.seq)
            .collect()
    }

    /// Total ops recorded.
    pub fn len(&self) -> usize {
        self.ops.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Count of acked ops.
    pub fn acked_count(&self) -> usize {
        self.ops
            .lock()
            .unwrap()
            .iter()
            .filter(|o| o.outcome == Outcome::Acked)
            .count()
    }

    /// Count of errored ops.
    pub fn err_count(&self) -> usize {
        self.ops
            .lock()
            .unwrap()
            .iter()
            .filter(|o| matches!(o.outcome, Outcome::Err(_)))
            .count()
    }
}
