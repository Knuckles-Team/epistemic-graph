//! Execution-trace records and runtime-quota types
//! (EG-DECISION-ENGINE-R133.1).
//!
//! The engine stores each agent run as a typed trace fact and decides the
//! run's runtime quota; the harness enforces what the decision handed it.
//! This module owns the wire records only: the quota defaults, the tenant
//! ceilings and the request-resolution behaviour are separate decisions.

use serde::{Deserialize, Serialize};

use crate::contract::BoundedVec;

/// The largest number of steps one stored run trace may record.
pub const MAX_RUN_TRACE_STEPS: usize = 256;

/// The execution trace of one agent run, stored as a typed fact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RunTraceRecord {
    pub run_id: String,
    pub task_signature: String,
    /// The run's steps, in run order.
    pub steps: BoundedVec<String, MAX_RUN_TRACE_STEPS>,
    /// The outcome the run ended in; only a successful run admits
    /// compilation into a stored program (EG-DECISION-ENGINE-R135).
    pub outcome: String,
    /// The run's start clock, in milliseconds.
    pub started_at: u64,
}

/// The runtime quota the engine decided for one run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RuntimeQuota {
    pub wall_time_ms: u64,
    pub memory_bytes: u64,
    pub process_count: u32,
    pub grace_ms: u64,
}

#[cfg(test)]
mod tests;
