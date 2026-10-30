//! Programs compiled from successful runs, keyed by task signature
//! (EG-DECISION-ENGINE-R135.1).
//!
//! A successful run compiles into a stored state-machine program: literals
//! become parameters, step order is preserved, and a program holds at most
//! [`MAX_PROGRAM_STEPS`] steps. Lookup by task signature returns the stored
//! program, or none.

use serde::{Deserialize, Serialize};

use crate::contract::BoundedVec;

/// The largest number of steps one stored program may hold.
pub const MAX_PROGRAM_STEPS: usize = 40;

/// One step of a compiled program's state machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ProgramStep {
    pub state: String,
    pub action: String,
    /// The state this step transitions to.
    pub next: String,
}

/// A stored program: the compiled form of one successful run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CompiledProgram {
    pub task_signature: String,
    pub steps: BoundedVec<ProgramStep, MAX_PROGRAM_STEPS>,
    /// The names of the parameters the run's literals were replaced by.
    pub parameters: BoundedVec<String, 64>,
    pub source_run_id: String,
}

#[cfg(test)]
mod tests;
