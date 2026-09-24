//! The governed pointer behind every retrieval-learning activation (EH-396,
//! EH-397): which query adapter a space serves, which embedding generation a
//! logical graph resolves to.
//!
//! A pointer moves only by an audited event naming the principal and the
//! passing receipt that qualified its target, and every move is reversible:
//! `stack` holds the targets a rollback returns to (most recent last) and
//! `history` the bounded audit of every move.

use serde::{Deserialize, Serialize};

use crate::contract::BoundedVec;

/// Most events a pointer's stack and history keep (oldest dropped first).
pub const MAX_POINTER_HISTORY: usize = 64;

/// Which way an event moved a pointer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PointerTransition {
    Activated,
    RolledBack,
}

/// One audited pointer move.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PointerEvent {
    pub transition: PointerTransition,
    /// What the pointer names after the move (an adapter digest, a graph);
    /// `None` = the unadapted default.
    #[serde(default)]
    pub target: Option<String>,
    /// The receipt that qualified `target`.
    #[serde(default)]
    pub receipt_digest: Option<String>,
    pub principal: String,
    pub at_ms: u64,
}

/// A pointer, its rollback stack and its audit history.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PointerState {
    /// What the pointer is for (`adapter:<space digest>`, `generation:<graph>`).
    pub key: String,
    #[serde(default)]
    pub active: Option<PointerEvent>,
    #[serde(default)]
    pub stack: BoundedVec<PointerEvent, MAX_POINTER_HISTORY>,
    #[serde(default)]
    pub history: BoundedVec<PointerEvent, MAX_POINTER_HISTORY>,
}

impl PointerState {
    /// The active target, if any.
    pub fn target(&self) -> Option<&str> {
        self.active
            .as_ref()
            .and_then(|event| event.target.as_deref())
    }
}
