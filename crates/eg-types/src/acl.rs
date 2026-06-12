//! Identity / ACL wire types. They live here (not in `eg-core::isolation`)
//! because `protocol::Method::RegisterIdentity` carries an `AgentRole` over the
//! wire, and `protocol` is below `isolation` in the DAG. `eg-core::isolation`
//! re-exports both so its `IsolationLayer` and all call sites are unchanged.

use serde::{Deserialize, Serialize};

/// Role of an agent in the system hierarchy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentRole {
    /// System-level (can do anything).
    System,
    /// Manager agent with subordinates.
    Manager { subordinates: Vec<String> },
    /// Regular agent.
    Agent,
}

/// Agent identity for ACL checks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentIdentity {
    pub agent_id: String,
    pub role: AgentRole,
    /// Teams this agent belongs to.
    pub teams: Vec<String>,
}
