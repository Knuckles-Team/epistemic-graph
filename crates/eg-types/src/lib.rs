//! eg-types — the wire protocol + graph data model. Bottom of the engine crate
//! DAG: it depends only on serde. Everything else (`eg-core`, `eg-compute`,
//! `eg-server`, the `epistemic-graph` facade) depends on it, never the reverse.
//!
//! It also owns the pure-data types that the protocol enum embeds but whose
//! behavior lives upstream: `wire` (finance/datascience DTOs) and `acl`
//! (`AgentRole`/`AgentIdentity`). The upstream modules re-export these
//! (`pub use eg_types::wire::Order;`) so their algorithm code is unchanged —
//! the data lives at the bottom of the DAG, the logic stays where it belongs.

pub mod acl;
pub mod protocol;
pub mod row_predicate;
pub mod types;
pub mod wire;

// CONCEPT:EG-045 — the serializable compound-WHERE predicate AST lives at the
// bottom of the DAG so `eg-core` can evaluate it; `eg-query` decodes SQL into it.
pub use row_predicate::{CmpOp, RowPredicate};
