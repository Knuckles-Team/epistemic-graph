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
// CONCEPT:EG-KG.compute.uncertainty-values — probabilistic / uncertainty VALUE (distribution-valued
// properties). A stored value at the bottom of the DAG, NOT a wire `Op`.
pub mod distribution;
// CONCEPT:INT-P2-1 — the durable analytics-job plane's wire op (`JobOp`), gated
// `jobs`. Lives here (not in `eg-jobs`, which sits ABOVE eg-core in the DAG) for the
// SAME reason `acl::RbacAdminOp` does: `protocol::Method::AnalyticsJob` carries it
// over the wire, and `protocol` is bottom-of-DAG. Pure serde — no dep.
#[cfg(feature = "jobs")]
pub mod jobs;
pub mod protocol;
pub mod row_predicate;
pub mod types;
pub mod wire;

// CONCEPT:EG-KG.query.compound-predicate-decode — the serializable compound-WHERE predicate AST lives at the
// bottom of the DAG so `eg-core` can evaluate it; `eg-query` decodes SQL into it.
pub use row_predicate::{CmpOp, RowPredicate};

// CONCEPT:EG-KG.compute.uncertainty-values — surface the distribution VALUE at the crate root for callers.
pub use distribution::Distribution;
