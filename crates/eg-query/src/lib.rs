//! eg-query — the SQL/Cypher query surface (CONCEPT:KG-2.178).
//!
//! Read-only relational query over a single graph's nodes. ALL DataFusion/Arrow
//! code lives behind the `sql` cargo feature so a default build links none of it
//! (the engine stays lean for a Raspberry Pi).
//!
//! The `cypher` feature (CONCEPT:KG-2.179) is a DEP-FREE Cypher subset — a
//! hand-written recursive-descent parser compiled to the engine's own primitives
//! (the eg-core label index, `vf2_subgraph_match`, petgraph BFS). It pulls NO
//! DataFusion, so it ships in the lean Pi build.

#[cfg(feature = "sql")]
pub mod sql;

#[cfg(feature = "sql")]
pub use sql::{exec_sql, QueryResult};

#[cfg(feature = "cypher")]
pub mod cypher;

#[cfg(feature = "cypher")]
pub use cypher::exec_cypher;
