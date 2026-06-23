//! eg-query — the SQL/Cypher query surface (CONCEPT:KG-2.178).
//!
//! Read-only relational query over a single graph's nodes. ALL DataFusion/Arrow
//! code lives behind the `sql` cargo feature so a default build links none of it
//! (the engine stays lean for a Raspberry Pi). `cypher` is a dep-free placeholder
//! owned by another track.

#[cfg(feature = "sql")]
pub mod sql;

#[cfg(feature = "sql")]
pub use sql::{exec_sql, QueryResult};
