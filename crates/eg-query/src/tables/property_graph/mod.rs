//! SQL:2023 SQL/PGQ property-graph catalog model.
//!
//! A property graph is a read-only relational view. This module owns its
//! bounded, tenant-scoped definition and DDL model, but no data or executor.

mod model;
mod validate;

use serde::{Deserialize, Serialize};

pub use model::*;
pub use validate::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ElementKind {
    Vertex,
    Edge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DropBehavior {
    Restrict,
    Cascade,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GraphOwner {
    CurrentUser,
    SessionUser,
    Role(SqlIdentifier),
}
