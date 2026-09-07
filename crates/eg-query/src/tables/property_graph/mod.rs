//! SQL:2023 SQL/PGQ property-graph catalog model.
//!
//! A property graph is a read-only relational view. This module owns its
//! bounded, tenant-scoped definition and DDL model, but no data or executor.

mod alter;
mod catalog;
mod model;
/// Durable catalog rows, written by the store's sole SQL catalog transaction.
pub(crate) mod persist;
mod resolve;
mod validate;

use serde::{Deserialize, Serialize};

pub use catalog::*;
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

/// Kind of relational object occupying the table/view/graph namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelationKind {
    Table,
    View,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GraphOwner {
    CurrentUser,
    SessionUser,
    Role(SqlIdentifier),
}
