//! Arbitrary user-defined relational tables (CONCEPT:EG-018) — the first complete
//! increment of a real relational store living BESIDE the graph projection.
//!
//! Where `sql::providers` projects the GRAPH as a fixed schema-on-read `nodes` table,
//! this module lets an agent `CREATE TABLE` arbitrary typed tables (Prometheus
//! metrics, Langfuse time-series, stock bars, connector mirrors, ETL outputs), write
//! to them with full DML, and `SELECT … JOIN` them against the graph in ONE query.
//!
//! Three layers:
//!   * [`schema`] — the catalog data model: [`ColumnType`], [`Column`], [`TableSchema`],
//!     and the typed row [`Cell`] (+ coercion).
//!   * [`store`] — the durable redb [`TableStore`]: a catalog system table + a
//!     per-table row store, all writes commit-before-ack at `Durability::Immediate`,
//!     persisting across restart.
//!   * [`provider`] — Arrow materialization so each user table registers as a
//!     DataFusion `TableProvider` alongside `nodes`/`edges`.

pub mod provider;
pub mod schema;
pub mod store;

pub use schema::{Cell, Column, ColumnType, TableSchema};
pub use store::{ColEq, TableStore};
