//! SQL surface (CONCEPT:KG-2.178): read-only `SELECT ... FROM nodes ...` over one
//! graph via DataFusion. Schema-on-read — node property MessagePack blobs are
//! scanned into Arrow RecordBatches with a union-of-keys inferred schema plus a
//! raw `props: Binary` escape hatch; `json_get*` UDFs reach fields the inferred
//! schema widened or dropped.

mod exec;
mod providers;
mod udfs;

pub use exec::{exec_sql, QueryResult};
