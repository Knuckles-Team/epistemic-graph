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

/// Arbitrary user-defined relational tables (CONCEPT:EG-018) — the durable redb
/// table store + DataFusion materialization. Behind `sql` (needs Arrow/DataFusion/redb).
#[cfg(feature = "sql")]
pub mod tables;

#[cfg(feature = "sql")]
pub use sql::{
    classify, exec_sql, exec_sql_cached, exec_sql_typed, exec_sql_typed_with_tables,
    infer_param_sites, returning_columns, schema_probe_sql, AlterTablePlan, ColumnDef, CopyFormat,
    CopyPlan, CreateTablePlan, DeleteNodes, DeleteTable, DropTablePlan, InsertNode, InsertNodes,
    InsertSelect, InsertTable, ParamLiteralType, ParamSite, PgColType, QueryResult, SqlCache,
    StatementKind, TableWhereEq, TypedColumn, TypedQueryResult, UpdateNodes, UpdateTable, WhereEq,
};

#[cfg(feature = "sql")]
pub use tables::{
    Cell, ColCheck, ColEq, Column, ColumnType, CmpOp, TableSchema, TableStore, TableTxn, TxnOp,
};

#[cfg(feature = "cypher")]
pub mod cypher;

#[cfg(feature = "cypher")]
pub use cypher::{exec_cypher, exec_cypher_write};
