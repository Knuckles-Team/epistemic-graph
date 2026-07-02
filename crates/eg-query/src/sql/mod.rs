//! SQL surface (CONCEPT:KG-2.178, edges/joins + UDFs CONCEPT:KG-2.184): read-only
//! `SELECT ... FROM nodes [JOIN edges ...]` over one graph via DataFusion.
//! Schema-on-read — node property MessagePack blobs are scanned into Arrow
//! RecordBatches with a union-of-keys inferred schema plus a raw `props: Binary`
//! escape hatch; `json_get*` UDFs reach fields the inferred schema widened or
//! dropped. The `edges` table exposes the topology (`src/dst/rel/props`) for joins,
//! `epistemic_decay` salience-weights facts in-query, and `pagerank()`/
//! `betweenness()` table functions plus `var`/`cvar` aggregates (feature `finance`)
//! bring graph + finance kernels into SQL.

mod catalog;
mod classify;
mod exec;
mod providers;
mod tablefuncs;
mod udfs;

pub use classify::{
    classify, infer_param_sites, returning_columns, schema_probe_sql, AlterTablePlan, ColumnDef,
    CopyFormat, CopyPlan, CreateTablePlan, CreateViewPlan, DeleteNodes, DeleteNodesJoin,
    DeleteTable, DropTablePlan, DropViewPlan, InsertNode, InsertNodes, InsertNodesSelect,
    InsertSelect, InsertTable, OnConflict, OnConflictAction, ParamLiteralType, ParamSite,
    StatementKind, TableWhereEq, UpdateNodes, UpdateNodesJoin, UpdateTable, WhereEq,
};
pub use exec::{
    exec_sql, exec_sql_cached, exec_sql_typed, exec_sql_typed_with_tables, PgColType, QueryResult,
    TypedColumn, TypedQueryResult,
};
pub use providers::SqlCache;
