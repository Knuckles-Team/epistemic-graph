//! eg-query — the SQL/Cypher query surface (CONCEPT:EG-KG.query.read-only-sql-query).
//!
//! Read-only relational query over a single graph's nodes. ALL DataFusion/Arrow
//! code lives behind the `sql` cargo feature so a default build links none of it
//! (the engine stays lean for a Raspberry Pi).
//!
//! The `cypher` feature (CONCEPT:EG-KG.query.dep-free-behind) is a DEP-FREE Cypher subset — a
//! hand-written recursive-descent parser compiled to the engine's own primitives
//! (the eg-core label index, `vf2_subgraph_match`, petgraph BFS). It pulls NO
//! DataFusion, so it ships in the lean Pi build.
//!
//! ## ModalityContract (CONCEPT:E4) — deliberately NOT retrofitted
//! `eg-query` is a pure QUERY-EXECUTOR surface: it parses SQL/Cypher and compiles it to
//! scans/BFS over `eg-core`'s graph primitives, producing result rows — it owns no
//! persisted modality VALUE type of its own (its public items are query planners,
//! `exec_sql*` entry points, and column/plan descriptors, all transient). The stored
//! objects a query READS already carry `ModalityContract` in their own crates
//! (`eg-core::NodeChange`, `eg-tensor`, `eg-tsdb::SeriesMeta`, `eg-rdf::owl::ProofNode`,
//! …). Per the `eg-modality` README's own note that "pure protocol/executor crates with
//! no modality VALUE type of their own — `ModalityContract` may simply not apply, which
//! is a legitimate outcome, not a gap", this crate is a documented SKIP.

#[cfg(feature = "sql")]
pub mod sql;

/// Arbitrary user-defined relational tables (CONCEPT:EG-KG.query.register-user-tables-alongside) — the durable redb
/// table store + DataFusion materialization. Behind `sql` (needs Arrow/DataFusion/redb).
#[cfg(feature = "sql")]
pub mod tables;

#[cfg(feature = "sql")]
pub use sql::{
    classify, cypher_output_columns, default_spill_rows, exec_sql, exec_sql_arrow,
    exec_sql_arrow_cancellable, exec_sql_cached, exec_sql_over_tables, exec_sql_typed,
    exec_sql_typed_cancellable, exec_sql_typed_with_tables,
    exec_sql_typed_with_tables_cached_cancellable, exec_sql_typed_with_tables_cancellable,
    infer_param_sites, parse_create_ann_index, parse_cypher_call, plan_ann_search,
    plan_bm25_search, project_cypher_rows, returning_columns, schema_probe_sql, AlterTableAction,
    AlterTablePlan, AnnIndexPlan, AnnMethod, AnnSearchPlan, Bm25SearchPlan, CancellationToken,
    ColumnDef, ContinuousAggPlan, CopyFormat, CopyPlan, CreateFunctionPlan, CreateTablePlan,
    CreateViewPlan, CypherCallPlan, CypherColumn, DeleteNodes, DeleteNodesJoin, DeleteTable,
    DropFunctionPlan, DropTablePlan, DropViewPlan, HypertablePlan, InsertNode, InsertNodes,
    InsertNodesSelect, InsertSelect, InsertTable, OnConflict, OnConflictAction, ParamLiteralType,
    ParamSite, PgColType, QueryResult, SqlCache, SqlContextCache, StatementKind, StreamOutcome,
    TableWhereEq, TypedColumn, TypedQueryResult, UpdateNodes, UpdateNodesJoin, UpdateTable,
    VectorMetric, WhereEq,
};

#[cfg(feature = "sql")]
pub use tables::{
    migration::{
        conversion_may_be_lossy, MigrationPolicy, MigrationState, RollbackMetadata, SchemaMigration,
        SchemaMigrationApply, SchemaMigrationOperation, SchemaMigrationRecord, SchemaSnapshot,
        SecondaryIndexPolicy,
    },
    Cell, CmpOp, ColCheck, Column, ColumnType, ConflictAction, FunctionArg, FunctionReturns,
    StoredFunction, TableSchema, TableStore, TableTxn, TxnOp,
};

#[cfg(feature = "cypher")]
pub mod cypher;

#[cfg(feature = "cypher")]
pub use cypher::{
    classify_cypher, exec_cypher, exec_cypher_params, exec_cypher_write, exec_cypher_write_params,
    CypherProcedure, CypherStatementKind, Params, ProcRow, YieldValue,
};
