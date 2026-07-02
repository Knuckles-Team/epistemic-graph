//! SQL surface (CONCEPT:KG-2.178, edges/joins + UDFs CONCEPT:KG-2.184): read-only
//! `SELECT ... FROM nodes [JOIN edges ...]` over one graph via DataFusion.
//! Schema-on-read — node property MessagePack blobs are scanned into Arrow
//! RecordBatches with a union-of-keys inferred schema plus a raw `props: Binary`
//! escape hatch; `json_get*` UDFs reach fields the inferred schema widened or
//! dropped. The `edges` table exposes the topology (`src/dst/rel/props`) for joins,
//! `epistemic_decay` salience-weights facts in-query, and `pagerank()`/
//! `betweenness()` table functions plus `var`/`cvar` aggregates (feature `finance`)
//! bring graph + finance kernels into SQL.
//!
//! ## SQL window functions (CONCEPT:EG-089)
//! `<fn>() OVER (PARTITION BY … ORDER BY … <ROWS|RANGE frame>)` is DataFusion-backed:
//! DataFusion 43 provides the window operator + the full function set natively
//! (ranking `ROW_NUMBER`/`RANK`/`DENSE_RANK`/`NTILE`/`PERCENT_RANK`/`CUME_DIST`;
//! offset `LAG`/`LEAD`/`FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE`; aggregate
//! `SUM`/`AVG`/`MIN`/`MAX`/`COUNT OVER`; `ROWS`/`RANGE` frame specs with the standard
//! default frame). A window `SELECT` classifies as [`StatementKind::Read`] and runs
//! through the SAME `exec_sql` → `SessionContext::sql` path as any other query — no
//! separate planner/evaluator, no classify change needed. See
//! `tests/window_functions.rs` for the partition → order → frame coverage.

mod catalog;
mod classify;
mod exec;
/// SQL stored-function (`CREATE FUNCTION … LANGUAGE sql`) expansion (CONCEPT:EG-118) —
/// inline a scalar function body as a scalar subquery, expand a table function as a
/// parameterized-view subquery in `FROM`; reuses the SQL exec path (no new evaluator).
mod funcs;
/// Postgres-family extension parity (CONCEPT:EG-114/116/117/119): AGE `cypher()`,
/// pgvector ANN index pushdown, TimescaleDB hypertables/continuous-aggregates, and
/// ParadeDB `@@@` BM25 — the pure parse/plan/project layer.
mod pgfamily;
mod providers;
mod tablefuncs;
mod udfs;

pub use classify::{
    classify, infer_param_sites, json_pred_from_expr, mongo_match_to_preds, returning_columns,
    schema_probe_sql, AlterTablePlan, ColumnDef, CopyFormat, CopyPlan, CreateFunctionPlan,
    CreateTablePlan, CreateViewPlan, DeleteNodes, DeleteNodesJoin, DeleteTable, DropFunctionPlan,
    DropTablePlan, DropViewPlan, InsertNode, InsertNodes, InsertNodesSelect, InsertSelect,
    InsertTable, OnConflict, OnConflictAction, ParamLiteralType, ParamSite, StatementKind,
    TableWhereEq, UpdateNodes, UpdateNodesJoin, UpdateTable, WhereEq,
};
pub use exec::{
    exec_sql, exec_sql_cached, exec_sql_over_tables, exec_sql_typed, exec_sql_typed_with_tables,
    PgColType, QueryResult, TypedColumn, TypedQueryResult,
};
// CONCEPT:EG-114/116/117/119 — Postgres-family extension parity plans + planners.
pub use pgfamily::{
    cypher_output_columns, parse_create_ann_index, parse_cypher_call, plan_ann_search,
    plan_bm25_search, project_cypher_rows, AnnIndexPlan, AnnMethod, AnnSearchPlan, Bm25SearchPlan,
    ContinuousAggPlan, CypherCallPlan, CypherColumn, HypertablePlan, VectorMetric,
};
pub use providers::SqlCache;
