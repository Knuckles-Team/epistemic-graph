//! Arrow materialization of a user table (CONCEPT:EG-KG.query.register-user-tables-alongside) so it registers as a
//! DataFusion `TableProvider` ALONGSIDE the graph's `nodes`/`edges` tables — the
//! unified-engine payoff: a single SELECT can JOIN a user table to the graph.
//!
//! [`materialize`] turns a schema-aligned row set already scanned out of the redb
//! store into ONE Arrow `RecordBatch` whose schema is derived from the catalog
//! [`TableSchema`] (so the column types are the declared types, not schema-on-read
//! inference). [`UserTableProvider`] is the follow-up this module's original doc
//! comment flagged as explicit future work: a `TableProvider` that pushes
//! serial equality and schema-bound scalar secondary-index predicates down to
//! redb instead of an eager `TableStore::scan` — see its own doc for the exact
//! fallback boundary.

use std::sync::{Arc, RwLock};

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int64Builder,
    ListBuilder, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::ScalarValue;
use datafusion::datasource::MemTable;
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;

use super::index::SecondaryIndexLookup;
use super::schema::{Cell, ColumnType, TableSchema};
use super::store::TableStore;
use crate::sql::providers::NodesTableProvider;

/// The Arrow `DataType` a [`ColumnType`] materializes as. Coarse + pg-mappable: every
/// type lands on one of `Int64`/`Float64`/`Utf8`/`Boolean`/`Binary`, which the exec
/// path's `PgColType` mapping already covers, so a user-table column is never
/// lossy-dropped over the wire.
pub fn arrow_type(ty: ColumnType) -> DataType {
    match ty {
        // CONCEPT:EG-KG.query.table-schema-constraints/NE-002 — `TimestampTz` shares `Timestamp`'s i64-micros Arrow
        // shape (the calendar/zone semantics are enforced only on the write path, in
        // `Cell::coerce`); `Numeric`/`Uuid`/`Array` share `Text`/`Json`'s Utf8 shape
        // (each stores its canonical text/JSON form losslessly — see their doc
        // comments on `ColumnType`), so `materialize`'s existing Utf8 fallback arm
        // (matching `Cell::Text`/`Cell::Json` verbatim) needs no change of its own.
        ColumnType::Int | ColumnType::BigInt | ColumnType::Timestamp | ColumnType::TimestampTz => {
            DataType::Int64
        }
        ColumnType::Float | ColumnType::Double => DataType::Float64,
        ColumnType::Text
        | ColumnType::Json
        | ColumnType::Uuid
        | ColumnType::Numeric(_)
        | ColumnType::Array(_) => DataType::Utf8,
        ColumnType::Bool => DataType::Boolean,
        ColumnType::Bytes => DataType::Binary,
        // CONCEPT:EG-KG.query.pgvector-binary-wire — a pgvector column materializes as `List<Float32>`; the exec
        // path's `pg_col_type` maps a Float32-element list to the wire vector type.
        ColumnType::Vector(_) => {
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true)))
        }
    }
}

/// The Arrow [`SchemaRef`] for a user table's catalog schema.
pub fn arrow_schema(schema: &TableSchema) -> SchemaRef {
    let fields: Vec<Field> = schema
        .columns()
        .iter()
        .map(|c| Field::new(&c.name, arrow_type(c.ty), c.nullable))
        .collect();
    Arc::new(Schema::new(fields))
}

/// Materialize `(schema, rows)` into an Arrow `(SchemaRef, RecordBatch)` — the shape
/// the exec path registers as a DataFusion table. Each row is a schema-aligned
/// `Vec<Cell>` (already NULL-padded by the store scan).
pub fn materialize(
    schema: &TableSchema,
    rows: &[Vec<Cell>],
) -> Result<(SchemaRef, RecordBatch), String> {
    let arrow = arrow_schema(schema);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.columns().len());

    for (ci, col) in schema.columns().iter().enumerate() {
        let array: ArrayRef = match arrow_type(col.ty) {
            DataType::Int64 => {
                let mut b = Int64Builder::new();
                for row in rows {
                    match row.get(ci) {
                        Some(Cell::Int(i)) | Some(Cell::Timestamp(i)) => b.append_value(*i),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Float64 => {
                let mut b = Float64Builder::new();
                for row in rows {
                    match row.get(ci) {
                        Some(Cell::Float(f)) => b.append_value(*f),
                        Some(Cell::Int(i)) => b.append_value(*i as f64),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Boolean => {
                let mut b = BooleanBuilder::new();
                for row in rows {
                    match row.get(ci) {
                        Some(Cell::Bool(x)) => b.append_value(*x),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Binary => {
                let mut b = BinaryBuilder::new();
                for row in rows {
                    match row.get(ci) {
                        Some(Cell::Bytes(bytes)) => b.append_value(bytes),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            // CONCEPT:EG-KG.query.pgvector-binary-wire — a vector column → a `List<Float32>` array (one list per row).
            DataType::List(_) => {
                let mut b = ListBuilder::new(Float32Builder::new());
                for row in rows {
                    match row.get(ci) {
                        Some(Cell::Vector(v)) => {
                            for f in v {
                                b.values().append_value(*f);
                            }
                            b.append(true);
                        }
                        _ => b.append(false),
                    }
                }
                Arc::new(b.finish())
            }
            // Utf8 (Text + Json). Json cells render as their canonical JSON text.
            _ => {
                let mut b = StringBuilder::new();
                for row in rows {
                    match row.get(ci) {
                        Some(Cell::Text(s)) => b.append_value(s),
                        Some(Cell::Json(v)) => b.append_value(v.to_string()),
                        Some(Cell::Null) | None => b.append_null(),
                        // A scalar in a text column (shouldn't happen post-coerce) →
                        // its JSON rendering, never a hard error.
                        Some(other) => b.append_value(other.to_json().to_string()),
                    }
                }
                Arc::new(b.finish())
            }
        };
        columns.push(array);
    }

    let batch = RecordBatch::try_new(arrow.clone(), columns)
        .map_err(|e| format!("user table batch: {e}"))?;
    Ok((arrow, batch))
}

// ── user-table TableProvider: SERIAL-column point-get pushdown (CONCEPT:EG-KG.query.register-each-user-table) ──
//
// The redb row store keys every row `(table_name, rowid: u64)` (`TableStore`'s own
// module doc) — an internal physical identity, NOT necessarily the table's
// declared PRIMARY KEY. The ONE column whose SQL VALUE the store deterministically
// derives FROM that physical key is a `SERIAL`/`BIGSERIAL` column:
// `TableStore::insert_rows`'s `build_insert_cells` fills an omitted one with
// `rowid + 1`. So an equality on THAT column translates to a redb POINT GET
// (`TableStore::get_row`, O(log n) via the B-tree) instead of [`Self::full_batch`]'s
// full O(n) `TableStore::scan`.
//
// That mapping is NOT airtight, though: exactly like Postgres, a caller MAY supply
// (INSERT) or later (UPDATE) an explicit value into a nominally-SERIAL column,
// which can diverge it from `rowid + 1` for that one row. Trusting the guess
// outright would then silently DROP a matching row from the result — a
// correctness bug, not just a missed optimization — so [`UserTableProvider::scan`]
// always RE-CHECKS the fetched row's own cell against the target before returning
// it (`point_get`), and falls back to the full scan on ANY mismatch (or on no
// candidate at all). This makes the point get provably safe: it can only ever
// return exactly what the full scan would have, just faster in the common
// (un-overridden) case.
//
// Ordinary scalar equality/range predicates use the durable, schema-bound
// secondary-index directory when an owner has explicitly created one. The
// directory is maintained in the same redb write transaction as row inserts,
// updates, deletes, and referential cascades. A stale/malformed definition,
// unsupported type, missing entry, or candidate budget overflow returns to the
// authoritative full scan. ORDER BY/pagination callers can use the same
// deterministic entry order through the store catalog contract; DataFusion's
// TableProvider API does not pass ORDER BY expressions into `scan`, so this
// provider never fabricates an order or truncates an inexact result.
//
// Every OTHER predicate (equality on a non-serial column, e.g. `WHERE symbol =
// 'AAPL'`) still gets the SAME generic secondary-index pushdown `nodes` and every
// OTHER user table already had: the full-scan fallback delegates to
// `NodesTableProvider` (CONCEPT:AU-KG.retrieval.architecture-report) over whatever batch it ends up with, so this
// provider strictly ADDS the point-get fast path — it never regresses the
// existing per-column equality-index behavior.

/// Try the point-get fast path for `col = target` where `col` is `serial_col` and
/// `target` is `i64` (a `SERIAL`/`BIGSERIAL` column is always `Int`/`BigInt`,
/// CONCEPT:EG-KG.query.register-each-user-table — never `Timestamp`, despite sharing its Arrow type). `None` for
/// anything else: a different column, a non-equality predicate, or a non-integer
/// literal.
fn serial_column_eq(expr: &Expr, serial_col: &str) -> Option<i64> {
    let Expr::BinaryExpr(be) = expr else {
        return None;
    };
    if be.op != Operator::Eq {
        return None;
    }
    let (col, lit) = match (be.left.as_ref(), be.right.as_ref()) {
        (Expr::Column(c), Expr::Literal(v, _)) => (c, v),
        (Expr::Literal(v, _), Expr::Column(c)) => (c, v),
        _ => return None,
    };
    if col.name != serial_col {
        return None;
    }
    match lit {
        ScalarValue::Int8(Some(n)) => Some(*n as i64),
        ScalarValue::Int16(Some(n)) => Some(*n as i64),
        ScalarValue::Int32(Some(n)) => Some(*n as i64),
        ScalarValue::Int64(Some(n)) => Some(*n),
        ScalarValue::UInt8(Some(n)) => Some(*n as i64),
        ScalarValue::UInt16(Some(n)) => Some(*n as i64),
        ScalarValue::UInt32(Some(n)) => Some(*n as i64),
        ScalarValue::UInt64(Some(n)) => i64::try_from(*n).ok(),
        _ => None,
    }
}

/// `col = literal` naming `col`, regardless of which column — the shape
/// [`UserTableProvider::supports_filters_pushdown`] reports `Inexact` for. Reused
/// (rather than restricted to the serial column) so a predicate on any OTHER
/// column still reaches `scan`, which delegates to `NodesTableProvider`'s own
/// equality-index pushdown over the full-scanned batch — see the module doc.
fn is_equality_shape(expr: &Expr) -> bool {
    let Expr::BinaryExpr(be) = expr else {
        return false;
    };
    be.op == Operator::Eq
        && matches!(
            (be.left.as_ref(), be.right.as_ref()),
            (Expr::Column(_), Expr::Literal(..)) | (Expr::Literal(..), Expr::Column(_))
        )
}

/// A scalar literal that can be represented by the declared user-table type.
/// The engine deliberately accepts only the finite set that has a stable
/// secondary-index encoding; NULL and complex/list literals remain scan-only.
fn scalar_to_json(value: &ScalarValue) -> Option<serde_json::Value> {
    match value {
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
            Some(value.clone().into())
        }
        ScalarValue::Boolean(Some(value)) => Some((*value).into()),
        ScalarValue::Int8(Some(value)) => Some((*value as i64).into()),
        ScalarValue::Int16(Some(value)) => Some((*value as i64).into()),
        ScalarValue::Int32(Some(value)) => Some((*value as i64).into()),
        ScalarValue::Int64(Some(value)) => Some((*value).into()),
        ScalarValue::UInt8(Some(value)) => Some((*value as u64).into()),
        ScalarValue::UInt16(Some(value)) => Some((*value as u64).into()),
        ScalarValue::UInt32(Some(value)) => Some((*value as u64).into()),
        ScalarValue::UInt64(Some(value)) => Some((*value).into()),
        ScalarValue::Float32(Some(value)) => serde_json::Number::from_f64(*value as f64)
            .map(serde_json::Value::Number),
        ScalarValue::Float64(Some(value)) => {
            serde_json::Number::from_f64(*value).map(serde_json::Value::Number)
        }
        _ => None,
    }
}

fn reverse_index_operator(op: Operator) -> Operator {
    match op {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        other => other,
    }
}

/// Decode one DataFusion `column OP literal` expression into the bounded
/// catalog lookup contract.  The provider stays conservative: unsupported
/// expression shapes return `None`, so it cannot accidentally drop rows.
fn secondary_lookup(expr: &Expr, schema: &TableSchema) -> Option<SecondaryIndexLookup> {
    let Expr::BinaryExpr(binary) = expr else {
        return None;
    };
    let (column, literal, op) = match (binary.left.as_ref(), binary.right.as_ref()) {
        (Expr::Column(column), Expr::Literal(value, _)) => {
            (column, value, binary.op)
        }
        (Expr::Literal(value, _), Expr::Column(column)) => {
            (column, value, reverse_index_operator(binary.op))
        }
        _ => return None,
    };
    let ty = schema.column(&column.name)?.ty;
    let json = scalar_to_json(literal)?;
    let cell = Cell::coerce(&json, ty, true).ok()?;
    let column = column.name.clone();
    match op {
        Operator::Eq => Some(SecondaryIndexLookup::Eq { column, value: cell }),
        Operator::Lt => Some(SecondaryIndexLookup::Lt { column, value: cell }),
        Operator::LtEq => Some(SecondaryIndexLookup::Le { column, value: cell }),
        Operator::Gt => Some(SecondaryIndexLookup::Gt { column, value: cell }),
        Operator::GtEq => Some(SecondaryIndexLookup::Ge { column, value: cell }),
        _ => None,
    }
}

fn is_secondary_shape(expr: &Expr) -> bool {
    let Expr::BinaryExpr(binary) = expr else {
        return false;
    };
    matches!(
        binary.op,
        Operator::Eq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
    ) && matches!(
        (binary.left.as_ref(), binary.right.as_ref()),
        (Expr::Column(_), Expr::Literal(..)) | (Expr::Literal(..), Expr::Column(_))
    )
}

/// A user table's `TableProvider` (CONCEPT:EG-KG.query.register-each-user-table) — see the module section doc above for
/// the full pushdown design.
#[derive(Debug)]
pub(crate) struct UserTableProvider {
    schema: TableSchema,
    arrow_schema: SchemaRef,
    store: TableStore,
    /// Position of the ONE `SERIAL`/`BIGSERIAL` column, if any — the only column an
    /// equality can push down to a point get. `None` for a table with no serial
    /// column (every predicate falls back to the full scan + the generic
    /// equality-index delegate).
    serial_col: Option<usize>,
    /// The full, unfiltered batch — built at most once per provider instance
    /// (`TableStore::scan`), and only when a query pushed no serial-column
    /// equality this provider resolved (or the resolved point-get missed or failed
    /// re-verification).
    full: RwLock<Option<RecordBatch>>,
}

impl UserTableProvider {
    pub(crate) fn new(store: TableStore, schema: TableSchema) -> Self {
        let arrow_schema = arrow_schema(&schema);
        let serial_col = schema.columns().iter().position(|c| c.serial);
        Self {
            schema,
            arrow_schema,
            store,
            serial_col,
            full: RwLock::new(None),
        }
    }

    /// Try the point-get fast path: guess `rowid = target - 1`, fetch it, and
    /// RE-VERIFY the fetched row's own cell before trusting it (see the module
    /// doc). `Ok(None)` on ANY failure to resolve safely — a negative/
    /// non-representable target, no row at that rowid, or a verification mismatch
    /// — meaning the caller must fall back to the full scan, which remains correct
    /// in every one of those cases (the equality is reported `Inexact`, so
    /// DataFusion re-checks it regardless).
    fn point_get(&self, col: usize, target: i64) -> Result<Option<RecordBatch>, String> {
        let Some(rowid) = target.checked_sub(1).and_then(|r| u64::try_from(r).ok()) else {
            return Ok(None);
        };
        let Some(cells) = self.store.get_row(&self.schema.name, rowid)? else {
            return Ok(None);
        };
        if !matches!(cells.get(col), Some(Cell::Int(i)) if *i == target) {
            return Ok(None); // the serial invariant was overridden for this row
        }
        let (_, batch) = materialize(&self.schema, std::slice::from_ref(&cells))?;
        Ok(Some(batch))
    }

    /// The full, unfiltered table — [`TableStore::scan`]'s existing O(n) walk,
    /// memoized so a provider shared across repeat calls (`SqlContextCache`'s
    /// reused `BuiltCtx`) doesn't re-pay it per query.
    fn full_batch(&self) -> Result<RecordBatch, String> {
        if let Some(b) = self.full.read().unwrap().as_ref() {
            return Ok(b.clone());
        }
        let rows = self.store.scan(&self.schema.name)?;
        let (_, batch) = materialize(&self.schema, &rows)?;
        *self.full.write().unwrap() = Some(batch.clone());
        Ok(batch)
    }
}

#[async_trait]
impl TableProvider for UserTableProvider {
    fn schema(&self) -> SchemaRef {
        self.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// `Inexact` for a scalar equality/range expression. The index is only a
    /// row-reduction optimization; DataFusion re-applies the original filter
    /// above the scan. Other expression shapes remain `Unsupported` and use the
    /// full authoritative row scan.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                if is_equality_shape(f) || is_secondary_shape(f) {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if let Some(lookup) = filters
            .iter()
            .find_map(|filter| secondary_lookup(filter, &self.schema))
        {
            if let Some(rows) = self
                .store
                .secondary_index_rows(&self.schema.name, &lookup)
                .map_err(datafusion::error::DataFusionError::Execution)?
            {
                let (_, batch) = materialize(&self.schema, &rows)
                    .map_err(datafusion::error::DataFusionError::Execution)?;
                let mem = MemTable::try_new(self.arrow_schema.clone(), vec![vec![batch]])?;
                return mem.scan(state, projection, filters, limit).await;
            }
        }

        let pushed = self.serial_col.and_then(|ci| {
            let name = &self.schema.columns()[ci].name;
            filters
                .iter()
                .find_map(|f| serial_column_eq(f, name).map(|t| (ci, t)))
        });

        if let Some((ci, target)) = pushed {
            if let Some(batch) = self
                .point_get(ci, target)
                .map_err(datafusion::error::DataFusionError::Execution)?
            {
                let mem = MemTable::try_new(self.arrow_schema.clone(), vec![vec![batch]])?;
                return mem.scan(state, projection, &[], limit).await;
            }
            // Point-get missed/failed re-verification — fall through to the full
            // scan below exactly like the no-pushdown case; `Inexact` keeps this
            // correct (DataFusion re-applies the equality as a Filter above us).
        }

        // Fall back to the full batch, THEN apply the SAME generic per-column
        // equality-index pushdown every other user table already gets (delegating
        // to `NodesTableProvider`'s own `scan`, unmodified by this task).
        let batch = self
            .full_batch()
            .map_err(datafusion::error::DataFusionError::Execution)?;
        let inner = NodesTableProvider::new(self.arrow_schema.clone(), batch);
        inner.scan(state, projection, filters, limit).await
    }
}

/// `UserTableProvider` pushdown correctness (P10/W1.7-B). Every test runs real SQL
/// end-to-end through [`crate::sql::exec_sql_typed_with_tables`] (the same path
/// `build_ctx` wires this provider into), so a passing test proves the redb
/// point-get route and the full-scan-fallback route produce IDENTICAL answers to
/// what a plain scan always returned — the pushdown is provably never a
/// correctness boundary.
#[cfg(test)]
mod user_table_provider_tests {
    use super::*;
    use crate::tables::schema::Column;
    use eg_core::graph::GraphView;
    use serde_json::json;

    /// `id BIGINT SERIAL PRIMARY KEY, symbol TEXT` — the idiomatic auto-increment
    /// shape this provider's point-get pushdown targets.
    fn serial_pk_schema() -> TableSchema {
        let mut id = Column::new("id", ColumnType::BigInt, false, true);
        id.serial = true;
        TableSchema::new(
            "prices",
            vec![id, Column::new("symbol", ColumnType::Text, false, false)],
        )
    }

    /// A fresh store with `serial_pk_schema()`'s `prices` table, `symbol`s inserted
    /// in order (so row i gets `id = i + 1`, `SERIAL`'s own rule).
    fn store_with_rows(symbols: &[&str]) -> TableStore {
        let (store, _p) = TableStore::open_temp().unwrap();
        store.create_table(&serial_pk_schema(), false).unwrap();
        let cols = vec!["symbol".to_string()];
        let rows: Vec<Vec<serde_json::Value>> = symbols.iter().map(|s| vec![(*s).into()]).collect();
        store.insert_rows("prices", &cols, &rows).unwrap();
        store
    }

    /// No graph content is needed for these tests — only the user table.
    fn empty_view() -> GraphView {
        GraphView::default()
    }

    /// `WHERE id = <serial value>` resolves via `TableStore::get_row` (a redb point
    /// get) — proven correct by matching exactly the row a full scan would find.
    #[test]
    fn point_get_on_serial_pk_returns_the_matching_row() {
        let store = store_with_rows(&["AAPL", "MSFT", "GOOG"]);
        let r = crate::sql::exec_sql_typed_with_tables(
            &empty_view(),
            &store,
            "SELECT symbol FROM prices WHERE id = 2",
        )
        .unwrap();
        assert_eq!(r.rows, vec![vec![json!("MSFT")]]);
    }

    /// A serial-value equality that names no row (never allocated) returns zero
    /// rows, not an error — the point-get misses, falls back, and the fallback
    /// ALSO finds nothing (there is nothing to find).
    #[test]
    fn point_get_miss_returns_no_rows_not_an_error() {
        let store = store_with_rows(&["AAPL"]);
        let r = crate::sql::exec_sql_typed_with_tables(
            &empty_view(),
            &store,
            "SELECT symbol FROM prices WHERE id = 999",
        )
        .unwrap();
        assert!(r.rows.is_empty());
    }

    /// An equality on a NON-serial column (`symbol`, not `id`) still resolves
    /// correctly — via the full-scan fallback's `NodesTableProvider` delegate, the
    /// SAME generic equality-index pushdown every other user table already had.
    /// Proves this task's point-get addition never regressed that existing path.
    #[test]
    fn non_pk_equality_still_resolves_via_the_generic_index_delegate() {
        let store = store_with_rows(&["AAPL", "MSFT", "AAPL"]);
        let r = crate::sql::exec_sql_typed_with_tables(
            &empty_view(),
            &store,
            "SELECT id FROM prices WHERE symbol = 'AAPL' ORDER BY id",
        )
        .unwrap();
        assert_eq!(r.rows, vec![vec![json!(1)], vec![json!(3)]]);
    }

    /// SAFETY-CRITICAL case: a caller may supply an EXPLICIT value into a
    /// nominally-`SERIAL` column (legal, same as Postgres), diverging it from
    /// `rowid + 1`. The naive `rowid = value - 1` guess would land on the WRONG
    /// physical row (or none) — `point_get`'s re-verification against the fetched
    /// row's own cell must catch that and fall back to the full scan, which still
    /// finds the row correctly. This is the exact scenario the module doc's
    /// "provably safe" claim rests on.
    #[test]
    fn overridden_serial_value_is_still_resolved_correctly_via_fallback() {
        let (store, _p) = TableStore::open_temp().unwrap();
        store.create_table(&serial_pk_schema(), false).unwrap();
        // First (and only) insert explicitly supplies id=500 — NOT rowid+1 (=1).
        store
            .insert_rows(
                "prices",
                &["id".to_string(), "symbol".to_string()],
                &[vec![500i64.into(), "WEIRD".into()]],
            )
            .unwrap();
        let r = crate::sql::exec_sql_typed_with_tables(
            &empty_view(),
            &store,
            "SELECT symbol FROM prices WHERE id = 500",
        )
        .unwrap();
        assert_eq!(
            r.rows,
            vec![vec![json!("WEIRD")]],
            "an overridden serial value must still resolve correctly, not silently miss"
        );
        // The naive (wrong) guess for id=500 is rowid=499, which does not exist —
        // an un-reverified point-get would have returned zero rows here. Confirm
        // that specific miscarriage doesn't happen for a DIFFERENT absent value too.
        let miss = crate::sql::exec_sql_typed_with_tables(
            &empty_view(),
            &store,
            "SELECT symbol FROM prices WHERE id = 501",
        )
        .unwrap();
        assert!(miss.rows.is_empty());
    }

    /// An unfiltered `SELECT * FROM prices` still returns every row (the
    /// `full_batch` fallback, exercised with no predicate at all).
    #[test]
    fn unfiltered_query_returns_every_row() {
        let store = store_with_rows(&["AAPL", "MSFT", "GOOG"]);
        let r = crate::sql::exec_sql_typed_with_tables(
            &empty_view(),
            &store,
            "SELECT id FROM prices ORDER BY id",
        )
        .unwrap();
        assert_eq!(r.rows, vec![vec![json!(1)], vec![json!(2)], vec![json!(3)]]);
    }

    fn provider_direct() -> UserTableProvider {
        let store = store_with_rows(&["AAPL", "MSFT"]);
        UserTableProvider::new(store, serial_pk_schema())
    }

    /// Direct classification check (mirrors `NodesTableProvider`/`EdgesTableProvider`'s
    /// own `supports_filters_pushdown` tests): ANY `col = literal` equality is
    /// `Inexact` — including a non-serial column, since `scan`'s fallback still
    /// resolves it via the generic index delegate — and a non-equality predicate
    /// is `Unsupported`.
    #[test]
    fn supports_filters_pushdown_is_inexact_for_any_equality() {
        use datafusion::logical_expr::{col, lit};
        let p = provider_direct();
        let eq_id = col("id").eq(lit(1i64));
        let eq_symbol = col("symbol").eq(lit("AAPL"));
        let gt = col("id").gt(lit(1i64));
        let refs: Vec<&Expr> = vec![&eq_id, &eq_symbol, &gt];
        let got = p.supports_filters_pushdown(&refs).unwrap();
        assert_eq!(got[0], TableProviderFilterPushDown::Inexact, "serial pk eq");
        assert_eq!(got[1], TableProviderFilterPushDown::Inexact, "non-pk eq");
        assert_eq!(got[2], TableProviderFilterPushDown::Unsupported, "range");
    }

    /// A table with NO serial column at all (`serial_col` is `None`) never has a
    /// point-get candidate — every predicate falls back to the full scan +
    /// generic index delegate, and results are still correct.
    #[test]
    fn table_with_no_serial_column_still_answers_correctly_via_fallback() {
        let (store, _p) = TableStore::open_temp().unwrap();
        let schema = TableSchema::new(
            "plain",
            vec![
                Column::new("k", ColumnType::Text, false, true),
                Column::new("v", ColumnType::Int, true, false),
            ],
        );
        store.create_table(&schema, false).unwrap();
        store
            .insert_rows(
                "plain",
                &["k".to_string(), "v".to_string()],
                &[vec!["a".into(), 1i64.into()], vec!["b".into(), 2i64.into()]],
            )
            .unwrap();
        let r = crate::sql::exec_sql_typed_with_tables(
            &empty_view(),
            &store,
            "SELECT v FROM plain WHERE k = 'b'",
        )
        .unwrap();
        assert_eq!(r.rows, vec![vec![json!(2)]]);
    }
}
