//! Federated Iceberg-REST table function (CA-19, GOC-77 W01-W05, BUG-224) —
//! `iceberg('namespace.table'[, snapshot_id])` registered as a DataFusion table
//! function (CONCEPT:EG-KG.query.query-federation) alongside `nodes`/`edges`, so ONE
//! UQL/SQL query can `JOIN` an external Iceberg table (Lakekeeper-cataloged, or eg's
//! OWN Iceberg-REST catalog once DEC-CA-01's W0 reachability note is resolved for a
//! given deployment) with the local graph.
//!
//! ```sql
//! SELECT n.id, f.value FROM nodes n
//! JOIN iceberg('ca_e2e.p1_facts') f ON f.key = n.properties->>'key'
//! ```
//!
//! ## Design
//!
//! Uses the OFFICIAL `apache/iceberg-rust` crates CA-18 already adopted under the SAME
//! risk-accepted `.config/cargo-audit-allow.txt` posture (`iceberg`, `iceberg-storage-opendal`)
//! plus the REST-catalog client CA-18 did not need (`iceberg-catalog-rest`) — all three
//! reuse the workspace `arrow = "58.3"` `datafusion = "54"` already links, so there is no
//! new ABI-isolation boundary (CA-18's own `cargo tree -d` precedent).
//!
//! `TableFunctionImpl::call_with_args` is a SYNC DataFusion callback but the REST catalog
//! client is async (`reqwest`), so the one-shot catalog connect + `load_table` (needed to
//! learn the Arrow schema DataFusion's planner requires up front) runs on a throwaway
//! OS thread with its OWN single-use Tokio runtime, joined back synchronously
//! ([`block_on_iceberg`]) — this sidesteps the documented "nested `block_on` panics on a
//! current-thread runtime" hazard the module doc at the top of `sql/exec.rs` already
//! flags for this SQL surface, without requiring the caller to be inside any particular
//! runtime shape. [`IcebergTableProvider::scan`] repeats the same bridge so a later
//! DataFusion-supplied projection/filter set reaches the REAL `iceberg` crate scan
//! planner (manifest-level file pruning + column projection, with DataFusion retaining
//! an exact residual filter), then hands the resulting Arrow
//! batches to a `MemTable` for DataFusion to serve, exactly the "materialize once,
//! delegate to `MemTable`" idiom `UserTableProvider` already uses in this crate
//! (`crate::tables::provider`).
//!
//! ## Configuration (opt-in via env, unset ⇒ the function errors clearly rather than
//! silently returning nothing — matches every other `EPISTEMIC_GRAPH_*` knob's contract)
//!
//! * [`ICEBERG_FEDERATION_CATALOG_URI_ENV`] — REST catalog base URI (e.g.
//!   `http://lakekeeper.example/catalog`, or eg's own `--iceberg-addr` once reachable).
//! * [`ICEBERG_FEDERATION_WAREHOUSE_ENV`] — optional warehouse identifier.
//! * [`ICEBERG_FEDERATION_CREDENTIAL_ENV`] — optional `client_id:client_secret`
//!   (OAuth2 client-credentials, the SAME `lakekeeper-service` Keycloak-client shape
//!   CA-40 proved live against Lakekeeper).
//! * [`ICEBERG_FEDERATION_SCOPE_ENV`] — optional OAuth2 scope (CA-40 used `lakekeeper`
//!   explicitly rather than relying on a default).
//! * [`ICEBERG_FEDERATION_OAUTH2_URI_ENV`] — optional token endpoint override
//!   (`oauth2-server-uri`); when unset the REST catalog's own `/v1/oauth/tokens` is used.
//! * [`ICEBERG_FEDERATION_TOKEN_ENV`] — optional fixed bearer token, an alternative to
//!   the credential flow.
//!
//! ## BUG-224 (`Op::AsOf` -> `Lsn`, closed for the federated leg)
//!
//! `snapshot_id = lsn` 1:1 (`crates/eg-lake/src/iceberg.rs:100-101`, DEC-CA-01's version
//! identity contract) — so the table function's OPTIONAL second argument pins the scan to
//! an explicit committed snapshot/LSN via [`iceberg::scan::TableScanBuilder::snapshot_id`],
//! the SAME mechanism the authenticated `LoadTable` HTTP route's `?as_of=<LSN>` extension
//! already uses server-side (`src/server/lake/rest.rs`). This is the missing QUERY-TIME
//! caller BUG-224 flags ("no caller") for the federation leg specifically: a UQL/SQL
//! query can now pin a federated Iceberg read to the exact snapshot Trino's
//! `FOR VERSION AS OF <snapshot_id>` resolves (P4). The companion internal seam — eg's
//! OWN bi-temporal `Op::AsOf { ts, axis }` (a row-level valid-time filter, unrelated to
//! storage versioning) resolving to a concrete LSN for eg's OWN materialized lake table —
//! is the SEPARATE closure `eg_lake::LakeTable::iceberg_as_of_ts` now provides; see that
//! function's doc for why the two are not the same problem.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableFunctionArgs, TableFunctionImpl, TableProvider};
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use futures_util::TryStreamExt;
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::Datum;
use iceberg::{Catalog, CatalogBuilder, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;

/// REST catalog base URI (required; unset ⇒ `iceberg(...)` errors, never silently empty).
pub const ICEBERG_FEDERATION_CATALOG_URI_ENV: &str =
    "EPISTEMIC_GRAPH_ICEBERG_FEDERATION_CATALOG_URI";
/// Optional warehouse identifier passed to the REST catalog.
pub const ICEBERG_FEDERATION_WAREHOUSE_ENV: &str = "EPISTEMIC_GRAPH_ICEBERG_FEDERATION_WAREHOUSE";
/// Optional `client_id:client_secret` OAuth2 client-credentials pair.
pub const ICEBERG_FEDERATION_CREDENTIAL_ENV: &str = "EPISTEMIC_GRAPH_ICEBERG_FEDERATION_CREDENTIAL";
/// Optional OAuth2 scope (e.g. `lakekeeper`).
pub const ICEBERG_FEDERATION_SCOPE_ENV: &str = "EPISTEMIC_GRAPH_ICEBERG_FEDERATION_SCOPE";
/// Optional OAuth2 token-endpoint override (`oauth2-server-uri`).
pub const ICEBERG_FEDERATION_OAUTH2_URI_ENV: &str = "EPISTEMIC_GRAPH_ICEBERG_FEDERATION_OAUTH2_URI";
/// Optional fixed bearer token, an alternative to the credential flow.
pub const ICEBERG_FEDERATION_TOKEN_ENV: &str = "EPISTEMIC_GRAPH_ICEBERG_FEDERATION_TOKEN";

/// Observable pushdown counters for the most recent `iceberg(...)` scan (P1's proof shape:
/// "scan metrics show `files_skipped>0` ... and `columns_projected < columns_total`").
/// Filled from the `iceberg` crate's OWN manifest planner — [`total_data_files`] is the
/// selected snapshot's summary count (`total-data-files`), [`files_scanned`] is the number
/// of `FileScanTask`s the SAME planner produced under the pushed-down
/// projection/predicate. It counts planned files, not necessarily files opened when
/// a LIMIT stops the reader early. A selective filter that
/// still plans every file (no partition/stat alignment) legitimately reports
/// `files_skipped == 0`, and this type says so rather than fabricating a number.
#[derive(Clone, Debug, Default)]
pub struct IcebergPushdownStats {
    pub total_data_files: u64,
    pub files_scanned: u64,
    pub columns_total: usize,
    pub columns_projected: usize,
}

impl IcebergPushdownStats {
    pub fn files_skipped(&self) -> u64 {
        self.total_data_files.saturating_sub(self.files_scanned)
    }
}

/// Everything needed to (re)connect to one federated Iceberg-REST catalog, resolved
/// once from env at `iceberg(...)` call time (CONCEPT:EG-KG.query.query-federation).
#[derive(Clone, Debug)]
struct IcebergCatalogConfig {
    catalog_uri: String,
    warehouse: Option<String>,
    credential: Option<String>,
    scope: Option<String>,
    oauth2_uri: Option<String>,
    token: Option<String>,
}

impl IcebergCatalogConfig {
    fn from_env() -> DfResult<Self> {
        let catalog_uri = std::env::var(ICEBERG_FEDERATION_CATALOG_URI_ENV).map_err(|_| {
            DataFusionError::Execution(format!(
                "iceberg(...): {ICEBERG_FEDERATION_CATALOG_URI_ENV} is not set — no federated \
                 Iceberg-REST catalog configured (never silently empty)"
            ))
        })?;
        Ok(Self {
            catalog_uri,
            warehouse: std::env::var(ICEBERG_FEDERATION_WAREHOUSE_ENV).ok(),
            credential: std::env::var(ICEBERG_FEDERATION_CREDENTIAL_ENV).ok(),
            scope: std::env::var(ICEBERG_FEDERATION_SCOPE_ENV).ok(),
            oauth2_uri: std::env::var(ICEBERG_FEDERATION_OAUTH2_URI_ENV).ok(),
            token: std::env::var(ICEBERG_FEDERATION_TOKEN_ENV).ok(),
        })
    }

    fn catalog_props(&self) -> HashMap<String, String> {
        let mut props = HashMap::new();
        props.insert("uri".to_string(), self.catalog_uri.clone());
        if let Some(w) = &self.warehouse {
            props.insert("warehouse".to_string(), w.clone());
        }
        if let Some(c) = &self.credential {
            props.insert("credential".to_string(), c.clone());
        }
        if let Some(s) = &self.scope {
            props.insert("scope".to_string(), s.clone());
        }
        if let Some(u) = &self.oauth2_uri {
            props.insert("oauth2-server-uri".to_string(), u.clone());
        }
        if let Some(t) = &self.token {
            props.insert("token".to_string(), t.clone());
        }
        props
    }

    async fn connect(&self) -> iceberg::Result<iceberg_catalog_rest::RestCatalog> {
        RestCatalogBuilder::default()
            .with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::new()))
            .load("eg-federation", self.catalog_props())
            .await
    }
}

/// How long one iceberg catalog/table call may take before it is reported as a
/// stalled federation rather than a slow one. Generous: these are remote object
/// store and catalog round trips, and a large table's metadata scan is legitimately
/// slow. Finite so a catalog that never answers is an error, not a hung query.
const ICEBERG_FEDERATION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);

/// Bridge one async iceberg-crate future to a sync caller (see the module doc for why:
/// `TableFunctionImpl::call_with_args` is sync and `TableProvider::scan` runs inside
/// whatever runtime DataFusion's own executor already occupies, so this ALWAYS runs the
/// future on a fresh, throwaway single-use runtime on its OWN OS thread rather than
/// risking a nested `block_on` on the caller's runtime).
fn block_on_iceberg<F, T>(fut: F) -> DfResult<T>
where
    F: std::future::Future<Output = iceberg::Result<T>> + Send + 'static,
    T: Send + 'static,
{
    // The `JoinHandle::join` entry forbids an unbounded join because a wedged
    // worker takes its joiner with it. The deadline belongs on the OPERATION,
    // not the join: bounding the catalog/table future inside the runtime makes
    // a stalled iceberg call a DataFusion error with a cause, and leaves this
    // join prompt in every case -- which a deadline on the join itself could
    // not do, since it would abandon a live runtime thread holding the scan.
    // Invariant `deadline-on-the-operation`: docs/architecture/liveness_invariants.md.
    #[allow(clippy::disallowed_methods)]
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .map_err(|e| DataFusionError::Execution(format!("iceberg federation runtime: {e}")))?;
        rt.block_on(async move {
            match tokio::time::timeout(ICEBERG_FEDERATION_TIMEOUT, fut).await {
                Ok(result) => result
                    .map_err(|e| DataFusionError::Execution(format!("iceberg federation: {e}"))),
                Err(_) => Err(DataFusionError::Execution(format!(
                    "iceberg federation: the catalog did not answer within {}s",
                    ICEBERG_FEDERATION_TIMEOUT.as_secs()
                ))),
            }
        })
    })
    .join()
    .map_err(|_| DataFusionError::Execution("iceberg federation: worker thread panicked".into()))?
}

/// `iceberg('namespace.table'[, snapshot_id])` — see the module doc.
#[derive(Debug, Default)]
pub(crate) struct IcebergFunc;

fn literal_str(e: &Expr, ctx: &str) -> DfResult<String> {
    if let Expr::Literal(ScalarValue::Utf8(Some(s)), _) = e {
        return Ok(s.clone());
    }
    Err(DataFusionError::Execution(format!(
        "iceberg(...): {ctx} must be a string literal, got `{e}`"
    )))
}

fn literal_i64_opt(e: &Expr, ctx: &str) -> DfResult<i64> {
    if let Expr::Literal(sv, _) = e {
        match sv {
            ScalarValue::Int64(Some(v)) => return Ok(*v),
            ScalarValue::Int32(Some(v)) => return Ok(*v as i64),
            ScalarValue::UInt64(Some(v)) => return Ok(*v as i64),
            _ => {}
        }
    }
    Err(DataFusionError::Execution(format!(
        "iceberg(...): {ctx} must be an integer literal (a committed snapshot id / LSN — \
         DEC-CA-01's `snapshot_id = lsn` 1:1 contract), got `{e}`"
    )))
}

/// Connect and load only metadata needed for planning. The data scan waits for
/// DataFusion's projection and filters in [`IcebergTableProvider::scan`]. Pulled out of
/// [`IcebergFunc`]'s
/// `TableFunctionImpl` impl (which can only return `Arc<dyn TableProvider>`, no side
/// channel) so a test can assert on [`IcebergPushdownStats`] directly instead of
/// scraping `EXPLAIN` text or downcasting a trait object.
pub fn build_iceberg_provider(
    ident_str: &str,
    snapshot_id: Option<i64>,
) -> DfResult<Arc<IcebergTableProvider>> {
    let table_ident = TableIdent::from_strs(ident_str.split('.')).map_err(|e| {
        DataFusionError::Execution(format!(
            "iceberg(...): '{ident_str}' is not a valid `namespace.table` identifier: {e}"
        ))
    })?;

    let config = IcebergCatalogConfig::from_env()?;
    let ident_for_err = ident_str.to_string();
    let (table, schema, total_data_files) = block_on_iceberg(async move {
        let catalog = config.connect().await?;
        let table = catalog.load_table(&table_ident).await.map_err(|e| {
            // Typed, named error — NEVER an empty result (P1's negative case): a
            // dropped/nonexistent table surfaces its identity in the message.
            iceberg::Error::new(
                e.kind(),
                format!("iceberg federated table '{ident_for_err}' unavailable: {e}"),
            )
        })?;

        let total_data_files: u64 = snapshot_id
            .and_then(|id| table.metadata().snapshot_by_id(id))
            .or_else(|| table.metadata().current_snapshot())
            .and_then(|s| s.summary().additional_properties.get("total-data-files"))
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);

        let arrow_schema =
            iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())?;
        Ok((table, arrow_schema, total_data_files))
    })?;
    let columns_total = schema.fields().len();

    Ok(Arc::new(IcebergTableProvider {
        table,
        snapshot_id,
        schema: Arc::new(schema),
        stats: std::sync::RwLock::new(IcebergPushdownStats {
            total_data_files,
            // Before the first scan no pruning has been observed.
            files_scanned: total_data_files,
            columns_total,
            columns_projected: columns_total,
        }),
    }))
}

impl TableFunctionImpl for IcebergFunc {
    fn call_with_args(&self, args: TableFunctionArgs<'_, '_>) -> DfResult<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        if exprs.is_empty() || exprs.len() > 2 {
            return Err(DataFusionError::Execution(
                "iceberg(...) expects ('namespace.table'[, snapshot_id])".into(),
            ));
        }
        let ident_str = literal_str(&exprs[0], "table identifier")?;
        let snapshot_id = match exprs.get(1) {
            Some(e) => Some(literal_i64_opt(e, "snapshot id")?),
            None => None,
        };
        let provider = build_iceberg_provider(&ident_str, snapshot_id)?;
        Ok(provider as Arc<dyn TableProvider>)
    }
}

/// A metadata-only provider. Each DataFusion `scan` plans a real Iceberg scan
/// with the supplied projection and compatible predicates before materializing
/// batches into a `MemTable`.
#[derive(Debug)]
pub struct IcebergTableProvider {
    table: iceberg::table::Table,
    snapshot_id: Option<i64>,
    schema: SchemaRef,
    // `RwLock`, not a plain field: `columns_projected` is unknowable at construction
    // time — DataFusion only tells a `TableFunctionImpl` the table's NAME/args, never
    // the query's projection (that only reaches `TableProvider::scan`, an `&self`
    // method). `scan` updates this to the REAL pushed-down column count on every call,
    // so a caller reading `pushdown_stats()` AFTER running a query sees the query's
    // actual projection, not a construction-time guess.
    stats: std::sync::RwLock<IcebergPushdownStats>,
}

impl IcebergTableProvider {
    /// Real pushdown counters (P1's proof shape) — [`total_data_files`]/
    /// File and projection counts reflect the most recent `scan()` call. Read
    /// this after running a query under test.
    pub fn pushdown_stats(&self) -> IcebergPushdownStats {
        self.stats
            .read()
            .expect("pushdown stats lock poisoned")
            .clone()
    }

    fn record_scan_stats(&self, files_scanned: u64, columns_projected: usize) {
        let mut stats = self.stats.write().expect("pushdown stats lock poisoned");
        stats.files_scanned = files_scanned;
        stats.columns_projected = columns_projected;
        tracing::info!(
            total_data_files = stats.total_data_files,
            files_scanned = stats.files_scanned,
            files_skipped = stats.files_skipped(),
            "iceberg federation scan"
        );
    }
}

struct IcebergScanRequest {
    projected_schema: SchemaRef,
    columns: Option<Vec<String>>,
    predicate: Option<Predicate>,
    read_limit: Option<usize>,
}

impl IcebergScanRequest {
    fn new(
        schema: &SchemaRef,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Self> {
        let projected_schema = match projection {
            Some(indices) => Arc::new(schema.project(indices)?),
            None => schema.clone(),
        };
        let columns = projection.map(|indices| {
            indices
                .iter()
                .map(|&index| schema.field(index).name().clone())
                .collect()
        });
        let predicate = filters
            .iter()
            .filter_map(|filter| iceberg_predicate_for(filter, schema))
            .reduce(Predicate::and);
        // A pushed limit is safe only when no residual filter can discard rows.
        let read_limit = if filters.is_empty() { limit } else { None };
        Ok(Self {
            projected_schema,
            columns,
            predicate,
            read_limit,
        })
    }
}

async fn collect_scan_batches(
    scan: iceberg::scan::TableScan,
    read_limit: Option<usize>,
) -> iceberg::Result<Vec<RecordBatch>> {
    let mut stream = scan.to_arrow().await?;
    let mut batches = Vec::new();
    let mut rows = 0;
    while let Some(batch) = stream.try_next().await? {
        let remaining = read_limit.map(|max| max.saturating_sub(rows));
        let batch = batch.slice(
            0,
            remaining.unwrap_or(batch.num_rows()).min(batch.num_rows()),
        );
        rows += batch.num_rows();
        batches.push(batch);
        if read_limit.is_some_and(|max| rows >= max) {
            break;
        }
    }
    Ok(batches)
}

async fn materialize_iceberg_scan(
    table: iceberg::table::Table,
    snapshot_id: Option<i64>,
    request: IcebergScanRequest,
) -> iceberg::Result<(Vec<RecordBatch>, u64)> {
    let mut builder = table.scan();
    if let Some(columns) = request.columns {
        builder = builder.select(columns);
    }
    if let Some(predicate) = request.predicate {
        builder = builder.with_filter(predicate);
    }
    if let Some(id) = snapshot_id {
        builder = builder.snapshot_id(id);
    }
    let scan = builder.build()?;
    let files_scanned = scan
        .plan_files()
        .await?
        .try_collect::<Vec<_>>()
        .await?
        .len() as u64;
    let batches = collect_scan_batches(scan, request.read_limit).await?;
    Ok((batches, files_scanned))
}

#[async_trait::async_trait]
impl TableProvider for IcebergTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        datafusion::logical_expr::TableType::Base
    }

    /// `Inexact` for the small filter shape [`iceberg_predicate_for`] can translate —
    /// DataFusion still re-applies the original filter above the scan, exactly like
    /// `UserTableProvider`'s equality pushdown in `crate::tables::provider`.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        super::filter_pushdown::inexact_filter_pushdown(filters, |filter| {
            iceberg_predicate_for(filter, &self.schema).is_some()
        })
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let request = IcebergScanRequest::new(&self.schema, projection, filters, limit)?;
        let read_limit = request.read_limit;
        let projected_schema = request.projected_schema.clone();
        let (batches, files_scanned) = block_on_iceberg(materialize_iceberg_scan(
            self.table.clone(),
            self.snapshot_id,
            request,
        ))?;
        self.record_scan_stats(files_scanned, projected_schema.fields().len());
        MemTable::try_new(projected_schema, vec![batches])?
            .scan(state, None, &[], read_limit)
            .await
    }
}

/// Best-effort DataFusion `Expr` -> iceberg `Predicate` translation for the pushdown
/// forms P1's proof exercises: `col <op> literal` on a Utf8/Int64/Float64 column.
/// Anything else returns `None` (never a wrong/silent narrowing) and DataFusion's own
/// re-filter above the scan remains the ONLY enforcement, matching `Unsupported`.
fn iceberg_predicate_for(expr: &Expr, schema: &Schema) -> Option<Predicate> {
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr else {
        return None;
    };
    let (Expr::Column(col), Expr::Literal(lit, _)) = (left.as_ref(), right.as_ref()) else {
        return None;
    };
    let field = schema.field_with_name(&col.name).ok()?;
    let reference = Reference::new(col.name.clone());
    let datum = iceberg_datum_for_literal(lit, field.data_type())?;
    iceberg_predicate_for_op(*op, reference, datum)
}

/// The subset of DataFusion scalar literals this federation pushdown can encode
/// as an Iceberg [`Datum`]. Split out of [`iceberg_predicate_for`] (extract-method)
/// so each function stays within the per-function complexity cap.
fn iceberg_datum_for_literal(lit: &ScalarValue, column_type: &DataType) -> Option<Datum> {
    Some(match (column_type, lit) {
        (
            DataType::Utf8 | DataType::LargeUtf8,
            ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)),
        ) => Datum::string(s.clone()),
        (DataType::Int64, ScalarValue::Int64(Some(v))) => Datum::long(*v),
        (DataType::Int32, ScalarValue::Int32(Some(v))) => Datum::int(*v),
        (DataType::Float64, ScalarValue::Float64(Some(v))) if v.is_finite() => Datum::double(*v),
        _ => return None,
    })
}

/// The subset of comparison operators this federation pushdown encodes as an
/// Iceberg [`Predicate`]. Split out of [`iceberg_predicate_for`] (extract-method).
fn iceberg_predicate_for_op(op: Operator, reference: Reference, datum: Datum) -> Option<Predicate> {
    Some(match op {
        Operator::Eq => reference.equal_to(datum),
        Operator::NotEq => reference.not_equal_to(datum),
        Operator::Lt => reference.less_than(datum),
        Operator::LtEq => reference.less_than_or_equal_to(datum),
        Operator::Gt => reference.greater_than(datum),
        Operator::GtEq => reference.greater_than_or_equal_to(datum),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::Field;
    use datafusion::prelude::SessionContext;
    use iceberg::io::FileIO;
    use iceberg::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, ManifestListWriter,
        ManifestWriterBuilder, Struct, TableMetadata,
    };
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use serde_json::json;
    use std::fs::{self, File};
    use std::path::PathBuf;

    struct FixtureDir(PathBuf);

    impl Drop for FixtureDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn fixture_metadata(location: &str, manifest_list: &str) -> TableMetadata {
        serde_json::from_value(json!({
            "format-version": 2,
            "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
            "location": location,
            "last-sequence-number": 1,
            "last-updated-ms": 1,
            "last-column-id": 2,
            "current-schema-id": 0,
            "schemas": [{
                "type": "struct", "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "x", "required": true, "type": "long"},
                    {"id": 2, "name": "label", "required": true, "type": "string"}
                ]
            }],
            "default-spec-id": 0,
            "partition-specs": [{
                "spec-id": 0,
                "fields": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}]
            }],
            "last-partition-id": 1000,
            "default-sort-order-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "properties": {},
            "current-snapshot-id": 1,
            "snapshots": [{
                "snapshot-id": 1, "timestamp-ms": 1, "sequence-number": 1,
                "summary": {"operation": "append", "total-data-files": "2"},
                "manifest-list": manifest_list,
                "schema-id": 0
            }],
            "snapshot-log": [{"snapshot-id": 1, "timestamp-ms": 1}],
            "metadata-log": []
        }))
        .expect("valid two-file Iceberg metadata")
    }

    fn write_partition_file(path: &str, key: i64) -> u64 {
        let field = |name: &str, data_type: DataType, id: &str| {
            Field::new(name, data_type, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_owned(),
                id.to_owned(),
            )]))
        };
        let schema = Arc::new(Schema::new(vec![
            field("x", DataType::Int64, "1"),
            field("label", DataType::Utf8, "2"),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![key])) as ArrayRef,
                Arc::new(StringArray::from(vec![format!("row-{key}")])) as ArrayRef,
            ],
        )
        .expect("partition batch");
        let mut writer =
            ArrowWriter::try_new(File::create(path).expect("parquet file"), schema, None)
                .expect("parquet writer");
        writer.write(&batch).expect("write partition batch");
        writer.close().expect("close partition file");
        fs::metadata(path).expect("partition file size").len()
    }

    async fn write_partition_manifest(
        table: &iceberg::table::Table,
        location: &str,
        manifest_list: &str,
    ) {
        let snapshot = table.metadata().current_snapshot().expect("snapshot");
        let mut writer = ManifestWriterBuilder::new(
            table
                .file_io()
                .new_output(format!("{location}/metadata/data.avro"))
                .expect("manifest output"),
            Some(snapshot.snapshot_id()),
            snapshot.schema(table.metadata()).expect("snapshot schema"),
            table.metadata().default_partition_spec().as_ref().clone(),
        )
        .build_v2_data();
        for key in [1_i64, 2] {
            let path = format!("{location}/{key}.parquet");
            let size = write_partition_file(&path, key);
            writer
                .add_file(
                    DataFileBuilder::default()
                        .partition_spec_id(0)
                        .content(DataContentType::Data)
                        .file_path(path)
                        .file_format(DataFileFormat::Parquet)
                        .file_size_in_bytes(size)
                        .record_count(1)
                        .partition(Struct::from_iter([Some(Literal::long(key))]))
                        .build()
                        .expect("partition data file"),
                    1,
                )
                .expect("manifest entry");
        }
        let manifest = writer.write_manifest_file().await.expect("data manifest");
        let output = table
            .file_io()
            .new_output(&manifest_list)
            .expect("manifest list output")
            .writer()
            .await
            .expect("manifest list writer");
        let mut list = ManifestListWriter::v2(output, snapshot.snapshot_id(), None, 1);
        list.add_manifests(vec![manifest].into_iter())
            .expect("manifest list entries");
        list.close().await.expect("manifest list close");
    }

    async fn partitioned_provider() -> (FixtureDir, Arc<IcebergTableProvider>) {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("eh578-{}-{nonce}", std::process::id()));
        fs::create_dir_all(root.join("metadata")).expect("fixture metadata directory");
        let fixture = FixtureDir(root);
        let location = fixture.0.to_str().expect("utf8 fixture path");
        let manifest_list = format!("{location}/metadata/list.avro");
        let table = iceberg::table::Table::builder()
            .metadata(fixture_metadata(location, &manifest_list))
            .identifier(TableIdent::from_strs(["fixture", "partitioned"]).expect("table id"))
            .file_io(FileIO::new_with_fs())
            .metadata_location(format!("{location}/metadata/v1.json"))
            .runtime(iceberg::Runtime::current())
            .build()
            .expect("fixture table");
        write_partition_manifest(&table, location, &manifest_list).await;
        let schema = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
                .expect("arrow schema"),
        );
        let provider = Arc::new(IcebergTableProvider {
            table,
            snapshot_id: None,
            schema,
            stats: std::sync::RwLock::new(IcebergPushdownStats {
                total_data_files: 2,
                files_scanned: 2,
                columns_total: 2,
                columns_projected: 2,
            }),
        });
        (fixture, provider)
    }

    fn keys(batches: &[RecordBatch]) -> Vec<i64> {
        let mut result = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("x")
                    .expect("x column")
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("x is Int64")
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();
        result.sort_unstable();
        result
    }

    #[test]
    fn catalog_props_carries_every_configured_key() {
        let cfg = IcebergCatalogConfig {
            catalog_uri: "http://lakekeeper.example/catalog".to_string(),
            warehouse: Some("wh".to_string()),
            credential: Some("id:secret".to_string()),
            scope: Some("lakekeeper".to_string()),
            oauth2_uri: Some("http://kc/token".to_string()),
            token: None,
        };
        let props = cfg.catalog_props();
        assert_eq!(
            props.get("uri").unwrap(),
            "http://lakekeeper.example/catalog"
        );
        assert_eq!(props.get("warehouse").unwrap(), "wh");
        assert_eq!(props.get("credential").unwrap(), "id:secret");
        assert_eq!(props.get("scope").unwrap(), "lakekeeper");
        assert_eq!(props.get("oauth2-server-uri").unwrap(), "http://kc/token");
        assert!(!props.contains_key("token"));
    }

    #[test]
    fn missing_catalog_uri_errors_clearly_never_silently_empty() {
        // SAFETY: single-threaded test process env mutation, scoped to this test only.
        unsafe {
            std::env::remove_var(ICEBERG_FEDERATION_CATALOG_URI_ENV);
        }
        let err = IcebergCatalogConfig::from_env().unwrap_err();
        assert!(err.to_string().contains(ICEBERG_FEDERATION_CATALOG_URI_ENV));
    }

    #[test]
    fn pushdown_stats_files_skipped_never_underflows() {
        let stats = IcebergPushdownStats {
            total_data_files: 0,
            files_scanned: 3,
            columns_total: 2,
            columns_projected: 2,
        };
        assert_eq!(
            stats.files_skipped(),
            0,
            "scanned > total must clamp, not panic/wrap"
        );
    }
    #[test]
    fn predicate_datum_requires_matching_column_type() {
        assert!(
            iceberg_datum_for_literal(&ScalarValue::Int64(Some(7)), &DataType::Int64).is_some()
        );
        assert!(
            iceberg_datum_for_literal(&ScalarValue::Int32(Some(7)), &DataType::Int32).is_some()
        );
        assert!(
            iceberg_datum_for_literal(&ScalarValue::Utf8(Some("7".into())), &DataType::Int64)
                .is_none()
        );
        assert!(
            iceberg_datum_for_literal(&ScalarValue::Int64(Some(7)), &DataType::Int32).is_none()
        );
        assert!(iceberg_datum_for_literal(
            &ScalarValue::Float64(Some(f64::NAN)),
            &DataType::Float64
        )
        .is_none());
    }

    // `scan()` bridges to its own runtime while the Iceberg table's IO tasks
    // use this fixture runtime; keep workers available during the sync bridge.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn partitioned_manifest_prunes_files_without_changing_result_rows() {
        let (_fixture, provider) = partitioned_provider().await;
        let ctx = SessionContext::new();
        ctx.register_table("iceberg_fixture", provider.clone())
            .expect("register fixture table");

        let all = ctx
            .sql("SELECT x FROM iceberg_fixture")
            .await
            .expect("full scan plan")
            .collect()
            .await
            .expect("full scan rows");
        assert_eq!(keys(&all), vec![1, 2]);
        assert_eq!(provider.pushdown_stats().files_scanned, 2);

        let filtered = ctx
            .sql("SELECT x FROM iceberg_fixture WHERE x = 1")
            .await
            .expect("filtered scan plan")
            .collect()
            .await
            .expect("filtered scan rows");
        let expected = keys(&all)
            .into_iter()
            .filter(|key| *key == 1)
            .collect::<Vec<_>>();
        assert_eq!(keys(&filtered), expected);
        let stats = provider.pushdown_stats();
        assert_eq!(stats.total_data_files, 2);
        assert!(
            stats.files_scanned < stats.total_data_files,
            "selective identity partition predicate must prune a real manifest data file: {stats:?}"
        );
        assert_eq!(stats.columns_projected, 1);
    }
}
