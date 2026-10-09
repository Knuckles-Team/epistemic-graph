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
//! learn the Arrow schema DataFusion's planner requires up front) runs on a Tokio runtime
//! the provider OWNS ([`FederationRuntime`]) and is waited for from the sync caller over
//! a channel — this sidesteps the documented "nested `block_on` panics on a
//! current-thread runtime" hazard the module doc at the top of `sql/exec.rs` already
//! flags for this SQL surface, without requiring the caller to be inside any particular
//! runtime shape. The same runtime serves every later scan: the `iceberg` crate keeps a
//! handle to the runtime a table was loaded on and spawns that table's manifest and file
//! tasks there, so the runtime has to live as long as the table does — the provider holds
//! both. [`IcebergTableProvider::scan`] runs on it so a later
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

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::{Session, TableFunctionArgs, TableFunctionImpl, TableProvider};
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::scalar::ScalarValue;
use futures_util::TryStreamExt;
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::Datum;
use iceberg::{Catalog, CatalogBuilder, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;

use super::filter_shape::{classify_pushdown, ScanPlan};

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

    /// Connect, naming `runtime` as the runtime every table this catalog loads spawns
    /// its tasks on (the builder would otherwise capture whichever runtime calls it).
    async fn connect(
        &self,
        runtime: iceberg::Runtime,
    ) -> iceberg::Result<iceberg_catalog_rest::RestCatalog> {
        RestCatalogBuilder::default()
            .with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::new()))
            .with_runtime(runtime)
            .load("eg-federation", self.catalog_props())
            .await
    }
}

/// How long one iceberg catalog/table call may take before it is reported as a
/// stalled federation rather than a slow one. Generous: these are remote object
/// store and catalog round trips, and a large table's metadata scan is legitimately
/// slow. Finite so a catalog that never answers is an error, not a hung query.
const ICEBERG_FEDERATION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);

/// How much longer than [`ICEBERG_FEDERATION_TIMEOUT`] a sync caller waits for the
/// answer of a call that is itself bounded by that timeout.
const ICEBERG_FEDERATION_ANSWER_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

/// The Tokio runtime one provider's catalog and table calls run on.
///
/// The `iceberg` crate does not own a runtime: a table keeps a handle to the runtime it
/// was loaded on and spawns its manifest and data-file tasks there on every scan. A
/// runtime dropped after the load would cancel those tasks, so this one is owned by the
/// provider that holds the table and serves both the load and every scan. It is shut
/// down without blocking when the provider goes away, which is safe from inside another
/// runtime — where DataFusion usually drops a provider.
#[derive(Debug)]
struct FederationRuntime {
    handle: tokio::runtime::Handle,
    iceberg: iceberg::Runtime,
    owned: Option<tokio::runtime::Runtime>,
}

impl FederationRuntime {
    fn start() -> DfResult<Arc<Self>> {
        let owned = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .thread_name("eg-iceberg-federation")
            .build()
            .map_err(|e| DataFusionError::Execution(format!("iceberg federation runtime: {e}")))?;
        Ok(Arc::new(Self {
            handle: owned.handle().clone(),
            iceberg: iceberg::Runtime::new(&owned),
            owned: Some(owned),
        }))
    }

    /// Run `future` on this runtime and wait for it from a SYNC caller. The caller may
    /// itself be a runtime thread: it waits on a channel, never in a nested `block_on`.
    fn block_on<F, T>(&self, future: F) -> DfResult<T>
    where
        F: std::future::Future<Output = iceberg::Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        let (answer, answered) = std::sync::mpsc::sync_channel(1);
        self.handle.spawn(async move {
            // The receiver is gone only when the caller stopped waiting.
            let _ = answer.send(within_federation_timeout(future).await);
        });
        answered
            .recv_timeout(ICEBERG_FEDERATION_TIMEOUT + ICEBERG_FEDERATION_ANSWER_GRACE)
            .map_err(|_| {
                DataFusionError::Execution(
                    "iceberg federation: the catalog call ended without an answer".into(),
                )
            })?
    }

    /// Run `future` on this runtime and await it from an ASYNC caller on any runtime.
    async fn run<F, T>(&self, future: F) -> DfResult<T>
    where
        F: std::future::Future<Output = iceberg::Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        self.handle
            .spawn(within_federation_timeout(future))
            .await
            .map_err(|e| DataFusionError::Execution(format!("iceberg federation: {e}")))?
    }
}

impl Drop for FederationRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.owned.take() {
            runtime.shutdown_background();
        }
    }
}

/// Bound one catalog/table future by [`ICEBERG_FEDERATION_TIMEOUT`]. The deadline is on
/// the OPERATION, so a stalled iceberg call is a DataFusion error with a cause rather
/// than a wait nothing ends.
async fn within_federation_timeout<F, T>(future: F) -> DfResult<T>
where
    F: std::future::Future<Output = iceberg::Result<T>>,
{
    match tokio::time::timeout(ICEBERG_FEDERATION_TIMEOUT, future).await {
        Ok(result) => {
            result.map_err(|e| DataFusionError::Execution(format!("iceberg federation: {e}")))
        }
        Err(_) => Err(DataFusionError::Execution(format!(
            "iceberg federation: the catalog did not answer within {}s",
            ICEBERG_FEDERATION_TIMEOUT.as_secs()
        ))),
    }
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
    let runtime = FederationRuntime::start()?;
    let loading = load_table(
        config,
        runtime.iceberg.clone(),
        table_ident,
        ident_str.to_string(),
    );
    let table = runtime.block_on(loading)?;
    let schema = iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
        .map_err(|e| DataFusionError::Execution(format!("iceberg federation: {e}")))?;
    let total_data_files = total_data_files(&table, snapshot_id);
    let columns_total = schema.fields().len();

    Ok(Arc::new(IcebergTableProvider {
        table,
        runtime,
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

/// Connect to the catalog and load `table_ident`, binding the table to `runtime`.
async fn load_table(
    config: IcebergCatalogConfig,
    runtime: iceberg::Runtime,
    table_ident: TableIdent,
    ident_for_err: String,
) -> iceberg::Result<iceberg::table::Table> {
    let catalog = config.connect(runtime).await?;
    catalog.load_table(&table_ident).await.map_err(|e| {
        // Typed, named error — NEVER an empty result (P1's negative case): a
        // dropped/nonexistent table surfaces its identity in the message.
        iceberg::Error::new(
            e.kind(),
            format!("iceberg federated table '{ident_for_err}' unavailable: {e}"),
        )
    })
}

/// The data-file count the selected snapshot's summary records (`0` when it has none).
fn total_data_files(table: &iceberg::table::Table, snapshot_id: Option<i64>) -> u64 {
    snapshot_id
        .and_then(|id| table.metadata().snapshot_by_id(id))
        .or_else(|| table.metadata().current_snapshot())
        .and_then(|s| s.summary().additional_properties.get("total-data-files"))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
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
    /// The runtime `table` was loaded on and spawns its scan tasks on; kept for as long
    /// as the table is.
    runtime: Arc<FederationRuntime>,
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
        Ok(classify_pushdown(filters, |candidate| {
            iceberg_predicate_for(candidate, &self.schema).is_some()
        }))
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        pushed_exprs: &[Expr],
        limit: Option<usize>,
    ) -> ScanPlan {
        let projected_schema = match projection {
            Some(indices) => Arc::new(self.schema.project(indices)?),
            None => self.schema.clone(),
        };
        let columns = projection.map(|indices| {
            indices
                .iter()
                .map(|&index| self.schema.field(index).name().clone())
                .collect::<Vec<_>>()
        });
        let request = ScanRequest {
            columns,
            predicate: pushed_exprs
                .iter()
                .filter_map(|filter| iceberg_predicate_for(filter, &self.schema))
                .reduce(Predicate::and),
            snapshot_id: self.snapshot_id,
            // A pushed limit is safe only when no residual filter can discard rows.
            limit: if pushed_exprs.is_empty() { limit } else { None },
        };
        let read_limit = request.limit;
        let reading = read_table(self.table.clone(), request);
        let (batches, files_scanned) = self.runtime.run(reading).await?;
        let stats = {
            let mut stats = self.stats.write().expect("pushdown stats lock poisoned");
            stats.files_scanned = files_scanned;
            stats.columns_projected = projected_schema.fields().len();
            stats.clone()
        };
        tracing::info!(
            total_data_files = stats.total_data_files,
            files_scanned = stats.files_scanned,
            files_skipped = stats.files_skipped(),
            "iceberg federation scan"
        );
        let mem = MemTable::try_new(projected_schema, vec![batches])?;
        mem.scan(state, None, &[], read_limit).await
    }
}

/// What one DataFusion scan asks of the table.
struct ScanRequest {
    /// The projected column names (`None` = every column).
    columns: Option<Vec<String>>,
    /// The conjunction of the pushed filters the table format can evaluate.
    predicate: Option<Predicate>,
    snapshot_id: Option<i64>,
    /// Stop reading after this many rows.
    limit: Option<usize>,
}

/// Plan `request` against `table` and read it: the batches, and the number of data files
/// the manifest planner kept.
async fn read_table(
    table: iceberg::table::Table,
    request: ScanRequest,
) -> iceberg::Result<(Vec<arrow::record_batch::RecordBatch>, u64)> {
    let mut builder = table.scan();
    if let Some(columns) = request.columns {
        builder = builder.select(columns);
    }
    if let Some(predicate) = request.predicate {
        builder = builder.with_filter(predicate);
    }
    if let Some(id) = request.snapshot_id {
        builder = builder.snapshot_id(id);
    }
    let scan = builder.build()?;
    let files_scanned = scan
        .plan_files()
        .await?
        .try_collect::<Vec<_>>()
        .await?
        .len() as u64;
    let mut stream = scan.to_arrow().await?;
    let mut batches = Vec::new();
    let mut rows = 0;
    while let Some(batch) = stream.try_next().await? {
        let remaining = request.limit.map(|max| max.saturating_sub(rows));
        let batch = batch.slice(
            0,
            remaining.unwrap_or(batch.num_rows()).min(batch.num_rows()),
        );
        rows += batch.num_rows();
        batches.push(batch);
        if request.limit.is_some_and(|max| rows >= max) {
            break;
        }
    }
    Ok((batches, files_scanned))
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
    if schema.field_with_name(&col.name).is_err() {
        return None;
    }
    let reference = Reference::new(col.name.clone());
    let datum = iceberg_datum_for_literal(lit)?;
    iceberg_predicate_for_op(*op, reference, datum)
}

/// The subset of DataFusion scalar literals this federation pushdown can encode
/// as an Iceberg [`Datum`]. Split out of [`iceberg_predicate_for`] (extract-method)
/// so each function stays within the per-function complexity cap.
fn iceberg_datum_for_literal(lit: &ScalarValue) -> Option<Datum> {
    Some(match lit {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Datum::string(s.clone()),
        ScalarValue::Int64(Some(v)) => Datum::long(*v),
        ScalarValue::Int32(Some(v)) => Datum::long(*v as i64),
        ScalarValue::Float64(Some(v)) => Datum::double(*v),
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
    fn the_runtime_bridges_a_sync_caller_and_drops_inside_another_runtime() {
        let runtime = FederationRuntime::start().unwrap();
        assert_eq!(runtime.block_on(async { Ok(7) }).unwrap(), 7);
        let failed = runtime.block_on(async {
            Err::<(), _>(iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "catalog refused",
            ))
        });
        assert!(failed.unwrap_err().to_string().contains("catalog refused"));

        // A caller that is itself a runtime thread: the sync bridge waits on a channel
        // (no nested `block_on`), the async bridge awaits across runtimes, and dropping
        // the owner there must not panic the way dropping a Tokio runtime does.
        let caller = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        caller.block_on(async move {
            assert_eq!(runtime.block_on(async { Ok(8) }).unwrap(), 8);
            assert_eq!(runtime.run(async { Ok(9) }).await.unwrap(), 9);
            drop(runtime);
        });
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
}
