//! EH-524 end to end through the served `dispatch` path against a real `series.redb`:
//! `TsDefineSeries`, maintenance on `TsAppend`, revisions, refusals and tenancy.

use std::path::PathBuf;
use std::sync::Arc;

use eg_tsdb::derive::maintain::latest_versions;
use eg_tsdb::derive::Program;
use eg_tsdb::point::Point;
use eg_types::series_expr::DerivedSeriesReceipt;
use tokio::sync::RwLock;

use crate::protocol::{Method, Response, ResultPayload};
use crate::server::auth::{build_current_test_request, dispatch_test_on_heap};
use crate::server::state::ServerState;

const SECRET: &str = "eh524-derived-series";
const TENANT: &str = "tenant-shared";
const EXPR: &str = "zscore(ewma(v0, 4), 8)";

struct Fixture {
    state: Arc<RwLock<ServerState>>,
    path: PathBuf,
    next_id: std::sync::atomic::AtomicU64,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

impl Fixture {
    async fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "eg-derived-{}-{}.redb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut state = ServerState::new_for_test(SECRET, ServerState::test_isolation("system"));
        state.tsdb_store = Some(super::open_test_series_store(&path));
        Self {
            state: Arc::new(RwLock::new(state)),
            path,
            next_id: 1.into(),
        }
    }

    async fn call(&self, tenant: &str, method: Method) -> Response {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let request =
            build_current_test_request(SECRET, tenant, id, "__commons__", "system", method);
        dispatch_test_on_heap(&self.state, request).await
    }

    async fn append(&self, series: &str, points: &[(i64, f64)]) {
        let wire: Vec<(i64, Vec<f64>)> = points.iter().map(|&(t, v)| (t, vec![v])).collect();
        let response = self
            .call(
                TENANT,
                Method::TsAppend {
                    series_id: series.into(),
                    n_fields: 1,
                    bucket_ns: 1_000_000,
                    field_names: vec!["value".into()],
                    points_msgpack: rmp_serde::to_vec(&wire).unwrap(),
                },
            )
            .await;
        assert!(response.error.is_none(), "append: {:?}", response.error);
    }

    async fn define(&self, tenant: &str, series: &str, source: &str, expr: &str) -> Response {
        self.call(
            tenant,
            Method::TsDefineSeries {
                series_id: series.into(),
                source: source.into(),
                expr: expr.into(),
            },
        )
        .await
    }

    async fn range(&self, tenant: &str, series: &str) -> Vec<Point> {
        let response = self
            .call(
                tenant,
                Method::TsRange {
                    series_id: series.into(),
                    from: i64::MIN,
                    to: i64::MAX,
                },
            )
            .await;
        let raw: Vec<(i64, Vec<f64>)> = raw(&response);
        raw.into_iter()
            .map(|(ts, values)| Point { ts, values })
            .collect()
    }
}

impl Fixture {
    /// The tenants that own any series in the store.
    async fn tenants_with_series(&self) -> Vec<String> {
        let state = self.state.read().await;
        let store = state.tsdb_store.as_ref().unwrap();
        let mut tenants: Vec<String> = store
            .list_series()
            .unwrap()
            .iter()
            .filter_map(|encoded| eg_tsdb::store::SeriesKey::decode(encoded))
            .map(|key| key.tenant)
            .collect();
        tenants.sort_unstable();
        tenants.dedup();
        tenants
    }
}

fn raw<T: serde::de::DeserializeOwned>(response: &Response) -> T {
    match &response.result {
        Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(bytes).unwrap(),
        other => panic!(
            "expected a raw result, got {other:?} / {:?}",
            response.error
        ),
    }
}

fn walk(from: i64, to: i64) -> Vec<(i64, f64)> {
    (from..to)
        .map(|i| (i * 1_000, 100.0 + (i % 9) as f64 * 0.75 - (i % 4) as f64))
        .collect()
}

/// `DERIVE zscore(ewma(v0, 4), 8)` on the fly over `points`: `(ts, value bits)`.
fn on_the_fly(points: &[(i64, f64)]) -> Vec<(i64, u64)> {
    let expr = eg_plan::uql::parse_series_expr(EXPR).unwrap();
    let mut program = Program::compile(&expr).unwrap();
    points
        .iter()
        .filter_map(|&(ts, v)| program.step(&|_| Some(v)).map(|z| (ts, z.to_bits())))
        .collect()
}

fn latest(points: &[Point]) -> Vec<(i64, u64)> {
    latest_versions(points)
        .iter()
        .map(|p| (p.ts, p.values[0].to_bits()))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_materialised_series_equals_derive_and_follows_appends() {
    let _env = crate::crypto::acquire_test_env_read_lock().await;
    let fx = Fixture::new().await;
    let history = walk(0, 300);
    fx.append("px", &history).await;
    let receipt: DerivedSeriesReceipt = raw(&fx.define(TENANT, "px_z", "px", EXPR).await);
    assert_eq!(receipt.derived_from, "px");
    assert_eq!(receipt.expr, EXPR);
    assert!(receipt.digest.starts_with("sha256:") && receipt.caught_up);
    assert_eq!(receipt.last_ts, Some(299_000));
    fx.append("px", &walk(300, 420)).await;
    fx.append("px", &walk(420, 500)).await;
    let all = walk(0, 500);
    assert_eq!(latest(&fx.range(TENANT, "px_z").await), on_the_fly(&all));
    // Re-defining identically is idempotent: nothing new to derive.
    let again: DerivedSeriesReceipt = raw(&fx.define(TENANT, "px_z", "px", EXPR).await);
    assert_eq!((again.appended, again.digest), (0, receipt.digest));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_source_correction_appends_revisions_never_edits() {
    let _env = crate::crypto::acquire_test_env_read_lock().await;
    let fx = Fixture::new().await;
    let mut source = walk(0, 400);
    fx.append("px", &source).await;
    fx.define(TENANT, "px_z", "px", EXPR).await;
    let before = fx.range(TENANT, "px_z").await;
    // Correct the point at ts 350_000: the store appends a second version there.
    source[350].1 += 9.5;
    fx.append("px", &[source[350]]).await;
    let after = fx.range(TENANT, "px_z").await;
    assert!(after.len() > before.len(), "revisions are appended");
    let originals: Vec<&Point> = after.iter().filter(|p| p.values[1] == 0.0).collect();
    assert_eq!(
        originals.len(),
        before.len(),
        "no original version was edited"
    );
    assert!(after
        .iter()
        .filter(|p| p.values[1] == 1.0)
        .all(|p| p.ts >= 350_000));
    assert_eq!(latest(&after), on_the_fly(&source));
}

// spec: EG-REPO-INGEST-R004
#[tokio::test(flavor = "multi_thread")]
async fn bad_definitions_are_refused_and_other_tenants_see_nothing() {
    let _env = crate::crypto::acquire_test_env_read_lock().await;
    let fx = Fixture::new().await;
    fx.append("px", &walk(0, 50)).await;
    fx.define(TENANT, "px_z", "px", EXPR).await;
    let refusals = [
        ("nope", "missing", EXPR, "does not exist"),
        ("self", "self", EXPR, "itself"),
        ("px_z", "px", "rmean(v0, 3)", "different definition"),
        ("px", "px_z", "rmean(v0, 3)", "cycle"),
        ("wide", "px", "rmean(v3, 3)", "not a field"),
        ("bad", "px", "smooth(v0, 3)", "UQL_UNKNOWN_FUNCTION"),
    ];
    for (series, source, expr, why) in refusals {
        let response = fx.define(TENANT, series, source, expr).await;
        let error = response.error_detail.unwrap_or_default();
        assert!(
            response.error.as_deref() == Some(why) || error.contains(why),
            "{series} over {source}: code={:?}, detail={error}",
            response.error
        );
    }
    // A deployment serves one tenant: a request context for another tenant is refused
    // at authentication (`auth::claims::validate_deployment_binding`), before any
    // handler runs, for the define and for a read alike. Pin the stable code,
    // detail, and absence of any foreign tenant write or read.
    const FOREIGN_TENANT: &str = "request context tenant does not match graph tenant";
    let foreign = fx.define("tenant-other", "px_z2", "px", EXPR).await;
    assert_eq!(foreign.error.as_deref(), Some("AUTH_TENANT_MISMATCH"));
    let error = foreign.error_detail.unwrap_or_default();
    assert!(error.contains(FOREIGN_TENANT), "foreign define: {error}");
    let foreign_range = fx
        .call(
            "tenant-other",
            Method::TsRange {
                series_id: "px_z".into(),
                from: i64::MIN,
                to: i64::MAX,
            },
        )
        .await;
    assert_eq!(foreign_range.error.as_deref(), Some("AUTH_TENANT_MISMATCH"));
    assert!(
        foreign_range.result.is_none(),
        "a foreign tenant reads nothing"
    );
    let error = foreign_range.error_detail.unwrap_or_default();
    assert!(error.contains(FOREIGN_TENANT), "foreign range: {error}");
    assert_eq!(
        fx.tenants_with_series().await.len(),
        1,
        "only the owner's scope holds series"
    );
    assert!(!fx.range(TENANT, "px_z").await.is_empty());
}
