// CONCEPT:EG-091 — slow-query log.
//
// When `EPISTEMIC_GRAPH_SLOW_QUERY_MS` is set to a positive integer, any query
// whose end-to-end execution meets/exceeds that many milliseconds is logged as a
// structured `tracing::warn!` carrying the query kind, the (truncated) query
// text, the elapsed ms, and — where the call site has them — the plan/pipeline
// op count and a cost estimate. Unset / `0` / invalid ⇒ DISABLED: the only cost
// on the hot path is a single cached-threshold compare (`describe*` returns
// `None`, so no descriptor is even built).
//
// A Prometheus counter `epistemic_graph_slow_query_total` (feature `metrics`)
// counts the crossings; it stays at zero until the threshold is configured.

use std::sync::OnceLock;
use std::time::Duration;

use crate::protocol::Method;

/// Max query-text characters kept in the log line — avoid dumping a megabyte of
/// SQL into a log record.
const MAX_QUERY_CHARS: usize = 512;

/// Parsed once from `EPISTEMIC_GRAPH_SLOW_QUERY_MS`. `0` (also the unset/invalid
/// case) means slow-query logging is disabled.
fn threshold_ms() -> u64 {
    static T: OnceLock<u64> = OnceLock::new();
    *T.get_or_init(|| {
        std::env::var("EPISTEMIC_GRAPH_SLOW_QUERY_MS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(0)
    })
}

/// True when slow-query logging is enabled (threshold > 0). Cheap: a single load
/// of the once-parsed threshold.
#[inline]
pub fn enabled() -> bool {
    threshold_ms() > 0
}

/// A cheap, owned descriptor of a query worth timing, captured BEFORE the request
/// method is consumed by dispatch. Built only when logging is enabled; `None`
/// otherwise, so the disabled hot path allocates nothing.
pub struct SlowQuery {
    kind: &'static str,
    text: String,
    /// Plan/pipeline op count, where the surface exposes one (UnifiedQuery).
    plan_ops: Option<usize>,
    /// Cost estimate, where a cost model produced one at the call site.
    cost: Option<f64>,
}

impl SlowQuery {
    /// Build a descriptor for a query surface. `plan_ops` / `cost` are `None` when
    /// unavailable at the call site (they are logged as `None`). The query text is
    /// truncated to [`MAX_QUERY_CHARS`] with an ellipsis marker.
    pub fn new(
        kind: &'static str,
        text: &str,
        plan_ops: Option<usize>,
        cost: Option<f64>,
    ) -> Self {
        let mut t: String = text.chars().take(MAX_QUERY_CHARS).collect();
        if text.chars().nth(MAX_QUERY_CHARS).is_some() {
            t.push('…');
        }
        Self {
            kind,
            text: t,
            plan_ops,
            cost,
        }
    }

    /// Emit the structured warn line + bump the metric IFF `elapsed` met the
    /// threshold. A single compare when under threshold.
    pub fn log_if_slow(&self, elapsed: Duration) {
        let thr = threshold_ms();
        if thr == 0 {
            return;
        }
        let ms = elapsed.as_secs_f64() * 1000.0;
        if ms < thr as f64 {
            return;
        }
        crate::metrics::slow_query();
        tracing::warn!(
            target: "epistemic_graph::slow_query",
            kind = self.kind,
            elapsed_ms = ms,
            threshold_ms = thr,
            plan_ops = ?self.plan_ops,
            cost_estimate = ?self.cost,
            query = %self.text,
            "slow query (CONCEPT:EG-091)"
        );
    }
}

/// Describe a dispatch `Method` for slow-query timing (CONCEPT:EG-091). Returns
/// `None` instantly when logging is disabled or the method is not a query, so the
/// hot path pays a single bool check for non-query traffic.
pub fn describe(method: &Method) -> Option<SlowQuery> {
    if !enabled() {
        return None;
    }
    match method {
        Method::Sql { query, .. } => Some(SlowQuery::new("sql", query, None, None)),
        Method::CypherQuery { query } => Some(SlowQuery::new("cypher", query, None, None)),
        #[cfg(feature = "graphql")]
        Method::GraphQl { query, .. } => Some(SlowQuery::new("graphql", query, None, None)),
        #[cfg(feature = "query")]
        Method::UnifiedQuery { plan, .. } => Some(SlowQuery::new(
            "unified_query",
            "<plan>",
            Some(plan.ops.len()),
            None,
        )),
        #[cfg(feature = "query")]
        Method::UnifiedQueryText { text, .. } => {
            Some(SlowQuery::new("unified_query_text", text, None, None))
        }
        #[cfg(feature = "sparql")]
        Method::Sparql { query, .. } => Some(SlowQuery::new("sparql", query, None, None)),
        _ => None,
    }
}

/// Describe a raw SQL statement on a wire path for slow-query timing (CONCEPT:EG-091).
/// `None` when logging is disabled. Gated on `wire` (the module that calls it) rather
/// than `pgwire` specifically, so EVERY wire consumer (pgwire, sqlite-wire EG-075, and
/// the later mysql/mssql wires) links it — `pgwire` implies `wire`, so pg builds are
/// unchanged.
#[cfg(feature = "wire")]
pub fn describe_sql(sql: &str) -> Option<SlowQuery> {
    if !enabled() {
        return None;
    }
    Some(SlowQuery::new("pgwire_sql", sql, None, None))
}
