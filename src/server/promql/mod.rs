//! CONCEPT:EG-KG.query.prometheus-http-query-api — the Prometheus-compatible HTTP query API, layered over the
//! pure-Rust PromQL engine in `eg_tsdb::promql` and the durable `SeriesStore`.
//!
//! This is the FACADE half of EG-172: it adapts the observability tier's durable
//! time-series store to the engine's dependency-free [`SeriesSource`] and serves the
//! Prometheus HTTP API on the SAME hand-rolled obs listener that ingests logs:
//!
//!  * `GET`/`POST /api/v1/query`        — instant query (`query`, `time`)
//!  * `GET`/`POST /api/v1/query_range`  — range query (`query`, `start`, `end`, `step`)
//!  * `GET      /api/v1/labels`         — all label names
//!  * `GET      /api/v1/label/<n>/values` — all values for label `n`
//!
//! Responses use the exact Prometheus envelope
//! `{"status":"success","data":{"resultType":…,"result":[…]}}`; errors are
//! `{"status":"error","errorType":…,"error":…}`.
//!
//! ## Series identity
//!
//! The durable `SeriesStore` keys series by an OPAQUE string id. The PromQL layer
//! therefore encodes a metric's identity INTO that id using the canonical Prometheus
//! text form — `name{label="value",…}` (labels sorted) or a bare `name` — and
//! [`parse_series_id`] decodes it back into a [`Labels`] set. Selector resolution
//! enumerates ids via `SeriesStore::list_series` and filters by the parsed labels; a
//! series' value is field 0 of each stored point.

use std::sync::Arc;

use eg_tsdb::promql::{
    parse, Evaluator, Expr, LabelMatcher, Labels, PromqlError, RangeSeries, SeriesSource, Value,
    METRIC_NAME,
};
use eg_tsdb::store::SeriesStore;

use crate::server::obs::ObsState;

/// Nanoseconds per second.
const NS_PER_SEC: f64 = 1_000_000_000.0;

// ───────────────────────────── store adapter ─────────────────────────────

/// A [`SeriesSource`] over the durable [`SeriesStore`] (CONCEPT:EG-KG.query.prometheus-http-query-api).
struct StoreSource {
    store: Arc<SeriesStore>,
}

impl SeriesSource for StoreSource {
    fn select(
        &self,
        matchers: &[LabelMatcher],
        start: i64,
        end: i64,
    ) -> Vec<eg_tsdb::promql::LabeledSeries> {
        let ids = self.store.list_series().unwrap_or_default();
        let mut out = Vec::new();
        for id in ids {
            let labels = parse_series_id(&id);
            if matchers.iter().all(|m| m.matches(&labels)) {
                let points = self
                    .store
                    .range(&id, start, end)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|p| (p.ts, p.values.first().copied().unwrap_or(f64::NAN)))
                    .collect();
                out.push(eg_tsdb::promql::LabeledSeries { labels, points });
            }
        }
        out
    }

    fn label_names(&self) -> Vec<String> {
        let mut set = std::collections::BTreeSet::new();
        for id in self.store.list_series().unwrap_or_default() {
            for k in parse_series_id(&id).into_keys() {
                set.insert(k);
            }
        }
        set.into_iter().collect()
    }

    fn label_values(&self, name: &str) -> Vec<String> {
        let mut set = std::collections::BTreeSet::new();
        for id in self.store.list_series().unwrap_or_default() {
            if let Some(v) = parse_series_id(&id).remove(name) {
                set.insert(v);
            }
        }
        set.into_iter().collect()
    }
}

/// Decode a series id (`name{k="v",…}` or bare `name`) into a label set. A malformed
/// id degrades to `{__name__=<id>}` rather than erroring (the store is trusted).
pub fn parse_series_id(id: &str) -> Labels {
    let mut labels = Labels::new();
    match id.split_once('{') {
        None => {
            labels.insert(METRIC_NAME.to_string(), id.to_string());
        }
        Some((name, rest)) => {
            if !name.is_empty() {
                labels.insert(METRIC_NAME.to_string(), name.to_string());
            }
            let inner = rest.strip_suffix('}').unwrap_or(rest);
            for pair in split_top_commas(inner) {
                if let Some((k, v)) = pair.split_once('=') {
                    let k = k.trim();
                    let v = v.trim().trim_matches('"');
                    if !k.is_empty() {
                        labels.insert(k.to_string(), v.to_string());
                    }
                }
            }
        }
    }
    labels
}

/// Encode a label set into the canonical series id — the inverse of
/// [`parse_series_id`], used by ingesters that want PromQL-queryable series.
pub fn format_series_id(labels: &Labels) -> String {
    let name = labels.get(METRIC_NAME).cloned().unwrap_or_default();
    let pairs: Vec<String> = labels
        .iter()
        .filter(|(k, _)| k.as_str() != METRIC_NAME)
        .map(|(k, v)| format!("{k}=\"{v}\""))
        .collect();
    if pairs.is_empty() {
        name
    } else {
        format!("{name}{{{}}}", pairs.join(","))
    }
}

/// Split on commas that are NOT inside a quoted value.
fn split_top_commas(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_q = false;
    for c in s.chars() {
        match c {
            '"' => {
                in_q = !in_q;
                cur.push(c);
            }
            ',' if !in_q => {
                out.push(std::mem::take(&mut cur));
            }
            _ => cur.push(c),
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur);
    }
    out
}

// ───────────────────────────── HTTP entry ─────────────────────────────

/// An HTTP reply: `(status, content_type, body)`.
type Reply = (&'static str, &'static str, String);

/// Route + execute a Prometheus HTTP API request. Returns `(status, content_type,
/// body)`. `method` is the request method, `path`/`query` the split target, `body` the
/// (form-encoded, for POST) request body.
pub async fn handle(
    state: &Arc<ObsState>,
    method: &str,
    path: &str,
    query: &str,
    body: &str,
) -> Reply {
    // Merge query-string + (POST form) body params (both percent-decoded).
    let mut params = parse_form(query);
    if method == "POST" {
        params.extend(parse_form(body));
    }
    let store = state.series_store();
    if let Some(name) = path
        .strip_prefix("/api/v1/label/")
        .and_then(|r| r.strip_suffix("/values"))
    {
        let name = name.to_string();
        return label_listing(store, move |src| src.label_values(&name)).await;
    }
    match path {
        "/api/v1/labels" => label_listing(store, |src| src.label_names()).await,
        "/api/v1/query" => instant_query(store, &params).await,
        "/api/v1/query_range" => range_query(store, &params).await,
        _ => error_json("404 Not Found", "not_found", "unknown PromQL endpoint"),
    }
}

/// The first value of form parameter `key`, if present.
fn param(params: &[(String, String)], key: &str) -> Option<String> {
    params
        .iter()
        .find(|(pk, _)| pk == key)
        .map(|(_, v)| v.clone())
}

/// A label listing (`/labels` or `/label/<n>/values`), computed off the async runtime.
async fn label_listing<F>(store: Arc<SeriesStore>, list: F) -> Reply
where
    F: FnOnce(&StoreSource) -> Vec<String> + Send + 'static,
{
    let src = StoreSource { store };
    let names = tokio::task::spawn_blocking(move || list(&src))
        .await
        .unwrap_or_default();
    ok_json(&json_string_array("data", &names))
}

/// `/api/v1/query`: evaluate `query` at `time` (default: now).
async fn instant_query(store: Arc<SeriesStore>, params: &[(String, String)]) -> Reply {
    let Some(q) = param(params, "query") else {
        return missing_query();
    };
    let t = match param(params, "time")
        .map(|ts| parse_time_ns(&ts))
        .transpose()
    {
        Ok(t) => t.unwrap_or_else(now_ns),
        Err(e) => return error_json("400 Bad Request", "bad_data", &e),
    };
    evaluate(
        store,
        q,
        move |evaluator, ast| evaluator.eval_instant(ast, t),
        move |value| instant_data(value, t),
    )
    .await
}

/// `/api/v1/query_range`: evaluate `query` over `[start, end]` every `step`.
async fn range_query(store: Arc<SeriesStore>, params: &[(String, String)]) -> Reply {
    let Some(q) = param(params, "query") else {
        return missing_query();
    };
    let (Some(s), Some(e), Some(st)) = (
        param(params, "start"),
        param(params, "end"),
        param(params, "step"),
    ) else {
        return error_json(
            "400 Bad Request",
            "bad_data",
            "query_range requires start, end, step",
        );
    };
    let (Ok(start), Ok(end), Ok(step)) = (parse_time_ns(&s), parse_time_ns(&e), parse_step_ns(&st))
    else {
        return error_json("400 Bad Request", "bad_data", "invalid start/end/step");
    };
    evaluate(
        store,
        q,
        move |evaluator, ast| evaluator.eval_range(ast, start, end, step),
        |series| matrix_data(series),
    )
    .await
}

fn missing_query() -> Reply {
    error_json("400 Bad Request", "bad_data", "missing 'query' parameter")
}

/// Parse and evaluate `q` on the blocking pool, rendering a success with `render`;
/// a parse/evaluation error is `400 bad_data`, a failed task `500 internal`.
async fn evaluate<T, Eval, Render>(
    store: Arc<SeriesStore>,
    q: String,
    eval: Eval,
    render: Render,
) -> Reply
where
    T: Send + 'static,
    Eval: FnOnce(&Evaluator<'_>, &Expr) -> Result<T, PromqlError> + Send + 'static,
    Render: FnOnce(&T) -> serde_json::Value,
{
    let res = tokio::task::spawn_blocking(move || {
        let ast = parse(&q)?;
        let src = StoreSource { store };
        eval(&Evaluator::new(&src), &ast)
    })
    .await;
    match res {
        Ok(Ok(value)) => ok_json(&render(&value)),
        Ok(Err(e)) => error_json("400 Bad Request", "bad_data", &e.to_string()),
        Err(e) => error_json(
            "500 Internal Server Error",
            "internal",
            &format!("eval task failed: {e}"),
        ),
    }
}

// ───────────────────────────── JSON envelopes ─────────────────────────────

fn ok_json(data: &serde_json::Value) -> (&'static str, &'static str, String) {
    let env = serde_json::json!({ "status": "success", "data": data });
    ("200 OK", "application/json", env.to_string())
}

fn error_json(
    status: &'static str,
    error_type: &str,
    msg: &str,
) -> (&'static str, &'static str, String) {
    let env = serde_json::json!({ "status": "error", "errorType": error_type, "error": msg });
    (status, "application/json", env.to_string())
}

fn json_string_array(_key: &str, items: &[String]) -> serde_json::Value {
    serde_json::Value::Array(
        items
            .iter()
            .map(|s| serde_json::Value::String(s.clone()))
            .collect(),
    )
}

/// Prometheus formats a sample value as a decimal string, with NaN/Inf spelled out.
fn fmt_value(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v.is_infinite() {
        if v > 0.0 {
            "+Inf".to_string()
        } else {
            "-Inf".to_string()
        }
    } else {
        format!("{v}")
    }
}

/// Seconds (as a JSON number) for a ns timestamp.
fn ts_secs(ns: i64) -> serde_json::Value {
    serde_json::json!(ns as f64 / NS_PER_SEC)
}

fn labels_to_json(labels: &Labels) -> serde_json::Value {
    let mut m = serde_json::Map::new();
    for (k, v) in labels {
        m.insert(k.clone(), serde_json::Value::String(v.clone()));
    }
    serde_json::Value::Object(m)
}

/// The `data` object for an instant query result.
fn instant_data(v: &Value, t: i64) -> serde_json::Value {
    match v {
        Value::Scalar(s) => serde_json::json!({
            "resultType": "scalar",
            "result": [ ts_secs(t), fmt_value(*s) ],
        }),
        Value::Instant(samples) => {
            let result: Vec<serde_json::Value> = samples
                .iter()
                .map(|s| {
                    serde_json::json!({
                        "metric": labels_to_json(&s.labels),
                        "value": [ ts_secs(t), fmt_value(s.value) ],
                    })
                })
                .collect();
            serde_json::json!({ "resultType": "vector", "result": result })
        }
        Value::Range(series) => matrix_data(series),
    }
}

/// The `data` object for a matrix (range vector / range query) result.
fn matrix_data(series: &[RangeSeries]) -> serde_json::Value {
    let result: Vec<serde_json::Value> = series
        .iter()
        .map(|s| {
            let values: Vec<serde_json::Value> = s
                .points
                .iter()
                .map(|(ts, v)| serde_json::json!([ts_secs(*ts), fmt_value(*v)]))
                .collect();
            serde_json::json!({ "metric": labels_to_json(&s.labels), "values": values })
        })
        .collect();
    serde_json::json!({ "resultType": "matrix", "result": result })
}

// ───────────────────────────── param parsing ─────────────────────────────

/// Parse a `key=value&…` form (query string or form body), percent-decoding both
/// sides and treating `+` as a space.
fn parse_form(s: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for pair in s.split('&') {
        if pair.is_empty() {
            continue;
        }
        match pair.split_once('=') {
            Some((k, v)) => out.push((percent_decode(k), percent_decode(v))),
            None => out.push((percent_decode(pair), String::new())),
        }
    }
    out
}

/// Percent-decode a form component (`%XX` → byte, `+` → space). Invalid escapes are
/// passed through literally.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hi = hex_val(bytes[i + 1]);
                let lo = hex_val(bytes[i + 2]);
                match (hi, lo) {
                    (Some(h), Some(l)) => {
                        out.push((h << 4) | l);
                        i += 3;
                    }
                    _ => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).to_string()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Parse a Prometheus time (unix seconds, possibly fractional) into epoch-ns.
fn parse_time_ns(s: &str) -> Result<i64, String> {
    s.trim()
        .parse::<f64>()
        .map(|secs| (secs * NS_PER_SEC) as i64)
        .map_err(|_| format!("cannot parse time '{s}' (expected unix seconds)"))
}

/// Parse a step: either a float number of seconds, or a PromQL duration (`15s`, `1m`).
fn parse_step_ns(s: &str) -> Result<i64, String> {
    let s = s.trim();
    if let Ok(secs) = s.parse::<f64>() {
        let ns = (secs * NS_PER_SEC) as i64;
        return if ns > 0 {
            Ok(ns)
        } else {
            Err("step must be positive".into())
        };
    }
    // Duration form: reuse the engine parser via a synthetic range selector.
    match parse(&format!("x[{s}]")) {
        Ok(eg_tsdb::promql::Expr::Matrix { range_ns, .. }) if range_ns > 0 => Ok(range_ns),
        _ => Err(format!("cannot parse step '{s}'")),
    }
}

fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

// ───────────────────────────── tests (CONCEPT:EG-KG.query.prometheus-http-query-api) ─────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eg172_series_id_roundtrips_labels() {
        let mut l = Labels::new();
        l.insert(METRIC_NAME.to_string(), "http_requests_total".to_string());
        l.insert("job".to_string(), "api".to_string());
        l.insert("method".to_string(), "get".to_string());
        let id = format_series_id(&l);
        assert_eq!(id, r#"http_requests_total{job="api",method="get"}"#);
        assert_eq!(parse_series_id(&id), l);
    }

    #[test]
    fn eg172_bare_metric_id_parses_to_name_only() {
        let l = parse_series_id("up");
        assert_eq!(l.get(METRIC_NAME).unwrap(), "up");
        assert_eq!(l.len(), 1);
    }

    #[test]
    fn eg172_percent_decode_query_expression() {
        // "sum(rate(m[5m]))" URL-encoded.
        let enc = "query=sum%28rate%28m%5B5m%5D%29%29&time=100";
        let params = parse_form(enc);
        assert_eq!(
            params.iter().find(|(k, _)| k == "query").unwrap().1,
            "sum(rate(m[5m]))"
        );
        assert_eq!(
            parse_time_ns(&params.iter().find(|(k, _)| k == "time").unwrap().1).unwrap(),
            100 * 1_000_000_000
        );
    }

    #[test]
    fn eg172_step_parses_seconds_and_duration() {
        assert_eq!(parse_step_ns("15").unwrap(), 15 * 1_000_000_000);
        assert_eq!(parse_step_ns("1m").unwrap(), 60 * 1_000_000_000);
        assert!(parse_step_ns("0").is_err());
    }

    /// Pins every route and refusal of the Prometheus HTTP entry point over an empty
    /// store: label listings, parameter merging, time/step validation, parse errors,
    /// instant and range evaluation, and the unknown-endpoint 404.
    #[tokio::test]
    async fn eg172_handle_pins_routes_and_refusals() {
        let state = Arc::new(ObsState::in_memory(1024).unwrap());
        let bad = |error: &str| serde_json::json!({"status": "error", "errorType": "bad_data", "error": error});
        let cases: [(&str, &str, &str, &str, &str, serde_json::Value); 12] = [
            (
                "GET",
                "/api/v1/labels",
                "",
                "",
                "200 OK",
                serde_json::json!({"status": "success", "data": []}),
            ),
            (
                "GET",
                "/api/v1/label/job/values",
                "",
                "",
                "200 OK",
                serde_json::json!({"status": "success", "data": []}),
            ),
            (
                "GET",
                "/api/v1/query",
                "time=1",
                "",
                "400 Bad Request",
                bad("missing 'query' parameter"),
            ),
            (
                "POST",
                "/api/v1/query",
                "query=2",
                "time=soon",
                "400 Bad Request",
                bad("cannot parse time 'soon' (expected unix seconds)"),
            ),
            (
                "GET",
                "/api/v1/query",
                "query=1%2B1&time=100",
                "query=9",
                "200 OK",
                serde_json::json!({"status": "success", "data": {"resultType": "scalar", "result": [100.0, "2"]}}),
            ),
            (
                "POST",
                "/api/v1/query",
                "",
                "query=3&time=5",
                "200 OK",
                serde_json::json!({"status": "success", "data": {"resultType": "scalar", "result": [5.0, "3"]}}),
            ),
            (
                "GET",
                "/api/v1/query",
                "query=sum(&time=1",
                "",
                "400 Bad Request",
                serde_json::Value::Null,
            ),
            (
                "GET",
                "/api/v1/query_range",
                "query=1&start=0&end=10",
                "",
                "400 Bad Request",
                bad("query_range requires start, end, step"),
            ),
            (
                "GET",
                "/api/v1/query_range",
                "query=1&start=0&end=10&step=0",
                "",
                "400 Bad Request",
                bad("invalid start/end/step"),
            ),
            (
                "GET",
                "/api/v1/query_range",
                "query=1&start=10&end=0&step=5",
                "",
                "400 Bad Request",
                bad("end must be >= start"),
            ),
            (
                "GET",
                "/api/v1/query_range",
                "query=1%2B1&start=0&end=10&step=5s",
                "",
                "200 OK",
                serde_json::json!({"status": "success", "data": {"resultType": "matrix", "result": [{"metric": {}, "values": [[0.0, "2"], [5.0, "2"], [10.0, "2"]]}]}}),
            ),
            (
                "GET",
                "/api/v1/series",
                "",
                "",
                "404 Not Found",
                serde_json::json!({"status": "error", "errorType": "not_found", "error": "unknown PromQL endpoint"}),
            ),
        ];
        for (method, path, query, body, status, expected) in cases {
            let (got_status, content_type, got_body) =
                handle(&state, method, path, query, body).await;
            assert_eq!(
                (got_status, content_type),
                (status, "application/json"),
                "{method} {path}?{query}"
            );
            let got: serde_json::Value = serde_json::from_str(&got_body).unwrap();
            if expected.is_null() {
                assert_eq!(got["errorType"], "bad_data", "{got_body}");
            } else {
                assert_eq!(got, expected, "{method} {path}?{query}");
            }
        }
    }

    #[test]
    fn eg172_error_envelope_shape() {
        let (status, ctype, body) = error_json("400 Bad Request", "bad_data", "boom");
        assert_eq!(status, "400 Bad Request");
        assert_eq!(ctype, "application/json");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["status"], "error");
        assert_eq!(v["errorType"], "bad_data");
        assert_eq!(v["error"], "boom");
    }

    #[test]
    fn eg172_instant_vector_envelope_shape() {
        let samples = vec![eg_tsdb::promql::InstantSample {
            labels: {
                let mut l = Labels::new();
                l.insert("job".into(), "api".into());
                l
            },
            value: 42.0,
        }];
        let data = instant_data(&Value::Instant(samples), 100 * 1_000_000_000);
        assert_eq!(data["resultType"], "vector");
        assert_eq!(data["result"][0]["metric"]["job"], "api");
        assert_eq!(data["result"][0]["value"][0], 100.0);
        assert_eq!(data["result"][0]["value"][1], "42");
    }
}
