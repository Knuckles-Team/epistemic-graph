//! EG-162 search surface over HTTP (O2 / Elasticsearch `_search`).

use std::sync::Arc;

use super::super::parse::scalar_to_string;
use super::super::search::{LogQuery, DEFAULT_SEARCH_SIZE};
use super::super::{LogRecord, ObsState};
use super::query_param;

/// Route + execute a `_search` request → `(status, content_type, body)`.
///
/// Two modes on the SAME endpoint, discriminated by the JSON body:
///  * a raw SQL query — `{"sql": "SELECT …"}` (or `{"query":{"sql":"…"}}`, the O2
///    shape) — runs DataFusion over the `logs` table (segments + hot buffers);
///  * a structured log search — `{stream, start_time, end_time, query, size, …}` —
///    returns O2/ES-shaped hits UNIONed across the hot + cold tiers.
///
/// The stream may come from the path (`/api/<org>/<stream>/_search`) or the body.
pub(super) async fn handle_search(
    state: &Arc<ObsState>,
    path: &str,
    query: &str,
    body: &str,
) -> (&'static str, &'static str, String) {
    let val: serde_json::Value = if body.trim().is_empty() {
        serde_json::json!({})
    } else {
        match serde_json::from_str(body) {
            Ok(v) => v,
            Err(e) => {
                return (
                    "400 Bad Request",
                    "text/plain",
                    format!("parse _search JSON: {e}"),
                );
            }
        }
    };

    // SQL mode: top-level `sql`, or the O2 `{"query":{"sql":…}}` nesting.
    let sql = val.get("sql").and_then(|v| v.as_str()).or_else(|| {
        val.get("query")
            .and_then(|q| q.get("sql"))
            .and_then(|v| v.as_str())
    });
    if let Some(sql) = sql {
        return run_sql_search(state, sql).await;
    }

    // Structured search mode: resolve the stream (path wins, then body, then `?stream`).
    let Some(stream) = resolve_search_stream(path, query, &val) else {
        return (
            "400 Bad Request",
            "text/plain",
            "search requires a stream (path /api/<org>/<stream>/_search or body `stream`)"
                .to_string(),
        );
    };

    let q = parse_log_query(&val, stream);
    let st = state.clone();
    match ::tokio::task::spawn_blocking(move || st.search_logs(&q)).await {
        Ok(Ok(hits)) => ("200 OK", "application/json", es_search_response(&hits)),
        Ok(Err(error)) => {
            tracing::warn!(%error, "observability search failed");
            (
                "400 Bad Request",
                "text/plain",
                "observability search failed".to_string(),
            )
        }
        Err(_) => (
            "500 Internal Server Error",
            "text/plain",
            "observability search worker failed".to_string(),
        ),
    }
}

/// The SQL-mode branch of [`handle_search`]: run `sql` off the reactor via
/// `search_sql`. Extracted from [`handle_search`].
async fn run_sql_search(state: &Arc<ObsState>, sql: &str) -> (&'static str, &'static str, String) {
    let sql = sql.to_string();
    let st = state.clone();
    match ::tokio::task::spawn_blocking(move || st.search_sql(&sql)).await {
        Ok(Ok(res)) => ("200 OK", "application/json", sql_search_response(&res)),
        Ok(Err(error)) => {
            tracing::warn!(%error, "observability SQL search failed");
            (
                "400 Bad Request",
                "text/plain",
                "observability SQL search failed".to_string(),
            )
        }
        Err(_) => (
            "500 Internal Server Error",
            "text/plain",
            "observability SQL search worker failed".to_string(),
        ),
    }
}

/// Resolve the structured-search-mode stream: the path (`/api/<org>/<stream>/_search`)
/// wins, then a `stream`/`_stream`/`index`/`_index` body key, then the `?stream` query
/// param. Extracted from [`handle_search`].
fn resolve_search_stream(path: &str, query: &str, val: &serde_json::Value) -> Option<String> {
    search_stream_from_path(path)
        .or_else(|| search_stream_from_body(val))
        .or_else(|| query_param(query, "stream"))
}

/// The body-key fallback of [`resolve_search_stream`]: the first non-empty
/// `stream`/`_stream`/`index`/`_index` key.
fn search_stream_from_body(val: &serde_json::Value) -> Option<String> {
    for key in ["stream", "_stream", "index", "_index"] {
        if let Some(s) = val
            .get(key)
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
        {
            return Some(s.to_string());
        }
    }
    None
}

/// If `path` is `/api/<org>/<stream>/_search`, return `<stream>`; else `None`
/// (`/api/_search` and `/_search` carry the stream in the body).
fn search_stream_from_path(path: &str) -> Option<String> {
    let trimmed = path.trim_start_matches('/');
    let parts: Vec<&str> = trimmed.split('/').collect();
    // /api/<org>/<stream>/_search  → ["api", org, stream, "_search"]
    if parts.len() == 4 && parts[0] == "api" && parts[3] == "_search" && !parts[2].is_empty() {
        return Some(parts[2].to_string());
    }
    // /<stream>/_search → ["stream", "_search"] (but NOT the org-less "/api/_search").
    if parts.len() == 2 && parts[1] == "_search" && !parts[0].is_empty() && parts[0] != "api" {
        return Some(parts[0].to_string());
    }
    None
}

/// Build a [`LogQuery`] from the parsed `_search` JSON body. Tolerates the common
/// shapes: `start_time`/`end_time` (O2) or `from`/`to` for the window; a full-text
/// `query` string (bare, or the ES `{"query_string":{"query":…}}` nesting); a
/// `filters` object of attribute equalities + a `severity`; and `size`.
fn parse_log_query(val: &serde_json::Value, stream: String) -> LogQuery {
    let ts = |keys: &[&str]| -> Option<i64> {
        for k in keys {
            if let Some(n) = val.get(*k).and_then(|v| v.as_i64()) {
                return Some(n);
            }
        }
        None
    };
    let from = ts(&["start_time", "from_ts", "from"]).unwrap_or(i64::MIN);
    let to = ts(&["end_time", "to_ts", "to"]).unwrap_or(i64::MAX);

    let terms = search_terms(val);

    let severity = val
        .get("severity")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    let filters = attribute_filters(val);

    let size = val
        .get("size")
        .and_then(|v| v.as_u64())
        .map(|n| n as usize)
        .unwrap_or(DEFAULT_SEARCH_SIZE);

    LogQuery {
        stream,
        from,
        to,
        terms,
        filters,
        severity,
        size,
    }
}

/// Full-text terms: a bare `query`/`q` string, or ES `query.query_string.query`.
/// Extracted from [`parse_log_query`].
fn search_terms(val: &serde_json::Value) -> Option<String> {
    val.get("query")
        .and_then(|q| q.as_str())
        .map(str::to_string)
        .or_else(|| val.get("q").and_then(|v| v.as_str()).map(str::to_string))
        .or_else(|| {
            val.get("query")
                .and_then(|q| q.get("query_string"))
                .and_then(|qs| qs.get("query"))
                .and_then(|v| v.as_str())
                .map(str::to_string)
        })
        .filter(|s| !s.trim().is_empty())
}

/// The `filters` object's attribute equalities. Extracted from [`parse_log_query`].
fn attribute_filters(val: &serde_json::Value) -> Vec<(String, String)> {
    val.get("filters")
        .and_then(|v| v.as_object())
        .into_iter()
        .flatten()
        .map(|(k, v)| (k.clone(), scalar_to_string(v)))
        .collect()
}

/// Render one log record as an Elasticsearch/O2 `_source` object.
fn record_source(r: &LogRecord) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    obj.insert("_timestamp".into(), serde_json::json!(r.ts));
    obj.insert("stream".into(), serde_json::json!(r.stream));
    obj.insert("severity".into(), serde_json::json!(r.severity));
    obj.insert("message".into(), serde_json::json!(r.body));
    for (k, v) in &r.attrs {
        // Never let an attribute clobber a reserved field.
        if !obj.contains_key(k) {
            obj.insert(k.clone(), serde_json::json!(v));
        }
    }
    serde_json::Value::Object(obj)
}

/// The Elasticsearch/O2 `_search` response envelope over the matched records.
fn es_search_response(hits: &[LogRecord]) -> String {
    let items: Vec<serde_json::Value> = hits
        .iter()
        .map(|r| {
            serde_json::json!({
                "_index": r.stream,
                "_score": 1.0,
                "_source": record_source(r),
            })
        })
        .collect();
    serde_json::json!({
        "took": 0,
        "timed_out": false,
        "hits": {
            "total": { "value": hits.len(), "relation": "eq" },
            "hits": items,
        }
    })
    .to_string()
}

/// The SQL `_search` response: columns + rows, plus row objects (`hits`) keyed by
/// column name (the O2 SQL result shape).
fn sql_search_response(res: &eg_query::TypedQueryResult) -> String {
    let cols: Vec<&str> = res.columns.iter().map(|c| c.name.as_str()).collect();
    let hits: Vec<serde_json::Value> = res
        .rows
        .iter()
        .map(|row| {
            let mut obj = serde_json::Map::new();
            for (i, cell) in row.iter().enumerate() {
                if let Some(name) = cols.get(i) {
                    obj.insert((*name).to_string(), cell.clone());
                }
            }
            serde_json::Value::Object(obj)
        })
        .collect();
    serde_json::json!({
        "took": 0,
        "total": res.rows.len(),
        "columns": cols,
        "hits": hits,
    })
    .to_string()
}
