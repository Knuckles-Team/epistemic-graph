//! CONCEPT:EG-163 — the distributed-trace HTTP facade: OTLP-JSON span ingest plus a
//! Jaeger/OpenObserve-compatible trace-search / assembly / service-dependency API,
//! served on the SAME hand-rolled obs listener that ingests logs and answers PromQL.
//!
//! This is the FACADE half of EG-163; the span model + indexed store + trace
//! assembly + dependency-graph derivation live in the dependency-free
//! [`eg_tsdb::traces`] crate module. Together with EG-160/161 (logs) and EG-172
//! (metrics/PromQL) this completes the observability trilogy.
//!
//! ## Routes (all on the obs listener)
//!
//!  * `POST /v1/traces`                    — OTLP/HTTP JSON span ingest
//!    (`resourceSpans[].scopeSpans[].spans[]`; resource `service.name` → the span's
//!    service). Full protobuf OTLP is a documented follow-up.
//!  * `GET`/`POST /api/traces`             — search traces by `service`, `operation`,
//!    `minDuration`/`maxDuration` (Go-duration or ns), `start`/`end` (epoch-ns),
//!    `tags` (attribute equalities) and `limit`; returns matching traces assembled
//!    as `parent → children` span trees.
//!  * `GET /api/traces/<trace_id>`         — the full assembled trace.
//!  * `GET /api/dependencies`              — the derived service→service call graph
//!    (edges with call counts), the piece that leans on our graph engine.

use std::collections::BTreeMap;
use std::sync::Arc;

use eg_tsdb::traces::{AssembledTrace, ServiceEdge, Span, SpanEvent, SpanNode, TraceQuery};

use crate::server::obs::ObsState;

/// Route + execute a distributed-trace request → `(status, content_type, body)`.
/// `method` is the request method, `path`/`query` the split target, `body` the request
/// body (OTLP-JSON for ingest; optional JSON search predicate for `POST /api/traces`).
pub async fn handle(
    state: &Arc<ObsState>,
    method: &str,
    path: &str,
    query: &str,
    body: &str,
) -> (&'static str, &'static str, String) {
    // ── OTLP-JSON ingest ─────────────────────────────────────────────────────
    if path == "/v1/traces" {
        if method != "POST" {
            return ("405 Method Not Allowed", "text/plain", "POST only".to_string());
        }
        let spans = match parse_otlp_traces(body) {
            Ok(s) => s,
            Err(e) => return ("400 Bad Request", "text/plain", e),
        };
        let store = state.trace_store();
        let accepted = tokio::task::spawn_blocking(move || store.add_spans(spans))
            .await
            .unwrap_or(0);
        return (
            "200 OK",
            "application/json",
            format!("{{\"partialSuccess\":{{}},\"accepted\":{accepted}}}"),
        );
    }

    // ── service-dependency graph ─────────────────────────────────────────────
    if path == "/api/dependencies" || path == "/api/services/dependencies" {
        let store = state.trace_store();
        let edges = tokio::task::spawn_blocking(move || store.service_dependencies())
            .await
            .unwrap_or_default();
        return ("200 OK", "application/json", dependencies_response(&edges));
    }

    // ── single-trace assembly: /api/traces/<trace_id> ────────────────────────
    if let Some(trace_id) = path.strip_prefix("/api/traces/") {
        if trace_id.is_empty() {
            return ("404 Not Found", "text/plain", "missing trace id".to_string());
        }
        let trace_id = trace_id.to_string();
        let store = state.trace_store();
        let assembled = tokio::task::spawn_blocking(move || store.assemble(&trace_id))
            .await
            .ok()
            .flatten();
        return match assembled {
            Some(t) => ("200 OK", "application/json", single_trace_response(&t)),
            None => ("404 Not Found", "text/plain", "unknown trace".to_string()),
        };
    }

    // ── trace search: /api/traces ────────────────────────────────────────────
    if path == "/api/traces" {
        // Body (JSON) wins for POST; otherwise the query string carries the predicate.
        let q = if method == "POST" && !body.trim().is_empty() {
            match serde_json::from_str::<serde_json::Value>(body) {
                Ok(v) => trace_query_from_json(&v),
                Err(e) => {
                    return (
                        "400 Bad Request",
                        "text/plain",
                        format!("parse trace query JSON: {e}"),
                    )
                }
            }
        } else {
            trace_query_from_query_string(query)
        };
        let store = state.trace_store();
        let hits = tokio::task::spawn_blocking(move || store.search(&q))
            .await
            .unwrap_or_default();
        return ("200 OK", "application/json", search_response(&hits));
    }

    ("404 Not Found", "text/plain", "unknown trace path".to_string())
}

// ── OTLP-JSON trace parsing ─────────────────────────────────────────────────────

/// Parse an OTLP/HTTP JSON `ExportTraceServiceRequest`
/// (`resourceSpans[].scopeSpans[].spans[]`) into [`Span`]s. The service name comes
/// from the resource's `service.name` attribute (else `"unknown"`). Each span's
/// duration is `endTimeUnixNano - startTimeUnixNano`.
pub fn parse_otlp_traces(body: &str) -> Result<Vec<Span>, String> {
    let root: serde_json::Value =
        serde_json::from_str(body).map_err(|e| format!("parse OTLP trace JSON: {e}"))?;
    let mut out = Vec::new();
    let resource_spans = root
        .get("resourceSpans")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    for rs in &resource_spans {
        let service = rs
            .get("resource")
            .and_then(|r| r.get("attributes"))
            .and_then(|v| v.as_array())
            .and_then(|attrs| {
                attrs.iter().find_map(|a| {
                    if a.get("key").and_then(|v| v.as_str()) == Some("service.name") {
                        a.get("value").map(otlp_anyvalue)
                    } else {
                        None
                    }
                })
            })
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "unknown".to_string());

        for ss in rs
            .get("scopeSpans")
            .and_then(|v| v.as_array())
            .into_iter()
            .flatten()
        {
            for sp in ss
                .get("spans")
                .and_then(|v| v.as_array())
                .into_iter()
                .flatten()
            {
                out.push(otlp_span(sp, &service));
            }
        }
    }
    Ok(out)
}

/// Normalize one OTLP-JSON span object into a [`Span`].
fn otlp_span(sp: &serde_json::Value, service: &str) -> Span {
    let start = otlp_nano(sp, "startTimeUnixNano");
    let end = otlp_nano(sp, "endTimeUnixNano");
    Span {
        trace_id: sp
            .get("traceId")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
        span_id: sp
            .get("spanId")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
        parent_span_id: sp
            .get("parentSpanId")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
        service: service.to_string(),
        operation: sp
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
        start_time: start,
        duration: end.saturating_sub(start).max(0),
        status: otlp_status(sp),
        attributes: otlp_attrs(sp.get("attributes")),
        events: otlp_events(sp.get("events")),
    }
}

/// Read an OTLP nanosecond timestamp field (string or number).
fn otlp_nano(v: &serde_json::Value, key: &str) -> i64 {
    match v.get(key) {
        Some(n) => n
            .as_str()
            .and_then(|s| s.parse::<i64>().ok())
            .or_else(|| n.as_i64())
            .unwrap_or(0),
        None => 0,
    }
}

/// Map an OTLP status object (`{code}`) onto a status text. `STATUS_CODE_ERROR`/`2`
/// ⇒ `ERROR`; `STATUS_CODE_OK`/`1` ⇒ `OK`; unset ⇒ empty.
fn otlp_status(sp: &serde_json::Value) -> String {
    let code = sp.get("status").and_then(|s| s.get("code"));
    match code {
        Some(c) => {
            let s = c.as_str().map(|x| x.to_string()).unwrap_or_default();
            let n = c.as_i64().unwrap_or(-1);
            if s.contains("ERROR") || n == 2 {
                "ERROR".to_string()
            } else if s.contains("OK") || n == 1 {
                "OK".to_string()
            } else {
                String::new()
            }
        }
        None => String::new(),
    }
}

/// Collect an OTLP `attributes` array (`[{key,value:{stringValue|…}}]`) into a map.
fn otlp_attrs(v: Option<&serde_json::Value>) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    if let Some(arr) = v.and_then(|v| v.as_array()) {
        for a in arr {
            if let (Some(k), Some(val)) = (a.get("key").and_then(|v| v.as_str()), a.get("value")) {
                out.insert(k.to_string(), otlp_anyvalue(val));
            }
        }
    }
    out
}

/// Collect an OTLP `events` array into [`SpanEvent`]s.
fn otlp_events(v: Option<&serde_json::Value>) -> Vec<SpanEvent> {
    let mut out = Vec::new();
    if let Some(arr) = v.and_then(|v| v.as_array()) {
        for e in arr {
            out.push(SpanEvent {
                ts: otlp_nano(e, "timeUnixNano"),
                name: e
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
                attrs: otlp_attrs(e.get("attributes")),
            });
        }
    }
    out
}

/// Unwrap an OTLP `AnyValue` (`{stringValue|intValue|doubleValue|boolValue}`).
fn otlp_anyvalue(v: &serde_json::Value) -> String {
    for key in ["stringValue", "intValue", "doubleValue", "boolValue"] {
        if let Some(inner) = v.get(key) {
            return match inner {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Null => String::new(),
                other => other.to_string(),
            };
        }
    }
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

// ── trace-query parsing ─────────────────────────────────────────────────────────

/// Build a [`TraceQuery`] from a JSON search body (POST `/api/traces`).
fn trace_query_from_json(v: &serde_json::Value) -> TraceQuery {
    let str_of = |k: &str| v.get(k).and_then(|x| x.as_str()).filter(|s| !s.is_empty()).map(str::to_string);
    let i64_of = |k: &str| v.get(k).and_then(|x| x.as_i64());
    let dur_of = |k: &str| {
        v.get(k).and_then(|x| {
            x.as_i64()
                .or_else(|| x.as_str().and_then(parse_duration_ns))
        })
    };
    let mut tags = Vec::new();
    if let Some(obj) = v.get("tags").and_then(|t| t.as_object()) {
        for (k, val) in obj {
            let s = match val {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            tags.push((k.clone(), s));
        }
    }
    TraceQuery {
        service: str_of("service"),
        operation: str_of("operation"),
        min_duration: dur_of("minDuration").or_else(|| dur_of("min_duration")),
        max_duration: dur_of("maxDuration").or_else(|| dur_of("max_duration")),
        from: i64_of("start").or_else(|| i64_of("from")).unwrap_or(i64::MIN),
        to: i64_of("end").or_else(|| i64_of("to")).unwrap_or(i64::MAX),
        tags,
        limit: v.get("limit").and_then(|x| x.as_u64()).unwrap_or(0) as usize,
    }
}

/// Build a [`TraceQuery`] from a query string (GET `/api/traces?service=…&…`). `tags`
/// accepts repeated `tag=k:v` (or `tag=k=v`) pairs.
fn trace_query_from_query_string(query: &str) -> TraceQuery {
    let mut q = TraceQuery {
        from: i64::MIN,
        to: i64::MAX,
        ..TraceQuery::default()
    };
    for (k, v) in parse_query_pairs(query) {
        match k.as_str() {
            "service" if !v.is_empty() => q.service = Some(v),
            "operation" if !v.is_empty() => q.operation = Some(v),
            "minDuration" | "min_duration" => {
                q.min_duration = v.parse::<i64>().ok().or_else(|| parse_duration_ns(&v))
            }
            "maxDuration" | "max_duration" => {
                q.max_duration = v.parse::<i64>().ok().or_else(|| parse_duration_ns(&v))
            }
            "start" | "from" => {
                if let Ok(n) = v.parse::<i64>() {
                    q.from = n;
                }
            }
            "end" | "to" => {
                if let Ok(n) = v.parse::<i64>() {
                    q.to = n;
                }
            }
            "limit" => {
                if let Ok(n) = v.parse::<usize>() {
                    q.limit = n;
                }
            }
            "tag" | "tags" => {
                if let Some((tk, tv)) = v.split_once(':').or_else(|| v.split_once('=')) {
                    q.tags.push((tk.to_string(), tv.to_string()));
                }
            }
            _ => {}
        }
    }
    q
}

/// Parse a Go-style duration (`1s`, `500ms`, `2m`, `100us`, `50ns`) into nanoseconds.
/// A bare integer is treated as nanoseconds by the caller before this is reached.
fn parse_duration_ns(s: &str) -> Option<i64> {
    let s = s.trim();
    let (num, mult): (&str, i64) = if let Some(n) = s.strip_suffix("ns") {
        (n, 1)
    } else if let Some(n) = s.strip_suffix("us").or_else(|| s.strip_suffix("µs")) {
        (n, 1_000)
    } else if let Some(n) = s.strip_suffix("ms") {
        (n, 1_000_000)
    } else if let Some(n) = s.strip_suffix('s') {
        (n, 1_000_000_000)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 60_000_000_000)
    } else if let Some(n) = s.strip_suffix('h') {
        (n, 3_600_000_000_000)
    } else {
        (s, 1)
    };
    num.trim().parse::<f64>().ok().map(|f| (f * mult as f64) as i64)
}

/// Split a query string into decoded `(key,value)` pairs (`+` → space, `%XX` bytes).
fn parse_query_pairs(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| match pair.split_once('=') {
            Some((k, v)) => (pct_decode(k), pct_decode(v)),
            None => (pct_decode(pair), String::new()),
        })
        .collect()
}

/// Minimal percent-decoding (`+` → space, `%XX` → byte), for query params.
fn pct_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                let hi = (bytes[i + 1] as char).to_digit(16);
                let lo = (bytes[i + 2] as char).to_digit(16);
                if let (Some(h), Some(l)) = (hi, lo) {
                    out.push((h * 16 + l) as u8);
                    i += 3;
                    continue;
                }
                out.push(b'%');
            }
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).to_string()
}

// ── response rendering (Jaeger/OpenObserve-style JSON) ──────────────────────────

/// Render a span node (recursively) as a JSON object with nested `children`.
fn span_node_json(n: &SpanNode) -> serde_json::Value {
    let s = &n.span;
    serde_json::json!({
        "traceID": s.trace_id,
        "spanID": s.span_id,
        "parentSpanID": s.parent_span_id,
        "service": s.service,
        "operationName": s.operation,
        "startTime": s.start_time,
        "duration": s.duration,
        "status": s.status,
        "tags": s.attributes,
        "events": s.events.iter().map(|e| serde_json::json!({
            "timestamp": e.ts, "name": e.name, "attributes": e.attrs,
        })).collect::<Vec<_>>(),
        "children": n.children.iter().map(span_node_json).collect::<Vec<_>>(),
    })
}

/// Render an assembled trace as a JSON object.
fn trace_json(t: &AssembledTrace) -> serde_json::Value {
    serde_json::json!({
        "traceID": t.trace_id,
        "spanCount": t.span_count,
        "startTime": t.start_time,
        "duration": t.duration,
        "services": t.services,
        "spans": t.roots.iter().map(span_node_json).collect::<Vec<_>>(),
    })
}

/// The trace-search response envelope (`{data:[…], total}`).
fn search_response(hits: &[AssembledTrace]) -> String {
    serde_json::json!({
        "data": hits.iter().map(trace_json).collect::<Vec<_>>(),
        "total": hits.len(),
    })
    .to_string()
}

/// The single-trace response envelope (`{data:{…}}`).
fn single_trace_response(t: &AssembledTrace) -> String {
    serde_json::json!({ "data": trace_json(t) }).to_string()
}

/// The service-dependency graph response (`{data:[{parent,child,callCount}]}`).
fn dependencies_response(edges: &[ServiceEdge]) -> String {
    let items: Vec<serde_json::Value> = edges
        .iter()
        .map(|e| serde_json::json!({
            "parent": e.parent, "child": e.child, "callCount": e.call_count,
        }))
        .collect();
    serde_json::json!({ "data": items, "total": items.len() }).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CONCEPT:EG-163 — OTLP-JSON parses resourceSpans→scopeSpans→spans, derives the
    /// service from the resource `service.name`, and computes duration = end - start.
    #[test]
    fn eg163_otlp_json_parse_derives_service_and_duration() {
        let body = r#"{
          "resourceSpans":[{
            "resource":{"attributes":[{"key":"service.name","value":{"stringValue":"checkout"}}]},
            "scopeSpans":[{"spans":[
              {"traceId":"abc","spanId":"s1","name":"POST /charge",
               "startTimeUnixNano":"1000","endTimeUnixNano":"1600",
               "status":{"code":"STATUS_CODE_ERROR"},
               "attributes":[{"key":"http.status_code","value":{"intValue":"500"}}],
               "events":[{"timeUnixNano":"1500","name":"exception"}]},
              {"traceId":"abc","spanId":"s2","parentSpanId":"s1","name":"db.query",
               "startTimeUnixNano":"1100","endTimeUnixNano":"1300"}
            ]}]
          }]
        }"#;
        let spans = parse_otlp_traces(body).unwrap();
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0].service, "checkout");
        assert_eq!(spans[0].trace_id, "abc");
        assert_eq!(spans[0].span_id, "s1");
        assert_eq!(spans[0].operation, "POST /charge");
        assert_eq!(spans[0].duration, 600, "1600 - 1000");
        assert_eq!(spans[0].status, "ERROR");
        assert_eq!(spans[0].attributes.get("http.status_code").unwrap(), "500");
        assert_eq!(spans[0].events.len(), 1);
        assert_eq!(spans[0].events[0].name, "exception");
        assert_eq!(spans[1].parent_span_id, "s1");
    }

    /// CONCEPT:EG-163 — Go-style durations parse into nanoseconds.
    #[test]
    fn eg163_parse_duration_ns() {
        assert_eq!(parse_duration_ns("500ms"), Some(500_000_000));
        assert_eq!(parse_duration_ns("2s"), Some(2_000_000_000));
        assert_eq!(parse_duration_ns("100us"), Some(100_000));
        assert_eq!(parse_duration_ns("50ns"), Some(50));
        assert_eq!(parse_duration_ns("1m"), Some(60_000_000_000));
    }

    /// CONCEPT:EG-163 — a query string parses into a [`TraceQuery`] (service, duration,
    /// tag, limit), tolerating percent/`+` encoding.
    #[test]
    fn eg163_trace_query_from_query_string() {
        let q = trace_query_from_query_string("service=checkout&minDuration=500ms&tag=http.status_code:500&limit=5&operation=POST+%2Fcharge");
        assert_eq!(q.service.as_deref(), Some("checkout"));
        assert_eq!(q.min_duration, Some(500_000_000));
        assert_eq!(q.tags, vec![("http.status_code".to_string(), "500".to_string())]);
        assert_eq!(q.limit, 5);
        assert_eq!(q.operation.as_deref(), Some("POST /charge"));
    }

    /// CONCEPT:EG-163 — the full HTTP surface end-to-end over the SAME obs listener:
    /// OTLP-JSON ingest (`POST /v1/traces`), trace search (`GET /api/traces`), single
    /// trace assembly (`GET /api/traces/<id>`) and the service-dependency graph
    /// (`GET /api/dependencies`).
    #[tokio::test]
    async fn eg163_http_trace_ingest_search_assemble_dependencies_end_to_end() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let obs = std::sync::Arc::new(crate::server::obs::ObsState::in_memory(1024).unwrap());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let served = obs.clone();
        tokio::spawn(async move { crate::server::obs::serve(listener, served).await });

        // One trace, two services: gateway (root) → api (child), api errors.
        let otlp = r#"{
          "resourceSpans":[
            {"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"gateway"}}]},
             "scopeSpans":[{"spans":[
               {"traceId":"tr1","spanId":"g1","name":"GET /buy",
                "startTimeUnixNano":"1000","endTimeUnixNano":"3000"}]}]},
            {"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"api"}}]},
             "scopeSpans":[{"spans":[
               {"traceId":"tr1","spanId":"a1","parentSpanId":"g1","name":"charge",
                "startTimeUnixNano":"1500","endTimeUnixNano":"2800",
                "status":{"code":2},
                "attributes":[{"key":"http.status_code","value":{"intValue":"500"}}]}]}]}
          ]
        }"#;

        // Helpers: raw HTTP request/response over the loopback socket.
        async fn send(addr: std::net::SocketAddr, method: &str, path: &str, body: &str) -> String {
            let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
            let req = format!(
                "{method} {path} HTTP/1.1\r\nHost: x\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            sock.write_all(req.as_bytes()).await.unwrap();
            let mut resp = Vec::new();
            sock.read_to_end(&mut resp).await.unwrap();
            String::from_utf8_lossy(&resp).to_string()
        }
        let json_of = |text: &str| -> serde_json::Value {
            serde_json::from_str(text.split("\r\n\r\n").nth(1).unwrap()).unwrap()
        };

        // Ingest.
        let text = send(addr, "POST", "/v1/traces", otlp).await;
        assert!(text.starts_with("HTTP/1.1 200 OK"), "ingest: {text}");
        assert_eq!(json_of(&text)["accepted"], 2);

        // Search by service=api → the one trace, assembled with the gateway root and
        // the api span nested as a child.
        let text = send(addr, "GET", "/api/traces?service=api", "").await;
        assert!(text.starts_with("HTTP/1.1 200 OK"), "search: {text}");
        let v = json_of(&text);
        assert_eq!(v["total"], 1);
        assert_eq!(v["data"][0]["traceID"], "tr1");
        assert_eq!(v["data"][0]["duration"], 2000, "3000 - 1000");
        assert_eq!(v["data"][0]["spans"][0]["service"], "gateway");
        assert_eq!(v["data"][0]["spans"][0]["children"][0]["service"], "api");
        assert_eq!(v["data"][0]["spans"][0]["children"][0]["status"], "ERROR");

        // A duration floor above the trace duration filters it out.
        let text = send(addr, "GET", "/api/traces?minDuration=5s", "").await;
        assert_eq!(json_of(&text)["total"], 0);

        // Single-trace assembly by id.
        let text = send(addr, "GET", "/api/traces/tr1", "").await;
        assert!(text.starts_with("HTTP/1.1 200 OK"), "assemble: {text}");
        assert_eq!(json_of(&text)["data"]["spanCount"], 2);

        // Unknown trace → 404.
        let text = send(addr, "GET", "/api/traces/nope", "").await;
        assert!(text.starts_with("HTTP/1.1 404"), "unknown: {text}");

        // Service-dependency graph: gateway → api, count 1.
        let text = send(addr, "GET", "/api/dependencies", "").await;
        assert!(text.starts_with("HTTP/1.1 200 OK"), "deps: {text}");
        let v = json_of(&text);
        assert_eq!(v["total"], 1);
        assert_eq!(v["data"][0]["parent"], "gateway");
        assert_eq!(v["data"][0]["child"], "api");
        assert_eq!(v["data"][0]["callCount"], 1);
    }
}
