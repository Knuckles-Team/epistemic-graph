//! CONCEPT:EG-316 — the OTLP EXPORT half: push the engine's OWN observability signals
//! (its Prometheus metrics + its stored distributed-trace spans) OUT to an external
//! OpenTelemetry collector over OTLP/HTTP JSON.
//!
//! ## Why this exists
//!
//! The engine already INGESTS the full observability trilogy — logs (EG-160), metrics
//! + PromQL (EG-172) and OTLP-JSON traces (EG-163). This closes the loop on the EGRESS
//! side: the engine now also EMITS. It serializes:
//!
//!  * its own [`crate::metrics`] Prometheus registry (rendered to the text exposition
//!    format, then parsed) into an OTLP `ExportMetricsServiceRequest`
//!    (`resourceMetrics[].scopeMetrics[].metrics[]`), and
//!  * its stored [`eg_tsdb::traces::Span`]s into an OTLP `ExportTraceServiceRequest`
//!    (`resourceSpans[].scopeSpans[].spans[]`) — the EXACT shape [`super::super::traces::parse_otlp_traces`]
//!    reads, so an export round-trips back through our own ingest.
//!
//! The wire is OTLP/HTTP **JSON** (no protobuf/prost on the export path — kept lean);
//! the POST uses the same pure-Rust `ureq` client the federation surfaces link. The
//! export is opt-in: nothing is pushed unless [`export_once`] is called with a
//! collector endpoint (the binary wires it to `EPISTEMIC_GRAPH_OTLP_EXPORT_ENDPOINT`).

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use eg_tsdb::traces::Span;

use super::ObsState;

/// The `service.name` the engine advertises on its exported resource.
pub const EXPORT_SERVICE_NAME: &str = "epistemic-graph";
/// Env var carrying the OTLP/HTTP collector base endpoint (e.g. `http://collector:4318`).
/// Unset ⇒ nothing is exported.
pub const OTLP_EXPORT_ENDPOINT_ENV: &str = "EPISTEMIC_GRAPH_OTLP_EXPORT_ENDPOINT";

/// Wall-clock now in epoch nanoseconds.
fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

// ── OTLP metrics serialization (Prometheus text → ExportMetricsServiceRequest) ──

/// One parsed Prometheus exposition sample: a metric name, its label set, and value.
#[derive(Clone, Debug, PartialEq)]
struct PromSample {
    name: String,
    labels: Vec<(String, String)>,
    value: f64,
}

/// Parse a Prometheus text-exposition body (the output of [`crate::metrics::render`])
/// into flat samples, skipping `# HELP` / `# TYPE` comment lines and blank lines. Each
/// data line is `name{k="v",…} value [timestamp]`; the optional trailing timestamp is
/// ignored (the exporter stamps `now`). Malformed lines are skipped, never panicked on.
fn parse_prom_exposition(text: &str) -> Vec<PromSample> {
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // Split the metric identifier (name + optional `{labels}`) from `value [ts]`.
        let (ident, rest) = match line.find('{') {
            Some(brace) => {
                // name{...} rest → find the matching close brace.
                let Some(close) = line[brace..].find('}') else {
                    continue;
                };
                let close = brace + close;
                (&line[..=close], line[close + 1..].trim())
            }
            None => match line.find(char::is_whitespace) {
                Some(sp) => (&line[..sp], line[sp..].trim()),
                None => continue,
            },
        };
        let Some(value_tok) = rest.split_whitespace().next() else {
            continue;
        };
        let value = match value_tok {
            "NaN" => f64::NAN,
            "+Inf" => f64::INFINITY,
            "-Inf" => f64::NEG_INFINITY,
            v => match v.parse::<f64>() {
                Ok(n) => n,
                Err(_) => continue,
            },
        };
        let (name, labels) = split_ident(ident);
        if name.is_empty() {
            continue;
        }
        out.push(PromSample {
            name,
            labels,
            value,
        });
    }
    out
}

/// Split a `name{k="v",k2="v2"}` (or bare `name`) identifier into its name + labels.
fn split_ident(ident: &str) -> (String, Vec<(String, String)>) {
    match ident.split_once('{') {
        None => (ident.trim().to_string(), Vec::new()),
        Some((name, rest)) => {
            let inner = rest.strip_suffix('}').unwrap_or(rest);
            let mut labels = Vec::new();
            for pair in split_top_commas(inner) {
                if let Some((k, v)) = pair.split_once('=') {
                    let k = k.trim();
                    let v = v.trim().trim_matches('"');
                    if !k.is_empty() {
                        labels.push((k.to_string(), v.to_string()));
                    }
                }
            }
            (name.trim().to_string(), labels)
        }
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
            ',' if !in_q => out.push(std::mem::take(&mut cur)),
            _ => cur.push(c),
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur);
    }
    out
}

/// An OTLP `AnyValue` string attribute (`{"key":k,"value":{"stringValue":v}}`).
fn otlp_str_attr(key: &str, value: &str) -> serde_json::Value {
    serde_json::json!({ "key": key, "value": { "stringValue": value } })
}

/// Serialize the engine's Prometheus metrics text into an OTLP/HTTP JSON
/// `ExportMetricsServiceRequest` (CONCEPT:EG-316). Counter-family samples (name ending
/// `_total`/`_count`/`_sum`/`_bucket`) become a monotonic cumulative `sum`; everything
/// else a `gauge`. Labels ride as data-point attributes; `now_unix_nano` stamps every
/// point. Returns the JSON string.
pub fn otlp_metrics_json(metrics_text: &str, now_unix_nano: i64) -> String {
    let samples = parse_prom_exposition(metrics_text);
    let metrics: Vec<serde_json::Value> = samples
        .iter()
        .map(|s| {
            let attrs: Vec<serde_json::Value> =
                s.labels.iter().map(|(k, v)| otlp_str_attr(k, v)).collect();
            let point = serde_json::json!({
                "attributes": attrs,
                "asDouble": s.value,
                "timeUnixNano": now_unix_nano.to_string(),
            });
            if is_sum_metric(&s.name) {
                serde_json::json!({
                    "name": s.name,
                    "sum": {
                        "dataPoints": [point],
                        "aggregationTemporality": 2, // CUMULATIVE
                        "isMonotonic": true,
                    },
                })
            } else {
                serde_json::json!({ "name": s.name, "gauge": { "dataPoints": [point] } })
            }
        })
        .collect();
    serde_json::json!({
        "resourceMetrics": [{
            "resource": { "attributes": [otlp_str_attr("service.name", EXPORT_SERVICE_NAME)] },
            "scopeMetrics": [{
                "scope": { "name": EXPORT_SERVICE_NAME },
                "metrics": metrics,
            }],
        }]
    })
    .to_string()
}

/// A Prometheus counter/histogram/summary family (cumulative-monotonic) vs a gauge.
fn is_sum_metric(name: &str) -> bool {
    name.ends_with("_total")
        || name.ends_with("_count")
        || name.ends_with("_sum")
        || name.ends_with("_bucket")
}

// ── OTLP traces serialization (Span → ExportTraceServiceRequest) ────────────────

/// Map a status text back onto an OTLP status code object (the inverse of the EG-163
/// ingest mapping): `ERROR` ⇒ `STATUS_CODE_ERROR`, `OK` ⇒ `STATUS_CODE_OK`, else unset.
fn otlp_status_obj(status: &str) -> serde_json::Value {
    let code = match status {
        "ERROR" => "STATUS_CODE_ERROR",
        "OK" => "STATUS_CODE_OK",
        _ => "STATUS_CODE_UNSET",
    };
    serde_json::json!({ "code": code })
}

/// Serialize one span into an OTLP-JSON span object (the shape
/// [`super::super::traces::parse_otlp_traces`] parses).
fn span_to_otlp(sp: &Span) -> serde_json::Value {
    let attrs: Vec<serde_json::Value> = sp
        .attributes
        .iter()
        .map(|(k, v)| otlp_str_attr(k, v))
        .collect();
    let events: Vec<serde_json::Value> = sp
        .events
        .iter()
        .map(|e| {
            let ev_attrs: Vec<serde_json::Value> =
                e.attrs.iter().map(|(k, v)| otlp_str_attr(k, v)).collect();
            serde_json::json!({
                "timeUnixNano": e.ts.to_string(),
                "name": e.name,
                "attributes": ev_attrs,
            })
        })
        .collect();
    serde_json::json!({
        "traceId": sp.trace_id,
        "spanId": sp.span_id,
        "parentSpanId": sp.parent_span_id,
        "name": sp.operation,
        "startTimeUnixNano": sp.start_time.to_string(),
        "endTimeUnixNano": sp.end_time().to_string(),
        "status": otlp_status_obj(&sp.status),
        "attributes": attrs,
        "events": events,
    })
}

/// Serialize a batch of spans into an OTLP/HTTP JSON `ExportTraceServiceRequest`
/// (CONCEPT:EG-316), grouping spans by their `service` into one `resourceSpans` entry
/// each (resource `service.name` = the span's service). Returns the JSON string.
pub fn otlp_traces_json(spans: &[Span]) -> String {
    // Group by service, preserving first-seen order for a stable serialization.
    let mut order: Vec<String> = Vec::new();
    let mut by_service: std::collections::HashMap<String, Vec<&Span>> =
        std::collections::HashMap::new();
    for sp in spans {
        let svc = sp.service.clone();
        if !by_service.contains_key(&svc) {
            order.push(svc.clone());
        }
        by_service.entry(svc).or_default().push(sp);
    }
    let resource_spans: Vec<serde_json::Value> = order
        .iter()
        .map(|svc| {
            let spans_json: Vec<serde_json::Value> =
                by_service[svc].iter().map(|sp| span_to_otlp(sp)).collect();
            serde_json::json!({
                "resource": { "attributes": [otlp_str_attr("service.name", svc)] },
                "scopeSpans": [{
                    "scope": { "name": EXPORT_SERVICE_NAME },
                    "spans": spans_json,
                }],
            })
        })
        .collect();
    serde_json::json!({ "resourceSpans": resource_spans }).to_string()
}

// ── live gather + push ──────────────────────────────────────────────────────────

/// Collect every stored span out of the trace store (all traces, unbounded window),
/// flattening the assembled parent→child trees back to a flat span list.
fn gather_spans(state: &Arc<ObsState>) -> Vec<Span> {
    use eg_tsdb::traces::{SpanNode, TraceQuery};
    let q = TraceQuery {
        from: i64::MIN,
        to: i64::MAX,
        ..TraceQuery::default()
    };
    let mut out = Vec::new();
    fn walk(node: &SpanNode, out: &mut Vec<Span>) {
        out.push(node.span.clone());
        for c in &node.children {
            walk(c, out);
        }
    }
    for trace in state.trace_store().search(&q) {
        for root in &trace.roots {
            walk(root, &mut out);
        }
    }
    out
}

/// Push the engine's current metrics + stored spans to an OTLP/HTTP collector at
/// `endpoint` (CONCEPT:EG-316). Posts the metrics JSON to `{endpoint}/v1/metrics` and
/// the traces JSON to `{endpoint}/v1/traces` (the OTLP/HTTP defaults). Runs the
/// blocking `ureq` calls off the reactor. Returns `(metrics_ok, traces_ok)` — a dead
/// collector degrades to `Ok((false, false))` rather than failing the caller.
pub async fn export_once(state: Arc<ObsState>, endpoint: String) -> Result<(bool, bool), String> {
    let metrics_body = otlp_metrics_json(&crate::metrics::render(), now_ns());
    let spans = gather_spans(&state);
    let traces_body = otlp_traces_json(&spans);
    let endpoint = endpoint.trim_end_matches('/').to_string();

    tokio::task::spawn_blocking(move || {
        let post = |url: String, body: String| -> bool {
            ureq::post(&url)
                .set("content-type", "application/json")
                .send_string(&body)
                .is_ok()
        };
        let m = post(format!("{endpoint}/v1/metrics"), metrics_body);
        let t = post(format!("{endpoint}/v1/traces"), traces_body);
        Ok((m, t))
    })
    .await
    .map_err(|e| format!("otel export task failed: {e}"))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// CONCEPT:EG-316 — the engine's Prometheus metrics text serializes into a
    /// well-formed OTLP `ExportMetricsServiceRequest`: a counter (`_total`) becomes a
    /// monotonic `sum`, a gauge stays a `gauge`, labels ride as data-point attributes,
    /// and every point carries the service resource + a `timeUnixNano`.
    #[test]
    fn eg316_metrics_serialize_to_otlp_shape() {
        let text = "# HELP epistemic_graph_requests_total Requests\n\
                    # TYPE epistemic_graph_requests_total counter\n\
                    epistemic_graph_requests_total{op=\"Ping\"} 7\n\
                    epistemic_graph_in_flight_requests 3\n";
        let json = otlp_metrics_json(text, 1_700_000_000_000_000_000);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();

        // Resource carries service.name = epistemic-graph.
        let rm = &v["resourceMetrics"][0];
        assert_eq!(
            rm["resource"]["attributes"][0]["value"]["stringValue"],
            EXPORT_SERVICE_NAME
        );
        let metrics = rm["scopeMetrics"][0]["metrics"].as_array().unwrap();
        assert_eq!(metrics.len(), 2);

        // The `_total` counter → a monotonic cumulative sum, its label an attribute.
        let counter = metrics
            .iter()
            .find(|m| m["name"] == "epistemic_graph_requests_total")
            .unwrap();
        assert_eq!(counter["sum"]["isMonotonic"], true);
        assert_eq!(counter["sum"]["aggregationTemporality"], 2);
        let dp = &counter["sum"]["dataPoints"][0];
        assert_eq!(dp["asDouble"], 7.0);
        assert_eq!(dp["timeUnixNano"], "1700000000000000000");
        assert_eq!(dp["attributes"][0]["key"], "op");
        assert_eq!(dp["attributes"][0]["value"]["stringValue"], "Ping");

        // The bare gauge → a `gauge` with no label attributes.
        let gauge = metrics
            .iter()
            .find(|m| m["name"] == "epistemic_graph_in_flight_requests")
            .unwrap();
        assert_eq!(gauge["gauge"]["dataPoints"][0]["asDouble"], 3.0);
        assert!(gauge["gauge"]["dataPoints"][0]["attributes"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    /// CONCEPT:EG-316 — spans serialize into the OTLP `ExportTraceServiceRequest` shape
    /// and ROUND-TRIP back through the engine's OWN EG-163 OTLP-JSON parser to the same
    /// spans (service, ids, operation, duration, ERROR status, attribute all preserved).
    #[test]
    fn eg316_traces_serialize_to_otlp_and_roundtrip_through_ingest() {
        let spans = vec![
            Span {
                trace_id: "tr1".into(),
                span_id: "g1".into(),
                parent_span_id: String::new(),
                service: "gateway".into(),
                operation: "GET /buy".into(),
                start_time: 1000,
                duration: 2000,
                status: "OK".into(),
                attributes: BTreeMap::new(),
                events: Vec::new(),
            },
            Span {
                trace_id: "tr1".into(),
                span_id: "a1".into(),
                parent_span_id: "g1".into(),
                service: "api".into(),
                operation: "charge".into(),
                start_time: 1500,
                duration: 1300,
                status: "ERROR".into(),
                attributes: {
                    let mut m = BTreeMap::new();
                    m.insert("http.status_code".to_string(), "500".to_string());
                    m
                },
                events: Vec::new(),
            },
        ];
        let json = otlp_traces_json(&spans);

        // Two services → two resourceSpans entries.
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["resourceSpans"].as_array().unwrap().len(), 2);

        // Round-trip through the engine's own EG-163 OTLP-JSON span parser.
        let parsed = crate::server::traces::parse_otlp_traces(&json).unwrap();
        assert_eq!(parsed.len(), 2);
        let api = parsed.iter().find(|s| s.service == "api").unwrap();
        assert_eq!(api.trace_id, "tr1");
        assert_eq!(api.span_id, "a1");
        assert_eq!(api.parent_span_id, "g1");
        assert_eq!(api.operation, "charge");
        assert_eq!(api.duration, 1300, "endTime 2800 - startTime 1500");
        assert_eq!(api.status, "ERROR");
        assert_eq!(api.attributes.get("http.status_code").unwrap(), "500");
    }

    /// CONCEPT:EG-316 — the Prometheus exposition parser skips `# HELP`/`# TYPE` comment
    /// lines and blanks, and reads NaN/Inf value tokens.
    #[test]
    fn eg316_prom_exposition_parse_skips_comments_and_reads_special_values() {
        let text = "# HELP x help\n# TYPE x gauge\nx 1.5\n\ny{a=\"b\"} NaN\nz +Inf\n";
        let samples = parse_prom_exposition(text);
        assert_eq!(samples.len(), 3);
        assert_eq!(samples[0].name, "x");
        assert_eq!(samples[0].value, 1.5);
        assert_eq!(samples[1].name, "y");
        assert_eq!(samples[1].labels, vec![("a".to_string(), "b".to_string())]);
        assert!(samples[1].value.is_nan());
        assert!(samples[2].value.is_infinite() && samples[2].value > 0.0);
    }
}
