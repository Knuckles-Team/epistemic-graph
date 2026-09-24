//! Wire-format parsers: OTLP/HTTP JSON logs, Elasticsearch `_bulk`/`_doc` and
//! JSON-lines, each normalised into a [`LogRecord`].

use std::collections::BTreeMap;

use super::{now_ns, LogRecord};

/// The searchable text for a record: the body plus its flattened attributes, so a
/// full-text query can match on either (schema-on-read).
pub(super) fn text_body(r: &LogRecord) -> String {
    let mut s = r.body.clone();
    if !r.severity.is_empty() {
        s.push(' ');
        s.push_str(&r.severity);
    }
    for (k, v) in &r.attrs {
        s.push(' ');
        s.push_str(k);
        s.push('=');
        s.push_str(v);
    }
    s
}

/// Extract a timestamp (epoch-ns) from a generic doc, tolerating the common shapes:
/// OTLP `timeUnixNano` (ns string/number), `@timestamp`/`timestamp`/`time` as epoch
/// ms or an ISO-8601-ish number. Falls back to `now`.
fn extract_ts(doc: &serde_json::Value) -> i64 {
    if let Some(n) = extract_nano_ts(doc) {
        return n;
    }
    if let Some(n) = extract_millis_ts(doc) {
        return n;
    }
    now_ns()
}

/// OTLP nano timestamp (string or number of nanoseconds). Half of [`extract_ts`]'s
/// field scan.
fn extract_nano_ts(doc: &serde_json::Value) -> Option<i64> {
    for key in ["timeUnixNano", "observedTimeUnixNano"] {
        if let Some(v) = doc.get(key) {
            if let Some(n) = v.as_str().and_then(|s| s.parse::<i64>().ok()) {
                return Some(n);
            }
            if let Some(n) = v.as_i64() {
                return Some(n);
            }
        }
    }
    None
}

/// Epoch-millis style fields → ns. A plain integer is treated as milliseconds (the
/// Elastic/O2 convention); a non-numeric (ISO string) is left for [`extract_ts`]'s
/// `now_ns()` fallback. The other half of [`extract_ts`]'s field scan.
fn extract_millis_ts(doc: &serde_json::Value) -> Option<i64> {
    for key in ["@timestamp", "timestamp", "time", "_timestamp"] {
        if let Some(v) = doc.get(key) {
            if let Some(ms) = v.as_i64() {
                return Some(ms.saturating_mul(1_000_000));
            }
            if let Some(ms) = v.as_f64() {
                return Some((ms * 1_000_000.0) as i64);
            }
        }
    }
    None
}

/// Extract the severity text from the common field names.
fn extract_severity(doc: &serde_json::Value) -> String {
    for key in ["severityText", "severity", "level", "log.level", "loglevel"] {
        if let Some(s) = doc.get(key).and_then(|v| v.as_str()) {
            return s.to_string();
        }
    }
    String::new()
}

/// Extract the message body from the common field names (OTLP `body.stringValue`,
/// or `message`/`msg`/`body`).
fn extract_body(doc: &serde_json::Value) -> String {
    if let Some(b) = doc.get("body") {
        if let Some(s) = b.get("stringValue").and_then(|v| v.as_str()) {
            return s.to_string();
        }
        if let Some(s) = b.as_str() {
            return s.to_string();
        }
    }
    for key in ["message", "msg", "log", "_message"] {
        if let Some(s) = doc.get(key).and_then(|v| v.as_str()) {
            return s.to_string();
        }
    }
    String::new()
}

/// The set of doc keys consumed into fixed [`LogRecord`] fields (so they are not
/// ALSO duplicated into `attrs`).
const RESERVED_KEYS: &[&str] = &[
    "timeUnixNano",
    "observedTimeUnixNano",
    "@timestamp",
    "timestamp",
    "time",
    "_timestamp",
    "severityText",
    "severity",
    "level",
    "log.level",
    "loglevel",
    "body",
    "message",
    "msg",
    "log",
    "_message",
    "stream",
    "_stream",
    "attributes",
];

/// Collect the remaining scalar fields of a doc into the dynamic attribute map, plus
/// any OTLP `attributes` array (`[{key,value:{stringValue}}]`).
fn extract_attrs(doc: &serde_json::Value) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    if let Some(obj) = doc.as_object() {
        for (k, v) in obj {
            if RESERVED_KEYS.contains(&k.as_str()) {
                continue;
            }
            out.insert(k.clone(), scalar_to_string(v));
        }
    }
    // OTLP-style attributes array.
    if let Some(arr) = doc.get("attributes").and_then(|v| v.as_array()) {
        for a in arr {
            if let (Some(k), Some(val)) = (a.get("key").and_then(|v| v.as_str()), a.get("value")) {
                out.insert(k.to_string(), otlp_anyvalue(val));
            }
        }
    }
    out
}

/// Render a JSON scalar (or nested value) as a compact attribute string.
pub(super) fn scalar_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Unwrap an OTLP `AnyValue` (`{stringValue|intValue|doubleValue|boolValue}`).
fn otlp_anyvalue(v: &serde_json::Value) -> String {
    for key in ["stringValue", "intValue", "doubleValue", "boolValue"] {
        if let Some(inner) = v.get(key) {
            return scalar_to_string(inner);
        }
    }
    scalar_to_string(v)
}

/// Resolve the stream for a doc: an explicit `stream`/`_stream` field wins, else the
/// caller's default (path/query-derived).
fn extract_stream(doc: &serde_json::Value, default_stream: &str) -> String {
    for key in ["stream", "_stream"] {
        if let Some(s) = doc.get(key).and_then(|v| v.as_str()) {
            if !s.is_empty() {
                return s.to_string();
            }
        }
    }
    default_stream.to_string()
}

/// Normalize a single generic doc into a [`LogRecord`] with the given default stream.
pub(super) fn doc_to_record(doc: &serde_json::Value, default_stream: &str) -> LogRecord {
    LogRecord {
        ts: extract_ts(doc),
        stream: extract_stream(doc, default_stream),
        severity: extract_severity(doc),
        body: extract_body(doc),
        attrs: extract_attrs(doc),
    }
}

/// Parse an OTLP/HTTP JSON `ExportLogsServiceRequest`
/// (`resourceLogs[].scopeLogs[].logRecords[]`) into records. The stream is derived
/// from the resource's `service.name` attribute, else `default_stream`.
pub fn parse_otlp_logs(body: &str, default_stream: &str) -> Result<Vec<LogRecord>, String> {
    let root: serde_json::Value =
        serde_json::from_str(body).map_err(|e| format!("parse OTLP JSON: {e}"))?;
    let mut out = Vec::new();
    let resource_logs = root
        .get("resourceLogs")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    for rl in &resource_logs {
        let stream = resource_log_stream(rl, default_stream);
        push_scope_log_records(rl, &stream, &mut out);
    }
    Ok(out)
}

/// Resolve one `resourceLogs` entry's stream name from its `resource.attributes`
/// `service.name`, falling back to `default_stream`. Extracted from
/// [`parse_otlp_logs`]'s per-resource loop.
fn resource_log_stream(rl: &serde_json::Value, default_stream: &str) -> String {
    let mut stream = default_stream.to_string();
    if let Some(attrs) = rl
        .get("resource")
        .and_then(|r| r.get("attributes"))
        .and_then(|v| v.as_array())
    {
        for a in attrs {
            if a.get("key").and_then(|v| v.as_str()) == Some("service.name") {
                if let Some(val) = a.get("value") {
                    let s = otlp_anyvalue(val);
                    if !s.is_empty() {
                        stream = s;
                    }
                }
            }
        }
    }
    stream
}

/// Push every log record from one `resourceLogs` entry's `scopeLogs[].logRecords[]`
/// into `out`. Extracted from [`parse_otlp_logs`]'s per-resource loop.
fn push_scope_log_records(rl: &serde_json::Value, stream: &str, out: &mut Vec<LogRecord>) {
    for sl in rl
        .get("scopeLogs")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
    {
        for lr in sl
            .get("logRecords")
            .and_then(|v| v.as_array())
            .into_iter()
            .flatten()
        {
            out.push(doc_to_record(lr, stream));
        }
    }
}

/// Parse an Elasticsearch `_bulk` NDJSON body: alternating action/source lines
/// (`{"index":{"_index":"logs"}}\n{doc}\n`). The `_index` names the stream (else
/// `default_stream`). `create`/`index` actions carry a following source line;
/// `delete` actions (no source) are skipped.
pub fn parse_es_bulk(body: &str, default_stream: &str) -> Vec<LogRecord> {
    let mut out = Vec::new();
    let mut lines = body.lines().filter(|l| !l.trim().is_empty());
    while let Some(action_line) = lines.next() {
        let action: serde_json::Value = match serde_json::from_str(action_line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        // The action object has ONE key: index|create|update|delete.
        let (op, meta) = match action.as_object().and_then(|o| o.iter().next()) {
            Some((k, v)) => (k.as_str(), v),
            None => continue,
        };
        if op == "delete" {
            continue; // no source line follows
        }
        let Some(source_line) = lines.next() else {
            break;
        };
        let doc: serde_json::Value = match serde_json::from_str(source_line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let stream = meta
            .get("_index")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or(default_stream);
        out.push(doc_to_record(&doc, stream));
    }
    out
}

/// Parse a plain JSON-lines body (one JSON object per line) into records.
pub fn parse_json_lines(body: &str, default_stream: &str) -> Vec<LogRecord> {
    let trimmed = body.trim_start();
    // Tolerate a single JSON array too (`[ {...}, {...} ]`).
    if trimmed.starts_with('[') {
        if let Ok(serde_json::Value::Array(arr)) = serde_json::from_str::<serde_json::Value>(body) {
            return arr
                .iter()
                .map(|d| doc_to_record(d, default_stream))
                .collect();
        }
    }
    body.lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .map(|d| doc_to_record(&d, default_stream))
        .collect()
}
