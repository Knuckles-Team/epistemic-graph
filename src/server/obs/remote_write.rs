//! CONCEPT:EG-316 — the Prometheus `remote_write` RECEIVER: accept snappy-compressed
//! protobuf `WriteRequest` POSTs and land their samples in the durable eg-tsdb
//! [`SeriesStore`], so an EXTERNAL Prometheus (or any `remote_write` producer) can PUSH
//! its scraped time-series INTO the engine — the ingest counterpart of the OTLP export
//! in [`super::otel_export`].
//!
//! ## The wire
//!
//! Prometheus `remote_write` frames a `prometheus.WriteRequest` protobuf, snappy-BLOCK
//! compresses it, and POSTs it (usually to `/api/v1/write`) with
//! `Content-Encoding: snappy`. The schema (hand-declared here with the `prost::Message`
//! derive — NO `.proto`, NO build.rs codegen) is:
//!
//! ```text
//! message WriteRequest { repeated TimeSeries timeseries = 1; }
//! message TimeSeries   { repeated Label labels = 1; repeated Sample samples = 2; }
//! message Label        { string name = 1; string value = 2; }
//! message Sample       { double value = 1; int64 timestamp = 2; } // ts = epoch MILLIS
//! ```
//!
//! Each series' identity is encoded into the opaque `SeriesStore` id as the canonical
//! Prometheus text form `name{k="v",…}` (labels sorted; `__name__` is the metric name),
//! MATCHING [`super::super::promql`]'s series-id convention so pushed samples are
//! queryable via the EG-172 PromQL API. Sample timestamps (millis) are widened to the
//! store's epoch-nanoseconds.

use std::sync::Arc;

use eg_tsdb::point::Point;

use super::ObsState;

/// TSDB time-partition width for a pushed metric series: 1 hour of wall-clock per chunk
/// (matching the log-series bucketing in [`super::ObsState::ingest`]).
const SERIES_BUCKET_NS: u64 = 3_600_000_000_000;

// ── the Prometheus remote-write protobuf schema (hand-declared, prost-derived) ──

/// `prometheus.WriteRequest` — the top-level remote-write payload (CONCEPT:EG-316).
#[derive(Clone, PartialEq, prost::Message)]
pub struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
}

/// `prometheus.TimeSeries` — one labelled series with its samples.
#[derive(Clone, PartialEq, prost::Message)]
pub struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<Sample>,
}

/// `prometheus.Label` — a `name`/`value` label pair (`__name__` = the metric name).
#[derive(Clone, PartialEq, prost::Message)]
pub struct Label {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

/// `prometheus.Sample` — a float value at an epoch-**milliseconds** timestamp.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Sample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

// ── decode + landing ────────────────────────────────────────────────────────────

/// Decode a raw remote-write request body into a [`WriteRequest`]: snappy-BLOCK
/// decompress, then protobuf-decode. Tolerates an already-uncompressed body (a producer
/// that skipped snappy) by falling back to a direct protobuf decode.
pub fn decode_write_request(body: &[u8]) -> Result<WriteRequest, String> {
    let decompressed = snap::raw::Decoder::new().decompress_vec(body);
    let bytes: Vec<u8> = match decompressed {
        Ok(b) => b,
        Err(_) => body.to_vec(), // maybe the producer sent it uncompressed
    };
    prost::Message::decode(bytes.as_slice())
        .or_else(|_| prost::Message::decode(body))
        .map_err(|e| format!("decode remote_write WriteRequest: {e}"))
}

/// Encode a [`WriteRequest`] to the snappy-BLOCK-compressed protobuf wire body — the
/// inverse of [`decode_write_request`], used by producers (and the tests).
pub fn encode_write_request(req: &WriteRequest) -> Vec<u8> {
    let raw = prost::Message::encode_to_vec(req);
    snap::raw::Encoder::new()
        .compress_vec(&raw)
        .unwrap_or(raw)
}

/// Encode a Prometheus label set into the canonical `SeriesStore`/PromQL series id
/// `name{k="v",…}` (labels sorted; `__name__` lifted out as the name). MATCHES
/// [`super::super::promql::format_series_id`] so pushed samples are PromQL-queryable.
pub fn series_id_from_labels(labels: &[Label]) -> String {
    let mut name = String::new();
    let mut pairs: Vec<(String, String)> = Vec::new();
    for l in labels {
        if l.name == "__name__" {
            name = l.value.clone();
        } else {
            pairs.push((l.name.clone(), l.value.clone()));
        }
    }
    // A Prometheus series is identified by `__name__`; without it there is no metric
    // to key on, so signal "skip" with an empty id (never a label-only `{…}` id).
    if name.is_empty() {
        return String::new();
    }
    pairs.sort();
    if pairs.is_empty() {
        name
    } else {
        let inner: Vec<String> = pairs.iter().map(|(k, v)| format!("{k}=\"{v}\"")).collect();
        format!("{name}{{{}}}", inner.join(","))
    }
}

/// The outcome of ingesting a remote-write request: series touched + samples landed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RemoteWriteOutcome {
    pub series: usize,
    pub samples: usize,
}

/// Land a decoded [`WriteRequest`]'s samples into the durable `SeriesStore` behind
/// `state`: one series per labelled `TimeSeries` (id = [`series_id_from_labels`]),
/// each sample a single-field [`Point`] at `timestamp_ms × 1e6` nanoseconds. Series
/// carrying no `__name__` (blank id) are skipped. Returns the ingest counts.
pub fn ingest_write_request(
    state: &Arc<ObsState>,
    req: &WriteRequest,
) -> Result<RemoteWriteOutcome, String> {
    let store = state.series_store();
    let field_names = vec!["value".to_string()];
    let mut outcome = RemoteWriteOutcome::default();
    for ts in &req.timeseries {
        let series_id = series_id_from_labels(&ts.labels);
        if series_id.is_empty() || ts.samples.is_empty() {
            continue;
        }
        let points: Vec<Point> = ts
            .samples
            .iter()
            .map(|s| Point {
                ts: s.timestamp.saturating_mul(1_000_000), // epoch millis → nanos
                values: vec![s.value],
            })
            .collect();
        store
            .append_batch(&series_id, 1, SERIES_BUCKET_NS, &field_names, &points)
            .map_err(|e| e.to_string())?;
        outcome.series += 1;
        outcome.samples += points.len();
    }
    Ok(outcome)
}

/// Route + execute a Prometheus `remote_write` POST (`/api/v1/write`) → `(status,
/// content_type, body)` (CONCEPT:EG-316). Decodes the snappy protobuf body and lands
/// its samples in the tsdb; a well-formed empty request is a no-op `204`. `raw_body`
/// is the ORIGINAL request bytes (snappy-compressed — the receiver must decode the
/// binary body itself, not the lossily UTF-8'd string the listener also has).
pub async fn handle(
    state: &Arc<ObsState>,
    method: &str,
    raw_body: &[u8],
) -> (&'static str, &'static str, String) {
    if method != "POST" {
        return (
            "405 Method Not Allowed",
            "text/plain",
            "POST only".to_string(),
        );
    }
    let req = match decode_write_request(raw_body) {
        Ok(r) => r,
        Err(e) => return ("400 Bad Request", "text/plain", e),
    };
    let st = state.clone();
    match tokio::task::spawn_blocking(move || ingest_write_request(&st, &req)).await {
        Ok(Ok(o)) => {
            if o.samples == 0 {
                ("204 No Content", "text/plain", String::new())
            } else {
                (
                    "200 OK",
                    "application/json",
                    format!(
                        "{{\"series\":{},\"samples\":{}}}",
                        o.series, o.samples
                    ),
                )
            }
        }
        Ok(Err(e)) => (
            "500 Internal Server Error",
            "text/plain",
            format!("remote_write ingest failed: {e}"),
        ),
        Err(e) => (
            "500 Internal Server Error",
            "text/plain",
            format!("remote_write task failed: {e}"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CONCEPT:EG-316 — a Prometheus label set encodes into the canonical PromQL series
    /// id `name{k="v",…}` (labels sorted; `__name__` lifted as the metric name).
    #[test]
    fn eg316_series_id_from_labels_is_canonical_and_sorted() {
        let labels = vec![
            Label {
                name: "__name__".into(),
                value: "http_requests_total".into(),
            },
            Label {
                name: "method".into(),
                value: "get".into(),
            },
            Label {
                name: "job".into(),
                value: "api".into(),
            },
        ];
        assert_eq!(
            series_id_from_labels(&labels),
            r#"http_requests_total{job="api",method="get"}"#
        );
        // A bare metric with no extra labels → just the name.
        assert_eq!(
            series_id_from_labels(&[Label {
                name: "__name__".into(),
                value: "up".into()
            }]),
            "up"
        );
    }

    /// CONCEPT:EG-316 — a `WriteRequest` round-trips through the snappy+protobuf wire
    /// codec (`encode_write_request` → `decode_write_request`) unchanged.
    #[test]
    fn eg316_write_request_snappy_protobuf_roundtrips() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![Label {
                    name: "__name__".into(),
                    value: "cpu".into(),
                }],
                samples: vec![
                    Sample {
                        value: 0.5,
                        timestamp: 1000,
                    },
                    Sample {
                        value: 0.9,
                        timestamp: 2000,
                    },
                ],
            }],
        };
        let wire = encode_write_request(&req);
        let back = decode_write_request(&wire).unwrap();
        assert_eq!(back, req);
    }

    /// CONCEPT:EG-316 — the receiver PARSES a Prometheus `WriteRequest` (snappy protobuf)
    /// into tsdb samples: two labelled series' samples land in the durable SeriesStore
    /// under their canonical PromQL series ids, at nanosecond timestamps (millis×1e6),
    /// readable back via `SeriesStore::range`.
    #[test]
    fn eg316_remote_write_parses_into_tsdb_samples() {
        let state = Arc::new(ObsState::in_memory(1024).unwrap());
        let req = WriteRequest {
            timeseries: vec![
                TimeSeries {
                    labels: vec![
                        Label {
                            name: "__name__".into(),
                            value: "node_cpu_seconds".into(),
                        },
                        Label {
                            name: "cpu".into(),
                            value: "0".into(),
                        },
                    ],
                    samples: vec![
                        Sample {
                            value: 1.0,
                            timestamp: 1, // 1 ms → 1_000_000 ns
                        },
                        Sample {
                            value: 2.0,
                            timestamp: 2,
                        },
                    ],
                },
                TimeSeries {
                    labels: vec![Label {
                        name: "__name__".into(),
                        value: "up".into(),
                    }],
                    samples: vec![Sample {
                        value: 1.0,
                        timestamp: 5,
                    }],
                },
            ],
        };

        // Land via the FULL wire path: encode → snappy → decode → ingest.
        let wire = encode_write_request(&req);
        let decoded = decode_write_request(&wire).unwrap();
        let outcome = ingest_write_request(&state, &decoded).unwrap();
        assert_eq!(
            outcome,
            RemoteWriteOutcome {
                series: 2,
                samples: 3
            }
        );

        // The samples are readable back out of the durable store under the canonical id.
        let store = state.series_store();
        let cpu = store
            .range(r#"node_cpu_seconds{cpu="0"}"#, 0, 1_000_000_000)
            .unwrap();
        assert_eq!(cpu.len(), 2);
        assert_eq!(cpu[0].ts, 1_000_000, "1 ms widened to 1_000_000 ns");
        assert_eq!(cpu[0].values[0], 1.0);
        assert_eq!(cpu[1].ts, 2_000_000);

        let up = store.range("up", 0, 1_000_000_000).unwrap();
        assert_eq!(up.len(), 1);
        assert_eq!(up[0].ts, 5_000_000);
        assert_eq!(up[0].values[0], 1.0);
    }

    /// CONCEPT:EG-316 — a series with no `__name__` (blank id) is skipped, not errored.
    #[test]
    fn eg316_remote_write_skips_nameless_series() {
        let state = Arc::new(ObsState::in_memory(1024).unwrap());
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![Label {
                    name: "job".into(),
                    value: "api".into(),
                }],
                samples: vec![Sample {
                    value: 1.0,
                    timestamp: 1,
                }],
            }],
        };
        let outcome = ingest_write_request(&state, &req).unwrap();
        assert_eq!(outcome, RemoteWriteOutcome::default());
    }
}
