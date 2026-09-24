//! The neutral telemetry sample every stored signal is normalised into (EH-408).
//!
//! EG already stores three telemetry shapes: log records (OTLP / Elasticsearch /
//! JSON-lines, `src/server/obs`), Prometheus samples (`remote_write`) and OTLP
//! spans (`eg-tsdb::traces`). Binding and rollup only need three things from any
//! of them: when it happened, which attributes it carries (OTLP resource
//! attributes or Prometheus labels — the resolution keys live there), and what it
//! measured. [`TelemetrySignal`] is exactly that, so the binding layer never
//! learns a storage format.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The attribute a span's emitting service is copied under when the span's own
/// attribute map does not already carry it (OTLP semantic convention).
pub const SERVICE_NAME_ATTRIBUTE: &str = "service.name";

/// The Prometheus label that carries a sample's metric name.
pub const METRIC_NAME_LABEL: &str = "__name__";

/// Severity texts that mark a log record as an error, compared case-insensitively.
/// OTLP severity texts, syslog levels and the common logger spellings.
const ERROR_SEVERITIES: &[&str] = &[
    "error",
    "err",
    "fatal",
    "critical",
    "crit",
    "alert",
    "emerg",
    "emergency",
    "panic",
];

/// Which of the three stored telemetry shapes a signal came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalKind {
    Log,
    Metric,
    Span,
}

/// Whether a log record or span reports a failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Ok,
    Error,
}

impl Outcome {
    /// A log record's outcome, read from its severity text.
    pub fn from_severity(severity: &str) -> Outcome {
        let lowered = severity.trim().to_ascii_lowercase();
        if ERROR_SEVERITIES.contains(&lowered.as_str()) {
            Outcome::Error
        } else {
            Outcome::Ok
        }
    }

    /// A span's outcome, read from its OTLP status text (`ERROR` / `OK` / unset).
    pub fn from_span_status(status: &str) -> Outcome {
        if status.trim().eq_ignore_ascii_case("error") {
            Outcome::Error
        } else {
            Outcome::Ok
        }
    }

    /// `1.0` for an error, `0.0` otherwise — the count an outcome adds to a rollup.
    pub fn error_weight(self) -> f64 {
        match self {
            Outcome::Ok => 0.0,
            Outcome::Error => 1.0,
        }
    }
}

/// What one signal measured.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "measure", rename_all = "snake_case")]
pub enum Measure {
    /// One log record.
    Log { outcome: Outcome },
    /// One span with its duration in microseconds.
    Span { duration_us: u64, outcome: Outcome },
    /// One Prometheus sample of the named metric.
    Metric { name: String, value: f64 },
}

/// One normalised telemetry sample.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TelemetrySignal {
    /// Event time, epoch milliseconds.
    pub ts_ms: u64,
    /// Where the sample is stored: the log stream, the metric series id, or the
    /// trace id. Carried into every observation's provenance.
    pub source: String,
    /// OTLP resource/span attributes or Prometheus labels.
    pub attributes: BTreeMap<String, String>,
    pub measure: Measure,
}

impl TelemetrySignal {
    /// A log record from `stream` with the given severity text.
    pub fn log(
        ts_ms: u64,
        stream: impl Into<String>,
        severity: &str,
        attributes: BTreeMap<String, String>,
    ) -> TelemetrySignal {
        TelemetrySignal {
            ts_ms,
            source: stream.into(),
            attributes,
            measure: Measure::Log {
                outcome: Outcome::from_severity(severity),
            },
        }
    }

    /// A span, from its nanosecond start/duration, status text and emitting
    /// service. The service is copied under [`SERVICE_NAME_ATTRIBUTE`] unless the
    /// attributes already name one, so the standard service rule binds it.
    pub fn span(
        trace_id: impl Into<String>,
        timing_ns: (i64, i64),
        status: &str,
        service: &str,
        mut attributes: BTreeMap<String, String>,
    ) -> TelemetrySignal {
        let (start_ns, duration_ns) = timing_ns;
        if !service.is_empty() {
            attributes
                .entry(SERVICE_NAME_ATTRIBUTE.to_string())
                .or_insert_with(|| service.to_string());
        }
        TelemetrySignal {
            ts_ms: nanos_to_millis(start_ns),
            source: trace_id.into(),
            attributes,
            measure: Measure::Span {
                duration_us: nanos_to_micros(duration_ns),
                outcome: Outcome::from_span_status(status),
            },
        }
    }

    /// One Prometheus sample. The metric name is read from the `__name__` label;
    /// `series_id` is the stored series identity (`name{k="v",…}`).
    pub fn metric(
        ts_ms: u64,
        series_id: impl Into<String>,
        labels: BTreeMap<String, String>,
        value: f64,
    ) -> TelemetrySignal {
        let name = labels.get(METRIC_NAME_LABEL).cloned().unwrap_or_default();
        TelemetrySignal {
            ts_ms,
            source: series_id.into(),
            attributes: labels,
            measure: Measure::Metric { name, value },
        }
    }

    /// Which stored shape this signal came from.
    pub fn kind(&self) -> SignalKind {
        match self.measure {
            Measure::Log { .. } => SignalKind::Log,
            Measure::Span { .. } => SignalKind::Span,
            Measure::Metric { .. } => SignalKind::Metric,
        }
    }
}

fn nanos_to_millis(ns: i64) -> u64 {
    u64::try_from(ns.max(0)).unwrap_or(0) / 1_000_000
}

fn nanos_to_micros(ns: i64) -> u64 {
    u64::try_from(ns.max(0)).unwrap_or(0) / 1_000
}
