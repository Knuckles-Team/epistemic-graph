//! Tenant-scoped reads of stored telemetry (EH-408).
//!
//! The observability store keys tenancy differently per shape, so each shape
//! states its own rule:
//!
//! * a log stream IS its tenancy key (`LogRecord::stream`): a caller may read
//!   the stream named exactly its tenant or any `<tenant>/…` stream, and names
//!   the streams it wants explicitly -- a foreign stream refuses the request;
//! * spans and metric samples are read only when they carry the caller's
//!   tenant under [`TENANT_ATTRIBUTE`] (an OTLP resource/span attribute, a
//!   Prometheus label). Unmarked telemetry belongs to no caller. A span is read
//!   when its own start lies in the window, from traces that start in it.
//!
//! Every read is bounded: more than [`MAX_SIGNALS`] signals in one window is a
//! typed refusal, never a silent truncation.

use eg_stream::telemetry::TelemetrySignal;

use crate::server::obs::{LogQuery, LogRecord, ObsState};

/// The attribute/label that marks a span or metric sample with its tenant.
/// An underscore, not a dot, so the same name is a legal Prometheus label.
pub(super) const TENANT_ATTRIBUTE: &str = "eg_tenant";

/// The most signals one derivation reads.
pub(super) const MAX_SIGNALS: usize = 200_000;

const WINDOW_TOO_LARGE: &str =
    "TELEMETRY_WINDOW_TOO_LARGE: the window holds more telemetry than one derivation reads";

/// A half-open event-time window `[from_ms, to_ms)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Window {
    from_ms: u64,
    to_ms: u64,
}

impl Window {
    pub(super) fn new(from_ms: u64, to_ms: u64) -> Result<Self, String> {
        if from_ms >= to_ms {
            return Err("INVALID_ARGUMENT: TelemetryDerive needs from_ms < to_ms".to_string());
        }
        Ok(Self { from_ms, to_ms })
    }

    pub(super) fn start_ns(self) -> i64 {
        millis_to_nanos(self.from_ms)
    }

    pub(super) fn end_ns(self) -> i64 {
        millis_to_nanos(self.to_ms)
    }

    #[cfg(feature = "traces")]
    fn contains_ns(self, ns: i64) -> bool {
        ns >= self.start_ns() && ns < self.end_ns()
    }
}

fn millis_to_nanos(ms: u64) -> i64 {
    i64::try_from(ms)
        .unwrap_or(i64::MAX)
        .saturating_mul(1_000_000)
}

fn nanos_to_millis(ns: i64) -> u64 {
    u64::try_from(ns.max(0)).unwrap_or(0) / 1_000_000
}

/// Whether `stream` lies in `tenant`'s namespace.
pub(super) fn stream_in_tenant(stream: &str, tenant: &str) -> bool {
    !tenant.is_empty()
        && (stream == tenant
            || stream
                .strip_prefix(tenant)
                .is_some_and(|rest| rest.starts_with('/')))
}

/// Refuse the request when any named stream lies outside the caller's tenant.
pub(super) fn require_tenant_streams(tenant: &str, streams: &[String]) -> Result<(), String> {
    match streams
        .iter()
        .find(|stream| !stream_in_tenant(stream, tenant))
    {
        Some(_) => Err(
            "ACCESS_DENIED: TelemetryDerive may read only log streams in the caller's tenant"
                .to_string(),
        ),
        None => Ok(()),
    }
}

/// Every signal the caller may read in `window`.
pub(super) fn tenant_signals(
    obs: &ObsState,
    tenant: &str,
    window: &Window,
    streams: &[String],
) -> Result<Vec<TelemetrySignal>, String> {
    require_tenant_streams(tenant, streams)?;
    let mut signals = Vec::new();
    push_logs(obs, window, streams, &mut signals)?;
    push_spans(obs, tenant, window, &mut signals);
    push_metrics(obs, tenant, window, &mut signals)?;
    if signals.len() > MAX_SIGNALS {
        return Err(WINDOW_TOO_LARGE.to_string());
    }
    Ok(signals)
}

pub(super) fn log_signal(record: &LogRecord) -> TelemetrySignal {
    TelemetrySignal::log(
        nanos_to_millis(record.ts),
        record.stream.clone(),
        &record.severity,
        record.attrs.clone(),
    )
}

fn push_logs(
    obs: &ObsState,
    window: &Window,
    streams: &[String],
    signals: &mut Vec<TelemetrySignal>,
) -> Result<(), String> {
    for stream in streams {
        let query = LogQuery {
            from: window.start_ns(),
            to: window.end_ns(),
            size: MAX_SIGNALS + 1,
            ..LogQuery::all(stream.clone())
        };
        let records = obs.search_logs(&query)?;
        if records.len() > MAX_SIGNALS {
            return Err(WINDOW_TOO_LARGE.to_string());
        }
        signals.extend(records.iter().map(log_signal));
    }
    Ok(())
}

#[cfg(feature = "traces")]
fn push_spans(obs: &ObsState, tenant: &str, window: &Window, signals: &mut Vec<TelemetrySignal>) {
    use eg_tsdb::traces::{SpanNode, TraceQuery};

    fn flatten<'a>(node: &'a SpanNode, out: &mut Vec<&'a eg_tsdb::traces::Span>) {
        out.push(&node.span);
        for child in &node.children {
            flatten(child, out);
        }
    }

    let query = TraceQuery {
        from: window.start_ns(),
        to: window.end_ns(),
        tags: vec![(TENANT_ATTRIBUTE.to_string(), tenant.to_string())],
        ..TraceQuery::new(MAX_SIGNALS + 1)
    };
    for trace in obs.trace_store().search(&query) {
        let mut spans = Vec::new();
        for root in &trace.roots {
            flatten(root, &mut spans);
        }
        signals.extend(
            spans
                .into_iter()
                .filter(|span| span_in_scope(span, tenant, window))
                .map(span_signal),
        );
    }
}

#[cfg(not(feature = "traces"))]
fn push_spans(
    _obs: &ObsState,
    _tenant: &str,
    _window: &Window,
    _signals: &mut Vec<TelemetrySignal>,
) {
}

#[cfg(feature = "traces")]
fn span_in_scope(span: &eg_tsdb::traces::Span, tenant: &str, window: &Window) -> bool {
    span.attributes.get(TENANT_ATTRIBUTE).map(String::as_str) == Some(tenant)
        && window.contains_ns(span.start_time)
}

#[cfg(feature = "traces")]
pub(super) fn span_signal(span: &eg_tsdb::traces::Span) -> TelemetrySignal {
    TelemetrySignal::span(
        span.trace_id.clone(),
        (span.start_time, span.duration),
        &span.status,
        &span.service,
        span.attributes.clone(),
    )
}

/// Log series share the store with remote-write metrics; they are read as logs.
#[cfg(feature = "promql")]
const LOG_SERIES_PREFIX: &str = "obs:logs:";

#[cfg(feature = "promql")]
fn push_metrics(
    obs: &ObsState,
    tenant: &str,
    window: &Window,
    signals: &mut Vec<TelemetrySignal>,
) -> Result<(), String> {
    let store = obs.series_store();
    let series = store
        .list_series()
        .map_err(|_| "telemetry metric series are unavailable".to_string())?;
    for series_id in series
        .iter()
        .filter(|id| !id.starts_with(LOG_SERIES_PREFIX))
    {
        let labels = crate::server::promql::parse_series_id(series_id);
        if labels.get(TENANT_ATTRIBUTE).map(String::as_str) != Some(tenant) {
            continue;
        }
        let points = store
            .range(series_id, window.start_ns(), window.end_ns())
            .map_err(|_| "telemetry metric samples are unavailable".to_string())?;
        signals.extend(points.iter().filter_map(|point| {
            let value = *point.values.first()?;
            Some(TelemetrySignal::metric(
                nanos_to_millis(point.ts),
                series_id.clone(),
                labels.clone(),
                value,
            ))
        }));
        if signals.len() > MAX_SIGNALS {
            return Err(WINDOW_TOO_LARGE.to_string());
        }
    }
    Ok(())
}

#[cfg(not(feature = "promql"))]
fn push_metrics(
    _obs: &ObsState,
    _tenant: &str,
    _window: &Window,
    _signals: &mut Vec<TelemetrySignal>,
) -> Result<(), String> {
    Ok(())
}
