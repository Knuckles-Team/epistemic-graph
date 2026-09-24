//! Bound telemetry → `BehaviourObservation` rollups (EH-408).
//!
//! Bound signals are grouped per entity and per tumbling window, and each group
//! becomes one [`BehaviourObservation`]: request volume, errors, error ratio,
//! rate and latency percentiles, plus the provenance that makes it
//! observation-class evidence — which resolution rules bound it, which stored
//! streams/series/traces it read, how many signals of each kind, and the exact
//! span of event time it covers. Raw points stay where they are stored; the
//! observation cites them.
//!
//! How each signal counts:
//! * a log record or span is one request, and one error when it reports one;
//!   a span also contributes its duration as a latency sample;
//! * a metric sample counts only under a declared [`MetricRole`]. Request and
//!   error counters contribute their INCREASE since the series' previous sample
//!   (a drop is a counter reset, so the new value is the increase); a series'
//!   first sample only establishes the baseline. A latency gauge contributes
//!   its value as a latency sample. A sample of an undeclared metric, and a
//!   non-finite sample (a Prometheus staleness marker), is counted in
//!   [`RollupReport::ignored_metric_samples`] and nowhere else.
//!
//! The result does not depend on the order signals arrive in: they are put in
//! one canonical order first, so sums and counter deltas are reproduced exactly.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::binding::{EntityDirectory, EntityRef, Resolution, Unresolved};
use super::signal::{Measure, SignalKind, TelemetrySignal};

/// How a declared metric contributes to a rollup.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricRole {
    /// A monotonic request counter.
    Requests,
    /// A monotonic error counter.
    Errors,
    /// A latency gauge in milliseconds.
    LatencyMs,
}

/// The rollup window and the declared metric roles.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RollupPolicy {
    /// Tumbling window width in milliseconds (`0` is treated as `1`).
    pub window_ms: u64,
    /// Metric name → role. Metrics not named here are ignored.
    #[serde(default)]
    pub metric_roles: BTreeMap<String, MetricRole>,
}

/// Where an observation's numbers came from.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ObservationProvenance {
    /// The resolution rules that bound the contributing signals.
    pub rules: BTreeSet<String>,
    /// The stored streams, series and traces the signals were read from.
    pub sources: BTreeSet<String>,
    /// How many signals of each kind contributed.
    pub signal_counts: BTreeMap<SignalKind, u64>,
    /// Earliest and latest contributing event time, epoch milliseconds.
    pub first_ts_ms: u64,
    pub last_ts_ms: u64,
}

/// One entity's behaviour over one window — observation-class evidence.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BehaviourObservation {
    pub id: String,
    pub entity: EntityRef,
    pub window_start_ms: u64,
    pub window_end_ms: u64,
    pub requests: f64,
    pub errors: f64,
    /// `errors / requests`, `0` when there were no requests.
    pub error_ratio: f64,
    /// Requests per second over the window.
    pub rate_per_sec: f64,
    pub latency_p50_ms: Option<f64>,
    pub latency_p95_ms: Option<f64>,
    pub provenance: ObservationProvenance,
}

/// A signal that did not bind, and why.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UnresolvedSignal {
    pub kind: SignalKind,
    pub source: String,
    pub ts_ms: u64,
    pub reason: Unresolved,
}

/// Everything one rollup produced.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RollupReport {
    /// Ordered by entity, then window start.
    pub observations: Vec<BehaviourObservation>,
    pub unresolved: Vec<UnresolvedSignal>,
    pub ignored_metric_samples: u64,
}

/// What one signal adds to its bucket.
struct Contribution {
    requests: f64,
    errors: f64,
    latency_ms: Option<f64>,
}

#[derive(Default)]
struct Bucket {
    requests: f64,
    errors: f64,
    latencies_ms: Vec<f64>,
    provenance: ObservationProvenance,
}

impl Bucket {
    fn add(&mut self, signal: &TelemetrySignal, rule: &str, contribution: Contribution) {
        self.requests += contribution.requests;
        self.errors += contribution.errors;
        self.latencies_ms.extend(contribution.latency_ms);
        let provenance = &mut self.provenance;
        if provenance.sources.is_empty() {
            provenance.first_ts_ms = signal.ts_ms;
        }
        provenance.last_ts_ms = signal.ts_ms;
        provenance.rules.insert(rule.to_string());
        provenance.sources.insert(signal.source.clone());
        *provenance.signal_counts.entry(signal.kind()).or_default() += 1;
    }
}

/// Bind every signal and roll the bound ones up per entity and window.
pub fn rollup(
    policy: &RollupPolicy,
    directory: &EntityDirectory,
    signals: &[TelemetrySignal],
) -> RollupReport {
    let window_ms = policy.window_ms.max(1);
    let mut ordered: Vec<&TelemetrySignal> = signals.iter().collect();
    ordered.sort_by(|left, right| canonical_order(left, right));

    let mut report = RollupReport::default();
    let mut buckets: BTreeMap<(EntityRef, u64), Bucket> = BTreeMap::new();
    let mut counters: BTreeMap<String, f64> = BTreeMap::new();
    for signal in ordered {
        let Some(contribution) = contribution(policy, &mut counters, signal) else {
            report.ignored_metric_samples += 1;
            continue;
        };
        match directory.resolve(signal) {
            Resolution::Bound { entity, rule } => {
                let start = signal.ts_ms - signal.ts_ms % window_ms;
                buckets
                    .entry((entity, start))
                    .or_default()
                    .add(signal, &rule, contribution);
            }
            Resolution::Unresolved(reason) => report.unresolved.push(UnresolvedSignal {
                kind: signal.kind(),
                source: signal.source.clone(),
                ts_ms: signal.ts_ms,
                reason,
            }),
        }
    }
    report.observations = buckets
        .into_iter()
        .map(|((entity, start), bucket)| observation(entity, start, window_ms, bucket))
        .collect();
    report
}

/// The stable id of the observation of `entity` over the window starting at `start`.
pub fn observation_id(entity: &EntityRef, window_start_ms: u64) -> String {
    format!(
        "behaviour:{}:{}:{}",
        entity.class.label(),
        entity.id,
        window_start_ms
    )
}

fn observation(
    entity: EntityRef,
    start: u64,
    window_ms: u64,
    bucket: Bucket,
) -> BehaviourObservation {
    let mut latencies = bucket.latencies_ms;
    latencies.sort_by(f64::total_cmp);
    let error_ratio = if bucket.requests > 0.0 {
        bucket.errors / bucket.requests
    } else {
        0.0
    };
    BehaviourObservation {
        id: observation_id(&entity, start),
        entity,
        window_start_ms: start,
        window_end_ms: start.saturating_add(window_ms),
        requests: bucket.requests,
        errors: bucket.errors,
        error_ratio,
        rate_per_sec: bucket.requests * 1000.0 / window_ms as f64,
        latency_p50_ms: nearest_rank(&latencies, 50),
        latency_p95_ms: nearest_rank(&latencies, 95),
        provenance: bucket.provenance,
    }
}

/// Nearest-rank percentile of an ascending slice.
fn nearest_rank(sorted: &[f64], percentile: usize) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let rank = (percentile * sorted.len()).div_ceil(100).max(1);
    sorted.get(rank - 1).copied()
}

/// What `signal` adds, or `None` for a sample of an undeclared metric or a
/// non-finite sample.
fn contribution(
    policy: &RollupPolicy,
    counters: &mut BTreeMap<String, f64>,
    signal: &TelemetrySignal,
) -> Option<Contribution> {
    match &signal.measure {
        Measure::Log { outcome } => Some(Contribution {
            requests: 1.0,
            errors: outcome.error_weight(),
            latency_ms: None,
        }),
        Measure::Span {
            duration_us,
            outcome,
        } => Some(Contribution {
            requests: 1.0,
            errors: outcome.error_weight(),
            latency_ms: Some(*duration_us as f64 / 1000.0),
        }),
        Measure::Metric { name, value } => {
            // A non-finite sample (a Prometheus staleness marker) measures nothing.
            if !value.is_finite() {
                return None;
            }
            let role = policy.metric_roles.get(name)?;
            Some(metric_contribution(*role, counters, &signal.source, *value))
        }
    }
}

fn metric_contribution(
    role: MetricRole,
    counters: &mut BTreeMap<String, f64>,
    series: &str,
    value: f64,
) -> Contribution {
    let mut contribution = Contribution {
        requests: 0.0,
        errors: 0.0,
        latency_ms: None,
    };
    match role {
        MetricRole::Requests => contribution.requests = counter_increase(counters, series, value),
        MetricRole::Errors => contribution.errors = counter_increase(counters, series, value),
        MetricRole::LatencyMs => contribution.latency_ms = Some(value),
    }
    contribution
}

/// The increase of a monotonic counter since its series' previous sample.
fn counter_increase(counters: &mut BTreeMap<String, f64>, series: &str, value: f64) -> f64 {
    match counters.insert(series.to_string(), value) {
        None => 0.0,
        Some(previous) if value >= previous => value - previous,
        Some(_) => value,
    }
}

/// One total order over signals, so a rollup never depends on arrival order.
fn canonical_order(left: &TelemetrySignal, right: &TelemetrySignal) -> Ordering {
    left.ts_ms
        .cmp(&right.ts_ms)
        .then_with(|| left.source.cmp(&right.source))
        .then_with(|| left.kind().cmp(&right.kind()))
        .then_with(|| left.attributes.cmp(&right.attributes))
        .then_with(|| measure_order(&left.measure, &right.measure))
}

fn measure_order(left: &Measure, right: &Measure) -> Ordering {
    match (left, right) {
        (Measure::Log { outcome: a }, Measure::Log { outcome: b }) => a.cmp(b),
        (
            Measure::Span {
                duration_us: da,
                outcome: a,
            },
            Measure::Span {
                duration_us: db,
                outcome: b,
            },
        ) => da.cmp(db).then_with(|| a.cmp(b)),
        (
            Measure::Metric {
                name: na,
                value: va,
            },
            Measure::Metric {
                name: nb,
                value: vb,
            },
        ) => na.cmp(nb).then_with(|| va.total_cmp(vb)),
        // Signals of different kinds were already ordered by `kind()`.
        _ => Ordering::Equal,
    }
}
