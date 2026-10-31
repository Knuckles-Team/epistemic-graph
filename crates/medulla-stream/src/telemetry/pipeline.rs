//! One pass from telemetry to facts (EH-408 / EH-409): bind, roll up, detect,
//! correlate, check conformance.

use serde::{Deserialize, Serialize};

use super::binding::{Aggregation, DeclaredEntity, EntityDirectory, ResolutionPolicy};
use super::conformance::{check_conformance, ConformanceViolation, DeclaredState};
use super::derive::{
    correlate_incidents, detect_anomalies, AnomalyRule, HealthAnomaly, Incident, IncidentRule,
};
use super::rollup::{rollup, BehaviourObservation, RollupPolicy, UnresolvedSignal};
use super::signal::TelemetrySignal;

/// Everything declared about how telemetry becomes facts.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TelemetryPolicy {
    pub resolution: ResolutionPolicy,
    pub rollup: RollupPolicy,
    #[serde(default)]
    pub anomalies: Vec<AnomalyRule>,
    #[serde(default)]
    pub incidents: Vec<IncidentRule>,
}

/// What the architecture declares: the individuals and their resolution keys,
/// and how each is declared to behave.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Declarations {
    pub entities: Vec<DeclaredEntity>,
    #[serde(default)]
    pub health: Vec<DeclaredState>,
    /// Part-of relations behaviour also rolls up along (a Pod → its Workload).
    #[serde(default)]
    pub aggregations: Vec<Aggregation>,
}

/// Every fact derived from one batch of telemetry.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TelemetryFacts {
    pub observations: Vec<BehaviourObservation>,
    pub anomalies: Vec<HealthAnomaly>,
    pub incidents: Vec<Incident>,
    pub violations: Vec<ConformanceViolation>,
    /// Signals that did not bind, with the reason — reported, never dropped silently.
    pub unresolved: Vec<UnresolvedSignal>,
    pub ignored_metric_samples: u64,
}

/// Bind, roll up, detect, correlate and check conformance in one pass.
pub fn derive_facts(
    policy: &TelemetryPolicy,
    declarations: &Declarations,
    signals: &[TelemetrySignal],
) -> TelemetryFacts {
    let directory = EntityDirectory::build(&policy.resolution, &declarations.entities)
        .with_aggregations(&declarations.aggregations);
    let report = rollup(&policy.rollup, &directory, signals);
    let anomalies = detect_anomalies(&policy.anomalies, &report.observations);
    let incidents = correlate_incidents(&policy.incidents, &anomalies);
    let violations = check_conformance(&declarations.health, &report.observations);
    TelemetryFacts {
        observations: report.observations,
        anomalies,
        incidents,
        violations,
        unresolved: report.unresolved,
        ignored_metric_samples: report.ignored_metric_samples,
    }
}
