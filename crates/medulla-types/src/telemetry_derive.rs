//! EH-408 / EH-409 — the receipt of one served telemetry derivation.
//!
//! `Method::TelemetryDerive` reads the caller's stored logs, metrics and spans
//! over one time window, binds them to the ontology individuals of the request
//! graph, derives `BehaviourObservation` / `HealthAnomaly` / `Incident` /
//! `ConformanceViolation` facts, and writes them into that graph in one
//! governed graph write. The receipt counts what was read and derived and names
//! every fact written; the facts themselves carry their provenance in the graph.

use serde::{Deserialize, Serialize};

/// What one derivation read, derived and wrote.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TelemetryDeriveReceipt {
    /// Stored log records, metric samples and spans read in the caller's scope.
    pub signals_read: u64,
    pub observations: u64,
    pub anomalies: u64,
    pub incidents: u64,
    pub violations: u64,
    /// Signals that bound to no single declared individual.
    pub unresolved: u64,
    /// Metric samples of an undeclared metric, or non-finite.
    pub ignored_metric_samples: u64,
    /// Individuals whose declared resolution keys or declared health did not
    /// parse; they were skipped, not guessed.
    pub invalid_declarations: u64,
    pub nodes_written: u64,
    pub edges_written: u64,
    /// The id of every fact node written, in write order.
    pub fact_ids: Vec<String>,
}
