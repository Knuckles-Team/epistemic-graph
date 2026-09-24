//! Telemetry as evidence about ontology entities (EH-408 / EH-409).
//!
//! EG already stores logs, metrics and traces and already runs CEP. This module
//! is the missing binding between them and the ontology:
//!
//! 1. [`signal`] — every stored log record, Prometheus sample and span is
//!    normalised into a [`TelemetrySignal`] (time, attributes, measure).
//! 2. [`binding`] — declared [`ResolutionRule`]s bind each signal to the
//!    Server / Service / Host / Deployment / Agent individual it came from, or
//!    report it [`Unresolved`] with the reason; nothing is guessed.
//! 3. [`rollup`] — bound signals roll up per entity and window into
//!    [`BehaviourObservation`]s with provenance.
//! 4. [`derive`] — declared CEP patterns over the rollups yield
//!    [`HealthAnomaly`] facts; declared CEP patterns over the anomalies yield
//!    [`Incident`] facts.
//! 5. [`conformance`] — declared health versus observed behaviour yields
//!    [`ConformanceViolation`] facts.
//! 6. [`facts`] — every fact projects onto graph nodes and edges typed by the
//!    infrastructure ontology, linked to its entity and to what it was derived
//!    from.
//!
//! [`pipeline::derive_facts`] runs 2–5 in one pass. The whole module is synchronous, pure
//! and deterministic: the same signals, entities, declarations and policy
//! always produce the same facts with the same ids.

pub mod binding;
pub mod conformance;
pub mod derive;
pub mod facts;
pub mod pipeline;
pub mod rollup;
pub mod signal;

#[cfg(test)]
mod tests;

pub use binding::{
    DeclaredEntity, EntityClass, EntityDirectory, EntityRef, Resolution, ResolutionPolicy,
    ResolutionRule, Unresolved,
};
pub use conformance::{
    check_conformance, ConformanceViolation, DeclaredHealth, DeclaredState, ViolationKind,
};
pub use derive::{
    correlate_incidents, detect_anomalies, AnomalyRule, HealthAnomaly, Incident, IncidentRule,
};
pub use facts::{fact_graph, FactEdge, FactGraph, FactNode};
pub use pipeline::{derive_facts, Declarations, TelemetryFacts, TelemetryPolicy};
pub use rollup::{
    rollup, BehaviourObservation, MetricRole, ObservationProvenance, RollupPolicy, RollupReport,
    UnresolvedSignal,
};
pub use signal::{Measure, Outcome, SignalKind, TelemetrySignal};
