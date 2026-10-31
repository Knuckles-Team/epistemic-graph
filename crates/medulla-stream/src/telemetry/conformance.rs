//! Declared-versus-observed conformance (EH-409).
//!
//! The architecture declares how an entity should behave — a service is
//! declared healthy within an error-ratio and latency envelope, or declared
//! retired. The rollups say how it did behave. Every observation that
//! contradicts its entity's declaration is one [`ConformanceViolation`] fact,
//! citing both sides: the declaration it breaks (and where that was declared)
//! and the observation that breaks it.
//!
//! The absence of telemetry is never a violation: silence is not evidence that
//! a declared-healthy entity is unhealthy. Declare an `Absence` CEP rule for
//! that case instead.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::binding::EntityRef;
use super::rollup::BehaviourObservation;

/// What the architecture declares about an entity's behaviour.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "health", rename_all = "snake_case")]
pub enum DeclaredHealth {
    /// In service, within this envelope.
    Healthy {
        max_error_ratio: f64,
        #[serde(default)]
        max_latency_p95_ms: Option<f64>,
    },
    /// Decommissioned: it should serve nothing.
    Retired,
}

/// One declaration and where it came from.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DeclaredState {
    pub entity: EntityRef,
    pub health: DeclaredHealth,
    /// The declaring node or document (a manifest, a CMDB record, …).
    pub declared_by: String,
}

/// How an observation contradicts its declaration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ViolationKind {
    /// Declared healthy, observed over its error-ratio ceiling.
    ErroringWhileDeclaredHealthy,
    /// Declared healthy, observed over its p95 latency ceiling.
    SlowWhileDeclaredHealthy,
    /// Declared retired, observed serving requests.
    ActiveWhileDeclaredRetired,
}

impl ViolationKind {
    pub fn label(self) -> &'static str {
        match self {
            ViolationKind::ErroringWhileDeclaredHealthy => "erroring_while_declared_healthy",
            ViolationKind::SlowWhileDeclaredHealthy => "slow_while_declared_healthy",
            ViolationKind::ActiveWhileDeclaredRetired => "active_while_declared_retired",
        }
    }
}

/// One observation contradicting one declaration.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ConformanceViolation {
    pub id: String,
    pub kind: ViolationKind,
    pub entity: EntityRef,
    pub declared: DeclaredHealth,
    pub declared_by: String,
    pub observation: String,
    pub window_start_ms: u64,
    pub window_end_ms: u64,
    pub observed_error_ratio: f64,
    pub observed_latency_p95_ms: Option<f64>,
}

/// Every contradiction between `declared` and `observations`, ordered by
/// observation then kind. An entity with several declarations is checked
/// against each.
pub fn check_conformance(
    declared: &[DeclaredState],
    observations: &[BehaviourObservation],
) -> Vec<ConformanceViolation> {
    let mut by_entity: BTreeMap<&EntityRef, Vec<&DeclaredState>> = BTreeMap::new();
    for state in declared {
        by_entity.entry(&state.entity).or_default().push(state);
    }
    let mut violations = Vec::new();
    for observation in observations {
        for state in by_entity.get(&observation.entity).into_iter().flatten() {
            for kind in contradictions(&state.health, observation) {
                violations.push(violation(kind, state, observation));
            }
        }
    }
    violations
}

/// The ways one observation contradicts one declaration.
fn contradictions(
    health: &DeclaredHealth,
    observation: &BehaviourObservation,
) -> Vec<ViolationKind> {
    match health {
        DeclaredHealth::Healthy {
            max_error_ratio,
            max_latency_p95_ms,
        } => {
            let slow = match (max_latency_p95_ms, observation.latency_p95_ms) {
                (Some(ceiling), Some(observed)) => observed > *ceiling,
                _ => false,
            };
            [
                (
                    observation.error_ratio > *max_error_ratio,
                    ViolationKind::ErroringWhileDeclaredHealthy,
                ),
                (slow, ViolationKind::SlowWhileDeclaredHealthy),
            ]
            .into_iter()
            .filter_map(|(broken, kind)| broken.then_some(kind))
            .collect()
        }
        DeclaredHealth::Retired if observation.requests > 0.0 => {
            vec![ViolationKind::ActiveWhileDeclaredRetired]
        }
        DeclaredHealth::Retired => Vec::new(),
    }
}

fn violation(
    kind: ViolationKind,
    state: &DeclaredState,
    observation: &BehaviourObservation,
) -> ConformanceViolation {
    ConformanceViolation {
        id: format!(
            "conformance:{}:{}:{}",
            kind.label(),
            observation.id,
            state.declared_by
        ),
        kind,
        entity: observation.entity.clone(),
        declared: state.health.clone(),
        declared_by: state.declared_by.clone(),
        observation: observation.id.clone(),
        window_start_ms: observation.window_start_ms,
        window_end_ms: observation.window_end_ms,
        observed_error_ratio: observation.error_ratio,
        observed_latency_p95_ms: observation.latency_p95_ms,
    }
}
