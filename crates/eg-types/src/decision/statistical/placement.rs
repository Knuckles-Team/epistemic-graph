//! The fixed deployment targets offered to placement decisions.
//!
//! This registry defines candidate identity only. Selection and feasibility
//! belong to the placement decision; no target-specific planner lives here.

use serde::{Deserialize, Serialize};

use crate::capacity_lease::LeasePriority;

/// Supported deployment targets. Systemd is not a placement target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PlacementTarget {
    InProcess,
    Compose,
    Helm,
    Swarm,
}

impl PlacementTarget {
    /// Every registered target, in a stable order.
    pub const ALL: [PlacementTarget; 4] = [Self::InProcess, Self::Compose, Self::Helm, Self::Swarm];

    /// The declared-option id a decision candidate for this target carries.
    pub fn option_id(self) -> &'static str {
        match self {
            Self::InProcess => "placement:in_process",
            Self::Compose => "placement:compose",
            Self::Helm => "placement:helm",
            Self::Swarm => "placement:swarm",
        }
    }

    /// The target named by a declared-option id, or `None` outside the registry.
    pub fn from_option_id(option_id: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|target| target.option_id() == option_id)
    }
}

/// Resource demand for one replica of the workload to place.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PlacementSubject {
    pub workload_iri: String,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub replicas: u32,
    pub needs_persistent_volume: bool,
}

/// Request-side constraints; feasibility and policy tightening are evaluated separately.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PlacementConstraints {
    pub requires_ha: bool,
    pub forbidden_targets: Vec<PlacementTarget>,
    pub max_relative_cost: Option<u64>,
    pub capacity_cells: Vec<String>,
    pub priority: LeasePriority,
}

/// Typed, evaluate-only placement input. Checking it never acquires capacity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PlacementRequestBody {
    pub subject: PlacementSubject,
    pub constraints: PlacementConstraints,
    /// Declared-option ids, validated against the closed target registry.
    pub candidate_targets: Vec<String>,
}

impl PlacementRequestBody {
    /// Refuse malformed candidates and non-positive replica/resource demand.
    pub fn check(&self) -> Result<(), String> {
        if self.candidate_targets.is_empty() {
            return Err("EMPTY_PLACEMENT_CANDIDATES: at least one target is required".into());
        }
        let mut seen = std::collections::BTreeSet::new();
        for option_id in &self.candidate_targets {
            if PlacementTarget::from_option_id(option_id).is_none() {
                return Err(format!("UNSUPPORTED_PLACEMENT_TARGET: '{option_id}'"));
            }
            if !seen.insert(option_id) {
                return Err(format!("DUPLICATE_PLACEMENT_TARGET: '{option_id}'"));
            }
        }
        if self.subject.replicas == 0 {
            return Err("ZERO_PLACEMENT_REPLICAS: at least one replica is required".into());
        }
        if self.subject.cpu_millis == 0 || self.subject.memory_bytes == 0 {
            return Err("ZERO_PLACEMENT_DEMAND: CPU and memory demand must be positive".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: EG-PLACEMENT-R001.3
    #[test]
    fn every_target_round_trips_through_its_stable_option_id() {
        let expected = [
            (PlacementTarget::InProcess, "placement:in_process"),
            (PlacementTarget::Compose, "placement:compose"),
            (PlacementTarget::Helm, "placement:helm"),
            (PlacementTarget::Swarm, "placement:swarm"),
        ];
        assert_eq!(PlacementTarget::ALL, expected.map(|(target, _)| target));
        for (target, option_id) in expected {
            assert_eq!(target.option_id(), option_id);
            assert_eq!(PlacementTarget::from_option_id(option_id), Some(target));
        }
    }

    // spec: EG-PLACEMENT-R001.3
    #[test]
    fn systemd_and_other_unregistered_option_ids_are_refused() {
        for option_id in [
            "placement:systemd",
            "placement:unknown",
            "placement:InProcess",
            "placement:in-process",
            "compose",
            "placement:compose ",
            "",
        ] {
            assert_eq!(PlacementTarget::from_option_id(option_id), None);
        }
    }

    // spec: EG-PLACEMENT-R001.3
    #[test]
    fn target_wire_names_round_trip_and_systemd_is_refused() {
        for (target, name) in
            PlacementTarget::ALL
                .into_iter()
                .zip(["in_process", "compose", "helm", "swarm"])
        {
            let json = format!("\"{name}\"");
            assert_eq!(serde_json::to_string(&target).unwrap(), json);
            assert_eq!(
                serde_json::from_str::<PlacementTarget>(&json).unwrap(),
                target
            );
        }
        assert!(serde_json::from_str::<PlacementTarget>("\"systemd\"").is_err());
    }

    fn accepted_body() -> PlacementRequestBody {
        PlacementRequestBody {
            subject: PlacementSubject {
                workload_iri: "eg:graphos-core".into(),
                cpu_millis: 2000,
                memory_bytes: 4 * 1024 * 1024 * 1024,
                replicas: 2,
                needs_persistent_volume: false,
            },
            constraints: PlacementConstraints {
                requires_ha: true,
                forbidden_targets: vec![PlacementTarget::InProcess],
                max_relative_cost: Some(3),
                capacity_cells: vec!["cell-a".into()],
                priority: LeasePriority::Orchestration,
            },
            candidate_targets: PlacementTarget::ALL
                .into_iter()
                .map(|target| target.option_id().to_owned())
                .collect(),
        }
    }

    // spec: EG-PLACEMENT-R001.4
    #[test]
    fn a_valid_placement_body_is_accepted_and_round_trips() {
        let body = accepted_body();
        assert_eq!(body.check(), Ok(()));
        let json = serde_json::to_string(&body).unwrap();
        let decoded: PlacementRequestBody = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, body);
        assert_eq!(decoded.check(), Ok(()));
    }

    // spec: EG-PLACEMENT-R001.4
    #[test]
    fn empty_candidates_are_refused() {
        let mut body = accepted_body();
        body.candidate_targets.clear();
        assert!(body
            .check()
            .unwrap_err()
            .starts_with("EMPTY_PLACEMENT_CANDIDATES:"));
    }

    // spec: EG-PLACEMENT-R001.4
    #[test]
    fn duplicate_candidates_are_refused() {
        let mut body = accepted_body();
        body.candidate_targets
            .push(body.candidate_targets[0].clone());
        assert_eq!(
            body.check().unwrap_err(),
            "DUPLICATE_PLACEMENT_TARGET: 'placement:in_process'"
        );
    }

    // spec: EG-PLACEMENT-R001.4
    #[test]
    fn unknown_candidate_option_ids_are_refused() {
        for option_id in ["placement:systemd", "placement:unknown", "compose", ""] {
            let mut body = accepted_body();
            body.candidate_targets.push(option_id.into());
            assert_eq!(
                body.check().unwrap_err(),
                format!("UNSUPPORTED_PLACEMENT_TARGET: '{option_id}'")
            );
        }
    }

    // spec: EG-PLACEMENT-R001.4
    #[test]
    fn zero_replicas_are_refused() {
        let mut body = accepted_body();
        body.subject.replicas = 0;
        assert!(body
            .check()
            .unwrap_err()
            .starts_with("ZERO_PLACEMENT_REPLICAS:"));
    }

    // spec: EG-PLACEMENT-R001.4
    #[test]
    fn zero_cpu_demand_is_refused() {
        let mut body = accepted_body();
        body.subject.cpu_millis = 0;
        assert!(body
            .check()
            .unwrap_err()
            .starts_with("ZERO_PLACEMENT_DEMAND:"));
    }

    // spec: EG-PLACEMENT-R001.4
    #[test]
    fn zero_memory_demand_is_refused() {
        let mut body = accepted_body();
        body.subject.memory_bytes = 0;
        assert!(body
            .check()
            .unwrap_err()
            .starts_with("ZERO_PLACEMENT_DEMAND:"));
    }

    // spec: EG-PLACEMENT-R001.4
    #[test]
    fn unknown_body_subject_and_constraint_fields_are_refused() {
        for path in ["", "subject", "constraints"] {
            let mut json = serde_json::to_value(accepted_body()).unwrap();
            let object = if path.is_empty() {
                &mut json
            } else {
                &mut json[path]
            };
            object["unexpected"] = serde_json::json!(true);
            assert!(serde_json::from_value::<PlacementRequestBody>(json).is_err());
        }
    }
}
