//! The fixed deployment targets offered to placement decisions.
//!
//! This registry defines candidate identity only. Selection and feasibility
//! belong to the placement decision; no target-specific planner lives here.

use serde::{Deserialize, Serialize};

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
}
