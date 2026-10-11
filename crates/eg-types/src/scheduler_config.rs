//! Authoritative scheduler policy data (EG-DURABLE-KERNEL-R076.1).
//!
//! The served setting, admission wiring and persistence are R076.2. Consumers
//! must validate this model before use; workers never choose their own policy.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Default fraction of host cores, in thousandths.
pub const DEFAULT_CORES_PER_MILLE: u16 = 360;
/// Default memory budget per worker: exactly three binary gibibytes.
pub const DEFAULT_MEMORY_PER_WORKER_BYTES: u64 = 3 * 1024 * 1024 * 1024;

fn default_cores_per_mille() -> u16 {
    DEFAULT_CORES_PER_MILLE
}

fn default_memory_per_worker_bytes() -> u64 {
    DEFAULT_MEMORY_PER_WORKER_BYTES
}

/// Engine-owned policy. Pool size and queue bound must be configured explicitly.
/// Zero workers, zero CPU share, or zero queue depth can disable that resource;
/// no implicit minimum may override those limits. Lane IDs are engine-defined
/// worker lanes, not the decision engine's fast/medium/slow ingestion lanes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SchedulerConfig {
    pub total_workers: u32,
    #[serde(default)]
    pub lane_floors: BTreeMap<String, u32>,
    #[serde(default = "default_cores_per_mille")]
    pub cores_per_mille: u16,
    #[serde(default = "default_memory_per_worker_bytes")]
    pub memory_per_worker_bytes: u64,
    pub max_queue_depth: u64,
}

/// Invalid scheduler policy, refused before it can govern admission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InvalidSchedulerConfig {
    BlankLane,
    CoresPerMilleOutOfRange,
    ZeroMemoryPerWorker,
    OversubscribedFloors { total_workers: u32 },
}

impl std::fmt::Display for InvalidSchedulerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BlankLane => write!(f, "scheduler lane must not be blank"),
            Self::CoresPerMilleOutOfRange => {
                write!(f, "scheduler cores_per_mille must be at most 1000")
            }
            Self::ZeroMemoryPerWorker => {
                write!(f, "scheduler memory_per_worker_bytes must be nonzero")
            }
            Self::OversubscribedFloors { total_workers } => {
                write!(
                    f,
                    "scheduler lane floors exceed total workers {total_workers}"
                )
            }
        }
    }
}

impl std::error::Error for InvalidSchedulerConfig {}

impl SchedulerConfig {
    /// Validate all policy bounds without allocating workers or changing state.
    pub fn validate(&self) -> Result<(), InvalidSchedulerConfig> {
        if self.cores_per_mille > 1000 {
            return Err(InvalidSchedulerConfig::CoresPerMilleOutOfRange);
        }
        if self.memory_per_worker_bytes == 0 {
            return Err(InvalidSchedulerConfig::ZeroMemoryPerWorker);
        }
        let mut available = self.total_workers;
        for (lane, floor) in &self.lane_floors {
            if lane.trim().is_empty() {
                return Err(InvalidSchedulerConfig::BlankLane);
            }
            // Subtract rather than sum: even u32::MAX floors cannot wrap.
            available = available.checked_sub(*floor).ok_or(
                InvalidSchedulerConfig::OversubscribedFloors {
                    total_workers: self.total_workers,
                },
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn config() -> SchedulerConfig {
        serde_json::from_value(json!({
            "total_workers": 4,
            "lane_floors": {"general": 1, "hydration": 2},
            "max_queue_depth": 100
        }))
        .unwrap()
    }

    // spec: EG-PLACEMENT-R002.2
    // spec: EG-DURABLE-KERNEL-R076.1
    #[test]
    fn defaults_and_lane_floors_round_trip() {
        let config = config();
        assert_eq!(config.cores_per_mille, 360);
        assert_eq!(config.memory_per_worker_bytes, 3_221_225_472);
        assert_eq!(config.validate(), Ok(()));
        let encoded = serde_json::to_string(&config).unwrap();
        assert_eq!(
            serde_json::from_str::<SchedulerConfig>(&encoded).unwrap(),
            config
        );
    }

    // spec: EG-PLACEMENT-R002.2
    // spec: EG-DURABLE-KERNEL-R076.1
    #[test]
    fn explicit_autosize_and_queue_limits_round_trip() {
        let mut config = config();
        config.cores_per_mille = 1000;
        config.memory_per_worker_bytes = 1;
        config.max_queue_depth = u64::MAX;
        assert_eq!(config.validate(), Ok(()));
        let encoded = serde_json::to_value(&config).unwrap();
        assert_eq!(
            serde_json::from_value::<SchedulerConfig>(encoded).unwrap(),
            config
        );
    }

    // spec: EG-DURABLE-KERNEL-R076.1
    #[test]
    fn exact_floor_sum_is_accepted_and_oversubscription_is_refused() {
        let mut config = config();
        config.total_workers = 3;
        assert_eq!(config.validate(), Ok(()));
        config.total_workers = 2;
        assert_eq!(
            config.validate(),
            Err(InvalidSchedulerConfig::OversubscribedFloors { total_workers: 2 })
        );
    }

    // spec: EG-DURABLE-KERNEL-R076.1
    #[test]
    fn huge_floors_cannot_wrap_into_an_accepted_sum() {
        let mut config = config();
        config.total_workers = u32::MAX;
        config.lane_floors = BTreeMap::from([("a".into(), u32::MAX), ("b".into(), 1)]);
        assert_eq!(
            config.validate(),
            Err(InvalidSchedulerConfig::OversubscribedFloors {
                total_workers: u32::MAX
            })
        );
    }

    // spec: EG-DURABLE-KERNEL-R076.1
    #[test]
    fn zero_limits_disable_resources_without_an_implicit_floor() {
        let config: SchedulerConfig = serde_json::from_value(json!({
            "total_workers": 0, "cores_per_mille": 0, "max_queue_depth": 0
        }))
        .unwrap();
        assert!(config.lane_floors.is_empty());
        assert_eq!(config.validate(), Ok(()));
    }

    // spec: EG-DURABLE-KERNEL-R076.1
    #[test]
    fn invalid_policy_bounds_are_refused() {
        let mut config = config();
        config.cores_per_mille = 1001;
        assert_eq!(
            config.validate(),
            Err(InvalidSchedulerConfig::CoresPerMilleOutOfRange)
        );
        config.cores_per_mille = 360;
        config.memory_per_worker_bytes = 0;
        assert_eq!(
            config.validate(),
            Err(InvalidSchedulerConfig::ZeroMemoryPerWorker)
        );
        config.memory_per_worker_bytes = 1;
        config.lane_floors.insert("  ".into(), 0);
        assert_eq!(config.validate(), Err(InvalidSchedulerConfig::BlankLane));
    }

    // spec: EG-DURABLE-KERNEL-R076.1
    #[test]
    fn wire_requires_explicit_limits_and_rejects_unknown_or_negative_values() {
        for value in [
            json!({"total_workers": 1}),
            json!({"max_queue_depth": 1}),
            json!({"total_workers": 1, "max_queue_depth": 1, "worker_policy": true}),
            json!({"total_workers": -1, "max_queue_depth": 1}),
            json!({"total_workers": 1, "max_queue_depth": -1}),
            json!({"total_workers": 1, "max_queue_depth": 1, "cores_per_mille": 65536}),
        ] {
            assert!(serde_json::from_value::<SchedulerConfig>(value).is_err());
        }
    }
}
