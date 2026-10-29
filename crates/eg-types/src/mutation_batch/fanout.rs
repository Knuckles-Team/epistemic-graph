//! Per-sub-batch shard fan-out report (MEDULLA-DURABLE-KERNEL-R099.1).
//!
//! A cross-graph request carries at most [`MAX_FANOUT_SUB_BATCHES`] sub-batches.
//! Each sub-batch reports the shard writer it queued on, its queue and commit
//! times, and its own outcome. The report adds no cross-graph atomicity
//! (MEDULLA-DURABLE-KERNEL-R084.7 holds that decision).

use serde::{Deserialize, Serialize};

/// The largest number of sub-batches one fan-out request may carry.
pub const MAX_FANOUT_SUB_BATCHES: usize = 256;

/// One sub-batch's terminal outcome.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum ShardFanoutOutcome {
    Committed,
    /// The sub-batch failed with its own stable error code.
    Failed {
        code: String,
        message: String,
    },
}

/// The fan-out evidence for one sub-batch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardFanoutReport {
    pub graph: String,
    pub shard_index: u32,
    pub queued_us: u64,
    pub commit_us: u64,
    pub outcome: ShardFanoutOutcome,
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: MEDULLA-DURABLE-KERNEL-R099.1
    #[test]
    fn report_round_trips() {
        let reports = vec![
            ShardFanoutReport {
                graph: "tenant-a/orders".into(),
                shard_index: 3,
                queued_us: 41,
                commit_us: 1_250,
                outcome: ShardFanoutOutcome::Committed,
            },
            ShardFanoutReport {
                graph: "tenant-a/stock".into(),
                shard_index: 0,
                queued_us: u64::MAX,
                commit_us: 0,
                outcome: ShardFanoutOutcome::Failed {
                    code: "WRITE_CONFLICT".into(),
                    message: "expected version 4, found 5".into(),
                },
            },
        ];
        let json = serde_json::to_vec(&reports).unwrap();
        assert_eq!(
            serde_json::from_slice::<Vec<ShardFanoutReport>>(&json).unwrap(),
            reports
        );
        let packed = rmp_serde::to_vec_named(&reports).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<Vec<ShardFanoutReport>>(&packed).unwrap(),
            reports
        );
        assert_eq!(MAX_FANOUT_SUB_BATCHES, 256);
        let unknown = serde_json::json!({
            "graph": "g", "shard_index": 1, "queued_us": 1, "commit_us": 1,
            "outcome": {"status": "committed"}, "extra": true
        });
        assert!(serde_json::from_value::<ShardFanoutReport>(unknown).is_err());
    }
}
