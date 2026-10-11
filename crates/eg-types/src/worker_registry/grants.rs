//! Pure capacity sizing. Served admission, lane selection and reservations remain R075.

use super::WorkerRecord;
use crate::scheduler_config::{InvalidSchedulerConfig, SchedulerConfig};
use std::num::NonZeroU32;

/// Per-request facts, not another scheduler policy. The engine supplies current
/// available memory and classifies the selected lane; opaque lane names alone
/// cannot identify a writer. The caller's optional ceiling can only lower a grant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkerSizingInput {
    pub request_ceiling: Option<u32>,
    pub available_memory_bytes: u64,
    pub is_write_lane: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerCapacityRefusal {
    InvalidConfig(InvalidSchedulerConfig),
    NoCores,
    CpuShareDisabled,
    PoolDisabled,
    QueueDisabled,
    InsufficientMemory,
    RequestCeilingZero,
    WriterWidthZero,
}

/// A refusal always exposes zero slots; a grant is necessarily nonzero.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerSlotGrant {
    Granted(NonZeroU32),
    Refused(WorkerCapacityRefusal),
}

impl WorkerSlotGrant {
    pub fn slots(&self) -> u32 {
        match self {
            Self::Granted(slots) => slots.get(),
            Self::Refused(_) => 0,
        }
    }
}

/// Size without allocating, reserving lanes, or admitting a task. The CPU share
/// rounds down, with a floor of one only for a positive share and host core count.
/// Every other hard bound is applied *after* that floor, including live available
/// memory, registered memory, the caller ceiling, pool size and write-lane width.
pub fn granted_slots(
    record: &WorkerRecord,
    config: &SchedulerConfig,
    input: WorkerSizingInput,
    shard_writer_width: u32,
) -> WorkerSlotGrant {
    use WorkerCapacityRefusal::*;
    if let Err(error) = config.validate() {
        return WorkerSlotGrant::Refused(InvalidConfig(error));
    }
    let memory_slots =
        record.memory_bytes.min(input.available_memory_bytes) / config.memory_per_worker_bytes;
    let limits = [
        (u64::from(record.cores), NoCores),
        (u64::from(config.cores_per_mille), CpuShareDisabled),
        (u64::from(config.total_workers), PoolDisabled),
        (config.max_queue_depth, QueueDisabled),
        (memory_slots, InsufficientMemory),
        (
            u64::from(input.request_ceiling.unwrap_or(u32::MAX)),
            RequestCeilingZero,
        ),
        (
            u64::from(if input.is_write_lane {
                shard_writer_width
            } else {
                u32::MAX
            }),
            WriterWidthZero,
        ),
    ];
    if let Some((_, reason)) = limits.iter().find(|(limit, _)| *limit == 0) {
        return WorkerSlotGrant::Refused(reason.clone());
    }
    let core_slots = (u64::from(record.cores) * u64::from(config.cores_per_mille) / 1000).max(1);
    let slots = core_slots
        .min(u64::from(config.total_workers))
        .min(memory_slots)
        .min(limits[5].0)
        .min(limits[6].0) as u32;
    // All bounds above are positive and at most u32::MAX.
    WorkerSlotGrant::Granted(NonZeroU32::new(slots).expect("positive checked capacity bounds"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (WorkerRecord, SchedulerConfig, WorkerSizingInput) {
        let record = WorkerRecord {
            worker_id: "worker".into(),
            cores: 16,
            memory_bytes: 64 * 1024 * 1024 * 1024,
            lanes_supported: ["general".into()].into(),
            last_heartbeat_ms: 0,
            expires_at_ms: 100,
        };
        let config =
            serde_json::from_str(r#"{"total_workers":100,"max_queue_depth":100}"#).unwrap();
        let input = WorkerSizingInput {
            request_ceiling: None,
            available_memory_bytes: record.memory_bytes,
            is_write_lane: false,
        };
        (record, config, input)
    }

    // spec: EG-PLACEMENT-R002.3
    #[test]
    fn each_hard_bound_limits_the_grant() {
        let (mut record, mut config, mut input) = fixture();
        assert_eq!(granted_slots(&record, &config, input, 0).slots(), 5); // core-bound
        record.memory_bytes = config.memory_per_worker_bytes * 2;
        assert_eq!(granted_slots(&record, &config, input, 0).slots(), 2);
        record.memory_bytes = u64::MAX;
        input.available_memory_bytes = config.memory_per_worker_bytes * 3;
        assert_eq!(granted_slots(&record, &config, input, 0).slots(), 3);
        input.request_ceiling = Some(2);
        assert_eq!(granted_slots(&record, &config, input, 9).slots(), 2);
        input.is_write_lane = true;
        assert_eq!(granted_slots(&record, &config, input, 1).slots(), 1);
        input.request_ceiling = None;
        config.total_workers = 2;
        assert_eq!(granted_slots(&record, &config, input, 9).slots(), 2);
    }

    // spec: EG-PLACEMENT-R002.3
    #[test]
    fn floor_one_never_overrides_a_zero_hard_limit() {
        use WorkerCapacityRefusal::*;
        let (mut record, config, input) = fixture();
        record.cores = 1;
        assert_eq!(granted_slots(&record, &config, input, 1).slots(), 1);
        let cases = [
            (
                WorkerSizingInput {
                    available_memory_bytes: 0,
                    ..input
                },
                1,
                InsufficientMemory,
            ),
            (
                WorkerSizingInput {
                    available_memory_bytes: config.memory_per_worker_bytes - 1,
                    ..input
                },
                1,
                InsufficientMemory,
            ),
            (
                WorkerSizingInput {
                    request_ceiling: Some(0),
                    ..input
                },
                1,
                RequestCeilingZero,
            ),
            (
                WorkerSizingInput {
                    is_write_lane: true,
                    ..input
                },
                0,
                WriterWidthZero,
            ),
        ];
        for (input, width, reason) in cases {
            let grant = granted_slots(&record, &config, input, width);
            assert_eq!(grant.slots(), 0);
            assert_eq!(grant, WorkerSlotGrant::Refused(reason));
        }
        record.memory_bytes = 0;
        assert_eq!(
            granted_slots(&record, &config, input, 1),
            WorkerSlotGrant::Refused(InsufficientMemory)
        );
    }

    // spec: EG-PLACEMENT-R002.3
    #[test]
    fn disabled_resources_invalid_policy_and_large_values_are_safe() {
        use WorkerCapacityRefusal::*;
        let (mut record, mut config, input) = fixture();
        record.cores = 0;
        assert_eq!(
            granted_slots(&record, &config, input, 1),
            WorkerSlotGrant::Refused(NoCores)
        );
        record.cores = u32::MAX;
        for (cpu, pool, depth, reason) in [
            (0, 1, 1, CpuShareDisabled),
            (360, 0, 1, PoolDisabled),
            (360, 1, 0, QueueDisabled),
        ] {
            config.cores_per_mille = cpu;
            config.total_workers = pool;
            config.max_queue_depth = depth;
            assert_eq!(
                granted_slots(&record, &config, input, 1),
                WorkerSlotGrant::Refused(reason)
            );
        }
        config.cores_per_mille = 1000;
        config.total_workers = u32::MAX;
        config.max_queue_depth = u64::MAX;
        config.memory_per_worker_bytes = 1;
        record.memory_bytes = u64::MAX;
        let input = WorkerSizingInput {
            available_memory_bytes: u64::MAX,
            ..input
        };
        assert_eq!(granted_slots(&record, &config, input, 0).slots(), u32::MAX);
        config.memory_per_worker_bytes = 0;
        assert_eq!(
            granted_slots(&record, &config, input, 0),
            WorkerSlotGrant::Refused(InvalidConfig(InvalidSchedulerConfig::ZeroMemoryPerWorker))
        );
    }
}
