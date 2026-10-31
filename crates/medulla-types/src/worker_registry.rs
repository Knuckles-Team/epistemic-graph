//! Pure worker registry producer for MEDULLA-DURABLE-KERNEL-R074.1.
//! This module does not serve RPCs, authenticate workers, or persist records.

mod grants;
pub use grants::{granted_slots, WorkerCapacityRefusal, WorkerSizingInput, WorkerSlotGrant};

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Worker-supplied capability facts. These do not grant scheduler capacity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct WorkerRegistration {
    pub worker_id: String,
    pub cores: u32,
    pub memory_bytes: u64,
    /// Opaque engine-configured lane IDs; membership is checked by served admission.
    pub lanes_supported: BTreeSet<String>,
}

impl WorkerRegistration {
    pub fn validate(&self) -> Result<(), WorkerRegistryError> {
        if self.worker_id.trim().is_empty() {
            return Err(WorkerRegistryError::EmptyWorkerId);
        }
        if self.cores == 0 {
            return Err(WorkerRegistryError::ZeroCores);
        }
        if self.memory_bytes == 0 {
            return Err(WorkerRegistryError::ZeroMemory);
        }
        if self.lanes_supported.is_empty() {
            return Err(WorkerRegistryError::NoSupportedLanes);
        }
        if self
            .lanes_supported
            .iter()
            .any(|lane| lane.trim().is_empty())
        {
            return Err(WorkerRegistryError::EmptyLaneId);
        }
        Ok(())
    }
}

/// Registry-owned liveness plus the registered capacity facts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct WorkerRecord {
    pub worker_id: String,
    pub cores: u32,
    pub memory_bytes: u64,
    /// Opaque engine-configured lane IDs; membership is checked by served admission.
    pub lanes_supported: BTreeSet<String>,
    pub last_heartbeat_ms: u64,
    pub expires_at_ms: u64,
}

impl WorkerRecord {
    /// Expiry is inclusive: the worker is unavailable at its deadline.
    pub fn is_expired(&self, now_ms: u64) -> bool {
        now_ms >= self.expires_at_ms
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum WorkerRegistryError {
    EmptyWorkerId,
    ZeroCores,
    ZeroMemory,
    NoSupportedLanes,
    EmptyLaneId,
    ZeroTtl,
    DeadlineOverflow,
    AlreadyRegistered,
    UnknownWorker,
    ExpiredWorker,
    ClockRegression,
}

/// Deterministic state transitions; the serving owner supplies its clock and TTL.
#[derive(Clone, Debug, Default)]
pub struct WorkerRegistry {
    records: BTreeMap<String, WorkerRecord>,
}

fn deadline(now_ms: u64, ttl_ms: u64) -> Result<u64, WorkerRegistryError> {
    if ttl_ms == 0 {
        return Err(WorkerRegistryError::ZeroTtl);
    }
    now_ms
        .checked_add(ttl_ms)
        .ok_or(WorkerRegistryError::DeadlineOverflow)
}

impl WorkerRegistry {
    /// A live identity cannot be overwritten; an expired worker may register anew.
    pub fn register(
        &mut self,
        registration: WorkerRegistration,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<WorkerRecord, WorkerRegistryError> {
        registration.validate()?;
        let expires_at_ms = deadline(now_ms, ttl_ms)?;
        if self
            .records
            .get(&registration.worker_id)
            .is_some_and(|record| !record.is_expired(now_ms))
        {
            return Err(WorkerRegistryError::AlreadyRegistered);
        }
        let record = WorkerRecord {
            worker_id: registration.worker_id,
            cores: registration.cores,
            memory_bytes: registration.memory_bytes,
            lanes_supported: registration.lanes_supported,
            last_heartbeat_ms: now_ms,
            expires_at_ms,
        };
        self.records
            .insert(record.worker_id.clone(), record.clone());
        Ok(record)
    }

    /// Refused heartbeats never create a worker or mutate its prior record.
    pub fn heartbeat(
        &mut self,
        worker_id: &str,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<WorkerRecord, WorkerRegistryError> {
        let record = self
            .records
            .get_mut(worker_id)
            .ok_or(WorkerRegistryError::UnknownWorker)?;
        if record.is_expired(now_ms) {
            return Err(WorkerRegistryError::ExpiredWorker);
        }
        if now_ms < record.last_heartbeat_ms {
            return Err(WorkerRegistryError::ClockRegression);
        }
        let expires_at_ms = deadline(now_ms, ttl_ms)?;
        record.last_heartbeat_ms = now_ms;
        record.expires_at_ms = record.expires_at_ms.max(expires_at_ms);
        Ok(record.clone())
    }

    pub fn get_live(&self, worker_id: &str, now_ms: u64) -> Option<&WorkerRecord> {
        self.records
            .get(worker_id)
            .filter(|record| !record.is_expired(now_ms))
    }

    /// Removes expired entries in deterministic worker-id order.
    pub fn expire(&mut self, now_ms: u64) -> Vec<WorkerRecord> {
        let mut expired = Vec::new();
        self.records.retain(|_, record| {
            if record.is_expired(now_ms) {
                expired.push(record.clone());
                false
            } else {
                true
            }
        });
        expired
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registration() -> WorkerRegistration {
        WorkerRegistration {
            worker_id: "worker-a".into(),
            cores: 16,
            memory_bytes: 64 * 1024 * 1024 * 1024,
            lanes_supported: BTreeSet::from([
                "general".into(),
                "hydration".into(),
                "custom-engine-lane".into(),
            ]),
        }
    }

    // spec: MEDULLA-DURABLE-KERNEL-R074.1
    // spec: MEDULLA-PLACEMENT-R002.1
    #[test]
    fn capacity_round_trips_and_heartbeat_extends_expiry() {
        let mut registry = WorkerRegistry::default();
        let first = registry.register(registration(), 100, 50).unwrap();
        let json = serde_json::to_string(&first).unwrap();
        assert_eq!(serde_json::from_str::<WorkerRecord>(&json).unwrap(), first);
        assert_eq!(first.cores, 16);
        assert_eq!(first.memory_bytes, 64 * 1024 * 1024 * 1024);
        assert_eq!(first.lanes_supported, registration().lanes_supported);
        let renewed = registry.heartbeat("worker-a", 140, 50).unwrap();
        assert_eq!(renewed.expires_at_ms, 190);
        assert_eq!(renewed.last_heartbeat_ms, 140);
        assert_eq!(renewed.cores, first.cores);
        assert!(registry.get_live("worker-a", 189).is_some());
        assert!(registry.get_live("worker-a", 190).is_none());
        assert_eq!(registry.expire(190), vec![renewed]);
        assert!(registry.expire(190).is_empty());
    }

    // spec: MEDULLA-DURABLE-KERNEL-R074.1
    #[test]
    fn invalid_registration_never_inserts_a_worker() {
        let mut registry = WorkerRegistry::default();
        let mut invalid = registration();
        invalid.worker_id = " ".into();
        assert_eq!(
            registry.register(invalid, 0, 1),
            Err(WorkerRegistryError::EmptyWorkerId)
        );
        let mut invalid = registration();
        invalid.cores = 0;
        assert_eq!(
            registry.register(invalid, 0, 1),
            Err(WorkerRegistryError::ZeroCores)
        );
        let mut invalid = registration();
        invalid.memory_bytes = 0;
        assert_eq!(
            registry.register(invalid, 0, 1),
            Err(WorkerRegistryError::ZeroMemory)
        );
        let mut invalid = registration();
        invalid.lanes_supported.clear();
        assert_eq!(
            registry.register(invalid, 0, 1),
            Err(WorkerRegistryError::NoSupportedLanes)
        );
        assert_eq!(
            registry.register(registration(), 0, 0),
            Err(WorkerRegistryError::ZeroTtl)
        );
        assert_eq!(
            registry.register(registration(), u64::MAX, 1),
            Err(WorkerRegistryError::DeadlineOverflow)
        );
        let mut invalid = registration();
        invalid.lanes_supported.insert(" ".into());
        assert_eq!(
            registry.register(invalid, 0, 1),
            Err(WorkerRegistryError::EmptyLaneId)
        );
        assert!(registry.records.is_empty());
    }

    // spec: MEDULLA-DURABLE-KERNEL-R074.1
    #[test]
    fn unknown_expired_and_regressing_heartbeats_are_refused_without_mutation() {
        let mut registry = WorkerRegistry::default();
        assert_eq!(
            registry.heartbeat("missing", 0, 1),
            Err(WorkerRegistryError::UnknownWorker)
        );
        let first = registry.register(registration(), 100, 50).unwrap();
        for (now, ttl, error) in [
            (150, 50, WorkerRegistryError::ExpiredWorker),
            (99, 50, WorkerRegistryError::ClockRegression),
            (140, 0, WorkerRegistryError::ZeroTtl),
            (140, u64::MAX, WorkerRegistryError::DeadlineOverflow),
        ] {
            assert_eq!(registry.heartbeat("worker-a", now, ttl), Err(error));
            assert_eq!(registry.records["worker-a"], first);
        }
    }

    // spec: MEDULLA-DURABLE-KERNEL-R074.1
    #[test]
    fn live_duplicate_refused_and_expired_identity_can_reregister() {
        let mut registry = WorkerRegistry::default();
        registry.register(registration(), 100, 50).unwrap();
        assert_eq!(
            registry.register(registration(), 149, 50),
            Err(WorkerRegistryError::AlreadyRegistered)
        );
        assert_eq!(
            registry
                .register(registration(), 150, 50)
                .unwrap()
                .expires_at_ms,
            200
        );
        assert_eq!(
            registry
                .heartbeat("worker-a", 160, 1)
                .unwrap()
                .expires_at_ms,
            200
        );
    }
}
