//! EH-406 over a real shard: the throttle step is durable, narrows only the
//! ceiling, leaves the operator's declared capacity alone, and counts each
//! window once.

use eg_types::capacity_lease::{CapacityCell, CapacityResourceClass};
use eg_types::capacity_throttle::{
    CapacityThrottle, CapacityThrottlePolicy, ErrorBudgetSample, ThrottleAction,
};
use eg_types::native_control::{
    CapacityCellUpdateRequest, CapacityStatusRequest, CapacityThrottleRequest,
    CapacityThrottleResult, NativeControlSchemaVersion,
};

use super::super::super::shard::Shard;
use super::super::super::{decode_durable, DurableCrypto};
use crate::protocol::Method;

const GRAPH: &str = "graph-throttle";

struct Harness {
    path: std::path::PathBuf,
    shard: Shard,
    #[cfg(feature = "security")]
    tail: super::super::super::AuditTailCache,
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

impl Harness {
    fn open(tag: &str) -> Self {
        let path = crate::redb_store::temp_path("eg-capacity-throttle", tag);
        let shard = Shard::open(&path).expect("temp shard opens");
        Self {
            path,
            shard,
            #[cfg(feature = "security")]
            tail: super::super::super::AuditTailCache::new(),
        }
    }

    fn commit(&mut self, method: &Method) -> Result<Vec<u8>, String> {
        #[cfg(feature = "security")]
        return super::super::commit(
            &self.shard,
            GRAPH,
            method,
            DurableCrypto::none(),
            &mut self.tail,
        );
        #[cfg(not(feature = "security"))]
        super::super::commit(&self.shard, GRAPH, method, DurableCrypto::none())
    }

    fn cell(&self, cell_id: &str) -> CapacityCell {
        let request = CapacityStatusRequest {
            schema_version: NativeControlSchemaVersion::V1,
            tenant_ref: "tenant-a".to_string(),
            cell_id: Some(cell_id.to_string()),
            lease_id: None,
            max_count: 1,
            cursor: None,
        };
        let status =
            super::super::read(&self.shard, GRAPH, &request, DurableCrypto::none()).unwrap();
        status.cells.into_iter().next().expect("cell exists")
    }
}

fn declared_cell(cell_id: &str, throttled: bool) -> CapacityCell {
    let policy = CapacityThrottlePolicy {
        error_budget_ppm: 50_000,
        recovery_ppm: 10_000,
        min_samples: 100,
        decrease_per_mille: 500,
        increase_step: 4,
        floor: 2,
        cooldown_ms: 1_000,
    };
    CapacityCell {
        cell_id: cell_id.to_string(),
        parent_id: None,
        resource_class: CapacityResourceClass::LlmGenerator,
        capacity: 64,
        reserved_floor: 8,
        epoch: 1,
        policy_digest: "d".repeat(64),
        updated_at_ms: 0,
        throttle: throttled.then(|| CapacityThrottle::open(policy, 64)),
    }
}

fn declare(harness: &mut Harness, cell: CapacityCell) {
    let method = Method::UpdateCapacityCell {
        request: CapacityCellUpdateRequest {
            schema_version: NativeControlSchemaVersion::V1,
            cell,
            expected_epoch: None,
            now_ms: 1,
        },
    };
    harness.commit(&method).unwrap();
}

fn report(
    harness: &mut Harness,
    cell_id: &str,
    errors: u64,
    at_ms: u64,
) -> Result<CapacityThrottleResult, String> {
    let method = Method::ThrottleCapacityCell {
        request: CapacityThrottleRequest {
            schema_version: NativeControlSchemaVersion::V1,
            cell_id: cell_id.to_string(),
            sample: ErrorBudgetSample {
                requests: 1_000,
                errors,
                window_end_ms: at_ms,
            },
            now_ms: at_ms,
        },
    };
    harness
        .commit(&method)
        .and_then(|bytes| decode_durable::<CapacityThrottleResult>(&bytes))
}

#[test]
fn an_error_burst_narrows_the_durable_ceiling_and_recovery_gives_it_back() {
    let mut harness = Harness::open("aimd");
    declare(&mut harness, declared_cell("llm", true));
    let narrowed = report(&mut harness, "llm", 400, 10_000).unwrap();
    assert_eq!(narrowed.action.action, ThrottleAction::Narrowed);
    let stored = harness.cell("llm");
    assert_eq!(stored.effective_capacity(), 32);
    assert_eq!(
        stored.capacity, 64,
        "the declared capacity is the operator's"
    );
    // The same window again is refused, and nothing moves.
    assert!(report(&mut harness, "llm", 0, 10_000)
        .unwrap_err()
        .contains("THROTTLE_STALE_SAMPLE"));
    let recovered = report(&mut harness, "llm", 0, 20_000).unwrap();
    assert_eq!(recovered.action.action, ThrottleAction::Recovered);
    assert_eq!(harness.cell("llm").effective_capacity(), 36);
    assert_eq!(harness.cell("llm").throttle.unwrap().history.len(), 2);
}

#[test]
fn a_cell_without_a_declared_policy_refuses_the_automatic_step() {
    let mut harness = Harness::open("no-policy");
    declare(&mut harness, declared_cell("plain", false));
    let refused = report(&mut harness, "plain", 900, 10_000).unwrap_err();
    assert!(refused.contains("THROTTLE_NO_POLICY"), "{refused}");
    assert_eq!(harness.cell("plain").effective_capacity(), 64);
}
