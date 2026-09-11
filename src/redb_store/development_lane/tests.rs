// Test partitions are modules so KISS measures each bounded unit rather
// than an include-expanded aggregate. Shared fixture helpers are re-exported
// below without changing the production module API.
use super::super::{decode_durable, resource_decode, resource_encode, DurableCrypto, NODES};
use super::super::{property_string, read_one_node};
use super::finish::ACTIVE_HOLD_REQUIRES_TERMINAL_WORK_ITEM;
use super::identity::{hold_load, method_name};
use super::lifecycle_shared::observation_fresh;
use super::links::checkpoint_lifecycle_status_matches;
use super::quota::{
    adjust, empty_charge, global_policy_equal, hold_id, load_counter, load_policy, scope_key,
    worktree_key, Metric,
};
use super::reserve_support::{policy_pressure, pressure_max, reserve_counter_check};
use super::results::{load_invocation, store_invocation, REDACTED_PRIVATE_ID};
use super::rows::{clear_native_graph_rows, lane_op_id};
use super::types::{LaneDecision, Scope, ScopeCounter};
use super::validation::request_digest;
use super::{
    commit_development_lane, read_development_lane, read_development_lane_status,
    validate_checkpoint_lane_links, DurableLaneCounter, DurableLaneHold, DurableLaneInvocation,
    DurableLanePolicy, COUNTERS, GLOBAL_POLICY_KEY, HOLDS, INVOCATIONS, LANE_INDEX, MAX_DISK_BYTES,
    MAX_INVOCATIONS_PER_TENANT, POLICIES, PRESSURE_INDEX, REPOSITORY_BRANCH_INDEX, WORKTREE_INDEX,
    WORK_ITEM_INDEX,
};
use crate::epistemic_operations::{
    DevelopmentLaneCleanupCompleteRequest, DevelopmentLaneCleanupCompleteResult,
    DevelopmentLaneFinishRequest, DevelopmentLaneFinishRequestTerminalState,
    DevelopmentLaneFinishResult, DevelopmentLaneHold, DevelopmentLaneHoldHostTargetKind,
    DevelopmentLaneHoldState, DevelopmentLaneIntent, DevelopmentLaneObserveRequest,
    DevelopmentLaneObserveResult, DevelopmentLaneQueryRequest, DevelopmentLaneQuotaPolicy,
    DevelopmentLaneQuotaUpdateRequest, DevelopmentLaneQuotaUpdateResult,
    DevelopmentLaneRenewRequest, DevelopmentLaneRenewResult, DevelopmentLaneReserveRequest,
    DevelopmentLaneResult, DevelopmentLaneStatusRequest, ResourceReservationRecordState,
};
use crate::epistemic_operations::{
    DevelopmentLaneCleanupCompleteRequestSchemaVersion, DevelopmentLaneFinishRequestSchemaVersion,
    DevelopmentLaneIntentSchemaVersion, DevelopmentLaneObserveRequestSchemaVersion,
    DevelopmentLaneRenewRequestSchemaVersion, DevelopmentLaneReserveRequestSchemaVersion,
    ResourceCapacitySnapshot, ResourceRequirement, ResourceReservationRecord,
    ResourceReservationRecordTargetKind, ResourceTargetSnapshot, ResourceTargetSnapshotKind,
};
use crate::epistemic_operations::{
    DevelopmentLaneCleanupCompleteResultDecision, DevelopmentLaneFinishResultDecision,
    DevelopmentLaneObserveResultDecision, DevelopmentLaneQueryResultDecision,
    DevelopmentLaneQuotaUpdateResultDecision, DevelopmentLaneRenewResultDecision,
    DevelopmentLaneResultDecision,
};
use crate::mutation_batch::{
    DurabilityDomain, IncarnationId, LogicalName, MutationBatch, MutationBatchCommit,
    MutationOperation, MutationOutboxIntent, MutationScopeIdentity, MutationSurface, ScopeTenantId,
    VersionExpectation, MUTATION_BATCH_VERSION,
};
use crate::protocol::Method;
use crate::redb_store::shard::{Shard, ShardWrite};
use eg_storage::{GraphShardOwner, ScopedRead};
use std::path::PathBuf;
use std::sync::{Arc, Barrier};

#[path = "test_fixture.rs"]
mod test_fixture;
#[path = "test_helpers.rs"]
mod test_helpers;
#[path = "test_seed.rs"]
mod test_seed;

use test_fixture::*;
use test_helpers::*;
use test_seed::*;

#[path = "test_cleanup_a.rs"]
mod test_cleanup_a;
#[path = "test_cleanup_b.rs"]
mod test_cleanup_b;
#[path = "test_finish_a.rs"]
mod test_finish_a;
#[path = "test_finish_b.rs"]
mod test_finish_b;
#[path = "test_finish_c.rs"]
mod test_finish_c;
#[path = "test_lifecycle_a.rs"]
mod test_lifecycle_a;
#[path = "test_lifecycle_b.rs"]
mod test_lifecycle_b;
#[path = "test_observe.rs"]
mod test_observe;
#[path = "test_reserve_a.rs"]
mod test_reserve_a;
#[path = "test_reserve_b.rs"]
mod test_reserve_b;
#[path = "test_status_a.rs"]
mod test_status_a;
#[path = "test_status_b.rs"]
mod test_status_b;
#[path = "test_status_c.rs"]
mod test_status_c;
