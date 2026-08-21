//! Pure proposal planning for M3 cross-node elasticity (CONCEPT:EG-KG.sharding.cross-node-elasticity).
//!
//! This module deliberately stops at a typed, deterministic **proposal**.  It does not
//! talk to Raft, move a graph, change a placement record, or call an actuator.  A
//! controller may persist the returned [`MoveProposal`] and then drive the existing
//! placement/reshard state machines.  Keeping the planner pure makes it safe to run
//! repeatedly on every replica and makes a proposal replayable after a restart.
//!
//! The planner scores all eight resource axes.  Resident-node count is included, but
//! can never be the only reason a move is selected: bytes, reads, writes, queue depth,
//! fsync latency, CPU, and fan-out all participate in the pressure score and in the
//! target-capacity gate.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::NodeId;

/// Basis-point scale used by pressure and policy thresholds.
pub const PRESSURE_SCALE_BP: u64 = 10_000;
/// A deliberately bounded planner input.  A fleet controller can partition a larger
/// inventory into deterministic planning windows instead of creating an unbounded
/// per-million-item allocation in one process.
pub const MAX_PLACEMENTS: usize = 100_000;
pub const MAX_NODES: usize = 4_096;
pub const MAX_ACTIVE_MOVES: usize = 100_000;
pub const MAX_FOLLOWERS: usize = 64;
pub const MAX_GRAPH_ID_BYTES: usize = 4_096;

/// The eight axes used for pressure, target fit, and SLO admission.  All values are
/// rates or resident quantities in the same units as the corresponding capacity.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceVector {
    pub resident_bytes: u64,
    pub resident_nodes: u64,
    pub read_ops_per_sec: u64,
    pub write_ops_per_sec: u64,
    pub queue_depth: u64,
    pub fsync_p99_micros: u64,
    /// CPU millicores (or an equivalent normalized per-second budget).
    pub cpu_millis: u64,
    pub fanout_units_per_sec: u64,
}

impl ResourceVector {
    fn saturating_add(self, other: Self) -> Self {
        Self {
            resident_bytes: self.resident_bytes.saturating_add(other.resident_bytes),
            resident_nodes: self.resident_nodes.saturating_add(other.resident_nodes),
            read_ops_per_sec: self.read_ops_per_sec.saturating_add(other.read_ops_per_sec),
            write_ops_per_sec: self
                .write_ops_per_sec
                .saturating_add(other.write_ops_per_sec),
            queue_depth: self.queue_depth.saturating_add(other.queue_depth),
            fsync_p99_micros: self.fsync_p99_micros.saturating_add(other.fsync_p99_micros),
            cpu_millis: self.cpu_millis.saturating_add(other.cpu_millis),
            fanout_units_per_sec: self
                .fanout_units_per_sec
                .saturating_add(other.fanout_units_per_sec),
        }
    }

    fn checked_sub(self, other: Self) -> Option<Self> {
        Some(Self {
            resident_bytes: self.resident_bytes.checked_sub(other.resident_bytes)?,
            resident_nodes: self.resident_nodes.checked_sub(other.resident_nodes)?,
            read_ops_per_sec: self.read_ops_per_sec.checked_sub(other.read_ops_per_sec)?,
            write_ops_per_sec: self
                .write_ops_per_sec
                .checked_sub(other.write_ops_per_sec)?,
            queue_depth: self.queue_depth.checked_sub(other.queue_depth)?,
            fsync_p99_micros: self.fsync_p99_micros.checked_sub(other.fsync_p99_micros)?,
            cpu_millis: self.cpu_millis.checked_sub(other.cpu_millis)?,
            fanout_units_per_sec: self
                .fanout_units_per_sec
                .checked_sub(other.fanout_units_per_sec)?,
        })
    }

    fn fits_in(self, limits: Self) -> bool {
        self.resident_bytes <= limits.resident_bytes
            && self.resident_nodes <= limits.resident_nodes
            && self.read_ops_per_sec <= limits.read_ops_per_sec
            && self.write_ops_per_sec <= limits.write_ops_per_sec
            && self.queue_depth <= limits.queue_depth
            && self.fsync_p99_micros <= limits.fsync_p99_micros
            && self.cpu_millis <= limits.cpu_millis
            && self.fanout_units_per_sec <= limits.fanout_units_per_sec
    }

    /// Weighted pressure in basis points.  Values over capacity are retained up to
    /// 200% so an overloaded source is still distinguishable from a merely full one.
    /// The weights are intentionally fixed and versioned as part of the planner
    /// contract; changing them should be a new policy/planner version.
    pub fn pressure_bp(self, limits: Self) -> u64 {
        const WEIGHTS_BP: [u64; 8] = [
            1_600, // resident bytes
            600,   // resident nodes
            1_200, // reads
            1_600, // writes
            1_200, // queue
            1_200, // fsync
            1_400, // CPU
            1_200, // fan-out
        ];
        let values = [
            self.resident_bytes,
            self.resident_nodes,
            self.read_ops_per_sec,
            self.write_ops_per_sec,
            self.queue_depth,
            self.fsync_p99_micros,
            self.cpu_millis,
            self.fanout_units_per_sec,
        ];
        let caps = [
            limits.resident_bytes,
            limits.resident_nodes,
            limits.read_ops_per_sec,
            limits.write_ops_per_sec,
            limits.queue_depth,
            limits.fsync_p99_micros,
            limits.cpu_millis,
            limits.fanout_units_per_sec,
        ];
        let weighted = values
            .into_iter()
            .zip(caps)
            .zip(WEIGHTS_BP)
            .map(|((value, cap), weight)| {
                let ratio = if cap == 0 {
                    2 * PRESSURE_SCALE_BP
                } else {
                    ((value as u128 * PRESSURE_SCALE_BP as u128) / cap as u128)
                        .min(2 * PRESSURE_SCALE_BP as u128) as u64
                };
                ratio.saturating_mul(weight)
            })
            .sum::<u64>();
        weighted / PRESSURE_SCALE_BP
    }

    fn validate_limits(self) -> bool {
        self.resident_bytes > 0
            && self.resident_nodes > 0
            && self.read_ops_per_sec > 0
            && self.write_ops_per_sec > 0
            && self.queue_depth > 0
            && self.fsync_p99_micros > 0
            && self.cpu_millis > 0
            && self.fanout_units_per_sec > 0
    }
}

/// Per-axis headroom after a prospective addition.  A controller can expose this
/// directly without turning the planner into an actuator.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceHeadroom {
    pub resident_bytes: u64,
    pub resident_nodes: u64,
    pub read_ops_per_sec: u64,
    pub write_ops_per_sec: u64,
    pub queue_depth: u64,
    pub fsync_p99_micros: u64,
    pub cpu_millis: u64,
    pub fanout_units_per_sec: u64,
}

impl ResourceHeadroom {
    fn from_limits_and_load(limits: ResourceVector, load: ResourceVector) -> Self {
        let remaining = |limit: u64, used: u64| limit.saturating_sub(used);
        Self {
            resident_bytes: remaining(limits.resident_bytes, load.resident_bytes),
            resident_nodes: remaining(limits.resident_nodes, load.resident_nodes),
            read_ops_per_sec: remaining(limits.read_ops_per_sec, load.read_ops_per_sec),
            write_ops_per_sec: remaining(limits.write_ops_per_sec, load.write_ops_per_sec),
            queue_depth: remaining(limits.queue_depth, load.queue_depth),
            fsync_p99_micros: remaining(limits.fsync_p99_micros, load.fsync_p99_micros),
            cpu_millis: remaining(limits.cpu_millis, load.cpu_millis),
            fanout_units_per_sec: remaining(limits.fanout_units_per_sec, load.fanout_units_per_sec),
        }
    }

    pub fn fits(&self, load: ResourceVector) -> bool {
        load.resident_bytes <= self.resident_bytes
            && load.resident_nodes <= self.resident_nodes
            && load.read_ops_per_sec <= self.read_ops_per_sec
            && load.write_ops_per_sec <= self.write_ops_per_sec
            && load.queue_depth <= self.queue_depth
            && load.fsync_p99_micros <= self.fsync_p99_micros
            && load.cpu_millis <= self.cpu_millis
            && load.fanout_units_per_sec <= self.fanout_units_per_sec
    }
}

/// Whether a node may receive a new primary/follower or hydrated shard.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeAvailability {
    Eligible,
    Draining,
    Quarantined,
}

/// A live node's observed load and hard admission limits.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeCapacity {
    pub node_id: NodeId,
    pub limits: ResourceVector,
    pub observed: ResourceVector,
    pub network_bytes_per_sec: u64,
    pub object_tier_free_bytes: u64,
    pub availability: NodeAvailability,
}

impl NodeCapacity {
    pub fn headroom(&self, additional: ResourceVector) -> ResourceHeadroom {
        ResourceHeadroom::from_limits_and_load(
            self.limits,
            self.observed.saturating_add(additional),
        )
    }

    pub fn projected_pressure_bp(&self, additional: ResourceVector) -> u64 {
        self.observed
            .saturating_add(additional)
            .pressure_bp(self.limits)
    }

    fn validate(&self) -> bool {
        self.limits.validate_limits() && self.network_bytes_per_sec > 0
    }
}

/// Placement state is carried into the proposal so a caller cannot accidentally
/// treat a cold or already-hydrating shard as a resident primary move.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlacementState {
    Resident,
    Follower,
    Cold,
    Hydrating,
    Snapshotting,
    DeltaCatchUp,
    FencedCutover,
    Draining,
    Quarantined,
}

/// The immutable placement/load observation for one graph shard.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardPlacement {
    pub graph: String,
    pub shard_id: u64,
    pub primary_node: NodeId,
    pub follower_nodes: Vec<NodeId>,
    pub state: PlacementState,
    pub load: ResourceVector,
    /// Estimated bytes generated by the delta stream during the bounded copy
    /// window.  This is a cost estimate only; the durable CDC cursor remains in
    /// [`MoveCheckpoint`].
    pub delta_bytes_per_sec: u64,
    pub object_tier_bytes: u64,
    pub placement_epoch: u64,
    pub last_transition_tick: u64,
}

impl ShardPlacement {
    fn key(&self) -> (&str, u64) {
        (&self.graph, self.shard_id)
    }
}

/// Why the planner is moving a shard.  `Hydrate` is the object-tier return path;
/// other kinds use snapshot + delta + fenced cutover.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MoveKind {
    MovePrimary,
    AddFollower,
    Hydrate,
}

impl MoveKind {
    fn as_bytes(self) -> &'static [u8] {
        match self {
            Self::MovePrimary => b"move_primary",
            Self::AddFollower => b"add_follower",
            Self::Hydrate => b"hydrate",
        }
    }
}

/// Durable/replayable move phases.  The planner can resume any non-terminal phase;
/// it never declares a move complete merely because a proposal was emitted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MovePhase {
    Snapshot,
    DeltaCatchUp,
    FencedCutover,
    Hydrate,
    Complete,
    Aborted,
}

impl MovePhase {
    fn active(self) -> bool {
        !matches!(self, Self::Complete | Self::Aborted)
    }
}

/// Stable IDs shared by every phase of one move.  A restart or retry must reuse
/// these IDs; a new target/epoch produces a different identity and cannot overwrite
/// the old move.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MoveIdentity {
    pub move_id: String,
    pub snapshot_id: String,
    pub delta_stream_id: String,
    pub fence_id: String,
}

impl MoveIdentity {
    fn for_placement(
        graph: &str,
        shard_id: u64,
        source: NodeId,
        target: NodeId,
        placement_epoch: u64,
        kind: MoveKind,
    ) -> Self {
        let shard_bytes = shard_id.to_be_bytes();
        let source_bytes = source.to_be_bytes();
        let target_bytes = target.to_be_bytes();
        let epoch_bytes = placement_epoch.to_be_bytes();
        let move_id = digest_parts(
            b"epistemic-graph/cross-node-move/v1\0",
            [
                graph.as_bytes(),
                shard_bytes.as_slice(),
                source_bytes.as_slice(),
                target_bytes.as_slice(),
                epoch_bytes.as_slice(),
                kind.as_bytes(),
            ],
        );
        let snapshot_id = digest_parts(
            b"epistemic-graph/cross-node-snapshot/v1\0",
            [move_id.as_bytes()],
        );
        let delta_stream_id = digest_parts(
            b"epistemic-graph/cross-node-delta/v1\0",
            [move_id.as_bytes(), snapshot_id.as_bytes()],
        );
        let fence_id = digest_parts(
            b"epistemic-graph/cross-node-fence/v1\0",
            [move_id.as_bytes(), delta_stream_id.as_bytes()],
        );
        Self {
            move_id,
            snapshot_id,
            delta_stream_id,
            fence_id,
        }
    }
}

/// Durable progress identity for a snapshot/delta move.  `delta_cursor` and
/// `applied_delta_count` are monotonic checkpoints, not an in-memory indication of
/// success.  The final fence ID is the no-loss/no-duplication cutover token.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MoveCheckpoint {
    pub identity: MoveIdentity,
    pub graph: String,
    pub shard_id: u64,
    pub kind: MoveKind,
    pub source_node: NodeId,
    pub target_node: NodeId,
    pub placement_epoch: u64,
    pub snapshot_epoch: u64,
    pub phase: MovePhase,
    pub delta_cursor: u64,
    pub copied_bytes: u64,
    pub applied_delta_count: u64,
    pub last_transition_tick: u64,
}

impl MoveCheckpoint {
    pub fn new(
        placement: &ShardPlacement,
        source_node: NodeId,
        target_node: NodeId,
        kind: MoveKind,
        phase: MovePhase,
        now_tick: u64,
    ) -> Self {
        let identity = MoveIdentity::for_placement(
            &placement.graph,
            placement.shard_id,
            source_node,
            target_node,
            placement.placement_epoch,
            kind,
        );
        Self {
            identity,
            graph: placement.graph.clone(),
            shard_id: placement.shard_id,
            kind,
            source_node,
            target_node,
            placement_epoch: placement.placement_epoch,
            snapshot_epoch: placement.placement_epoch,
            phase,
            delta_cursor: 0,
            copied_bytes: 0,
            applied_delta_count: 0,
            last_transition_tick: now_tick,
        }
    }

    pub fn validate(&self) -> bool {
        if self.graph.is_empty()
            || self.graph.len() > MAX_GRAPH_ID_BYTES
            || self.source_node == self.target_node
            || !self.phase.active()
        {
            return false;
        }
        let expected = MoveIdentity::for_placement(
            &self.graph,
            self.shard_id,
            self.source_node,
            self.target_node,
            self.placement_epoch,
            self.kind,
        );
        self.identity == expected
    }
}

/// Cost budget for one proposed move.  It includes both the network snapshot/delta
/// path and object-tier hydration, so cold data is not treated as free.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MovementCost {
    pub snapshot_bytes: u64,
    pub delta_bytes: u64,
    pub network_bytes: u64,
    pub object_tier_bytes: u64,
    pub estimated_millis: u64,
    pub budget_units: u64,
}

impl MovementCost {
    fn estimate(
        placement: &ShardPlacement,
        kind: MoveKind,
        target_network_bytes_per_sec: u64,
        delta_window_seconds: u64,
    ) -> Self {
        let snapshot_bytes = if matches!(kind, MoveKind::Hydrate) {
            0
        } else {
            placement.load.resident_bytes
        };
        let delta_bytes = if matches!(kind, MoveKind::Hydrate) {
            0
        } else {
            placement
                .delta_bytes_per_sec
                .saturating_mul(delta_window_seconds)
        };
        let object_tier_bytes = if matches!(kind, MoveKind::Hydrate) {
            placement.object_tier_bytes
        } else {
            0
        };
        let network_bytes = snapshot_bytes.saturating_add(delta_bytes);
        let transfer_millis = if network_bytes == 0 {
            0
        } else {
            network_bytes
                .saturating_mul(1_000)
                .saturating_add(target_network_bytes_per_sec.saturating_sub(1))
                / target_network_bytes_per_sec.max(1)
        };
        Self {
            snapshot_bytes,
            delta_bytes,
            network_bytes,
            object_tier_bytes,
            estimated_millis: transfer_millis,
            budget_units: network_bytes.saturating_add(object_tier_bytes),
        }
    }
}

/// Policy bounds are explicit hard limits.  The planner never scales beyond these
/// values and uses the same policy for fresh and resumed proposals.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ElasticityPolicy {
    pub scale_up_threshold_bp: u16,
    pub hysteresis_bp: u16,
    pub cooldown_ticks: u64,
    pub max_proposals: usize,
    pub max_network_bytes: u64,
    pub max_budget_units: u64,
    pub delta_window_seconds: u64,
    pub max_queue_depth: u64,
    pub max_fsync_p99_micros: u64,
    pub max_fanout_units_per_sec: u64,
    pub hydrate_read_ops_per_sec: u64,
    pub allow_hydration: bool,
}

impl Default for ElasticityPolicy {
    fn default() -> Self {
        Self {
            scale_up_threshold_bp: 8_000,
            hysteresis_bp: 500,
            cooldown_ticks: 300,
            max_proposals: 32,
            max_network_bytes: 512 * 1024 * 1024,
            max_budget_units: 512 * 1024 * 1024,
            delta_window_seconds: 30,
            max_queue_depth: 100_000,
            max_fsync_p99_micros: 50_000,
            max_fanout_units_per_sec: 100_000,
            hydrate_read_ops_per_sec: 1,
            allow_hydration: true,
        }
    }
}

impl ElasticityPolicy {
    fn validate(&self) -> bool {
        self.scale_up_threshold_bp <= PRESSURE_SCALE_BP as u16
            && (self.scale_up_threshold_bp as u64 + self.hysteresis_bp as u64)
                <= 2 * PRESSURE_SCALE_BP
            && self.max_proposals > 0
            && self.max_proposals <= MAX_PLACEMENTS
            && self.max_network_bytes > 0
            && self.max_budget_units > 0
            && self.delta_window_seconds > 0
            && self.max_queue_depth > 0
            && self.max_fsync_p99_micros > 0
            && self.max_fanout_units_per_sec > 0
    }
}

/// Pure planner input.  The vectors must be sorted by node ID and `(graph, shard)`;
/// this makes the result independent of map/hash iteration order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlannerInput {
    pub planning_epoch: u64,
    pub topology_epoch: u64,
    pub now_tick: u64,
    pub nodes: Vec<NodeCapacity>,
    pub placements: Vec<ShardPlacement>,
    pub active_moves: Vec<MoveCheckpoint>,
    pub policy: ElasticityPolicy,
}

impl PlannerInput {
    pub fn validate(&self) -> Result<(), PlannerError> {
        if self.nodes.len() > MAX_NODES
            || self.placements.len() > MAX_PLACEMENTS
            || self.active_moves.len() > MAX_ACTIVE_MOVES
            || !self.policy.validate()
        {
            return Err(PlannerError::InvalidInput(
                "elasticity input exceeds a bound or has invalid policy".to_string(),
            ));
        }
        if self
            .nodes
            .windows(2)
            .any(|pair| pair[0].node_id >= pair[1].node_id)
            || self
                .placements
                .windows(2)
                .any(|pair| pair[0].key() >= pair[1].key())
            || self
                .active_moves
                .windows(2)
                .any(|pair| pair[0].identity.move_id >= pair[1].identity.move_id)
        {
            return Err(PlannerError::InvalidInput(
                "elasticity input must be canonically sorted".to_string(),
            ));
        }
        if self.nodes.iter().any(|node| !node.validate()) {
            return Err(PlannerError::InvalidInput(
                "node capacity contains a zero or invalid limit".to_string(),
            ));
        }
        let node_ids = self
            .nodes
            .iter()
            .map(|node| node.node_id)
            .collect::<BTreeSet<_>>();
        let mut shard_keys = BTreeSet::new();
        for placement in &self.placements {
            if placement.graph.is_empty()
                || placement.graph.len() > MAX_GRAPH_ID_BYTES
                || !node_ids.contains(&placement.primary_node)
                || placement.follower_nodes.len() > MAX_FOLLOWERS
                || placement
                    .follower_nodes
                    .iter()
                    .any(|node| *node == placement.primary_node || !node_ids.contains(node))
            {
                return Err(PlannerError::InvalidInput(
                    "placement has an invalid graph, node, or follower".to_string(),
                ));
            }
            let mut followers = placement.follower_nodes.clone();
            followers.sort_unstable();
            followers.dedup();
            if followers.len() != placement.follower_nodes.len()
                || placement
                    .follower_nodes
                    .windows(2)
                    .any(|pair| pair[0] >= pair[1])
                || !shard_keys.insert(placement.key())
            {
                return Err(PlannerError::InvalidInput(
                    "placement contains duplicate follower or shard identity".to_string(),
                ));
            }
        }
        let mut move_ids = BTreeSet::new();
        let mut move_keys = BTreeSet::new();
        for checkpoint in &self.active_moves {
            if !checkpoint.validate()
                || !node_ids.contains(&checkpoint.source_node)
                || !node_ids.contains(&checkpoint.target_node)
                || !shard_keys.contains(&(checkpoint.graph.as_str(), checkpoint.shard_id))
                || !move_ids.insert(checkpoint.identity.move_id.clone())
                || !move_keys.insert((checkpoint.graph.as_str(), checkpoint.shard_id))
            {
                return Err(PlannerError::InvalidInput(
                    "active move checkpoint is invalid or duplicated".to_string(),
                ));
            }
        }
        Ok(())
    }
}

/// Why a candidate was not emitted.  These are honest, bounded reasons suitable for
/// an audit/event record; no planner failure is silently turned into a verified move.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanAbortReason {
    CooldownActive { until_tick: u64 },
    HysteresisNotCrossed { pressure_bp: u64, trigger_bp: u64 },
    DuplicateInFlight,
    InvalidCheckpoint,
    StaleTopology,
    SloRisk,
    BudgetExceeded,
    InsufficientCapacity,
    NoSafeTarget,
    ProposalLimit,
    SourceLoadMismatch,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanAbort {
    pub graph: String,
    pub shard_id: u64,
    pub reason: PlanAbortReason,
}

/// A proposal only.  Applying this object remains the responsibility of the
/// existing policy-gated placement/reshard owner; the planner has no actuation path.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MoveProposal {
    pub proposal_id: String,
    pub move_id: String,
    pub graph: String,
    pub shard_id: u64,
    pub kind: MoveKind,
    pub source_node: NodeId,
    pub target_node: NodeId,
    pub from_state: PlacementState,
    pub source_pressure_before_bp: u64,
    pub target_pressure_after_bp: u64,
    pub cost: MovementCost,
    pub checkpoint: MoveCheckpoint,
    /// The fence identity that a later actuator must present for an atomic cutover.
    pub no_loss_fence_id: String,
    /// Stable deduplication key for retries and restart reconciliation.
    pub dedupe_key: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ElasticityPlan {
    pub planner_version: u16,
    pub planning_epoch: u64,
    pub topology_epoch: u64,
    pub input_digest: String,
    pub proposals: Vec<MoveProposal>,
    pub aborts: Vec<PlanAbort>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PlannerError {
    #[error("invalid elasticity planner input: {0}")]
    InvalidInput(String),
    #[error("could not fingerprint elasticity planner input: {0}")]
    Fingerprint(String),
}

/// Stateless M3 planner.  Constructing or calling it has no side effects.
#[derive(Clone, Copy, Debug, Default)]
pub struct CrossNodeElasticityPlanner;

impl CrossNodeElasticityPlanner {
    pub fn plan(&self, input: &PlannerInput) -> Result<ElasticityPlan, PlannerError> {
        input.validate()?;
        let encoded = serde_json::to_vec(input)
            .map_err(|error| PlannerError::Fingerprint(error.to_string()))?;
        let input_digest = digest_parts(
            b"epistemic-graph/cross-node-input/v1\0",
            [encoded.as_slice()],
        );
        let trigger_bp = (input.policy.scale_up_threshold_bp as u64)
            .saturating_add(input.policy.hysteresis_bp as u64);
        let mut plan = ElasticityPlan {
            planner_version: 1,
            planning_epoch: input.planning_epoch,
            topology_epoch: input.topology_epoch,
            input_digest,
            proposals: Vec::new(),
            aborts: Vec::new(),
        };
        let mut planned_additions = std::collections::BTreeMap::<NodeId, ResourceVector>::new();
        let mut planned_removals = std::collections::BTreeMap::<NodeId, ResourceVector>::new();
        let mut network_budget_used = 0u64;
        let mut total_budget_used = 0u64;

        for placement in &input.placements {
            let active = input.active_moves.iter().find(|move_| {
                move_.graph == placement.graph && move_.shard_id == placement.shard_id
            });
            if let Some(checkpoint) = active {
                if !checkpoint.validate() {
                    plan.aborts
                        .push(Self::abort(placement, PlanAbortReason::InvalidCheckpoint));
                    continue;
                }
                if checkpoint.placement_epoch != placement.placement_epoch
                    || checkpoint.source_node != placement.primary_node
                {
                    plan.aborts
                        .push(Self::abort(placement, PlanAbortReason::StaleTopology));
                    continue;
                }
                if checkpoint.target_node == placement.primary_node {
                    plan.aborts
                        .push(Self::abort(placement, PlanAbortReason::DuplicateInFlight));
                    continue;
                }
                let Some(target) = input
                    .nodes
                    .iter()
                    .find(|node| node.node_id == checkpoint.target_node)
                else {
                    plan.aborts
                        .push(Self::abort(placement, PlanAbortReason::NoSafeTarget));
                    continue;
                };
                if target.availability != NodeAvailability::Eligible {
                    plan.aborts
                        .push(Self::abort(placement, PlanAbortReason::NoSafeTarget));
                    continue;
                }
                if plan.proposals.len() >= input.policy.max_proposals {
                    plan.aborts
                        .push(Self::abort(placement, PlanAbortReason::ProposalLimit));
                    continue;
                }
                let Some((source_load, target_load)) = Self::projected_loads(
                    input,
                    placement,
                    checkpoint.target_node,
                    &planned_additions,
                    &planned_removals,
                ) else {
                    plan.aborts
                        .push(Self::abort(placement, PlanAbortReason::SourceLoadMismatch));
                    continue;
                };
                let projected_target = target_load.saturating_add(placement.load);
                let cost = MovementCost::estimate(
                    placement,
                    checkpoint.kind,
                    target.network_bytes_per_sec,
                    input.policy.delta_window_seconds,
                );
                if !projected_target.fits_in(target.limits) {
                    plan.aborts.push(Self::abort(
                        placement,
                        PlanAbortReason::InsufficientCapacity,
                    ));
                    continue;
                }
                if Self::violates_slo(projected_target, &input.policy) {
                    plan.aborts
                        .push(Self::abort(placement, PlanAbortReason::SloRisk));
                    continue;
                }
                if !Self::within_budget(cost, network_budget_used, total_budget_used, &input.policy)
                {
                    plan.aborts
                        .push(Self::abort(placement, PlanAbortReason::BudgetExceeded));
                    continue;
                }
                let proposal = Self::proposal(
                    input,
                    placement,
                    checkpoint.kind,
                    checkpoint.clone(),
                    source_load,
                    projected_target,
                    cost,
                );
                network_budget_used = network_budget_used.saturating_add(cost.network_bytes);
                total_budget_used = total_budget_used.saturating_add(cost.budget_units);
                Self::record_load_delta(
                    &mut planned_additions,
                    &mut planned_removals,
                    checkpoint.source_node,
                    checkpoint.target_node,
                    placement.load,
                    checkpoint.kind,
                );
                plan.proposals.push(proposal);
                continue;
            }

            let Some(source) = input
                .nodes
                .iter()
                .find(|node| node.node_id == placement.primary_node)
            else {
                plan.aborts
                    .push(Self::abort(placement, PlanAbortReason::NoSafeTarget));
                continue;
            };
            let Some((source_load, _)) = Self::projected_loads(
                input,
                placement,
                placement.primary_node,
                &planned_additions,
                &planned_removals,
            ) else {
                plan.aborts
                    .push(Self::abort(placement, PlanAbortReason::SourceLoadMismatch));
                continue;
            };
            let source_pressure = source_load.pressure_bp(source.limits);
            let kind = match placement.state {
                PlacementState::Cold => {
                    if !input.policy.allow_hydration
                        || placement.load.read_ops_per_sec < input.policy.hydrate_read_ops_per_sec
                    {
                        plan.aborts.push(Self::abort(
                            placement,
                            PlanAbortReason::HysteresisNotCrossed {
                                pressure_bp: source_pressure,
                                trigger_bp,
                            },
                        ));
                        continue;
                    }
                    MoveKind::Hydrate
                }
                PlacementState::Hydrating
                | PlacementState::Snapshotting
                | PlacementState::DeltaCatchUp
                | PlacementState::FencedCutover => {
                    plan.aborts
                        .push(Self::abort(placement, PlanAbortReason::DuplicateInFlight));
                    continue;
                }
                PlacementState::Quarantined => {
                    plan.aborts
                        .push(Self::abort(placement, PlanAbortReason::NoSafeTarget));
                    continue;
                }
                PlacementState::Resident | PlacementState::Follower | PlacementState::Draining => {
                    if source.availability != NodeAvailability::Draining
                        && source_pressure < trigger_bp
                    {
                        plan.aborts.push(Self::abort(
                            placement,
                            PlanAbortReason::HysteresisNotCrossed {
                                pressure_bp: source_pressure,
                                trigger_bp,
                            },
                        ));
                        continue;
                    }
                    MoveKind::MovePrimary
                }
            };
            if input.now_tick
                < placement
                    .last_transition_tick
                    .saturating_add(input.policy.cooldown_ticks)
            {
                plan.aborts.push(Self::abort(
                    placement,
                    PlanAbortReason::CooldownActive {
                        until_tick: placement
                            .last_transition_tick
                            .saturating_add(input.policy.cooldown_ticks),
                    },
                ));
                continue;
            }
            if plan.proposals.len() >= input.policy.max_proposals {
                plan.aborts
                    .push(Self::abort(placement, PlanAbortReason::ProposalLimit));
                continue;
            }

            let target = Self::select_target(
                input,
                placement,
                kind,
                &planned_additions,
                &planned_removals,
                network_budget_used,
                total_budget_used,
            );
            let (target, projected_target, cost) = match target {
                Ok(value) => value,
                Err(reason) => {
                    plan.aborts.push(Self::abort(placement, reason));
                    continue;
                }
            };
            let checkpoint = MoveCheckpoint::new(
                placement,
                placement.primary_node,
                target.node_id,
                kind,
                if kind == MoveKind::Hydrate {
                    MovePhase::Hydrate
                } else {
                    MovePhase::Snapshot
                },
                input.now_tick,
            );
            let proposal = Self::proposal(
                input,
                placement,
                kind,
                checkpoint,
                source_load,
                projected_target,
                cost,
            );
            network_budget_used = network_budget_used.saturating_add(cost.network_bytes);
            total_budget_used = total_budget_used.saturating_add(cost.budget_units);
            Self::record_load_delta(
                &mut planned_additions,
                &mut planned_removals,
                placement.primary_node,
                target.node_id,
                placement.load,
                kind,
            );
            plan.proposals.push(proposal);
        }

        plan.proposals
            .sort_by(|left, right| left.proposal_id.cmp(&right.proposal_id));
        plan.aborts.sort_by(|left, right| {
            (&left.graph, left.shard_id).cmp(&(&right.graph, right.shard_id))
        });
        Ok(plan)
    }

    fn abort(placement: &ShardPlacement, reason: PlanAbortReason) -> PlanAbort {
        PlanAbort {
            graph: placement.graph.clone(),
            shard_id: placement.shard_id,
            reason,
        }
    }

    fn projected_loads(
        input: &PlannerInput,
        placement: &ShardPlacement,
        target_node: NodeId,
        additions: &std::collections::BTreeMap<NodeId, ResourceVector>,
        removals: &std::collections::BTreeMap<NodeId, ResourceVector>,
    ) -> Option<(ResourceVector, ResourceVector)> {
        let source = input
            .nodes
            .iter()
            .find(|node| node.node_id == placement.primary_node)?;
        let target = input
            .nodes
            .iter()
            .find(|node| node.node_id == target_node)?;
        let source_base = source.observed.saturating_add(
            *additions
                .get(&source.node_id)
                .unwrap_or(&ResourceVector::default()),
        );
        let source_load = source_base.checked_sub(
            *removals
                .get(&source.node_id)
                .unwrap_or(&ResourceVector::default()),
        )?;
        let target_base = target.observed.saturating_add(
            *additions
                .get(&target.node_id)
                .unwrap_or(&ResourceVector::default()),
        );
        Some((source_load, target_base))
    }

    fn violates_slo(load: ResourceVector, policy: &ElasticityPolicy) -> bool {
        load.queue_depth > policy.max_queue_depth
            || load.fsync_p99_micros > policy.max_fsync_p99_micros
            || load.fanout_units_per_sec > policy.max_fanout_units_per_sec
    }

    fn within_budget(
        cost: MovementCost,
        network_used: u64,
        budget_used: u64,
        policy: &ElasticityPolicy,
    ) -> bool {
        cost.network_bytes <= policy.max_network_bytes.saturating_sub(network_used)
            && cost.budget_units <= policy.max_budget_units.saturating_sub(budget_used)
    }

    fn select_target<'a>(
        input: &'a PlannerInput,
        placement: &ShardPlacement,
        kind: MoveKind,
        additions: &std::collections::BTreeMap<NodeId, ResourceVector>,
        removals: &std::collections::BTreeMap<NodeId, ResourceVector>,
        network_used: u64,
        budget_used: u64,
    ) -> Result<(&'a NodeCapacity, ResourceVector, MovementCost), PlanAbortReason> {
        let mut best: Option<(&NodeCapacity, ResourceVector, MovementCost, u64)> = None;
        let mut saw_eligible = false;
        let mut saw_capacity = false;
        let mut saw_slo = false;
        let mut saw_budget = false;
        for target in &input.nodes {
            if target.node_id == placement.primary_node
                || target.availability != NodeAvailability::Eligible
                || placement.follower_nodes.contains(&target.node_id)
            {
                continue;
            }
            saw_eligible = true;
            let Some((_, target_base)) =
                Self::projected_loads(input, placement, target.node_id, additions, removals)
            else {
                continue;
            };
            let projected_target = target_base.saturating_add(placement.load);
            if !projected_target.fits_in(target.limits) {
                continue;
            }
            saw_capacity = true;
            if Self::violates_slo(projected_target, &input.policy) {
                saw_slo = true;
                continue;
            }
            let cost = MovementCost::estimate(
                placement,
                kind,
                target.network_bytes_per_sec,
                input.policy.delta_window_seconds,
            );
            if !Self::within_budget(cost, network_used, budget_used, &input.policy) {
                saw_budget = true;
                continue;
            }
            // The target pressure is part of the deterministic selection key.  A
            // node ID breaks ties, so two replicas choose the same target.
            let pressure = projected_target.pressure_bp(target.limits);
            let should_replace = match best.as_ref() {
                None => true,
                Some((best_target, _, _, best_pressure)) => {
                    (pressure, target.node_id) < (*best_pressure, best_target.node_id)
                }
            };
            if should_replace {
                best = Some((target, projected_target, cost, pressure));
            }
        }
        match best {
            Some((target, projected, cost, _)) => Ok((target, projected, cost)),
            None if saw_budget => Err(PlanAbortReason::BudgetExceeded),
            None if saw_slo => Err(PlanAbortReason::SloRisk),
            None if saw_capacity => Err(PlanAbortReason::NoSafeTarget),
            None if saw_eligible => Err(PlanAbortReason::InsufficientCapacity),
            None => Err(PlanAbortReason::NoSafeTarget),
        }
    }

    fn record_load_delta(
        additions: &mut std::collections::BTreeMap<NodeId, ResourceVector>,
        removals: &mut std::collections::BTreeMap<NodeId, ResourceVector>,
        source: NodeId,
        target: NodeId,
        load: ResourceVector,
        kind: MoveKind,
    ) {
        if kind == MoveKind::MovePrimary {
            let entry = removals.entry(source).or_default();
            *entry = entry.saturating_add(load);
        }
        let entry = additions.entry(target).or_default();
        *entry = entry.saturating_add(load);
    }

    fn proposal(
        input: &PlannerInput,
        placement: &ShardPlacement,
        kind: MoveKind,
        checkpoint: MoveCheckpoint,
        source_load: ResourceVector,
        target_load: ResourceVector,
        cost: MovementCost,
    ) -> MoveProposal {
        let planning_epoch = input.planning_epoch.to_be_bytes();
        let topology_epoch = input.topology_epoch.to_be_bytes();
        let proposal_id = digest_parts(
            b"epistemic-graph/cross-node-proposal/v1\0",
            [
                checkpoint.identity.move_id.as_bytes(),
                planning_epoch.as_slice(),
                topology_epoch.as_slice(),
            ],
        );
        let source = input
            .nodes
            .iter()
            .find(|node| node.node_id == checkpoint.source_node)
            .expect("validated source node");
        let target = input
            .nodes
            .iter()
            .find(|node| node.node_id == checkpoint.target_node)
            .expect("validated target node");
        MoveProposal {
            proposal_id,
            move_id: checkpoint.identity.move_id.clone(),
            graph: placement.graph.clone(),
            shard_id: placement.shard_id,
            kind,
            source_node: checkpoint.source_node,
            target_node: checkpoint.target_node,
            from_state: placement.state,
            source_pressure_before_bp: source_load.pressure_bp(source.limits),
            target_pressure_after_bp: target_load.pressure_bp(target.limits),
            cost,
            no_loss_fence_id: checkpoint.identity.fence_id.clone(),
            dedupe_key: checkpoint.identity.move_id.clone(),
            checkpoint,
        }
    }
}

fn digest_parts<'a, I>(domain: &[u8], parts: I) -> String
where
    I: IntoIterator<Item = &'a [u8]>,
{
    let mut digest = Sha256::new();
    digest.update(domain);
    for part in parts {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part);
    }
    hex::encode(digest.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vector(value: u64) -> ResourceVector {
        ResourceVector {
            resident_bytes: value,
            resident_nodes: value,
            read_ops_per_sec: value,
            write_ops_per_sec: value,
            queue_depth: value,
            fsync_p99_micros: value,
            cpu_millis: value,
            fanout_units_per_sec: value,
        }
    }

    fn node(node_id: NodeId, observed: u64) -> NodeCapacity {
        NodeCapacity {
            node_id,
            limits: vector(100),
            observed: vector(observed),
            network_bytes_per_sec: 1_000,
            object_tier_free_bytes: 10_000,
            availability: NodeAvailability::Eligible,
        }
    }

    fn placement(graph: &str, load: u64) -> ShardPlacement {
        ShardPlacement {
            graph: graph.to_string(),
            shard_id: 1,
            primary_node: 1,
            follower_nodes: Vec::new(),
            state: PlacementState::Resident,
            load: vector(load),
            delta_bytes_per_sec: 10,
            object_tier_bytes: 0,
            placement_epoch: 7,
            last_transition_tick: 0,
        }
    }

    fn input(placement: ShardPlacement) -> PlannerInput {
        PlannerInput {
            planning_epoch: 11,
            topology_epoch: 3,
            now_tick: 1_000,
            nodes: vec![node(1, 90), node(2, 0)],
            placements: vec![placement],
            active_moves: Vec::new(),
            policy: ElasticityPolicy {
                cooldown_ticks: 0,
                ..ElasticityPolicy::default()
            },
        }
    }

    #[test]
    fn pressure_uses_multiple_axes_not_resident_node_count_alone() {
        let p = placement("graph-a", 0);
        let mut i = input(p);
        i.nodes[0].observed.resident_nodes = 1;
        i.nodes[0].observed.resident_bytes = 100;
        i.nodes[0].observed.read_ops_per_sec = 100;
        i.nodes[0].observed.write_ops_per_sec = 100;
        i.nodes[0].observed.queue_depth = 100;
        i.nodes[0].observed.fsync_p99_micros = 100;
        i.nodes[0].observed.cpu_millis = 100;
        i.nodes[0].observed.fanout_units_per_sec = 100;
        let result = CrossNodeElasticityPlanner.plan(&i).expect("valid plan");
        assert_eq!(result.proposals.len(), 1);
        assert!(result.proposals[0].source_pressure_before_bp > 8_500);
    }

    #[test]
    fn retry_reuses_move_identity_and_active_checkpoint() {
        let first_input = input(placement("graph-a", 80));
        let planner = CrossNodeElasticityPlanner;
        let first = planner.plan(&first_input).expect("valid plan");
        let proposal = &first.proposals[0];
        let mut resumed_input = first_input.clone();
        let mut checkpoint = proposal.checkpoint.clone();
        checkpoint.phase = MovePhase::DeltaCatchUp;
        checkpoint.delta_cursor = 42;
        resumed_input.active_moves = vec![checkpoint];
        let resumed = planner.plan(&resumed_input).expect("valid resumed plan");
        assert_eq!(resumed.proposals[0].move_id, proposal.move_id);
        assert_eq!(resumed.proposals[0].checkpoint.delta_cursor, 42);
        assert_eq!(
            resumed.proposals[0].no_loss_fence_id,
            proposal.no_loss_fence_id
        );
    }

    #[test]
    fn capacity_and_slo_are_explicit_abort_reasons() {
        let mut p = placement("graph-a", 80);
        p.load.queue_depth = 99;
        let mut i = input(p);
        i.nodes[1].limits = vector(50);
        let result = CrossNodeElasticityPlanner.plan(&i).expect("valid plan");
        assert_eq!(result.proposals.len(), 0);
        assert_eq!(
            result.aborts[0].reason,
            PlanAbortReason::InsufficientCapacity
        );

        i.nodes[1].limits = vector(100);
        i.policy.max_queue_depth = 10;
        let result = CrossNodeElasticityPlanner.plan(&i).expect("valid plan");
        assert_eq!(result.aborts[0].reason, PlanAbortReason::SloRisk);
    }
}
