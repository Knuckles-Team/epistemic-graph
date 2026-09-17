//! Durable online-move journal for one virtual partition, plus the startup
//! recovery checks that pair each active journal with its placement fence.
//!
//! Split out of `placement.rs` (CCCC burn-down lane L-raft-b). That file was
//! already over the KISS whole-file `lines_per_file`/`functions_per_file`
//! thresholds, so the named predicates that bring `validate`,
//! `permits_successor`, and `validate_move_recovery_state` under the
//! complexity caps live here with the type they describe; the parent's own
//! counts only go down. The parent re-exports [`MoveStage`] and
//! [`PartitionMoveJournal`], so every existing path is unchanged.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use super::{
    GroupId, PartitionKey, PartitionState, PlacementEntry, MAX_PARTITION_MOVE_GRAPHS,
    MAX_PARTITION_MOVE_GRAPH_BYTES, MOVE_JOURNAL_NODE_PREFIX,
};

/// Longest single graph name a move inventory may carry.
const MAX_PARTITION_MOVE_GRAPH_NAME_BYTES: usize = 4_096;

/// Crash-recovery stages for an online partition move.  Every transition is stored
/// in [`super::PLACEMENT_GRAPH`] through Raft before the next side effect begins.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MoveStage {
    Planned,
    Moving,
    Transferring,
    ReadyForCutover,
    CutoverCommitted,
    Aborting,
    Completed,
    Aborted,
}

impl MoveStage {
    pub fn terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Aborted)
    }
}

/// Durable move intent and progress.  `graphs` is immutable after planning;
/// `completed_graphs` advances only after each graph passes the durable-presence
/// barrier.  A restart can therefore resume without a caller-supplied remainder.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionMoveJournal {
    pub move_id: String,
    pub key: PartitionKey,
    pub source: GroupId,
    pub target: GroupId,
    pub original_epoch: u64,
    pub graphs: Vec<String>,
    pub completed_graphs: Vec<String>,
    pub stage: MoveStage,
}

impl PartitionMoveJournal {
    pub fn new(
        entry: &PlacementEntry,
        target: GroupId,
        mut graphs: Vec<String>,
    ) -> Result<Self, String> {
        if entry.group == target || !matches!(entry.state, PartitionState::Active) {
            return Err(
                "partition move requires an active source and a distinct target".to_string(),
            );
        }
        if !graph_inventory_is_within_limits(&graphs) {
            return Err("partition move graph inventory exceeds the limit".to_string());
        }
        graphs.sort();
        graphs.dedup();
        if !graph_names_are_valid(&graphs) {
            return Err("partition move graph inventory is invalid".to_string());
        }
        Ok(Self {
            move_id: partition_move_id(&entry.key, entry.epoch, target, &graphs),
            key: entry.key.clone(),
            source: entry.group,
            target,
            original_epoch: entry.epoch,
            graphs,
            completed_graphs: Vec::new(),
            stage: MoveStage::Planned,
        })
    }

    pub(super) fn node_id(&self) -> String {
        format!("{MOVE_JOURNAL_NODE_PREFIX}{}", self.move_id)
    }

    pub fn validate(&self) -> bool {
        self.move_id == partition_move_id(&self.key, self.original_epoch, self.target, &self.graphs)
            && self.key.range_start <= self.key.range_end
            && self.source != self.target
            && graph_inventory_is_within_limits(&self.graphs)
            && graph_names_are_valid(&self.graphs)
            && completed_graphs_are_valid(self)
            && progress_matches_stage(self)
    }

    /// Whether `next` is a monotonic update of this exact durable move. This
    /// rejects a stale driver regressing an abort/cutover or dropping completed
    /// graph evidence. An aborted move may be explicitly retried from `Planned`;
    /// its deterministic id is stable because abort does not bump the route epoch.
    pub(crate) fn permits_successor(&self, next: &Self) -> bool {
        if !next.validate() || !same_move_identity(self, next) {
            return false;
        }
        let retry = self.stage == MoveStage::Aborted && next.stage == MoveStage::Planned;
        retry || (retains_completed_graphs(self, next) && stage_step_is_permitted(self, next))
    }
}

/// The deterministic move id: a digest over the partition preimage, the source
/// epoch, the target group, and the sorted graph inventory.
fn partition_move_id(
    key: &PartitionKey,
    original_epoch: u64,
    target: GroupId,
    graphs: &[String],
) -> String {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"epistemic-graph/partition-move/v1\0");
    digest.update(key.tenant.as_bytes());
    digest.update(key.range_start.to_be_bytes());
    digest.update(key.range_end.to_be_bytes());
    digest.update(original_epoch.to_be_bytes());
    digest.update(target.to_be_bytes());
    for graph in graphs {
        digest.update((graph.len() as u64).to_be_bytes());
        digest.update(graph.as_bytes());
    }
    hex::encode(digest.finalize())
}

fn graph_inventory_is_within_limits(graphs: &[String]) -> bool {
    graphs.len() <= MAX_PARTITION_MOVE_GRAPHS
        && graphs
            .iter()
            .try_fold(0usize, |total, graph| total.checked_add(graph.len()))
            .is_some_and(|total| total <= MAX_PARTITION_MOVE_GRAPH_BYTES)
}

fn graph_names_are_valid(graphs: &[String]) -> bool {
    graphs
        .iter()
        .all(|graph| !graph.is_empty() && graph.len() <= MAX_PARTITION_MOVE_GRAPH_NAME_BYTES)
}

fn is_sorted_unique(values: &[String]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

/// Both inventories are strictly sorted, and every completed graph belongs to
/// the immutable planned inventory.
fn completed_graphs_are_valid(journal: &PartitionMoveJournal) -> bool {
    is_sorted_unique(&journal.graphs)
        && is_sorted_unique(&journal.completed_graphs)
        && journal
            .completed_graphs
            .iter()
            .all(|graph| journal.graphs.binary_search(graph).is_ok())
}

fn no_graph_completed(journal: &PartitionMoveJournal) -> bool {
    journal.completed_graphs.is_empty()
}

fn every_graph_completed(journal: &PartitionMoveJournal) -> bool {
    journal.completed_graphs == journal.graphs
}

/// The completed-graph evidence each stage requires. Exhaustive over
/// [`MoveStage`]: a new stage must state its own progress rule.
fn progress_matches_stage(journal: &PartitionMoveJournal) -> bool {
    match journal.stage {
        MoveStage::Planned | MoveStage::Moving => no_graph_completed(journal),
        MoveStage::ReadyForCutover | MoveStage::CutoverCommitted | MoveStage::Completed => {
            every_graph_completed(journal)
        }
        MoveStage::Transferring | MoveStage::Aborting | MoveStage::Aborted => true,
    }
}

/// The immutable fields of a move: its id, partition, route, and inventory.
fn move_identity(
    journal: &PartitionMoveJournal,
) -> (&str, &PartitionKey, GroupId, GroupId, u64, &[String]) {
    (
        &journal.move_id,
        &journal.key,
        journal.source,
        journal.target,
        journal.original_epoch,
        &journal.graphs,
    )
}

/// Every immutable field of the move is identical in both journals.
fn same_move_identity(current: &PartitionMoveJournal, next: &PartitionMoveJournal) -> bool {
    move_identity(current) == move_identity(next)
}

/// A successor never drops completed-graph evidence.
fn retains_completed_graphs(current: &PartitionMoveJournal, next: &PartitionMoveJournal) -> bool {
    current
        .completed_graphs
        .iter()
        .all(|graph| next.completed_graphs.binary_search(graph).is_ok())
}

/// The monotonic stage transition table (self-loops are idempotent re-writes).
fn stage_step_is_permitted(current: &PartitionMoveJournal, next: &PartitionMoveJournal) -> bool {
    matches!(
        (current.stage, next.stage),
        (MoveStage::Planned, MoveStage::Planned)
            | (MoveStage::Planned, MoveStage::Moving)
            | (MoveStage::Planned, MoveStage::Transferring)
            | (MoveStage::Planned, MoveStage::Aborting)
            | (MoveStage::Moving, MoveStage::Moving)
            | (MoveStage::Moving, MoveStage::Transferring)
            | (MoveStage::Moving, MoveStage::Aborting)
            | (MoveStage::Transferring, MoveStage::Transferring)
            | (MoveStage::Transferring, MoveStage::ReadyForCutover)
            | (MoveStage::Transferring, MoveStage::Aborting)
            | (MoveStage::ReadyForCutover, MoveStage::ReadyForCutover)
            | (MoveStage::ReadyForCutover, MoveStage::CutoverCommitted)
            | (MoveStage::ReadyForCutover, MoveStage::Aborting)
            | (MoveStage::CutoverCommitted, MoveStage::CutoverCommitted)
            | (MoveStage::CutoverCommitted, MoveStage::Completed)
            | (MoveStage::Aborting, MoveStage::Aborting)
            | (MoveStage::Aborting, MoveStage::CutoverCommitted)
            | (MoveStage::Aborting, MoveStage::Aborted)
            | (MoveStage::Completed, MoveStage::Completed)
            | (MoveStage::Aborted, MoveStage::Aborted)
    )
}

/// Which of the three crash-reachable placement fences a partition row shows
/// for one move journal. They are mutually exclusive for a valid journal
/// (`source != target`).
#[derive(Clone, Copy)]
pub(crate) struct PlacementFence {
    /// The row still routes to the source at the planned epoch, not yet moving.
    pub(crate) active_source: bool,
    /// The row routes to the source at the planned epoch and is moving to the target.
    pub(crate) moving_source: bool,
    /// The fenced cutover committed: the row routes to the target at a later epoch.
    pub(crate) active_target: bool,
}

impl PlacementFence {
    pub(crate) fn observe(entry: &PlacementEntry, journal: &PartitionMoveJournal) -> Self {
        let on_source = entry.group == journal.source && entry.epoch == journal.original_epoch;
        Self {
            active_source: on_source && entry.state == PartitionState::Active,
            moving_source: on_source
                && entry.state
                    == (PartitionState::Moving {
                        target: journal.target,
                    }),
            active_target: entry.group == journal.target
                && entry.epoch > journal.original_epoch
                && entry.state == PartitionState::Active,
        }
    }

    fn source_route(self) -> bool {
        self.active_source || self.moving_source
    }
}

/// These are the only stage/placement combinations a crash can leave. In
/// particular, a transferring journal cannot legitimately be behind an
/// already-committed cutover: ReadyForCutover is persisted first. Exhaustive
/// over [`MoveStage`].
fn stage_matches_fence(stage: MoveStage, fence: PlacementFence) -> bool {
    match stage {
        MoveStage::Planned => fence.source_route(),
        MoveStage::Moving | MoveStage::Transferring => fence.moving_source,
        MoveStage::ReadyForCutover => fence.moving_source || fence.active_target,
        MoveStage::CutoverCommitted => fence.active_target,
        // An abort can race the irreversible fence. Recovery recognizes the
        // target route and rolls forward rather than attempting rollback.
        MoveStage::Aborting => fence.source_route() || fence.active_target,
        MoveStage::Completed | MoveStage::Aborted => false,
    }
}

/// Each active journal claims a distinct partition, has a placement row, and
/// agrees with that row's fence.
pub(super) fn validate_active_journals(
    entries: &[PlacementEntry],
    active: &[PartitionMoveJournal],
) -> Result<(), String> {
    let mut claimed = HashSet::new();
    for journal in active {
        let key = &journal.key;
        if !claimed.insert((key.tenant.as_str(), key.range_start, key.range_end)) {
            return Err("multiple active move journals claim one partition".to_string());
        }
        let entry = entries
            .iter()
            .find(|entry| entry.key == journal.key)
            .ok_or_else(|| "active move journal has no placement entry".to_string())?;
        if !stage_matches_fence(journal.stage, PlacementFence::observe(entry, journal)) {
            return Err("move journal and placement fence disagree".to_string());
        }
    }
    Ok(())
}

fn drives_moving_entry(
    journal: &PartitionMoveJournal,
    entry: &PlacementEntry,
    target: GroupId,
) -> bool {
    journal.key == entry.key
        && journal.source == entry.group
        && journal.target == target
        && journal.original_epoch == entry.epoch
}

/// Every partition stuck in `Moving` has exactly one durable recovery driver.
pub(super) fn validate_moving_partitions(
    entries: &[PlacementEntry],
    active: &[PartitionMoveJournal],
) -> Result<(), String> {
    for entry in entries {
        let PartitionState::Moving { target } = entry.state else {
            continue;
        };
        let drivers = active
            .iter()
            .filter(|journal| drives_moving_entry(journal, entry, target))
            .count();
        if drivers != 1 {
            return Err("moving partition has no unique durable recovery journal".to_string());
        }
    }
    Ok(())
}
