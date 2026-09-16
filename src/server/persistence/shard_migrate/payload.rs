//! Graph-scoped payload replacement under the admitted graft reservation.

use super::MigrationReport;
#[cfg(feature = "security")]
use crate::redb_store::PROVENANCE_ANCHOR_MEMBERS;
use crate::redb_store::{capacity_lease, development_lane, work_item_capability};
use crate::redb_store::{
    AUDIT, CHANGE_BLOBS, CHANGE_CURSORS, CHANGE_ENVELOPES, CHANGE_EVIDENCE, CHANGE_FEATURES,
    CHANGE_LINEAGE, CHANGE_POLICIES, CONTENT_VERSIONS, EDGES, LEDGER, NODES,
    RESOURCE_ANTI_AFFINITY, RESOURCE_CONCURRENCY, RESOURCE_DISK_POLICIES, RESOURCE_EXCLUSIVITY,
    RESOURCE_FAIRNESS, RESOURCE_HOSTS, RESOURCE_RESERVATIONS, RESOURCE_RESERVATION_ATTEMPTS,
    RESOURCE_RESERVATION_TENANT_INDEX, SEMANTIC, WORK_ITEM_COMMAND_SEQUENCE,
};
use eg_storage::{GraphShardOwner, OwnerRowScope, OwnerRowScopeStart, ScopedRead};
use eg_transaction::{GraftOwnerWrite, OwnerPayloadTransfer, OwnerPayloadWrite};
use redb::TableDefinition;

/// Copy the graph-shard payload under the destination's exact graft
/// reservation.  The callback is invoked by the mutation kernel while the
/// destination ledger transaction is still open; no pre-graft maintenance row
/// is created.
pub(crate) struct GraphShardPayloadTransfer<'a> {
    report: Option<&'a mut MigrationReport>,
}

impl<'a> GraphShardPayloadTransfer<'a> {
    pub(crate) fn for_migration(report: &'a mut MigrationReport) -> Self {
        Self {
            report: Some(report),
        }
    }

    pub(crate) fn without_report() -> Self {
        Self { report: None }
    }
}

impl OwnerPayloadTransfer<GraphShardOwner, GraphShardOwner> for GraphShardPayloadTransfer<'_> {
    fn transfer_owner_payload(
        &mut self,
        source: &ScopedRead<'_, GraphShardOwner>,
        destination: &GraftOwnerWrite<'_, '_, GraphShardOwner>,
        _identity: &eg_types::MutationScopeIdentity,
    ) -> Result<u64, String> {
        clear_every_owner_table(destination)?;
        let mut discarded = MigrationReport::default();
        let report = self.report.as_deref_mut().unwrap_or(&mut discarded);
        let before = counted_owner_rows(report)?;
        copy_every_owner_table(source, destination, report)?;
        let after = counted_owner_rows(report)?;
        after
            .checked_sub(before)
            .ok_or_else(|| "owner-row migration counters moved backwards".to_string())
    }
}

/// Remove a previous partial payload stage.  The owner accessor is already
/// restricted to the destination graph, and the surrounding kernel transaction
/// has proved the graft reservation before this sweep is reachable.
fn clear_every_owner_table(write: &impl OwnerPayloadWrite) -> Result<(), String> {
    clear_core_and_provenance_rows(write)?;
    clear_resource_rows(write)?;
    clear_change_authority_rows(write)?;
    clear_capacity_lease_rows(write)?;
    clear_work_item_capability_rows(write)?;
    clear_development_lane_rows(write)?;
    Ok(())
}

fn clear_scoped_table<K, V>(
    write: &impl OwnerPayloadWrite,
    table: TableDefinition<'static, K, V>,
) -> Result<(), String>
where
    K: redb::Key + 'static,
    for<'k> K::SelfType<'k>: OwnerRowScope,
    V: redb::Value + 'static,
{
    write.open_scoped_table(table)?.purge_scope_rows()
}

fn clear_core_and_provenance_rows(write: &impl OwnerPayloadWrite) -> Result<(), String> {
    clear_scoped_table(write, NODES)?;
    clear_scoped_table(write, EDGES)?;
    clear_scoped_table(write, LEDGER)?;
    clear_scoped_table(write, SEMANTIC)?;
    #[cfg(feature = "security")]
    {
        clear_scoped_table(write, AUDIT)?;
        clear_scoped_table(write, PROVENANCE_ANCHOR_MEMBERS)?;
    }
    clear_scoped_table(write, WORK_ITEM_COMMAND_SEQUENCE)?;
    Ok(())
}

fn clear_resource_rows(write: &impl OwnerPayloadWrite) -> Result<(), String> {
    clear_scoped_table(write, RESOURCE_RESERVATIONS)?;
    clear_scoped_table(write, RESOURCE_RESERVATION_TENANT_INDEX)?;
    clear_scoped_table(write, RESOURCE_RESERVATION_ATTEMPTS)?;
    clear_scoped_table(write, RESOURCE_HOSTS)?;
    clear_scoped_table(write, RESOURCE_EXCLUSIVITY)?;
    clear_scoped_table(write, RESOURCE_FAIRNESS)?;
    clear_scoped_table(write, RESOURCE_CONCURRENCY)?;
    clear_scoped_table(write, RESOURCE_ANTI_AFFINITY)?;
    clear_scoped_table(write, RESOURCE_DISK_POLICIES)?;
    Ok(())
}

fn clear_change_authority_rows(write: &impl OwnerPayloadWrite) -> Result<(), String> {
    clear_scoped_table(write, CHANGE_ENVELOPES)?;
    clear_scoped_table(write, CONTENT_VERSIONS)?;
    clear_scoped_table(write, CHANGE_CURSORS)?;
    clear_scoped_table(write, CHANGE_BLOBS)?;
    clear_scoped_table(write, CHANGE_FEATURES)?;
    clear_scoped_table(write, CHANGE_EVIDENCE)?;
    clear_scoped_table(write, CHANGE_POLICIES)?;
    clear_scoped_table(write, CHANGE_LINEAGE)?;
    Ok(())
}

fn clear_capacity_lease_rows(write: &impl OwnerPayloadWrite) -> Result<(), String> {
    clear_scoped_table(write, capacity_lease::CELLS)?;
    clear_scoped_table(write, capacity_lease::LEASES)?;
    clear_scoped_table(write, capacity_lease::USAGE)?;
    clear_scoped_table(write, capacity_lease::IDEMPOTENCY)?;
    Ok(())
}

fn clear_work_item_capability_rows(write: &impl OwnerPayloadWrite) -> Result<(), String> {
    clear_scoped_table(write, work_item_capability::CAPABILITIES)?;
    clear_scoped_table(write, work_item_capability::INVOCATIONS)?;
    clear_scoped_table(write, work_item_capability::NATIVE_WORK_ITEMS)?;
    Ok(())
}

fn clear_development_lane_rows(write: &impl OwnerPayloadWrite) -> Result<(), String> {
    clear_scoped_table(write, development_lane::HOLDS)?;
    clear_scoped_table(write, development_lane::TENANT_INDEX)?;
    clear_scoped_table(write, development_lane::LANE_INDEX)?;
    clear_scoped_table(write, development_lane::REPOSITORY_BRANCH_INDEX)?;
    clear_scoped_table(write, development_lane::WORKTREE_INDEX)?;
    clear_scoped_table(write, development_lane::WORK_ITEM_INDEX)?;
    clear_scoped_table(write, development_lane::COUNTERS)?;
    clear_scoped_table(write, development_lane::PRESSURE_INDEX)?;
    clear_scoped_table(write, development_lane::POLICIES)?;
    clear_scoped_table(write, development_lane::INVOCATIONS)?;
    Ok(())
}

/// Every scope-prefixed owner table of one graph, in three cohesive passes.
fn copy_every_owner_table(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
    report: &mut MigrationReport,
) -> Result<(), String> {
    copy_core_graph_rows(read, write, report)?;
    copy_change_authority_rows(read, write, report)?;
    copy_capability_and_resource_rows(read, write, report)
}

/// The canonical payload-transfer result is the number of owner rows copied.
/// Keep it derived from the same counters that the migration report exposes so
/// the graft callback cannot invent a zero (or maintain a second row counter).
fn counted_owner_rows(report: &MigrationReport) -> Result<u64, String> {
    let mut total = 0_u64;
    for rows in [
        report.nodes,
        report.edges,
        report.ledger,
        report.semantic,
        report.audit,
        report.auxiliary,
        report.capability_and_resource,
    ] {
        total = total
            .checked_add(rows)
            .ok_or_else(|| "owner-row migration counters overflowed".to_string())?;
    }
    Ok(total)
}

/// Copy one scope-prefixed owner table's rows for the graph both sides are bound to.
///
/// The routing predicate every per-table copy used to carry is gone: the read is
/// bounded to one graph by its capability, so `scope_rows` yields that graph's rows and
/// no other's, and the destination write refuses a key that names anything else. Values
/// are re-inserted exactly as stored — never decoded, re-derived or re-sealed.
fn copy_scoped_table<K, V>(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
    table: TableDefinition<'static, K, V>,
) -> Result<u64, String>
where
    K: redb::Key + 'static,
    for<'k> K::SelfType<'k>: OwnerRowScope + OwnerRowScopeStart<'k>,
    V: redb::Value + 'static,
{
    let source = read.scoped_owner_table(table)?;
    let mut destination = write.open_scoped_table(table)?;
    let mut rows = 0u64;
    for row in source.scope_rows()? {
        let (key, value) = row?;
        destination.insert(key.value(), value.value())?;
        rows = rows.saturating_add(1);
    }
    Ok(rows)
}

/// nodes + edges + ledger + semantic_store + audit_chain — the tables the report
/// counts individually.
fn copy_core_graph_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
    report: &mut MigrationReport,
) -> Result<(), String> {
    report.nodes += copy_scoped_table(read, write, NODES)?;
    report.edges += copy_scoped_table(read, write, EDGES)?;
    report.ledger += copy_scoped_table(read, write, LEDGER)?;
    report.semantic += copy_scoped_table(read, write, SEMANTIC)?;
    report.audit += copy_scoped_table(read, write, AUDIT)?;
    Ok(())
}

/// The 8 governed ChangeEnvelope tables, in two passes so neither unit's complexity is
/// dominated by a straight-line chain of fallible calls.
fn copy_change_authority_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
    report: &mut MigrationReport,
) -> Result<(), String> {
    report.auxiliary += copy_change_core_rows(read, write)?;
    report.auxiliary += copy_change_side_rows(read, write)?;
    Ok(())
}

/// change_envelopes + content_versions + change_cursors + change_blobs.
fn copy_change_core_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
) -> Result<u64, String> {
    let mut rows = copy_scoped_table(read, write, CHANGE_ENVELOPES)?;
    rows += copy_scoped_table(read, write, CONTENT_VERSIONS)?;
    rows += copy_scoped_table(read, write, CHANGE_CURSORS)?;
    rows += copy_scoped_table(read, write, CHANGE_BLOBS)?;
    Ok(rows)
}

/// change_features + change_evidence + change_policies + change_lineage.
fn copy_change_side_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
) -> Result<u64, String> {
    let mut rows = copy_scoped_table(read, write, CHANGE_FEATURES)?;
    rows += copy_scoped_table(read, write, CHANGE_EVIDENCE)?;
    rows += copy_scoped_table(read, write, CHANGE_POLICIES)?;
    rows += copy_scoped_table(read, write, CHANGE_LINEAGE)?;
    Ok(rows)
}

// ── BUG-CX-016 / BUG-CX-054 / undocumented-gap tables ───────────────────────
//
// Everything below was found missing from this file by the mechanical table-vs-migrate
// inventory an earlier lane ran: every table constant reachable from `redb_store.rs`,
// `capacity_lease.rs`, `development_lane.rs` and `work_item_capability.rs`,
// cross-checked against what this file actually moved. `provenance_anchor_members`
// (BUG-CX-016), all 9 `RESOURCE_*` tables + `development_lane`'s full 10 +
// `NATIVE_WORK_ITEMS` (BUG-CX-054), and — not named in any existing bug — the 4
// `capacity_lease` tables and the 2 remaining `work_item_capability` tables are the
// SAME "a table gets a writer and never gets a migrator" defect class, in two more
// subsystems. Under the kernel the census itself is the backstop: `Shard::open`
// materializes every declared table, so a table missing HERE is the only way a row can
// still be left behind, which is why the list stays grouped and readable.

/// Every table found missing from `migrate_shards` beyond the pre-existing core/change
/// coverage, grouped as its own pass so a reshard cannot drop provenance-anchor,
/// resource-reservation, development-lane, capacity-lease or work-item-capability
/// authority the way it silently did before this fix.
fn copy_capability_and_resource_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
    report: &mut MigrationReport,
) -> Result<(), String> {
    let mut rows = copy_provenance_and_sequence_rows(read, write)?;
    rows += copy_resource_rows(read, write)?;
    rows += copy_development_lane_rows(read, write)?;
    rows += copy_capacity_lease_rows(read, write)?;
    rows += copy_work_item_capability_rows(read, write)?;
    report.capability_and_resource += rows;
    Ok(())
}

/// provenance_anchor_members + work_item_command_sequence (BUG-CX-016/BUG-CX-054 class).
fn copy_provenance_and_sequence_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
) -> Result<u64, String> {
    let sequence = copy_scoped_table(read, write, WORK_ITEM_COMMAND_SEQUENCE)?;
    #[cfg(feature = "security")]
    let anchors = copy_scoped_table(read, write, PROVENANCE_ANCHOR_MEMBERS)?;
    #[cfg(not(feature = "security"))]
    let anchors = 0u64;
    Ok(sequence + anchors)
}

/// Every RESOURCE_* table (BUG-CX-054): reservations, tenant index, attempts, hosts,
/// exclusivity, fairness, concurrency, anti-affinity, disk policies.
fn copy_resource_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
) -> Result<u64, String> {
    Ok(copy_resource_core_rows(read, write)? + copy_resource_policy_rows(read, write)?)
}

/// resource_reservations + tenant_index + attempts + hosts (BUG-CX-054).
fn copy_resource_core_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
) -> Result<u64, String> {
    let mut rows = copy_scoped_table(read, write, RESOURCE_RESERVATIONS)?;
    rows += copy_scoped_table(read, write, RESOURCE_RESERVATION_TENANT_INDEX)?;
    rows += copy_scoped_table(read, write, RESOURCE_RESERVATION_ATTEMPTS)?;
    rows += copy_scoped_table(read, write, RESOURCE_HOSTS)?;
    Ok(rows)
}

/// resource_exclusivity + fairness + concurrency + anti_affinity + disk_policies
/// (BUG-CX-054).
fn copy_resource_policy_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
) -> Result<u64, String> {
    let mut rows = copy_scoped_table(read, write, RESOURCE_EXCLUSIVITY)?;
    rows += copy_scoped_table(read, write, RESOURCE_FAIRNESS)?;
    rows += copy_scoped_table(read, write, RESOURCE_CONCURRENCY)?;
    rows += copy_scoped_table(read, write, RESOURCE_ANTI_AFFINITY)?;
    rows += copy_scoped_table(read, write, RESOURCE_DISK_POLICIES)?;
    Ok(rows)
}

/// All 10 development_lane_* tables (BUG-CX-054/BUG-CX-096), so a reshard carries the
/// same authority the lane's own writers treat as live.
fn copy_development_lane_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
) -> Result<u64, String> {
    Ok(copy_lane_core_rows(read, write)? + copy_lane_index_rows(read, write)?)
}

/// development_lane holds + counters + pressure_index + policies + invocations
/// (BUG-CX-054/096).
fn copy_lane_core_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
) -> Result<u64, String> {
    let mut rows = copy_scoped_table(read, write, development_lane::HOLDS)?;
    rows += copy_scoped_table(read, write, development_lane::COUNTERS)?;
    rows += copy_scoped_table(read, write, development_lane::PRESSURE_INDEX)?;
    rows += copy_scoped_table(read, write, development_lane::POLICIES)?;
    rows += copy_scoped_table(read, write, development_lane::INVOCATIONS)?;
    Ok(rows)
}

/// The 5 development_lane secondary indexes: tenant, lane, repository/branch, worktree,
/// work item (BUG-CX-096).
fn copy_lane_index_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
) -> Result<u64, String> {
    let mut rows = copy_scoped_table(read, write, development_lane::TENANT_INDEX)?;
    rows += copy_scoped_table(read, write, development_lane::LANE_INDEX)?;
    rows += copy_scoped_table(read, write, development_lane::REPOSITORY_BRANCH_INDEX)?;
    rows += copy_scoped_table(read, write, development_lane::WORKTREE_INDEX)?;
    rows += copy_scoped_table(read, write, development_lane::WORK_ITEM_INDEX)?;
    Ok(rows)
}

/// capacity_cells + leases + usage + idempotency (undocumented BUG-CX-054-class gap).
fn copy_capacity_lease_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
) -> Result<u64, String> {
    let mut rows = copy_scoped_table(read, write, capacity_lease::CELLS)?;
    rows += copy_scoped_table(read, write, capacity_lease::LEASES)?;
    rows += copy_scoped_table(read, write, capacity_lease::USAGE)?;
    rows += copy_scoped_table(read, write, capacity_lease::IDEMPOTENCY)?;
    Ok(rows)
}

/// work_item_claim_capabilities + its invocations + native_work_item_authority
/// (BUG-CX-054 + undocumented gap).
fn copy_work_item_capability_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    write: &impl OwnerPayloadWrite,
) -> Result<u64, String> {
    let mut rows = copy_scoped_table(read, write, work_item_capability::CAPABILITIES)?;
    rows += copy_scoped_table(read, write, work_item_capability::INVOCATIONS)?;
    rows += copy_scoped_table(read, write, work_item_capability::NATIVE_WORK_ITEMS)?;
    Ok(rows)
}

// ── the file-wide rows ──────────────────────────────────────────────────────
