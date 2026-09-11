use super::super::{resource_decode, DurableCrypto};
use super::links::{durable_hold_bounds, text};
use super::{
    DurableLaneHold, COUNTERS, HOLDS, INVOCATIONS, LANE_INDEX, POLICIES, PRESSURE_INDEX,
    REPOSITORY_BRANCH_INDEX, TENANT_INDEX, WORKTREE_INDEX, WORK_ITEM_INDEX,
};
use crate::epistemic_operations::DevelopmentLaneHoldState;
use crate::redb_store::shard::{Shard, ShardWrite};
use eg_storage::{
    GraphShardOwner, OwnerRowScope, OwnerRowScopeStart, PhysicalWriteCapability, ScopedOwnerTable,
    ScopedOwnerTableMut,
};
use redb::{AccessGuard, Range};

/// Canonical accessors for both scoped read and scoped write views.  The
/// storage crate exposes the same operations on two concrete guard types, so
/// these macros keep the ownership-sensitive traversal in one source body.
macro_rules! lane_row_range {
    ($table:expr, $start:expr, $end:expr, read) => {{
        $table.range_inclusive($start, $end)
    }};
    ($table:expr, $start:expr, $end:expr, write) => {{
        $table.range_inclusive($start, $end)
    }};
}

macro_rules! lane_visit_scope_rows {
    ($table:expr, $visit:expr, read) => {{
        for row in $table.scope_rows()? {
            let (key, value) = row?;
            if !$visit(key.value(), value.value())? {
                break;
            }
        }
        Ok(())
    }};
    ($table:expr, $visit:expr, write) => {{
        for row in $table.scope_rows()? {
            let (key, value) = row?;
            if !$visit(key.value(), value.value())? {
                break;
            }
        }
        Ok(())
    }};
}

/// Read access to one scope-prefixed lane table, over either the scoped read of
/// it or the scoped write of it inside an admitted group.
///
/// `ScopedOwnerTable` and `ScopedOwnerTableMut` share no trait of their own, and
/// every helper below that only *reads* a lane row is reached from both the
/// query path (a `ScopedRead`) and the mutation path (a `ShardWrite` member).
/// This is exactly the role `redb::ReadableTable` played for those helpers
/// before the kernel cut, narrowed to the three accessors they use.
///
/// [`LaneRows::visit_scope_rows`] is a visitor rather than an iterator because
/// the read view's guards outlive the view and the write view's do not: one
/// `impl Iterator` cannot name both item lifetimes.
pub(crate) trait LaneRows<K, V>
where
    K: redb::Key + 'static,
    for<'k> K::SelfType<'k>: OwnerRowScope,
    V: redb::Value + 'static,
{
    /// One row of this scope.  A key naming another scope is refused.
    fn row<'k>(&self, key: K::SelfType<'k>) -> Result<Option<AccessGuard<'_, V>>, String>;

    /// Every row between two inclusive bounds of this scope.  Double-ended, so
    /// a caller wanting the greatest row of a bounded family takes the tail.
    fn row_range<'k>(
        &self,
        start: K::SelfType<'k>,
        end: K::SelfType<'k>,
    ) -> Result<Range<'_, K, V>, String>;

    /// Every row of THIS scope in key order, until `visit` answers `false`.
    fn visit_scope_rows(
        &self,
        visit: &mut dyn for<'r> FnMut(K::SelfType<'r>, V::SelfType<'r>) -> Result<bool, String>,
    ) -> Result<(), String>
    where
        for<'k> K::SelfType<'k>: OwnerRowScopeStart<'k>;
}

impl<K, V> LaneRows<K, V> for ScopedOwnerTable<K, V>
where
    K: redb::Key + 'static,
    for<'k> K::SelfType<'k>: OwnerRowScope,
    V: redb::Value + 'static,
{
    fn row<'k>(&self, key: K::SelfType<'k>) -> Result<Option<AccessGuard<'_, V>>, String> {
        self.get(key)
    }

    fn row_range<'k>(
        &self,
        start: K::SelfType<'k>,
        end: K::SelfType<'k>,
    ) -> Result<Range<'_, K, V>, String> {
        lane_row_range!(self, start, end, read)
    }

    fn visit_scope_rows(
        &self,
        visit: &mut dyn for<'r> FnMut(K::SelfType<'r>, V::SelfType<'r>) -> Result<bool, String>,
    ) -> Result<(), String>
    where
        for<'k> K::SelfType<'k>: OwnerRowScopeStart<'k>,
    {
        lane_visit_scope_rows!(self, visit, read)
    }
}

impl<'w, K, V> LaneRows<K, V> for ScopedOwnerTableMut<'w, K, V>
where
    K: redb::Key + 'static,
    for<'k> K::SelfType<'k>: OwnerRowScope,
    V: redb::Value + 'static,
{
    fn row<'k>(&self, key: K::SelfType<'k>) -> Result<Option<AccessGuard<'_, V>>, String> {
        self.get(key)
    }

    fn row_range<'k>(
        &self,
        start: K::SelfType<'k>,
        end: K::SelfType<'k>,
    ) -> Result<Range<'_, K, V>, String> {
        lane_row_range!(self, start, end, write)
    }

    fn visit_scope_rows(
        &self,
        visit: &mut dyn for<'r> FnMut(K::SelfType<'r>, V::SelfType<'r>) -> Result<bool, String>,
    ) -> Result<(), String>
    where
        for<'k> K::SelfType<'k>: OwnerRowScopeStart<'k>,
    {
        lane_visit_scope_rows!(self, visit, write)
    }
}

/// Every key of one scoped table's own scope, projected and collected before a
/// single row is removed.
///
/// A scope-bounded table has no `retain`, and a scan borrows the table it
/// walks, so "remove every row of this graph" is collect-then-remove.
pub(super) fn scope_keys<K, V, T, F>(
    table: &ScopedOwnerTableMut<'_, K, V>,
    select: F,
) -> Result<Vec<T>, String>
where
    K: redb::Key + 'static,
    for<'k> K::SelfType<'k>: OwnerRowScope + OwnerRowScopeStart<'k>,
    V: redb::Value + 'static,
    F: for<'k> Fn(K::SelfType<'k>) -> T,
{
    let mut keys = Vec::new();
    for row in table.scope_rows()? {
        let (key, _) = row?;
        keys.push(select(key.value()));
    }
    Ok(keys)
}

/// One shard operation id for a lane write, unique per ATTEMPT.
///
/// `shard::drain_batch` explains why it may not be derived from the request:
/// re-admitting the same id at a moved version fails the ledger's whole-batch
/// identity comparison rather than replaying, and this path's exactly-once is
/// already carried by the lane's own invocation row.  The process id and the
/// monotonic counter make it unique within a process; the caller's
/// authoritative clock separates one process generation from the next.
pub(super) fn lane_op_id(graph: &str, method: &str, now_ms: u64) -> String {
    static ATTEMPT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let attempt = ATTEMPT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!(
        "lane/{method}/{graph}/{now_ms}/{}/{attempt}",
        std::process::id()
    )
}

/// Every hold row of this scope must already be terminally drained.  A live or
/// retained-unpruned hold is an authority, not cache data, so the graph
/// lifecycle fails closed here rather than deleting it.
pub(super) fn require_drained_holds(
    holds: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    holds.visit_scope_rows(&mut |_: (&str, &str), value: &[u8]| {
        let row: DurableLaneHold = resource_decode(value, crypto)?;
        durable_hold_bounds(&row)?;
        if matches!(
            row.hold.state,
            DevelopmentLaneHoldState::Allocating
                | DevelopmentLaneHoldState::Active
                | DevelopmentLaneHoldState::Submitted
                | DevelopmentLaneHoldState::Released
                | DevelopmentLaneHoldState::Expired
                | DevelopmentLaneHoldState::CleanupPending
        ) || row.hold.active_count_charged
            || row.hold.retained_disk_bytes != 0
        {
            return Err("development lane graph lifecycle requires drained holds".to_string());
        }
        Ok(true)
    })
}

/// Remove every hold row of this scope.
pub(super) fn clear_lane_holds(
    holds: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    graph: &str,
) -> Result<(), String> {
    for hold_id in scope_keys(&*holds, |(_, hold_id): (&str, &str)| hold_id.to_string())? {
        holds.remove((graph, hold_id.as_str()))?;
    }
    Ok(())
}

/// Remove every lane identity/exclusivity index row for `graph`.
pub(super) fn clear_lane_identity_indexes(
    graph: &str,
    tenant_index: &mut ScopedOwnerTableMut<(&str, &str, &str), &str>,
    lane_index: &mut ScopedOwnerTableMut<(&str, &str, &str), &str>,
    branch_index: &mut ScopedOwnerTableMut<(&str, &str, &str), &str>,
    worktree_index: &mut ScopedOwnerTableMut<(&str, &str), &str>,
    work_item_index: &mut ScopedOwnerTableMut<(&str, &str, u64), &str>,
) -> Result<(), String> {
    for (tenant, hold_id) in scope_keys(
        &*tenant_index,
        |(_, tenant, hold_id): (&str, &str, &str)| (tenant.to_string(), hold_id.to_string()),
    )? {
        tenant_index.remove((graph, tenant.as_str(), hold_id.as_str()))?;
    }
    for (tenant, lane_id) in
        scope_keys(&*lane_index, |(_, tenant, lane_id): (&str, &str, &str)| {
            (tenant.to_string(), lane_id.to_string())
        })?
    {
        lane_index.remove((graph, tenant.as_str(), lane_id.as_str()))?;
    }
    for (tenant, branch) in
        scope_keys(&*branch_index, |(_, tenant, branch): (&str, &str, &str)| {
            (tenant.to_string(), branch.to_string())
        })?
    {
        branch_index.remove((graph, tenant.as_str(), branch.as_str()))?;
    }
    for worktree in scope_keys(&*worktree_index, |(_, worktree): (&str, &str)| {
        worktree.to_string()
    })? {
        worktree_index.remove((graph, worktree.as_str()))?;
    }
    for (work_item_id, attempt) in scope_keys(
        &*work_item_index,
        |(_, work_item, attempt): (&str, &str, u64)| (work_item.to_string(), attempt),
    )? {
        work_item_index.remove((graph, work_item_id.as_str(), attempt))?;
    }
    Ok(())
}

/// Remove every lane counter, pressure-index, policy and replay row for
/// `graph`, so a same-name recreation cannot inherit any of them.
pub(super) fn clear_lane_quota_and_replay_rows(
    graph: &str,
    counters: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    pressure_index: &mut ScopedOwnerTableMut<(&str, &str, &str, &str, u64, &str), u8>,
    policies: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    invocations: &mut ScopedOwnerTableMut<(&str, &str, &str), &[u8]>,
) -> Result<(), String> {
    for counter_key in scope_keys(&*counters, |(_, counter_key): (&str, &str)| {
        counter_key.to_string()
    })? {
        counters.remove((graph, counter_key.as_str()))?;
    }
    for (tenant, scope, metric, value, counter_key) in scope_keys(
        &*pressure_index,
        |(_, tenant, scope, metric, value, counter_key): (&str, &str, &str, &str, u64, &str)| {
            (
                tenant.to_string(),
                scope.to_string(),
                metric.to_string(),
                value,
                counter_key.to_string(),
            )
        },
    )? {
        pressure_index.remove((
            graph,
            tenant.as_str(),
            scope.as_str(),
            metric.as_str(),
            value,
            counter_key.as_str(),
        ))?;
    }
    for tenant in scope_keys(&*policies, |(_, tenant): (&str, &str)| tenant.to_string())? {
        policies.remove((graph, tenant.as_str()))?;
    }
    for (tenant, key) in scope_keys(&*invocations, |(_, tenant, key): (&str, &str, &str)| {
        (tenant.to_string(), key.to_string())
    })? {
        invocations.remove((graph, tenant.as_str(), key.as_str()))?;
    }
    Ok(())
}

/// Remove every lane row of `graph` from the ten tables the caller has open.
///
/// The physical half of BOTH the graph lifecycle clear and scope retirement --
/// one sweep, three openers -- so a table added to this module can never be
/// swept by one path and inherited by the other.  The lifecycle guard is
/// deliberately not here: ClearGraph/DeleteGraph must fail closed while a live
/// hold is still an authority ([`require_drained_holds`]), whereas a retired
/// generation has no claim left to protect.
#[allow(clippy::too_many_arguments)]
pub(super) fn clear_lane_graph_rows(
    graph: &str,
    holds: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    tenant_index: &mut ScopedOwnerTableMut<(&str, &str, &str), &str>,
    lane_index: &mut ScopedOwnerTableMut<(&str, &str, &str), &str>,
    branch_index: &mut ScopedOwnerTableMut<(&str, &str, &str), &str>,
    worktree_index: &mut ScopedOwnerTableMut<(&str, &str), &str>,
    work_item_index: &mut ScopedOwnerTableMut<(&str, &str, u64), &str>,
    counters: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    pressure_index: &mut ScopedOwnerTableMut<(&str, &str, &str, &str, u64, &str), u8>,
    policies: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    invocations: &mut ScopedOwnerTableMut<(&str, &str, &str), &[u8]>,
) -> Result<(), String> {
    text(graph, "lane graph").map_err(|_| "development lane graph key is invalid".to_string())?;
    clear_lane_holds(holds, graph)?;
    clear_lane_identity_indexes(
        graph,
        tenant_index,
        lane_index,
        branch_index,
        worktree_index,
        work_item_index,
    )?;
    clear_lane_quota_and_replay_rows(graph, counters, pressure_index, policies, invocations)
}

/// Clear every native lane row for a graph, in its own admitted maintenance
/// write.  A live or retained-unpruned hold is an authority, not cache data, so
/// ClearGraph/DeleteGraph must fail closed until its fenced cleanup is
/// complete.  Once all holds are terminally cleaned (or an explicitly aborted
/// tombstone), every lane table/index/counter/policy/replay row is removed in
/// the same transaction; a same-name recreation cannot inherit it.
pub(crate) fn clear_native_graph_rows(
    shard: &Shard,
    graph: &str,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let members = shard.graph_members(&[graph])?;
    let op_id = lane_op_id(graph, "clear", 0);
    let (group, batches) = shard.admit_maintenance(&members, &op_id)?;
    let write = ShardWrite::open(shard, &group, &members, &batches)?;
    let cleared = clear_native_graph_rows_in_wtx(&write, graph, crypto);
    write.finish()?;
    if let Err(error) = cleared {
        shard.mutations().abort_group(group)?;
        return Err(error);
    }
    shard.commit_drain(group, &batches, 0)
}

/// Write-side adapter used by graph Clear/Delete/checkpoint paths.  It
/// deliberately opens the complete lane table family here so callers cannot
/// clear the ordinary graph/resource rows and forget one lane index.
pub(crate) fn clear_native_graph_rows_in_wtx(
    write: &ShardWrite<'_>,
    graph: &str,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let member = write.graph(graph)?;
    let mut holds = member.open_scoped_table(HOLDS)?;
    let mut tenant_index = member.open_scoped_table(TENANT_INDEX)?;
    let mut lane_index = member.open_scoped_table(LANE_INDEX)?;
    let mut branch_index = member.open_scoped_table(REPOSITORY_BRANCH_INDEX)?;
    let mut worktree_index = member.open_scoped_table(WORKTREE_INDEX)?;
    let mut work_item_index = member.open_scoped_table(WORK_ITEM_INDEX)?;
    let mut counters = member.open_scoped_table(COUNTERS)?;
    let mut pressure_index = member.open_scoped_table(PRESSURE_INDEX)?;
    let mut policies = member.open_scoped_table(POLICIES)?;
    let mut invocations = member.open_scoped_table(INVOCATIONS)?;
    require_drained_holds(&holds, crypto)?;
    clear_lane_graph_rows(
        graph,
        &mut holds,
        &mut tenant_index,
        &mut lane_index,
        &mut branch_index,
        &mut worktree_index,
        &mut work_item_index,
        &mut counters,
        &mut pressure_index,
        &mut policies,
        &mut invocations,
    )
}

/// Variant for the compact MutationBatch path, which already owns the lane
/// hold/index/counter tables while applying the batch.  redb does not permit
/// opening the same table twice in one write transaction, so this adapter
/// opens only the remaining lane tables and reuses the existing guards.
#[allow(clippy::too_many_arguments)]
pub(crate) fn clear_native_graph_rows_in_wtx_with_lane_tables(
    write: &ShardWrite<'_>,
    graph: &str,
    holds: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    work_item_index: &mut ScopedOwnerTableMut<(&str, &str, u64), &str>,
    counters: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    pressure_index: &mut ScopedOwnerTableMut<(&str, &str, &str, &str, u64, &str), u8>,
    policies: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let member = write.graph(graph)?;
    let mut tenant_index = member.open_scoped_table(TENANT_INDEX)?;
    let mut lane_index = member.open_scoped_table(LANE_INDEX)?;
    let mut branch_index = member.open_scoped_table(REPOSITORY_BRANCH_INDEX)?;
    let mut worktree_index = member.open_scoped_table(WORKTREE_INDEX)?;
    let mut invocations = member.open_scoped_table(INVOCATIONS)?;
    require_drained_holds(holds, crypto)?;
    clear_lane_graph_rows(
        graph,
        holds,
        &mut tenant_index,
        &mut lane_index,
        &mut branch_index,
        &mut worktree_index,
        work_item_index,
        counters,
        pressure_index,
        policies,
        &mut invocations,
    )
}

/// Retire this module's rows for the capability's own graph scope.
///
/// The payload half of a scope retirement (`OwnerPayloadRetirement`): the scope
/// comes from the capability, never from an argument, so one graph's handle can
/// never sweep another's rows in the file they share.
///
/// The row work is [`clear_lane_graph_rows`], the same sweep the graph
/// lifecycle performs, reached through the capability's own scoped tables
/// instead of an admitted group member.  It carries no drained-hold guard:
/// retirement ends a generation the caller has already decided to end -- a
/// graft has copied it, a purge has authorized it -- while ClearGraph/
/// DeleteGraph must fail closed while a live hold is still an authority.
pub(crate) fn retire_graph_rows(
    write: &PhysicalWriteCapability<'_, GraphShardOwner>,
) -> Result<(), String> {
    let mut holds = write.scoped_owner_table_mut(HOLDS)?;
    let graph = holds.scope_key().to_string();
    let mut tenant_index = write.scoped_owner_table_mut(TENANT_INDEX)?;
    let mut lane_index = write.scoped_owner_table_mut(LANE_INDEX)?;
    let mut branch_index = write.scoped_owner_table_mut(REPOSITORY_BRANCH_INDEX)?;
    let mut worktree_index = write.scoped_owner_table_mut(WORKTREE_INDEX)?;
    let mut work_item_index = write.scoped_owner_table_mut(WORK_ITEM_INDEX)?;
    let mut counters = write.scoped_owner_table_mut(COUNTERS)?;
    let mut pressure_index = write.scoped_owner_table_mut(PRESSURE_INDEX)?;
    let mut policies = write.scoped_owner_table_mut(POLICIES)?;
    let mut invocations = write.scoped_owner_table_mut(INVOCATIONS)?;
    clear_lane_graph_rows(
        &graph,
        &mut holds,
        &mut tenant_index,
        &mut lane_index,
        &mut branch_index,
        &mut worktree_index,
        &mut work_item_index,
        &mut counters,
        &mut pressure_index,
        &mut policies,
        &mut invocations,
    )
}
