//! Private, graph-scoped EH-557 compute budget ledger.
//!
//! A debit is applied inside the same admitted ShardWrite as native
//! `SubmitWorkItems` rows. It measures total ingestion spend authorization;
//! short-lived `CapacityLease` rows independently fence worker concurrency.
//!
//! A page that cannot fit the remaining global budget is deferred, not a
//! transient delivery failure. The live outbox consumer must park that source
//! in durable state and reactivate it only after an explicitly authorized
//! top-up or new source snapshot. Until that transition exists, it must not
//! repeatedly lease the event until max attempts dead-letter it, and must not
//! ACK away the remaining eligible units. This module deliberately exposes no
//! public top-up method or implicit budget replenishment.

use eg_storage::{GraphShardOwner, PhysicalWriteCapability, ScopedOwnerTableMut};
use redb::TableDefinition;

use eg_types::native_control::{
    EnrichmentBudgetCheckpoint as BudgetCheckpoint, EnrichmentBudgetPark,
    EnrichmentBudgetReservation, NativeControlSchemaVersion, SubmitWorkItemsRequest,
};

use super::shard::{Shard, ShardWrite};
use super::{decode_durable, DurableCrypto};

pub(crate) const BUDGETS: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("repository_enrichment_budgets");
pub(crate) const POLICY_REVISIONS: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("repository_enrichment_policy_revisions");
pub(crate) const SUPERSESSIONS: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("repository_enrichment_supersessions");
/// One graph-wide pause per enrichment consumer. The conservative pause keeps
/// all of this graph's enrichment source events unclaimed until an authenticated
/// budget-policy revision explicitly reactivates it.
pub(crate) const PARKS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("repository_enrichment_parks");
const MAX_ENVELOPE_BYTES: usize = 512;
const MAX_ELIGIBLE_UNITS: u32 = 4096;

/// Only the engine-owned repository source commit path may construct this
/// value. A public outbox or SubmitWorkItems payload is not a seed authority.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceBudgetAuthority {
    pub(crate) tenant_id: String,
    pub(crate) source_envelope: String,
    pub(crate) snapshot_digest: String,
    pub(crate) repository_id: String,
    pub(crate) policy_digest: String,
    pub(crate) total_budget_units: u64,
    /// Source-committed ceiling. Zero on legacy rows denies reactivation.
    #[serde(default)]
    pub(crate) max_total_units: u64,
}

/// A budget seed may accompany only the engine-attested source intent in its
/// own ChangeEnvelope. The same check runs before proposal and on every Raft
/// apply, so a sealed authority cannot be attached to a different source.
pub(crate) fn validate_source_envelope(
    envelope: &eg_types::change_envelope::ChangeEnvelope,
    authority: Option<&SourceBudgetAuthority>,
) -> Result<(), String> {
    const TOPIC: &str = "repository.enrichment.pending";
    let pending: Vec<_> = envelope
        .mutation
        .outbox
        .iter()
        .filter(|intent| intent.topic == TOPIC)
        .collect();
    match (authority, pending.as_slice()) {
        (None, []) => Ok(()),
        (Some(authority), [intent]) if source_envelope_matches(envelope, authority, intent) => {
            validate_source_intent(envelope, authority, intent)
        }
        _ => Err("CONFLICT: repository enrichment source authority does not match envelope".into()),
    }
}

fn source_envelope_matches(
    envelope: &eg_types::change_envelope::ChangeEnvelope,
    authority: &SourceBudgetAuthority,
    intent: &eg_types::mutation_batch::MutationOutboxIntent,
) -> bool {
    authority.tenant_id == envelope.mutation.identity.tenant().as_str()
        && authority.source_envelope == envelope.envelope_id
        && intent.key == envelope.envelope_id
        && valid_digest(&authority.snapshot_digest)
        && !authority.repository_id.is_empty()
        && valid_digest(&authority.policy_digest)
        && authority.total_budget_units > 0
        && (authority.max_total_units == 0
            || authority.max_total_units >= authority.total_budget_units)
}

#[cfg(feature = "ast")]
fn validate_source_intent(
    envelope: &eg_types::change_envelope::ChangeEnvelope,
    authority: &SourceBudgetAuthority,
    intent: &eg_types::mutation_batch::MutationOutboxIntent,
) -> Result<(), String> {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Pending {
        budget_status: BudgetStatus,
        snapshot: crate::parser::enrichment_snapshot::EligibleSnapshot,
    }
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum BudgetStatus {
        Unreserved,
    }
    const MAX_BYTES: usize = 4 * 1024 * 1024;
    if intent.payload.len() > MAX_BYTES {
        return Err("CONFLICT: repository enrichment source intent exceeds bound".into());
    }
    let pending: Pending = eg_types::msgpack::decode_bounded(
        &intent.payload,
        eg_types::msgpack::MsgpackLimits::new(MAX_BYTES, 100_000, 64),
    )
    .map_err(|_| "CONFLICT: repository enrichment source intent is invalid")?;
    let BudgetStatus::Unreserved = pending.budget_status;
    let source = pending.snapshot;
    source
        .validate()
        .map_err(|_| "CONFLICT: repository enrichment source snapshot is invalid")?;
    let graph_matches = envelope
        .mutation
        .identity
        .scope()
        .graph_name()
        .is_some_and(|name| name.as_str() == source.graph.as_str());
    if !graph_matches
        || source.tenant_id != authority.tenant_id
        || source.source_envelope != authority.source_envelope
        || source.budget_units != authority.total_budget_units
        || source.repository_id != authority.repository_id
        || source.policy_digest != authority.policy_digest
        || source.digest().ok().as_deref() != Some(authority.snapshot_digest.as_str())
    {
        return Err("CONFLICT: repository enrichment source snapshot changed".into());
    }
    Ok(())
}

#[cfg(not(feature = "ast"))]
fn validate_source_intent(
    _envelope: &eg_types::change_envelope::ChangeEnvelope,
    _authority: &SourceBudgetAuthority,
    _intent: &eg_types::mutation_batch::MutationOutboxIntent,
) -> Result<(), String> {
    Err("CONFLICT: repository enrichment source requires AST authority".into())
}

fn valid_digest(value: &str) -> bool {
    eg_types::contract::Digest256::parse(value).is_ok()
}

/// The three persisted accounting buckets must partition the committed total.
/// Use checked arithmetic so a corrupt row cannot wrap back into balance.
fn budget_balances(row: &BudgetCheckpoint) -> bool {
    row.spent_units
        .checked_add(row.reserved_units)
        .and_then(|used| used.checked_add(row.remaining_units))
        == Some(row.total_budget_units)
}

/// Encode and seal a graph-owned budget row at the same storage boundary.
fn put_sealed_row<T: serde::Serialize>(
    rows: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    graph: &str,
    key: &str,
    value: &T,
    crypto: DurableCrypto<'_>,
    encode_error: &'static str,
) -> Result<(), String> {
    let bytes = rmp_serde::to_vec_named(value).map_err(|_| encode_error)?;
    rows.insert((graph, key), crypto.seal(&bytes).as_ref())?;
    Ok(())
}

/// Seed budget authority in the same admitted ShardWrite that stores the
/// engine-attested source envelope and outbox intent. Existing progressed rows
/// are never reset by a replay of that source commit.
pub(crate) fn seed_source_budget(
    write: &ShardWrite<'_>,
    graph: &str,
    authority: &SourceBudgetAuthority,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    if !valid_seed_authority(authority) {
        return Err("CONFLICT: repository enrichment source budget is invalid".into());
    }
    let scope = write.graph(graph)?;
    reactivation::seed_policy_revision(write, graph, authority, crypto)?;
    let mut rows = scope.open_scoped_table(BUDGETS)?;
    if let Some(existing) = rows.get((graph, authority.source_envelope.as_str()))? {
        let row: BudgetCheckpoint = decode_durable(&crypto.unseal(existing.value())?)?;
        if !valid_replayed_seed(&row, authority) {
            return Err("CONFLICT: repository enrichment source budget changed on replay".into());
        }
        return Ok(());
    }
    let row = BudgetCheckpoint {
        schema_version: 1,
        tenant_id: authority.tenant_id.clone(),
        snapshot_digest: authority.snapshot_digest.clone(),
        source_envelope: authority.source_envelope.clone(),
        next_index: 0,
        page_number: 0,
        reserved_units: 0,
        spent_units: 0,
        remaining_units: authority.total_budget_units,
        total_budget_units: authority.total_budget_units,
        last_page_key: String::new(),
    };
    put_sealed_row(
        &mut rows,
        graph,
        &authority.source_envelope,
        &row,
        crypto,
        "budget seed encode failed",
    )
}

fn valid_seed_authority(authority: &SourceBudgetAuthority) -> bool {
    !(authority.tenant_id.is_empty()
        || authority.tenant_id.len() > MAX_ENVELOPE_BYTES
        || !bounded_envelope(&authority.source_envelope)
        || !valid_digest(&authority.snapshot_digest)
        || !bounded_envelope(&authority.repository_id)
        || !valid_digest(&authority.policy_digest)
        || authority.total_budget_units == 0
        || (authority.max_total_units != 0
            && authority.max_total_units < authority.total_budget_units))
}

fn bounded_envelope(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_ENVELOPE_BYTES && !value.chars().any(char::is_control)
}

fn valid_replayed_seed(row: &BudgetCheckpoint, authority: &SourceBudgetAuthority) -> bool {
    row.schema_version == 1
        && row.tenant_id == authority.tenant_id
        && row.source_envelope == authority.source_envelope
        && row.snapshot_digest == authority.snapshot_digest
        && row.total_budget_units == authority.total_budget_units
        && budget_balances(row)
        && row.next_index <= MAX_ELIGIBLE_UNITS
        && row.page_number <= row.next_index
}

fn validate_request(
    request: &SubmitWorkItemsRequest,
    debit: &EnrichmentBudgetReservation,
) -> Result<(), String> {
    if !valid_request_identity(request, debit) || !valid_request_budget(debit) {
        return Err("INVALID_ARGUMENT: repository enrichment budget request is invalid".into());
    }
    let cost = request.requests.iter().try_fold(0_u64, |sum, child| {
        if !child.kind.starts_with("enrichment.")
            || !child.provenance_refs.contains(&debit.source_envelope)
        {
            return None;
        }
        let units = child.metadata.get("reserved_compute_units")?.as_u64()?;
        sum.checked_add(units)
    });
    if cost != Some(debit.reserve_units) {
        return Err("CONFLICT: repository enrichment admitted cost does not match budget".into());
    }
    Ok(())
}

fn valid_request_identity(
    request: &SubmitWorkItemsRequest,
    debit: &EnrichmentBudgetReservation,
) -> bool {
    debit.schema_version == NativeControlSchemaVersion::V1
        && valid_digest(&debit.snapshot_digest)
        && bounded_envelope(&debit.source_envelope)
        && debit
            .page_key
            .strip_prefix("repository-enrichment-page:")
            .is_some_and(valid_digest)
        && request.idempotency_key == debit.page_key
}

fn valid_request_budget(debit: &EnrichmentBudgetReservation) -> bool {
    !(debit.end_index <= debit.expected_next_index
        || debit.end_index > MAX_ELIGIBLE_UNITS
        || debit.expected_page_number >= MAX_ELIGIBLE_UNITS
        || debit.total_budget_units == 0
        || debit.reserve_units == 0
        || debit.reserve_units > debit.expected_remaining_units
        || debit.expected_remaining_units > debit.total_budget_units)
}

fn validate_checkpoint(
    row: &BudgetCheckpoint,
    tenant: &str,
    debit: &EnrichmentBudgetReservation,
) -> Result<(), String> {
    if !checkpoint_identity_matches(row, tenant, debit) || !checkpoint_cursor_is_valid(row) {
        return Err("CONFLICT: repository enrichment budget checkpoint is invalid".into());
    }
    Ok(())
}

fn checkpoint_identity_matches(
    row: &BudgetCheckpoint,
    tenant: &str,
    debit: &EnrichmentBudgetReservation,
) -> bool {
    row.schema_version == 1
        && row.tenant_id == tenant
        && row.snapshot_digest == debit.snapshot_digest
        && row.source_envelope == debit.source_envelope
        && row.total_budget_units == debit.total_budget_units
        && budget_balances(row)
}

fn checkpoint_cursor_is_valid(row: &BudgetCheckpoint) -> bool {
    if row.next_index > MAX_ELIGIBLE_UNITS || row.page_number > row.next_index {
        return false;
    }
    if row.page_number == 0 {
        return row.next_index == 0
            && row.reserved_units == 0
            && row.spent_units == 0
            && row.last_page_key.is_empty();
    }
    row.last_page_key
        .strip_prefix("repository-enrichment-page:")
        .is_some_and(valid_digest)
}

/// Return `None` only for an exact page replay. Every other stale state fails
/// closed, including a replay key paired with changed budget accounting.
fn advance_checkpoint(
    prior: Option<BudgetCheckpoint>,
    tenant: &str,
    debit: &EnrichmentBudgetReservation,
) -> Result<Option<BudgetCheckpoint>, String> {
    if !valid_request_budget(debit) {
        return Err("CONFLICT: repository enrichment budget admission is invalid".into());
    }
    let row = if let Some(mut prior) = prior {
        validate_checkpoint(&prior, tenant, debit)?;
        if same_page_replay(&prior, debit) {
            return Ok(None);
        }
        if prior.next_index != debit.expected_next_index
            || prior.page_number != debit.expected_page_number
            || prior.remaining_units != debit.expected_remaining_units
        {
            return Err("CONFLICT: repository enrichment budget CAS is stale".into());
        }
        prior.next_index = debit.end_index;
        prior.page_number += 1;
        prior.reserved_units = prior
            .reserved_units
            .checked_add(debit.reserve_units)
            .ok_or("CONFLICT: repository enrichment budget overflow")?;
        prior.remaining_units -= debit.reserve_units;
        prior.last_page_key = debit.page_key.clone();
        prior
    } else {
        // A public SubmitWorkItems request must not mint its own spending
        // authority. The source envelope commit must first seed this row from
        // engine-attested snapshot evidence in its own transaction.
        return Err("CONFLICT: repository enrichment budget authority is absent".into());
    };
    validate_checkpoint(&row, tenant, debit)?;
    Ok(Some(row))
}

fn same_page_replay(row: &BudgetCheckpoint, debit: &EnrichmentBudgetReservation) -> bool {
    row.last_page_key == debit.page_key
        && row.next_index == debit.end_index
        && row.page_number == debit.expected_page_number + 1
        && row.remaining_units == debit.expected_remaining_units - debit.reserve_units
}

/// Debit global budget atomically with the caller's native WorkItem writes.
/// Existing exact page identity is replay-safe; stale CAS and over-budget
/// requests abort the enclosing redb transaction.
pub(crate) fn reserve_submit_page(
    write: &ShardWrite<'_>,
    graph: &str,
    request: &SubmitWorkItemsRequest,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let Some(debit) = request.enrichment_budget.as_ref() else {
        return Ok(());
    };
    validate_request(request, debit)?;
    let scope = write.graph(graph)?;
    ensure_active_source(scope, graph, &debit.source_envelope)?;
    let mut rows = scope.open_scoped_table(BUDGETS)?;
    let prior: Option<BudgetCheckpoint> = rows
        .get((graph, debit.source_envelope.as_str()))?
        .map(|row| decode_durable(&crypto.unseal(row.value())?))
        .transpose()?;
    let Some(row) = advance_checkpoint(prior, &request.context.tenant_id, debit)? else {
        return Ok(());
    };
    put_sealed_row(
        &mut rows,
        graph,
        &debit.source_envelope,
        &row,
        crypto,
        "budget checkpoint encode failed",
    )
}

fn ensure_active_source(
    scope: &eg_transaction::AdmittedOwnerWrite<'_, GraphShardOwner>,
    graph: &str,
    source_envelope: &str,
) -> Result<(), String> {
    if scope
        .open_scoped_table(SUPERSESSIONS)?
        .get((graph, source_envelope))?
        .is_some()
    {
        return Err("CONFLICT: repository enrichment source was superseded".into());
    }
    Ok(())
}

pub(crate) fn read(
    shard: &Shard,
    graph: &str,
    source_envelope: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<BudgetCheckpoint>, String> {
    if source_envelope.is_empty() || source_envelope.len() > MAX_ENVELOPE_BYTES {
        return Err("INVALID_ARGUMENT: repository enrichment envelope is invalid".into());
    }
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    let rows = read.scoped_owner_table(BUDGETS)?;
    rows.get((graph, source_envelope))?
        .map(|row| decode_durable(&crypto.unseal(row.value())?))
        .transpose()
}

fn validate_park(park: &EnrichmentBudgetPark, budget: &BudgetCheckpoint) -> Result<(), String> {
    if !park_identity_matches(park, budget) || !park_requires_more_budget(park, budget) {
        return Err(
            "CONFLICT: repository enrichment park does not match underfunded authority".into(),
        );
    }
    Ok(())
}

fn park_identity_matches(park: &EnrichmentBudgetPark, budget: &BudgetCheckpoint) -> bool {
    budget.schema_version == 1
        && budget_balances(budget)
        && budget.page_number <= budget.next_index
        && park.schema_version == 1
        && park.source_envelope == budget.source_envelope
        && park.snapshot_digest == budget.snapshot_digest
        && park.next_index == budget.next_index
        && park.page_number == budget.page_number
        && park.remaining_units == budget.remaining_units
}

fn park_requires_more_budget(park: &EnrichmentBudgetPark, budget: &BudgetCheckpoint) -> bool {
    park.required_units > budget.remaining_units
        && park.required_units != 0
        && valid_digest(&park.policy_digest)
        && park.parked_at_ms != 0
}

/// Park before releasing a held outbox lease. A later claim loop must read
/// this marker first and refuse to claim any enrichment event on this graph.
/// No reactivation method is provided without a verified policy revision.
pub(crate) fn park_underfunded(
    write: &ShardWrite<'_>,
    graph: &str,
    park: &EnrichmentBudgetPark,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let scope = write.graph(graph)?;
    ensure_active_source(scope, graph, &park.source_envelope)?;
    let budget: BudgetCheckpoint = scope
        .open_scoped_table(BUDGETS)?
        .get((graph, park.source_envelope.as_str()))?
        .map(|row| decode_durable(&crypto.unseal(row.value())?))
        .transpose()?
        .ok_or("CONFLICT: repository enrichment budget authority is absent")?;
    validate_park(park, &budget)?;
    let mut rows = scope.open_scoped_table(PARKS)?;
    if let Some(existing) = rows.get(graph)? {
        let prior: EnrichmentBudgetPark = decode_durable(&crypto.unseal(existing.value())?)?;
        if prior.source_envelope == park.source_envelope
            && prior.snapshot_digest == park.snapshot_digest
            && prior.next_index == park.next_index
            && prior.page_number == park.page_number
            && prior.remaining_units == park.remaining_units
            && prior.required_units == park.required_units
            && prior.policy_digest == park.policy_digest
        {
            return Ok(());
        }
        return Err("CONFLICT: graph already has a different parked enrichment source".into());
    }
    let bytes = rmp_serde::to_vec_named(park).map_err(|_| "budget park encode failed")?;
    rows.insert(graph, crypto.seal(&bytes).as_ref())?;
    Ok(())
}

mod reactivation;
pub(crate) use reactivation::is_superseded;
pub(crate) use reactivation::read_policy_revision;
#[cfg(feature = "raft")]
pub(crate) use reactivation::stage_parked_reactivation;
#[cfg(feature = "raft")]
pub(crate) use reactivation::verify_reactivation_replay;
pub(crate) use reactivation::RepositoryEnrichmentPolicyRevision;

/// Read before each outbox claim, including the first claim after restart.
pub(crate) fn read_park(
    shard: &Shard,
    graph: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<EnrichmentBudgetPark>, String> {
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    read.scoped_owner_table(PARKS)?
        .get(graph)?
        .map(|row| decode_durable(&crypto.unseal(row.value())?))
        .transpose()
}

pub(crate) fn clear_graph_rows(write: &ShardWrite<'_>, graph: &str) -> Result<(), String> {
    write
        .graph(graph)?
        .open_scoped_table(POLICY_REVISIONS)?
        .purge_scope_rows()?;
    write
        .graph(graph)?
        .open_scoped_table(SUPERSESSIONS)?
        .purge_scope_rows()?;
    write
        .graph(graph)?
        .open_scoped_table(PARKS)?
        .purge_scope_rows()?;
    write
        .graph(graph)?
        .open_scoped_table(BUDGETS)?
        .purge_scope_rows()
}

pub(crate) fn retire_graph_rows(
    write: &PhysicalWriteCapability<'_, GraphShardOwner>,
) -> Result<(), String> {
    write
        .scoped_owner_table_mut(POLICY_REVISIONS)?
        .purge_scope_rows()?;
    write
        .scoped_owner_table_mut(SUPERSESSIONS)?
        .purge_scope_rows()?;
    write.scoped_owner_table_mut(PARKS)?.purge_scope_rows()?;
    write.scoped_owner_table_mut(BUDGETS)?.purge_scope_rows()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn debit(
        start: u32,
        page: u32,
        remaining: u64,
        end: u32,
        cost: u64,
    ) -> EnrichmentBudgetReservation {
        EnrichmentBudgetReservation {
            schema_version: NativeControlSchemaVersion::V1,
            snapshot_digest: "a".repeat(64),
            source_envelope: "repository-index:one".into(),
            page_key: format!("repository-enrichment-page:{}", "b".repeat(64)),
            expected_next_index: start,
            end_index: end,
            expected_page_number: page,
            expected_remaining_units: remaining,
            reserve_units: cost,
            total_budget_units: 10,
        }
    }

    #[test]
    fn global_budget_cas_admits_once_and_refuses_stale_or_over_budget() {
        let first = debit(0, 0, 10, 1, 6);
        assert!(advance_checkpoint(None, "tenant", &first).is_err());
        let seeded = BudgetCheckpoint {
            schema_version: 1,
            tenant_id: "tenant".into(),
            snapshot_digest: first.snapshot_digest.clone(),
            source_envelope: first.source_envelope.clone(),
            next_index: 0,
            page_number: 0,
            reserved_units: 0,
            spent_units: 0,
            remaining_units: 10,
            total_budget_units: 10,
            last_page_key: String::new(),
        };
        let row = advance_checkpoint(Some(seeded), "tenant", &first)
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                row.next_index,
                row.page_number,
                row.reserved_units,
                row.remaining_units
            ),
            (1, 1, 6, 4)
        );
        assert!(advance_checkpoint(Some(row.clone()), "tenant", &first)
            .unwrap()
            .is_none());
        let mut stale = debit(0, 0, 10, 2, 6);
        stale.page_key = format!("repository-enrichment-page:{}", "c".repeat(64));
        assert!(advance_checkpoint(Some(row.clone()), "tenant", &stale).is_err());
        let over_budget = debit(1, 1, 4, 2, 5);
        assert!(advance_checkpoint(Some(row.clone()), "tenant", &over_budget).is_err());
        assert!(advance_checkpoint(Some(row), "other-tenant", &debit(1, 1, 4, 2, 4)).is_err());
    }

    #[test]
    fn only_exact_underfunded_checkpoint_can_park() {
        let budget = BudgetCheckpoint {
            schema_version: 1,
            tenant_id: "tenant".into(),
            snapshot_digest: "a".repeat(64),
            source_envelope: "repository-index:one".into(),
            next_index: 7,
            page_number: 2,
            reserved_units: 6,
            spent_units: 0,
            remaining_units: 4,
            total_budget_units: 10,
            last_page_key: format!("repository-enrichment-page:{}", "b".repeat(64)),
        };
        let park = EnrichmentBudgetPark {
            schema_version: 1,
            source_envelope: budget.source_envelope.clone(),
            snapshot_digest: budget.snapshot_digest.clone(),
            next_index: budget.next_index,
            page_number: budget.page_number,
            remaining_units: budget.remaining_units,
            required_units: 5,
            policy_digest: "c".repeat(64),
            parked_at_ms: 1,
        };
        validate_park(&park, &budget).unwrap();
        let mut affordable = park.clone();
        affordable.required_units = 4;
        assert!(validate_park(&affordable, &budget).is_err());
        let mut stale = park;
        stale.next_index += 1;
        assert!(validate_park(&stale, &budget).is_err());
    }

    #[test]
    fn parked_marker_is_durable_before_a_lease_can_be_released() {
        let path = crate::redb_store::temp_path("eg-enrichment-park", "durable");
        let shard = Shard::open(&path).unwrap();
        let graph = "park-graph";
        let authority = SourceBudgetAuthority {
            tenant_id: "tenant".into(),
            source_envelope: "repository-index:one".into(),
            snapshot_digest: "a".repeat(64),
            repository_id: "repository".into(),
            policy_digest: "b".repeat(64),
            total_budget_units: 10,
            max_total_units: 10,
        };
        let commit = |tag: &str, apply: &dyn Fn(&ShardWrite<'_>) -> Result<(), String>| {
            let members = shard.graph_members(&[graph]).unwrap();
            let (group, batches) = shard.admit_maintenance(&members, tag).unwrap();
            let write = ShardWrite::open(&shard, &group, &members, &batches).unwrap();
            apply(&write).unwrap();
            write.finish().unwrap();
            shard.commit_drain(group, &batches, 1).unwrap();
        };
        commit("enrichment-park/seed", &|write| {
            seed_source_budget(write, graph, &authority, DurableCrypto::none())
        });
        let park = EnrichmentBudgetPark {
            schema_version: 1,
            source_envelope: authority.source_envelope.clone(),
            snapshot_digest: authority.snapshot_digest.clone(),
            next_index: 0,
            page_number: 0,
            remaining_units: 10,
            required_units: 11,
            policy_digest: "b".repeat(64),
            parked_at_ms: 2,
        };
        commit("enrichment-park/park", &|write| {
            park_underfunded(write, graph, &park, DurableCrypto::none())
        });
        assert_eq!(
            read_park(&shard, graph, DurableCrypto::none()).unwrap(),
            Some(park)
        );
        drop(shard);
        let reopened = Shard::open(&path).unwrap();
        assert!(read_park(&reopened, graph, DurableCrypto::none())
            .unwrap()
            .is_some());
        drop(reopened);
        let _ = std::fs::remove_file(path);
    }
}
