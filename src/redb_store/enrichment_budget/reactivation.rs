//! Checked, uncommitted budget-row transition for EH-557 reactivation.

use super::*;

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Supersession {
    schema_version: u8,
    old_source_envelope: String,
    old_snapshot_digest: String,
    new_source_envelope: String,
    new_snapshot_digest: String,
    replacement_policy_digest: String,
}

/// Source-scoped policy authority. The seed is committed with the immutable
/// source intent; each accepted top-up creates the next source's revision.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryEnrichmentPolicyRevision {
    pub(crate) schema_version: u8,
    pub(crate) tenant_id: String,
    pub(crate) graph: String,
    pub(crate) repository_id: String,
    pub(crate) source_envelope: String,
    pub(crate) snapshot_digest: String,
    pub(crate) policy_digest: String,
    pub(crate) total_budget_units: u64,
    /// Immutable source-committed operator ceiling; legacy rows decode as
    /// zero and cannot authorize a top-up.
    #[serde(default)]
    pub(crate) max_total_units: u64,
    pub(crate) sequence: u64,
    pub(crate) prior_source_envelope: Option<String>,
    pub(crate) prior_snapshot_digest: Option<String>,
    pub(crate) prior_policy_digest: Option<String>,
    pub(crate) caller_subject: Option<String>,
    pub(crate) verified_action: Option<String>,
    pub(crate) idempotency_key: Option<String>,
}

/// Read the committed source policy. Caller-supplied proposal bytes never
/// stand in for this row at the producer boundary.
pub(crate) fn read_policy_revision(
    shard: &Shard,
    graph: &str,
    source_envelope: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<RepositoryEnrichmentPolicyRevision>, String> {
    if source_envelope.is_empty() || source_envelope.len() > MAX_ENVELOPE_BYTES {
        return Err("INVALID_ARGUMENT: repository enrichment source is invalid".into());
    }
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    let rows = read.scoped_owner_table(POLICY_REVISIONS)?;
    rows.get((graph, source_envelope))?
        .map(|row| decode_durable(&crypto.unseal(row.value())?))
        .transpose()
}

pub(super) fn seed_policy_revision(
    write: &ShardWrite<'_>,
    graph: &str,
    authority: &SourceBudgetAuthority,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let scope = write.graph(graph)?;
    let mut rows = scope.open_scoped_table(POLICY_REVISIONS)?;
    let proposed = RepositoryEnrichmentPolicyRevision {
        schema_version: 1,
        tenant_id: authority.tenant_id.clone(),
        graph: graph.to_string(),
        repository_id: authority.repository_id.clone(),
        source_envelope: authority.source_envelope.clone(),
        snapshot_digest: authority.snapshot_digest.clone(),
        policy_digest: authority.policy_digest.clone(),
        total_budget_units: authority.total_budget_units,
        max_total_units: authority.max_total_units,
        sequence: 0,
        prior_source_envelope: None,
        prior_snapshot_digest: None,
        prior_policy_digest: None,
        caller_subject: None,
        verified_action: None,
        idempotency_key: None,
    };
    if let Some(existing) = rows.get((graph, authority.source_envelope.as_str()))? {
        let current: RepositoryEnrichmentPolicyRevision =
            decode_durable(&crypto.unseal(existing.value())?)?;
        if current != proposed {
            return Err("CONFLICT: repository enrichment source policy changed on replay".into());
        }
        return Ok(());
    }
    put_sealed_row(
        &mut rows,
        graph,
        &authority.source_envelope,
        &proposed,
        crypto,
        "repository enrichment source policy encode failed",
    )
}

/// A node may have an old local outbox delivery after a Raft leader changed.
/// The graph's replicated owner rows prove that source was replaced; its local
/// consumer may then acknowledge the stale event without spending or parking.
pub(crate) fn is_superseded(
    shard: &Shard,
    graph: &str,
    source_envelope: &str,
    snapshot_digest: &str,
    crypto: DurableCrypto<'_>,
) -> Result<bool, String> {
    if source_envelope.is_empty()
        || source_envelope.len() > MAX_ENVELOPE_BYTES
        || !valid_digest(snapshot_digest)
    {
        return Err(
            "INVALID_ARGUMENT: repository enrichment supersession lookup is invalid".into(),
        );
    }
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    let rows = read.scoped_owner_table(SUPERSESSIONS)?;
    let Some(row) = rows.get((graph, source_envelope))? else {
        return Ok(false);
    };
    let marker: Supersession = decode_durable(&crypto.unseal(row.value())?)?;
    if !valid_supersession_marker(&marker, source_envelope, snapshot_digest) {
        return Err("CONFLICT: repository enrichment supersession is invalid".into());
    }
    let next: BudgetCheckpoint = read
        .scoped_owner_table(BUDGETS)?
        .get((graph, marker.new_source_envelope.as_str()))?
        .map(|row| decode_durable(&crypto.unseal(row.value())?))
        .transpose()?
        .ok_or("CONFLICT: repository enrichment supersession lacks replacement budget")?;
    if next.source_envelope != marker.new_source_envelope
        || next.snapshot_digest != marker.new_snapshot_digest
    {
        return Err("CONFLICT: repository enrichment supersession target changed".into());
    }
    Ok(true)
}

fn valid_supersession_marker(
    marker: &Supersession,
    source_envelope: &str,
    snapshot_digest: &str,
) -> bool {
    marker.schema_version == 1
        && marker.old_source_envelope == source_envelope
        && marker.old_snapshot_digest == snapshot_digest
        && marker.new_source_envelope != source_envelope
        && valid_digest(&marker.new_snapshot_digest)
        && valid_digest(&marker.replacement_policy_digest)
}

/// Verify an exact Raft retry against durable owner rows. Later funded pages
/// may advance the replacement checkpoint, so compare its immutable authority
/// rather than requiring the original cursor bytes to remain unchanged.
pub(crate) fn verify_reactivation_replay(
    shard: &Shard,
    graph: &str,
    expected: &BudgetCheckpoint,
    park: &EnrichmentBudgetPark,
    replacement: &BudgetCheckpoint,
    revision: &RepositoryEnrichmentPolicyRevision,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    let saved_revision: RepositoryEnrichmentPolicyRevision = read
        .scoped_owner_table(POLICY_REVISIONS)?
        .get((graph, replacement.source_envelope.as_str()))?
        .map(|row| decode_durable(&crypto.unseal(row.value())?))
        .transpose()?
        .ok_or("CONFLICT: enrichment top-up replay lacks policy revision")?;
    if saved_revision != *revision {
        return Err("CONFLICT: enrichment top-up replay revision changed".into());
    }
    let marker: Supersession = read
        .scoped_owner_table(SUPERSESSIONS)?
        .get((graph, expected.source_envelope.as_str()))?
        .map(|row| decode_durable(&crypto.unseal(row.value())?))
        .transpose()?
        .ok_or("CONFLICT: enrichment top-up replay lacks supersession")?;
    if !replay_marker_matches(&marker, expected, park, replacement, revision) {
        return Err("CONFLICT: enrichment top-up replay supersession changed".into());
    }
    let checkpoint: BudgetCheckpoint = read
        .scoped_owner_table(BUDGETS)?
        .get((graph, replacement.source_envelope.as_str()))?
        .map(|row| decode_durable(&crypto.unseal(row.value())?))
        .transpose()?
        .ok_or("CONFLICT: enrichment top-up replay lacks replacement budget")?;
    if !replay_checkpoint_matches(&checkpoint, replacement) {
        return Err("CONFLICT: enrichment top-up replay budget changed".into());
    }
    Ok(())
}

fn replay_marker_matches(
    marker: &Supersession,
    expected: &BudgetCheckpoint,
    park: &EnrichmentBudgetPark,
    replacement: &BudgetCheckpoint,
    revision: &RepositoryEnrichmentPolicyRevision,
) -> bool {
    marker.schema_version == 1
        && marker.old_source_envelope == expected.source_envelope
        && marker.old_snapshot_digest == expected.snapshot_digest
        && marker.new_source_envelope == replacement.source_envelope
        && marker.new_snapshot_digest == replacement.snapshot_digest
        && marker.replacement_policy_digest == revision.policy_digest
        && park.policy_digest == revision.prior_policy_digest.as_deref().unwrap_or_default()
}

fn replay_checkpoint_matches(
    checkpoint: &BudgetCheckpoint,
    replacement: &BudgetCheckpoint,
) -> bool {
    checkpoint.schema_version == 1
        && checkpoint.tenant_id == replacement.tenant_id
        && checkpoint.source_envelope == replacement.source_envelope
        && checkpoint.snapshot_digest == replacement.snapshot_digest
        && checkpoint.total_budget_units == replacement.total_budget_units
        && budget_balances(checkpoint)
        && checkpoint.next_index >= replacement.next_index
        && checkpoint.page_number >= replacement.page_number
}

struct ReactivationRows<'a> {
    graph: &'a str,
    expected: &'a BudgetCheckpoint,
    expected_park: &'a EnrichmentBudgetPark,
    replacement: &'a BudgetCheckpoint,
    revision: &'a RepositoryEnrichmentPolicyRevision,
}

/// Compare and swap the parked budget rows inside an already admitted graph
/// transaction. The caller must authenticate the policy revision and, in this
/// same transaction, resolve the old outbox delivery and publish the revised
/// immutable source intent. This helper intentionally does not commit: clearing
/// a park on its own would make the old underfunded intent claimable again.
pub(crate) fn stage_parked_reactivation(
    write: &ShardWrite<'_>,
    graph: &str,
    expected: &BudgetCheckpoint,
    expected_park: &EnrichmentBudgetPark,
    replacement: &BudgetCheckpoint,
    revision: &RepositoryEnrichmentPolicyRevision,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    validate_reactivation_rows(
        expected,
        expected_park,
        replacement,
        &revision.policy_digest,
    )?;
    validate_policy_revision(revision, expected, expected_park, replacement)?;
    if revision.graph != graph {
        return Err("ACCESS_DENIED: repository enrichment revision graph differs".into());
    }
    let rows = ReactivationRows {
        graph,
        expected,
        expected_park,
        replacement,
        revision,
    };
    check_parked_reactivation(write, &rows, crypto)?;
    stage_reactivation_rows(write, &rows, crypto)
}

/// Compare all durable owner rows before writing any replacement row. The
/// caller retains the admitted write transaction across this check and stage.
fn check_parked_reactivation(
    write: &ShardWrite<'_>,
    rows: &ReactivationRows<'_>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let ReactivationRows {
        graph,
        expected,
        expected_park,
        replacement,
        ..
    } = *rows;
    let scope = write.graph(graph)?;
    let budgets = scope.open_scoped_table(BUDGETS)?;
    let old: BudgetCheckpoint = budgets
        .get((graph, expected.source_envelope.as_str()))?
        .map(|row| decode_durable(&crypto.unseal(row.value())?))
        .transpose()?
        .ok_or("CONFLICT: parked repository enrichment budget is absent")?;
    if old != *expected {
        return Err("CONFLICT: parked repository enrichment budget changed".into());
    }
    if budgets
        .get((graph, replacement.source_envelope.as_str()))?
        .is_some()
    {
        return Err("CONFLICT: replacement repository enrichment budget already exists".into());
    }
    let parks = scope.open_scoped_table(PARKS)?;
    let park: EnrichmentBudgetPark = parks
        .get(graph)?
        .map(|row| decode_durable(&crypto.unseal(row.value())?))
        .transpose()?
        .ok_or("CONFLICT: repository enrichment park is absent")?;
    if park != *expected_park {
        return Err("CONFLICT: repository enrichment park changed".into());
    }
    check_policy_and_supersession(write, rows, crypto)
}

fn check_policy_and_supersession(
    write: &ShardWrite<'_>,
    rows: &ReactivationRows<'_>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let ReactivationRows {
        graph,
        expected,
        replacement,
        ..
    } = *rows;
    let scope = write.graph(graph)?;
    let policies = scope.open_scoped_table(POLICY_REVISIONS)?;
    let old_policy: RepositoryEnrichmentPolicyRevision = policies
        .get((graph, expected.source_envelope.as_str()))?
        .map(|row| decode_durable(&crypto.unseal(row.value())?))
        .transpose()?
        .ok_or("CONFLICT: repository enrichment prior policy is absent")?;
    if !policy_source_matches(&old_policy, rows)
        || !policy_budget_and_sequence_match(&old_policy, rows)
    {
        return Err("CONFLICT: repository enrichment policy sequence changed".into());
    }
    if policies
        .get((graph, replacement.source_envelope.as_str()))?
        .is_some()
    {
        return Err("CONFLICT: repository enrichment replacement policy already exists".into());
    }
    let supersessions = scope.open_scoped_table(SUPERSESSIONS)?;
    if supersessions
        .get((graph, expected.source_envelope.as_str()))?
        .is_some()
    {
        return Err("CONFLICT: repository enrichment source is already superseded".into());
    }
    Ok(())
}

fn policy_source_matches(
    old: &RepositoryEnrichmentPolicyRevision,
    rows: &ReactivationRows<'_>,
) -> bool {
    old.schema_version == 1
        && old.graph == rows.graph
        && old.tenant_id == rows.expected.tenant_id
        && old.repository_id == rows.revision.repository_id
        && old.source_envelope == rows.expected.source_envelope
        && old.snapshot_digest == rows.expected.snapshot_digest
        && old.policy_digest == rows.expected_park.policy_digest
}

fn policy_budget_and_sequence_match(
    old: &RepositoryEnrichmentPolicyRevision,
    rows: &ReactivationRows<'_>,
) -> bool {
    old.total_budget_units == rows.expected.total_budget_units
        && old.max_total_units != 0
        && old.max_total_units >= rows.revision.total_budget_units
        && rows.revision.max_total_units == old.max_total_units
        && old.sequence.checked_add(1) == Some(rows.revision.sequence)
}

/// Stage all four row transitions in the same admitted transaction. Opening
/// each table before the first insert preserves the original failure boundary.
fn stage_reactivation_rows(
    write: &ShardWrite<'_>,
    rows: &ReactivationRows<'_>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let ReactivationRows {
        graph,
        expected,
        replacement,
        revision,
        ..
    } = *rows;
    let scope = write.graph(graph)?;
    let mut budgets = scope.open_scoped_table(BUDGETS)?;
    let mut parks = scope.open_scoped_table(PARKS)?;
    let mut policies = scope.open_scoped_table(POLICY_REVISIONS)?;
    let mut supersessions = scope.open_scoped_table(SUPERSESSIONS)?;
    put_sealed_row(
        &mut budgets,
        graph,
        &replacement.source_envelope,
        replacement,
        crypto,
        "repository enrichment replacement budget encode failed",
    )?;
    let marker = Supersession {
        schema_version: 1,
        old_source_envelope: expected.source_envelope.clone(),
        old_snapshot_digest: expected.snapshot_digest.clone(),
        new_source_envelope: replacement.source_envelope.clone(),
        new_snapshot_digest: replacement.snapshot_digest.clone(),
        replacement_policy_digest: revision.policy_digest.clone(),
    };
    put_sealed_row(
        &mut supersessions,
        graph,
        &expected.source_envelope,
        &marker,
        crypto,
        "repository enrichment supersession encode failed",
    )?;
    put_sealed_row(
        &mut policies,
        graph,
        &replacement.source_envelope,
        revision,
        crypto,
        "repository enrichment replacement policy encode failed",
    )?;
    parks.remove(graph)?;
    Ok(())
}

fn validate_policy_revision(
    revision: &RepositoryEnrichmentPolicyRevision,
    expected: &BudgetCheckpoint,
    park: &EnrichmentBudgetPark,
    replacement: &BudgetCheckpoint,
) -> Result<(), String> {
    if !revision_source_matches(revision, expected, replacement)
        || !revision_budget_matches(revision, park, replacement)
        || !revision_prior_and_caller_matches(revision, expected, park)
    {
        return Err("ACCESS_DENIED: repository enrichment policy revision is invalid".into());
    }
    Ok(())
}

fn revision_source_matches(
    revision: &RepositoryEnrichmentPolicyRevision,
    expected: &BudgetCheckpoint,
    replacement: &BudgetCheckpoint,
) -> bool {
    revision.schema_version == 1
        && bounded_envelope(&revision.graph)
        && revision.tenant_id == expected.tenant_id
        && bounded_envelope(&revision.repository_id)
        && revision.source_envelope == replacement.source_envelope
        && revision.snapshot_digest == replacement.snapshot_digest
}

fn revision_budget_matches(
    revision: &RepositoryEnrichmentPolicyRevision,
    park: &EnrichmentBudgetPark,
    replacement: &BudgetCheckpoint,
) -> bool {
    valid_digest(&revision.policy_digest)
        && revision.policy_digest != park.policy_digest
        && revision.total_budget_units == replacement.total_budget_units
        && revision.max_total_units >= revision.total_budget_units
        && revision.sequence != 0
}

fn revision_prior_and_caller_matches(
    revision: &RepositoryEnrichmentPolicyRevision,
    expected: &BudgetCheckpoint,
    park: &EnrichmentBudgetPark,
) -> bool {
    use crate::parser::enrichment_reactivation::TOP_UP_ACTION;
    revision.prior_source_envelope.as_deref() == Some(expected.source_envelope.as_str())
        && revision.prior_snapshot_digest.as_deref() == Some(expected.snapshot_digest.as_str())
        && revision.prior_policy_digest.as_deref() == Some(park.policy_digest.as_str())
        && revision
            .caller_subject
            .as_deref()
            .is_some_and(bounded_envelope)
        && revision.verified_action.as_deref() == Some(TOP_UP_ACTION)
        && revision
            .idempotency_key
            .as_deref()
            .is_some_and(bounded_envelope)
}

fn validate_reactivation_rows(
    expected: &BudgetCheckpoint,
    expected_park: &EnrichmentBudgetPark,
    replacement: &BudgetCheckpoint,
    replacement_policy_digest: &str,
) -> Result<(), String> {
    validate_park(expected_park, expected)?;
    let added = replacement
        .total_budget_units
        .checked_sub(expected.total_budget_units)
        .filter(|added| *added > 0)
        .ok_or("CONFLICT: repository enrichment top-up must increase budget")?;
    if !replacement_source_matches(
        expected,
        expected_park,
        replacement,
        replacement_policy_digest,
    ) || !replacement_cursor_matches(expected, replacement, added)
    {
        return Err("CONFLICT: repository enrichment replacement budget is invalid".into());
    }
    Ok(())
}

fn replacement_source_matches(
    expected: &BudgetCheckpoint,
    expected_park: &EnrichmentBudgetPark,
    replacement: &BudgetCheckpoint,
    replacement_policy_digest: &str,
) -> bool {
    replacement.schema_version == 1
        && replacement.tenant_id == expected.tenant_id
        && replacement.source_envelope != expected.source_envelope
        && bounded_envelope(&replacement.source_envelope)
        && valid_digest(&replacement.snapshot_digest)
        && replacement.snapshot_digest != expected.snapshot_digest
        && valid_digest(replacement_policy_digest)
        && replacement_policy_digest != expected_park.policy_digest
}

fn replacement_cursor_matches(
    expected: &BudgetCheckpoint,
    replacement: &BudgetCheckpoint,
    added: u64,
) -> bool {
    replacement.next_index == expected.next_index
        && replacement.page_number == expected.page_number
        && replacement.reserved_units == expected.reserved_units
        && replacement.spent_units == expected.spent_units
        && replacement.last_page_key == expected.last_page_key
        && expected.remaining_units.checked_add(added) == Some(replacement.remaining_units)
        && budget_balances(replacement)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn commit_maintenance(
        shard: &Shard,
        graph: &str,
        tag: &str,
        apply: &dyn Fn(&ShardWrite<'_>) -> Result<(), String>,
    ) {
        let members = shard.graph_members(&[graph]).unwrap();
        let (group, batches) = shard.admit_maintenance(&members, tag).unwrap();
        let write = ShardWrite::open(shard, &group, &members, &batches).unwrap();
        apply(&write).unwrap();
        write.finish().unwrap();
        shard.commit_drain(group, &batches, 1).unwrap();
    }

    fn source_authority(
        old: &BudgetCheckpoint,
        park: &EnrichmentBudgetPark,
    ) -> SourceBudgetAuthority {
        SourceBudgetAuthority {
            tenant_id: old.tenant_id.clone(),
            source_envelope: old.source_envelope.clone(),
            snapshot_digest: old.snapshot_digest.clone(),
            repository_id: "repository".into(),
            policy_digest: park.policy_digest.clone(),
            total_budget_units: old.total_budget_units,
            max_total_units: 14,
        }
    }

    fn assert_superseded(shard: &Shard, graph: &str, old: &BudgetCheckpoint) {
        assert!(is_superseded(
            shard,
            graph,
            &old.source_envelope,
            &old.snapshot_digest,
            DurableCrypto::none(),
        )
        .unwrap());
    }

    fn parked_rows() -> (BudgetCheckpoint, EnrichmentBudgetPark, BudgetCheckpoint) {
        let old = BudgetCheckpoint {
            schema_version: 1,
            tenant_id: "tenant".into(),
            snapshot_digest: "a".repeat(64),
            source_envelope: "source-one".into(),
            next_index: 1,
            page_number: 1,
            reserved_units: 6,
            spent_units: 0,
            remaining_units: 4,
            total_budget_units: 10,
            last_page_key: format!("repository-enrichment-page:{}", "c".repeat(64)),
        };
        let park = EnrichmentBudgetPark {
            schema_version: 1,
            source_envelope: old.source_envelope.clone(),
            snapshot_digest: old.snapshot_digest.clone(),
            next_index: old.next_index,
            page_number: old.page_number,
            remaining_units: old.remaining_units,
            required_units: 5,
            policy_digest: "b".repeat(64),
            parked_at_ms: 1,
        };
        let next = BudgetCheckpoint {
            snapshot_digest: "d".repeat(64),
            source_envelope: "source-two".into(),
            remaining_units: 8,
            total_budget_units: 14,
            ..old.clone()
        };
        (old, park, next)
    }

    fn revised_policy(
        old: &BudgetCheckpoint,
        next: &BudgetCheckpoint,
    ) -> RepositoryEnrichmentPolicyRevision {
        RepositoryEnrichmentPolicyRevision {
            schema_version: 1,
            tenant_id: old.tenant_id.clone(),
            graph: "reactivation-graph".into(),
            repository_id: "repository".into(),
            source_envelope: next.source_envelope.clone(),
            snapshot_digest: next.snapshot_digest.clone(),
            policy_digest: "e".repeat(64),
            total_budget_units: next.total_budget_units,
            max_total_units: 14,
            sequence: 1,
            prior_source_envelope: Some(old.source_envelope.clone()),
            prior_snapshot_digest: Some(old.snapshot_digest.clone()),
            prior_policy_digest: Some("b".repeat(64)),
            caller_subject: Some("operator".into()),
            verified_action: Some("repository:enrichment:budget:top_up".into()),
            idempotency_key: Some("topup-1".into()),
        }
    }

    #[test]
    fn revised_budget_preserves_admitted_cursor_and_spend() {
        let (old, park, next) = parked_rows();
        validate_reactivation_rows(&old, &park, &next, &"e".repeat(64)).unwrap();
        let mut reset_cursor = next.clone();
        reset_cursor.next_index = 0;
        assert!(validate_reactivation_rows(&old, &park, &reset_cursor, &"e".repeat(64)).is_err());
        let mut forged_remaining = next.clone();
        forged_remaining.remaining_units += 1;
        assert!(
            validate_reactivation_rows(&old, &park, &forged_remaining, &"e".repeat(64)).is_err()
        );
        assert!(validate_reactivation_rows(&old, &park, &next, &park.policy_digest).is_err());
    }

    #[test]
    fn reactivation_cas_rejects_stale_park_and_budget() {
        let path = crate::redb_store::temp_path("eg-enrichment-reactivation", "durable");
        let shard = Shard::open(&path).unwrap();
        let graph = "reactivation-graph";
        let (old, park, next) = parked_rows();
        commit_maintenance(&shard, graph, "reactivation/seed", &|write| {
            seed_source_budget(
                write,
                graph,
                &source_authority(&old, &park),
                DurableCrypto::none(),
            )?;
            let scope = write.graph(graph)?;
            let mut budgets = scope.open_scoped_table(BUDGETS)?;
            let bytes = rmp_serde::to_vec_named(&old).unwrap();
            budgets.insert((graph, old.source_envelope.as_str()), bytes.as_slice())?;
            Ok(())
        });
        let retained =
            read_policy_revision(&shard, graph, &old.source_envelope, DurableCrypto::none())
                .unwrap()
                .unwrap();
        assert_eq!(retained.max_total_units, 14);
        assert_eq!(retained.sequence, 0);
        commit_maintenance(&shard, graph, "reactivation/park", &|write| {
            park_underfunded(write, graph, &park, DurableCrypto::none())
        });
        let mut stale = park.clone();
        stale.parked_at_ms += 1;
        let members = shard.graph_members(&[graph]).unwrap();
        let (group, batches) = shard
            .admit_maintenance(&members, "reactivation/stale")
            .unwrap();
        let write = ShardWrite::open(&shard, &group, &members, &batches).unwrap();
        assert!(stage_parked_reactivation(
            &write,
            graph,
            &old,
            &stale,
            &next,
            &revised_policy(&old, &next),
            DurableCrypto::none(),
        )
        .is_err());
        drop(write);
        drop(group);
        assert!(
            read(&shard, graph, &next.source_envelope, DurableCrypto::none())
                .unwrap()
                .is_none()
        );
        assert_eq!(
            read_park(&shard, graph, DurableCrypto::none()).unwrap(),
            Some(park.clone())
        );
        let mut stale_budget = old.clone();
        stale_budget.last_page_key = format!("repository-enrichment-page:{}", "f".repeat(64));
        commit_maintenance(&shard, graph, "reactivation/revise", &|write| {
            assert!(stage_parked_reactivation(
                write,
                graph,
                &stale_budget,
                &park,
                &next,
                &revised_policy(&old, &next),
                DurableCrypto::none(),
            )
            .is_err());
            let mut stale_revision = revised_policy(&old, &next);
            stale_revision.sequence = 2;
            assert!(stage_parked_reactivation(
                write,
                graph,
                &old,
                &park,
                &next,
                &stale_revision,
                DurableCrypto::none(),
            )
            .is_err());
            stage_parked_reactivation(
                write,
                graph,
                &old,
                &park,
                &next,
                &revised_policy(&old, &next),
                DurableCrypto::none(),
            )?;
            assert!(
                ensure_active_source(write.graph(graph)?, graph, &old.source_envelope).is_err()
            );
            assert!(park_underfunded(write, graph, &park, DurableCrypto::none()).is_err());
            Ok(())
        });
        assert_eq!(
            read(&shard, graph, &next.source_envelope, DurableCrypto::none()).unwrap(),
            Some(next.clone())
        );
        let handle = shard.graph(graph).unwrap();
        let read = shard.read(&handle).unwrap();
        let policy_rows = read.scoped_owner_table(POLICY_REVISIONS).unwrap();
        let saved: RepositoryEnrichmentPolicyRevision = decode_durable(
            policy_rows
                .get((graph, next.source_envelope.as_str()))
                .unwrap()
                .unwrap()
                .value(),
        )
        .unwrap();
        assert_eq!(saved, revised_policy(&old, &next));
        assert!(is_superseded(
            &shard,
            graph,
            &old.source_envelope,
            &old.snapshot_digest,
            DurableCrypto::none(),
        )
        .unwrap());
        assert!(is_superseded(
            &shard,
            graph,
            &old.source_envelope,
            &"f".repeat(64),
            DurableCrypto::none(),
        )
        .is_err());
        assert!(read_park(&shard, graph, DurableCrypto::none())
            .unwrap()
            .is_none());
        drop(shard);
        let reopened = Shard::open(&path).unwrap();
        assert_superseded(&reopened, graph, &old);
        assert!(read_park(&reopened, graph, DurableCrypto::none())
            .unwrap()
            .is_none());
        drop(reopened);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn zero_page_top_up_persists_funded_source_and_supersession_across_reopen() {
        let path = crate::redb_store::temp_path("eg-enrichment-zero-topup", "durable");
        let shard = Shard::open(&path).unwrap();
        let graph = "reactivation-graph";
        let (mut old, _, mut next) = parked_rows();
        old.next_index = 0;
        old.page_number = 0;
        old.reserved_units = 0;
        old.remaining_units = 10;
        old.last_page_key.clear();
        next.next_index = 0;
        next.page_number = 0;
        next.reserved_units = 0;
        next.remaining_units = 14;
        next.last_page_key.clear();
        let park = EnrichmentBudgetPark {
            schema_version: 1,
            source_envelope: old.source_envelope.clone(),
            snapshot_digest: old.snapshot_digest.clone(),
            next_index: 0,
            page_number: 0,
            remaining_units: 10,
            required_units: 11,
            policy_digest: "b".repeat(64),
            parked_at_ms: 1,
        };
        commit_maintenance(&shard, graph, "zero-topup/seed", &|write| {
            seed_source_budget(
                write,
                graph,
                &source_authority(&old, &park),
                DurableCrypto::none(),
            )
        });
        commit_maintenance(&shard, graph, "zero-topup/park", &|write| {
            park_underfunded(write, graph, &park, DurableCrypto::none())
        });
        let revision = revised_policy(&old, &next);
        commit_maintenance(&shard, graph, "zero-topup/replace", &|write| {
            stage_parked_reactivation(
                write,
                graph,
                &old,
                &park,
                &next,
                &revision,
                DurableCrypto::none(),
            )
        });
        drop(shard);
        let reopened = Shard::open(&path).unwrap();
        assert_eq!(
            read(
                &reopened,
                graph,
                &next.source_envelope,
                DurableCrypto::none()
            )
            .unwrap(),
            Some(next.clone())
        );
        assert!(read_park(&reopened, graph, DurableCrypto::none())
            .unwrap()
            .is_none());
        assert_superseded(&reopened, graph, &old);
        verify_reactivation_replay(
            &reopened,
            graph,
            &old,
            &park,
            &next,
            &revision,
            DurableCrypto::none(),
        )
        .unwrap();
        drop(reopened);
        let _ = std::fs::remove_file(path);
    }
}
