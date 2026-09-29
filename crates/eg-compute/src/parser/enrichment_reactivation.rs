//! Pure EH-557 budget-revision planning. This does not authorize or commit a
//! top-up: the server must verify a control-plane caller and atomically compare
//! and swap the policy, parked intent, old/new snapshots, and budget rows.

use eg_types::native_control::{
    EnrichmentBudgetCheckpoint as DurableBudgetCheckpoint, EnrichmentBudgetPark,
};

use super::enrichment_snapshot::{EligibleSnapshot, EnrichmentBudgetCheckpoint};

pub use super::TOP_UP_ACTION;

/// Caller-proposed revision fields. The serving boundary must bind these to a
/// verified caller, retained policy authority, and a durable prior revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BudgetRevisionProposal {
    pub tenant_id: String,
    pub graph: String,
    pub repository_id: String,
    pub source_envelope: String,
    pub source_snapshot_digest: String,
    pub prior_policy_digest: String,
    pub replacement_policy_digest: String,
    pub replacement_total_units: u64,
    pub new_source_envelope: String,
    pub expected_policy_sequence: u64,
    pub next_policy_sequence: u64,
    pub caller_subject: String,
    pub verified_action: String,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReactivationPlan {
    pub old_snapshot_digest: String,
    pub added_units: u64,
    pub snapshot: EligibleSnapshot,
    pub checkpoint: DurableBudgetCheckpoint,
}

fn digest(value: &str) -> bool {
    eg_types::contract::Digest256::parse(value).is_ok()
}

fn bounded_identity(value: &str) -> bool {
    !value.is_empty() && value.len() <= 512 && !value.chars().any(char::is_control)
}

fn park_matches_checkpoint(
    old: &EligibleSnapshot,
    durable: &DurableBudgetCheckpoint,
    park: &EnrichmentBudgetPark,
    old_digest: &str,
) -> bool {
    park.schema_version == 1
        && park.source_envelope == old.source_envelope
        && park.snapshot_digest == old_digest
        && park.policy_digest == old.policy_digest
        && park.next_index == durable.next_index
        && park.page_number == durable.page_number
        && park.remaining_units == durable.remaining_units
        && park.required_units > durable.remaining_units
        && old
            .units
            .get(durable.next_index as usize)
            .is_some_and(|unit| unit.compute_units == park.required_units)
}

fn proposal_matches_source(
    old: &EligibleSnapshot,
    proposal: &BudgetRevisionProposal,
    old_digest: &str,
) -> bool {
    proposal.tenant_id == old.tenant_id
        && proposal.graph == old.graph
        && proposal.repository_id == old.repository_id
        && proposal.source_envelope == old.source_envelope
        && proposal.source_snapshot_digest == old_digest
        && proposal.prior_policy_digest == old.policy_digest
        && proposal.verified_action == TOP_UP_ACTION
}

fn proposal_has_valid_revision(old: &EligibleSnapshot, proposal: &BudgetRevisionProposal) -> bool {
    bounded_identity(&proposal.caller_subject)
        && bounded_identity(&proposal.idempotency_key)
        && bounded_identity(&proposal.new_source_envelope)
        && proposal.new_source_envelope != old.source_envelope
        && digest(&proposal.replacement_policy_digest)
        && proposal.replacement_policy_digest != old.policy_digest
        && proposal.expected_policy_sequence.checked_add(1) == Some(proposal.next_policy_sequence)
}

/// Derive a new immutable source snapshot while preserving every already
/// admitted unit and the exact old cursor/spend partition. `max_total_units`
/// must come from the server's current policy authority, not this proposal.
pub fn plan_reactivation(
    old: &EligibleSnapshot,
    durable: &DurableBudgetCheckpoint,
    park: &EnrichmentBudgetPark,
    proposal: &BudgetRevisionProposal,
    max_total_units: u64,
) -> Result<ReactivationPlan, String> {
    old.validate()
        .map_err(|_| "CONFLICT: old enrichment snapshot is invalid")?;
    let old_digest = old
        .digest()
        .map_err(|_| "CONFLICT: old enrichment snapshot digest is invalid")?;
    if durable.schema_version != 1
        || durable.tenant_id != old.tenant_id
        || durable.source_envelope != old.source_envelope
        || durable.snapshot_digest != old_digest
        || durable.total_budget_units != old.budget_units
    {
        return Err("CONFLICT: old enrichment budget authority is invalid".into());
    }
    let old_checkpoint = EnrichmentBudgetCheckpoint::from_durable(durable);
    old_checkpoint
        .validate(old)
        .map_err(|_| "CONFLICT: old enrichment checkpoint is invalid")?;
    if !park_matches_checkpoint(old, durable, park, &old_digest) {
        return Err("CONFLICT: enrichment park is stale".into());
    }
    if !proposal_matches_source(old, proposal, &old_digest)
        || !proposal_has_valid_revision(old, proposal)
    {
        return Err("ACCESS_DENIED: enrichment revision does not match source authority".into());
    }
    let added_units = proposal
        .replacement_total_units
        .checked_sub(old.budget_units)
        .filter(|delta| *delta > 0)
        .ok_or("CONFLICT: enrichment budget must increase")?;
    if proposal.replacement_total_units > max_total_units {
        return Err("CONFLICT: enrichment budget exceeds policy bound".into());
    }
    let remaining_units = durable
        .remaining_units
        .checked_add(added_units)
        .ok_or("CONFLICT: enrichment remaining budget overflow")?;
    let mut snapshot = old.clone();
    snapshot.source_envelope = proposal.new_source_envelope.clone();
    // The source bytes were committed by the original ChangeEnvelope. The
    // replacement envelope names this policy/budget revision; keeping both
    // references lets new WorkItems prove their source and revision separately.
    snapshot.policy_digest = proposal.replacement_policy_digest.clone();
    snapshot.budget_units = proposal.replacement_total_units;
    let snapshot_digest = snapshot
        .digest()
        .map_err(|_| "CONFLICT: revised enrichment snapshot is invalid")?;
    let checkpoint = DurableBudgetCheckpoint {
        schema_version: 1,
        tenant_id: durable.tenant_id.clone(),
        snapshot_digest,
        source_envelope: snapshot.source_envelope.clone(),
        next_index: durable.next_index,
        page_number: durable.page_number,
        reserved_units: durable.reserved_units,
        spent_units: durable.spent_units,
        remaining_units,
        total_budget_units: proposal.replacement_total_units,
        last_page_key: durable.last_page_key.clone(),
    };
    let revised_checkpoint = EnrichmentBudgetCheckpoint::from_durable(&checkpoint);
    revised_checkpoint
        .validate(&snapshot)
        .map_err(|_| "CONFLICT: revised enrichment checkpoint is invalid")?;
    Ok(ReactivationPlan {
        old_snapshot_digest: old_digest,
        added_units,
        snapshot,
        checkpoint,
    })
}

#[cfg(test)]
mod tests {
    use super::super::enrichment_snapshot::{EligibleUnit, EnrichmentWorkStage};
    use super::*;

    fn source(cost: u64, budget: u64) -> EligibleSnapshot {
        crate::test_support::repository_enrichment_snapshot(
            "tenant",
            "code",
            budget,
            (0..2)
                .map(|i| EligibleUnit {
                    content_digest: format!("sha256:{i:064x}"),
                    parser_capability_digest: "parser:v1".into(),
                    stage: EnrichmentWorkStage::Classical,
                    input_ref: format!("cas:sha256:{i:064x}"),
                    content_length: 1,
                    compute_units: cost,
                    demanded: false,
                })
                .collect(),
        )
    }

    fn checkpoint(source: &EligibleSnapshot, advanced: bool) -> DurableBudgetCheckpoint {
        let mut initial = EnrichmentBudgetCheckpoint::initial(source).unwrap();
        let last_page_key = if advanced {
            let key = initial.page_key(source, 1).unwrap();
            initial.next_index = 1;
            initial.page_number = 1;
            initial.reserved_units = 1;
            initial.remaining_units -= 1;
            key
        } else {
            String::new()
        };
        DurableBudgetCheckpoint {
            schema_version: 1,
            tenant_id: source.tenant_id.clone(),
            snapshot_digest: source.digest().unwrap(),
            source_envelope: source.source_envelope.clone(),
            next_index: initial.next_index as u32,
            page_number: initial.page_number,
            reserved_units: initial.reserved_units,
            spent_units: 0,
            remaining_units: initial.remaining_units,
            total_budget_units: source.budget_units,
            last_page_key,
        }
    }

    fn park(
        source: &EligibleSnapshot,
        row: &DurableBudgetCheckpoint,
        required: u64,
    ) -> EnrichmentBudgetPark {
        EnrichmentBudgetPark {
            schema_version: 1,
            source_envelope: source.source_envelope.clone(),
            snapshot_digest: source.digest().unwrap(),
            next_index: row.next_index,
            page_number: row.page_number,
            remaining_units: row.remaining_units,
            required_units: required,
            policy_digest: source.policy_digest.clone(),
            parked_at_ms: 1,
        }
    }

    fn proposal(source: &EligibleSnapshot, total: u64) -> BudgetRevisionProposal {
        BudgetRevisionProposal {
            tenant_id: source.tenant_id.clone(),
            graph: source.graph.clone(),
            repository_id: source.repository_id.clone(),
            source_envelope: source.source_envelope.clone(),
            source_snapshot_digest: source.digest().unwrap(),
            prior_policy_digest: source.policy_digest.clone(),
            replacement_policy_digest: "d".repeat(64),
            replacement_total_units: total,
            new_source_envelope: "source-two".into(),
            expected_policy_sequence: 4,
            next_policy_sequence: 5,
            caller_subject: "principal:operator".into(),
            verified_action: TOP_UP_ACTION.into(),
            idempotency_key: "revision-five".into(),
        }
    }

    #[test]
    fn partial_page_reactivation_carries_cursor_spend_and_new_page_identity() {
        let old = source(1, 1);
        let row = checkpoint(&old, true);
        let resumed =
            plan_reactivation(&old, &row, &park(&old, &row, 1), &proposal(&old, 2), 2).unwrap();
        assert_eq!(resumed.added_units, 1);
        assert_eq!(resumed.checkpoint.next_index, 1);
        assert_eq!(resumed.checkpoint.reserved_units, 1);
        assert_eq!(resumed.checkpoint.remaining_units, 1);
        assert_eq!(resumed.checkpoint.last_page_key, row.last_page_key);
        assert_eq!(resumed.snapshot.units, old.units);
        assert_eq!(resumed.snapshot.source_commit_ref, old.source_commit_ref);
        assert_ne!(resumed.snapshot.source_envelope, old.source_envelope);
        assert_ne!(resumed.checkpoint.snapshot_digest, row.snapshot_digest);
        let next = EnrichmentBudgetCheckpoint {
            schema_version: 1,
            snapshot_digest: resumed.checkpoint.snapshot_digest.clone(),
            next_index: 1,
            page_number: 1,
            reserved_units: 1,
            spent_units: 0,
            remaining_units: 1,
            last_batch_key: Some(row.last_page_key),
        };
        assert_ne!(
            next.page_key(&resumed.snapshot, 2).unwrap(),
            EnrichmentBudgetCheckpoint::initial(&old)
                .unwrap()
                .page_key(&old, 2)
                .unwrap()
        );
    }

    #[test]
    fn zero_page_park_resumes_without_renewing_old_units() {
        let old = source(2, 1);
        let row = checkpoint(&old, false);
        let resumed =
            plan_reactivation(&old, &row, &park(&old, &row, 2), &proposal(&old, 2), 2).unwrap();
        assert_eq!(resumed.checkpoint.next_index, 0);
        assert_eq!(resumed.checkpoint.reserved_units, 0);
        assert_eq!(resumed.checkpoint.remaining_units, 2);
    }

    #[test]
    fn settled_spend_and_outstanding_reservation_are_carried_once() {
        let mut old = source(1, 2);
        old.units[1].compute_units = 2;
        let mut row = checkpoint(&old, true);
        row.spent_units = 1;
        row.reserved_units = 0;
        let resumed =
            plan_reactivation(&old, &row, &park(&old, &row, 2), &proposal(&old, 3), 3).unwrap();
        assert_eq!(resumed.checkpoint.next_index, 1);
        assert_eq!(resumed.checkpoint.spent_units, 1);
        assert_eq!(resumed.checkpoint.reserved_units, 0);
        assert_eq!(resumed.checkpoint.remaining_units, 2);
        assert_eq!(resumed.checkpoint.total_budget_units, 3);
    }

    #[test]
    fn stale_park_wrong_source_and_unbounded_revision_fail_closed() {
        let old = source(2, 1);
        let row = checkpoint(&old, false);
        let mut marker = park(&old, &row, 2);
        marker.snapshot_digest = "f".repeat(64);
        assert!(plan_reactivation(&old, &row, &marker, &proposal(&old, 2), 2).is_err());
        let marker = park(&old, &row, 2);
        let mut other = proposal(&old, 2);
        other.tenant_id = "other".into();
        assert!(plan_reactivation(&old, &row, &marker, &other, 2).is_err());
        let mut other = proposal(&old, 3);
        assert!(plan_reactivation(&old, &row, &marker, &other, 2).is_err());
        other.replacement_total_units = 2;
        other.next_policy_sequence = 7;
        assert!(plan_reactivation(&old, &row, &marker, &other, 2).is_err());
    }
}
