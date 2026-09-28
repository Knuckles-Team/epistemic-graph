//! Sealed, engine-owned EH-557 top-up plan. The served route remains closed.
//! Every replica checks the immutable source and replacement intent before
//! entering the graph-shard transaction.

use crate::parser::enrichment_snapshot::EligibleSnapshot;
use crate::redb_store::enrichment_budget::RepositoryEnrichmentPolicyRevision;
use eg_types::mutation_batch::{RepositoryEnrichmentTopUp, VersionExpectation};
use eg_types::native_control::{EnrichmentBudgetCheckpoint, EnrichmentBudgetPark};
use eg_types::{MutationBatch, MutationBatchRecord, MutationBatchStatus, MutationOutboxRecord};
use serde::{Deserialize, Serialize};

const TOPIC: &str = "repository.enrichment.pending";
const CONSUMER: &str = "repository-enrichment-v1";
const MAX_INTENT_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnrichmentTopUpTransition {
    pub(crate) old_delivery: MutationOutboxRecord,
    pub(crate) consumer: String,
    pub(crate) replacement_batch: MutationBatch,
    pub(crate) expected_budget: EnrichmentBudgetCheckpoint,
    pub(crate) expected_park: EnrichmentBudgetPark,
    pub(crate) replacement_budget: EnrichmentBudgetCheckpoint,
    pub(crate) revision: RepositoryEnrichmentPolicyRevision,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingIntent {
    budget_status: PendingStatus,
    snapshot: EligibleSnapshot,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum PendingStatus {
    Unreserved,
}

fn snapshot(payload: &[u8]) -> Result<EligibleSnapshot, String> {
    if payload.len() > MAX_INTENT_BYTES {
        return Err("CONFLICT: enrichment top-up intent exceeds bound".into());
    }
    let pending: PendingIntent = eg_types::msgpack::decode_bounded(
        payload,
        eg_types::msgpack::MsgpackLimits::new(MAX_INTENT_BYTES, 100_000, 64),
    )
    .map_err(|_| "CONFLICT: enrichment top-up intent is invalid")?;
    let PendingStatus::Unreserved = pending.budget_status;
    pending
        .snapshot
        .validate()
        .map_err(|_| "CONFLICT: enrichment top-up snapshot is invalid")?;
    Ok(pending.snapshot)
}

fn bounded(value: &str) -> bool {
    !value.is_empty() && value.len() <= 512 && !value.chars().any(char::is_control)
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

struct BudgetValidation<'a>(&'a EnrichmentTopUpTransition);

impl std::ops::Deref for BudgetValidation<'_> {
    type Target = EnrichmentTopUpTransition;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl BudgetValidation<'_> {
    fn revision_is_invalid(&self) -> bool {
        self.expected_budget.schema_version != 1
            || self.expected_park.schema_version != 1
            || self.replacement_budget.schema_version != 1
            || self.revision.schema_version != 1
            || self.revision.sequence == 0
            || self.revision.max_total_units < self.revision.total_budget_units
    }

    fn revision_identity_is_invalid(&self) -> bool {
        !bounded(self.revision.caller_subject.as_deref().unwrap_or_default())
            || !bounded(self.revision.idempotency_key.as_deref().unwrap_or_default())
            || self.revision.verified_action.as_deref()
                != Some("repository:enrichment:budget:top_up")
            || !valid_digest(&self.revision.policy_digest)
            || self.revision.policy_digest == self.expected_park.policy_digest
    }

    fn budget_position_changed(&self) -> bool {
        self.expected_budget.next_index != self.replacement_budget.next_index
            || self.expected_budget.page_number != self.replacement_budget.page_number
            || self.expected_budget.reserved_units != self.replacement_budget.reserved_units
            || self.expected_budget.spent_units != self.replacement_budget.spent_units
            || self.expected_budget.last_page_key != self.replacement_budget.last_page_key
    }

    fn budget_accounting_is_invalid(&self, added: u64) -> bool {
        self.expected_budget.remaining_units.checked_add(added)
            != Some(self.replacement_budget.remaining_units)
            || self
                .expected_budget
                .spent_units
                .checked_add(self.expected_budget.reserved_units)
                .and_then(|spent| spent.checked_add(self.expected_budget.remaining_units))
                != Some(self.expected_budget.total_budget_units)
            || self
                .replacement_budget
                .spent_units
                .checked_add(self.replacement_budget.reserved_units)
                .and_then(|spent| spent.checked_add(self.replacement_budget.remaining_units))
                != Some(self.replacement_budget.total_budget_units)
    }

    fn validate_budget(&self) -> Result<(), String> {
        let added = self
            .replacement_budget
            .total_budget_units
            .checked_sub(self.expected_budget.total_budget_units)
            .filter(|delta| *delta > 0)
            .ok_or("CONFLICT: enrichment top-up must increase total budget")?;
        if self.revision_is_invalid()
            || self.revision_identity_is_invalid()
            || self.budget_position_changed()
            || self.budget_accounting_is_invalid(added)
        {
            return Err("CONFLICT: enrichment top-up budget or revision is invalid".into());
        }
        Ok(())
    }
}

struct AuthorityValidation<'a>(&'a EnrichmentTopUpTransition);

impl std::ops::Deref for AuthorityValidation<'_> {
    type Target = EnrichmentTopUpTransition;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl AuthorityValidation<'_> {
    fn delivery_is_invalid(&self, committed_at_ms: u64) -> bool {
        self.consumer != CONSUMER
            || self.old_delivery.intent.topic != TOPIC
            || self.old_delivery.intent.key != self.expected_budget.source_envelope
            || self
                .old_delivery
                .commit_sequence
                .is_none_or(|sequence| sequence == 0)
            || self.old_delivery.created_at_ms > committed_at_ms
            || !self.old_delivery.intent.headers.is_empty()
    }

    fn park_is_invalid(&self) -> bool {
        self.expected_park.source_envelope != self.expected_budget.source_envelope
            || self.expected_park.snapshot_digest != self.expected_budget.snapshot_digest
            || self.expected_park.next_index != self.expected_budget.next_index
            || self.expected_park.page_number != self.expected_budget.page_number
            || self.expected_park.remaining_units != self.expected_budget.remaining_units
            || self.expected_park.required_units <= self.expected_budget.remaining_units
    }

    fn prior_revision_is_invalid(&self, graph: &str) -> bool {
        self.expected_park.policy_digest
            != self
                .revision
                .prior_policy_digest
                .as_deref()
                .unwrap_or_default()
            || self.revision.graph != graph
            || self.revision.tenant_id != self.expected_budget.tenant_id
            || self.revision.prior_source_envelope.as_deref()
                != Some(self.expected_budget.source_envelope.as_str())
            || self.revision.prior_snapshot_digest.as_deref()
                != Some(self.expected_budget.snapshot_digest.as_str())
    }

    fn replacement_revision_is_invalid(&self) -> bool {
        self.replacement_budget.source_envelope != self.revision.source_envelope
            || self.replacement_budget.snapshot_digest != self.revision.snapshot_digest
            || self.replacement_budget.total_budget_units != self.revision.total_budget_units
    }

    fn validate_authority(&self, graph: &str, committed_at_ms: u64) -> Result<(), String> {
        if self.delivery_is_invalid(committed_at_ms)
            || self.park_is_invalid()
            || self.prior_revision_is_invalid(graph)
            || self.replacement_revision_is_invalid()
            || self.expected_park.parked_at_ms == 0
            || self.expected_park.parked_at_ms > committed_at_ms
        {
            return Err("CONFLICT: enrichment top-up authority does not match source".into());
        }
        Ok(())
    }
}

impl EnrichmentTopUpTransition {
    /// A retry may carry a newer Raft route fence only when the original
    /// parent and full transition receipt are already committed on this
    /// replica. Fresh writes must match their sealed parent fence exactly.
    pub(crate) fn validate_parent_fence(
        &self,
        route_epoch: u64,
        route_token: Option<u64>,
        prior: Option<&MutationBatchRecord>,
        committed_at_ms: u64,
    ) -> Result<(), String> {
        if let Some(record) = prior {
            record.validate()?;
            let proposed = rmp_serde::to_vec_named(&self.replacement_batch)
                .map_err(|_| "CONFLICT: enrichment top-up parent encode failed")?;
            let recorded = rmp_serde::to_vec_named(&record.batch)
                .map_err(|_| "CONFLICT: enrichment top-up receipt encode failed")?;
            let sealed_receipt = rmp_serde::to_vec_named(self)
                .map_err(|_| "CONFLICT: enrichment top-up transition encode failed")?;
            if record.status != MutationBatchStatus::Committed
                || proposed != recorded
                || record.result_msgpack.as_deref() != Some(sealed_receipt.as_slice())
                || record.committed_at_ms != committed_at_ms
            {
                return Err("IDEMPOTENCY_CONFLICT: enrichment top-up prior receipt differs".into());
            }
            return Ok(());
        }
        if route_epoch != self.replacement_batch.placement_epoch
            || route_token != self.replacement_batch.fencing_token
        {
            return Err("STALE_ROUTE: enrichment top-up fresh parent fence changed".into());
        }
        Ok(())
    }

    fn parent_batch_is_invalid(&self, committed_at_ms: u64) -> bool {
        !self.replacement_batch.is_repository_enrichment_top_up()
            || self.replacement_batch.identity != self.old_delivery.identity
            || self.replacement_batch.batch_id != self.revision.source_envelope
            || self.replacement_batch.idempotency_key()
                != self.revision.idempotency_key.as_deref().unwrap_or_default()
            || self.replacement_batch.created_at_ms != committed_at_ms
    }

    fn validate_parent_batch(&self, committed_at_ms: u64) -> Result<(), String> {
        if self.parent_batch_is_invalid(committed_at_ms) {
            return Err("CONFLICT: enrichment top-up parent batch differs".into());
        }
        let VersionExpectation::Graph(graph_version) = self.replacement_batch.version_expectation
        else {
            return Err("CONFLICT: enrichment top-up parent graph version is absent".into());
        };
        let expected_parent =
            MutationBatch::repository_enrichment_top_up(RepositoryEnrichmentTopUp {
                old_delivery: &self.old_delivery,
                policy_sequence: self.revision.sequence,
                revision_idempotency_key: self
                    .revision
                    .idempotency_key
                    .as_deref()
                    .unwrap_or_default(),
                replacement_intent: self.replacement_batch.outbox[0].clone(),
                replacement_batch_id: &self.revision.source_envelope,
                serving_principal: self.replacement_batch.serving_principal(),
                graph_version,
                placement_epoch: self.replacement_batch.placement_epoch,
                fencing_token: self
                    .replacement_batch
                    .fencing_token
                    .ok_or("CONFLICT: enrichment top-up parent fence is absent")?,
                created_at_ms: committed_at_ms,
            })?;
        let encoded_parent = rmp_serde::to_vec_named(&self.replacement_batch)
            .map_err(|_| "CONFLICT: enrichment top-up parent encode failed")?;
        let encoded_expected = rmp_serde::to_vec_named(&expected_parent)
            .map_err(|_| "CONFLICT: enrichment top-up expected parent encode failed")?;
        if encoded_parent != encoded_expected {
            return Err("CONFLICT: enrichment top-up parent authority changed".into());
        }
        Ok(())
    }

    fn old_snapshot_identity_is_invalid(&self, old: &EligibleSnapshot, graph: &str) -> bool {
        old.units
            .get(self.expected_budget.next_index as usize)
            .is_none_or(|unit| unit.compute_units != self.expected_park.required_units)
            || old.tenant_id != self.revision.tenant_id
            || old.graph != graph
            || old.repository_id != self.revision.repository_id
    }

    fn old_snapshot_budget_is_invalid(&self, old: &EligibleSnapshot) -> bool {
        old.source_envelope != self.expected_budget.source_envelope
            || old.policy_digest != self.expected_park.policy_digest
            || old.budget_units != self.expected_budget.total_budget_units
            || old.digest().ok().as_deref() != Some(self.expected_budget.snapshot_digest.as_str())
    }

    fn next_snapshot_is_invalid(&self, next: &EligibleSnapshot) -> bool {
        next.source_envelope != self.revision.source_envelope
            || next.policy_digest != self.revision.policy_digest
            || next.budget_units != self.revision.total_budget_units
            || next.digest().ok().as_deref() != Some(self.revision.snapshot_digest.as_str())
    }

    pub(crate) fn validate(&self, graph: &str, committed_at_ms: u64) -> Result<(), String> {
        BudgetValidation(self).validate_budget()?;
        AuthorityValidation(self).validate_authority(graph, committed_at_ms)?;
        self.validate_parent_batch(committed_at_ms)?;
        let replacement_intent = &self.replacement_batch.outbox[0];
        if self
            .old_delivery
            .identity
            .scope()
            .graph_name()
            .is_none_or(|name| name.as_str() != graph)
            || replacement_intent.topic != TOPIC
            || replacement_intent.key != self.revision.source_envelope
        {
            return Err("CONFLICT: enrichment top-up replacement intent is invalid".into());
        }
        let old = snapshot(&self.old_delivery.intent.payload)?;
        let next = snapshot(&replacement_intent.payload)?;
        if self.old_snapshot_identity_is_invalid(&old, graph)
            || self.old_snapshot_budget_is_invalid(&old)
            || self.next_snapshot_is_invalid(&next)
        {
            return Err("CONFLICT: enrichment top-up snapshot authority changed".into());
        }
        let mut expected_next = old;
        expected_next.source_envelope = self.revision.source_envelope.clone();
        expected_next.policy_digest = self.revision.policy_digest.clone();
        expected_next.budget_units = self.revision.total_budget_units;
        if next != expected_next {
            return Err("CONFLICT: enrichment top-up changed eligible source units".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::enrichment_snapshot::{EligibleUnit, EnrichmentWorkStage};
    use eg_types::mutation_batch::{CommittedVersion, COMPILED_BATCH_INCARNATION};
    use eg_types::MutationScopeIdentity;

    fn transition() -> EnrichmentTopUpTransition {
        let old = eg_compute::test_support::repository_enrichment_snapshot(
            "tenant-a",
            "graph-a",
            10,
            vec![EligibleUnit {
                content_digest: format!("sha256:{}", "d".repeat(64)),
                parser_capability_digest: "parser:v1".into(),
                stage: EnrichmentWorkStage::Classical,
                input_ref: format!("cas:sha256:{}", "d".repeat(64)),
                content_length: 1,
                compute_units: 11,
                demanded: false,
            }],
        );
        let old_digest = old.digest().unwrap();
        let mut next = old.clone();
        next.source_envelope = "source-two".into();
        next.policy_digest = "e".repeat(64);
        next.budget_units = 14;
        let next_digest = next.digest().unwrap();
        let identity =
            MutationScopeIdentity::fixed_graph("tenant-a", "graph-a", COMPILED_BATCH_INCARNATION)
                .unwrap();
        let old_delivery = MutationOutboxRecord {
            schema_version: eg_types::MUTATION_BATCH_VERSION,
            batch_id: "source-one".into(),
            ordinal: 0,
            identity,
            committed_version: CommittedVersion::Graph {
                source: 0,
                target: 1,
            },
            commit_sequence: Some(1),
            intent: super::super::pending_repository_intent(old, TOPIC),
            created_at_ms: 1,
        };
        let replacement_batch =
            MutationBatch::repository_enrichment_top_up(RepositoryEnrichmentTopUp {
                old_delivery: &old_delivery,
                policy_sequence: 1,
                revision_idempotency_key: "topup-1",
                replacement_intent: super::super::pending_repository_intent(next, TOPIC),
                replacement_batch_id: "source-two",
                serving_principal: &format!("principal:sha256:{}", "a".repeat(64)),
                graph_version: 1,
                placement_epoch: 1,
                fencing_token: 1,
                created_at_ms: 12,
            })
            .unwrap();
        EnrichmentTopUpTransition {
            old_delivery,
            consumer: CONSUMER.into(),
            replacement_batch,
            expected_budget: EnrichmentBudgetCheckpoint {
                schema_version: 1,
                tenant_id: "tenant-a".into(),
                snapshot_digest: old_digest.clone(),
                source_envelope: "source-one".into(),
                next_index: 0,
                page_number: 0,
                reserved_units: 0,
                spent_units: 0,
                remaining_units: 10,
                total_budget_units: 10,
                last_page_key: String::new(),
            },
            expected_park: EnrichmentBudgetPark {
                schema_version: 1,
                source_envelope: "source-one".into(),
                snapshot_digest: old_digest.clone(),
                next_index: 0,
                page_number: 0,
                remaining_units: 10,
                required_units: 11,
                policy_digest: "a".repeat(64),
                parked_at_ms: 11,
            },
            replacement_budget: EnrichmentBudgetCheckpoint {
                schema_version: 1,
                tenant_id: "tenant-a".into(),
                snapshot_digest: next_digest.clone(),
                source_envelope: "source-two".into(),
                next_index: 0,
                page_number: 0,
                reserved_units: 0,
                spent_units: 0,
                remaining_units: 14,
                total_budget_units: 14,
                last_page_key: String::new(),
            },
            revision: RepositoryEnrichmentPolicyRevision {
                schema_version: 1,
                tenant_id: "tenant-a".into(),
                graph: "graph-a".into(),
                repository_id: "repo".into(),
                source_envelope: "source-two".into(),
                snapshot_digest: next_digest,
                policy_digest: "e".repeat(64),
                total_budget_units: 14,
                max_total_units: 14,
                sequence: 1,
                prior_source_envelope: Some("source-one".into()),
                prior_snapshot_digest: Some(old_digest),
                prior_policy_digest: Some("a".repeat(64)),
                caller_subject: Some("principal:sha256:operator".into()),
                verified_action: Some("repository:enrichment:budget:top_up".into()),
                idempotency_key: Some("topup-1".into()),
            },
        }
    }

    #[test]
    fn sealed_top_up_is_source_bound_and_keeps_plaintext_out_of_log() {
        let plan = transition();
        plan.validate("graph-a", 12).unwrap();
        let command =
            super::super::ReplicatedMutation::enrichment_top_up(&plan, "graph-a", 12, "secret")
                .unwrap();
        let encoded = rmp_serde::to_vec_named(&command).unwrap();
        assert!(!encoded.windows(10).any(|part| part == b"source-one"));
        assert!(command
            .open_enrichment_top_up("graph-a", 12, "wrong")
            .is_err());
        assert!(command
            .open_enrichment_top_up("other", 12, "secret")
            .is_err());
        assert!(command
            .open_enrichment_top_up("graph-a", 12, "secret")
            .unwrap()
            .is_some());
        let mut forged = plan;
        forged.replacement_batch.outbox[0].key = "another-source".into();
        assert!(forged.validate("graph-a", 12).is_err());
        let mut forged = transition();
        forged.replacement_batch.outbox[0].payload.push(0);
        assert!(forged.validate("graph-a", 12).is_err());
    }

    #[test]
    fn serialized_top_up_reopens_only_at_its_original_graph_and_commit_time() {
        let plan = transition();
        let command =
            super::super::ReplicatedMutation::enrichment_top_up(&plan, "graph-a", 12, "secret")
                .unwrap();
        // The follower and a restarted leader receive serialized log entries,
        // not the leader's in-memory transition. Verify that the sealed record
        // survives that boundary with its graph and commit-time binding.
        let bytes = rmp_serde::to_vec_named(&command).unwrap();
        let replayed: super::super::ReplicatedMutation = rmp_serde::from_slice(&bytes).unwrap();
        let reopened = replayed
            .open_enrichment_top_up("graph-a", 12, "secret")
            .unwrap()
            .unwrap();
        reopened.validate("graph-a", 12).unwrap();
        assert!(replayed
            .open_enrichment_top_up("graph-a", 13, "secret")
            .is_err());
        assert!(replayed
            .open_enrichment_top_up("graph-b", 12, "secret")
            .is_err());
    }

    #[test]
    fn only_exact_committed_retry_may_cross_a_new_route_fence() {
        let plan = transition();
        assert!(plan.validate_parent_fence(1, Some(1), None, 12).is_ok());
        assert!(plan.validate_parent_fence(2, Some(2), None, 12).is_err());
        let mut record = eg_types::MutationBatchRecord {
            batch: plan.replacement_batch.clone(),
            identity: plan.replacement_batch.identity.clone(),
            status: MutationBatchStatus::Committed,
            committed_version: CommittedVersion::Graph {
                source: 1,
                target: 2,
            },
            result_msgpack: Some(rmp_serde::to_vec_named(&plan).unwrap()),
            committed_at_ms: 12,
        };
        assert!(plan
            .validate_parent_fence(2, Some(2), Some(&record), 12)
            .is_ok());
        // A follower replay after failover can use the new route only for the
        // byte-identical committed transition. Reusing the batch ID with a
        // changed intent payload must not inherit the old receipt.
        let mut altered = plan.clone();
        altered.replacement_batch.outbox[0].payload.push(0);
        assert!(altered
            .validate_parent_fence(2, Some(2), Some(&record), 12)
            .is_err());
        assert!(plan
            .validate_parent_fence(2, Some(2), Some(&record), 13)
            .is_err());
        record.result_msgpack.as_mut().unwrap().push(0);
        assert!(plan
            .validate_parent_fence(2, Some(2), Some(&record), 12)
            .is_err());
        record.result_msgpack = Some(rmp_serde::to_vec_named(&plan).unwrap());
        record.status = MutationBatchStatus::Aborted;
        assert!(plan
            .validate_parent_fence(2, Some(2), Some(&record), 12)
            .is_err());
    }
}
