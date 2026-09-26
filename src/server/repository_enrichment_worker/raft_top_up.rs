//! Private EH-557 top-up producer. No public Method routes here yet.

use std::sync::Arc;

use eg_types::mutation_batch::RepositoryEnrichmentTopUp;
use eg_types::MutationOutboxRecord;
use tokio::sync::RwLock;

use crate::parser::enrichment_reactivation::{plan_reactivation, BudgetRevisionProposal};
use crate::raft::{EnrichmentTopUpTransition, RaftMutationContext, RaftMutationTiming};
use crate::server::authority_context::VerifiedRequestContext;
use crate::server::persistence::PersistenceBackend;
use crate::server::state::ServerState;

struct PreparedTopUp {
    transition: EnrichmentTopUpTransition,
    routed: crate::raft::multi::RoutedRaftHandle,
    graph_type: crate::protocol::GraphType,
    committed_at_ms: u64,
}

/// The preparation reads several durable owner rows and can outlive a
/// placement change. Refuse a proposal through the stale handle before
/// `client_write`; the replicated parent fence remains the final authority if
/// placement changes after this preflight.
async fn require_current_local_leader(
    state: &Arc<RwLock<ServerState>>,
    graph: &str,
    prepared: &crate::raft::multi::RoutedRaftHandle,
) -> Result<(), String> {
    let multi = state
        .read()
        .await
        .multi_raft
        .clone()
        .ok_or("STALE_ROUTE: repository enrichment placement is unavailable")?;
    let current = multi
        .handle_for_graph(graph)
        .await
        .ok_or("STALE_ROUTE: repository enrichment group is unavailable")?;
    if !route_fence_matches(prepared, &current)
        || current.handle.current_leader().await != Some(current.handle.node_id)
    {
        return Err("STALE_ROUTE: repository enrichment top-up leader changed".into());
    }
    Ok(())
}

fn route_fence_matches(
    prepared: &crate::raft::multi::RoutedRaftHandle,
    current: &crate::raft::multi::RoutedRaftHandle,
) -> bool {
    same_placement(
        (prepared.placed, prepared.group_id, prepared.epoch),
        (current.placed, current.group_id, current.epoch),
    ) && prepared.handle.node_id == current.handle.node_id
}

fn same_placement(prepared: (bool, u64, u64), current: (bool, u64, u64)) -> bool {
    prepared.0 && current.0 && prepared.1 == current.1 && prepared.2 == current.2
}

/// Fetch graph policy and the source from committed owner rows. The proposal
/// only names them; it never supplies an outbox record, ceiling, or route fence.
async fn prepare(
    state: &Arc<RwLock<ServerState>>,
    verified: &VerifiedRequestContext,
    proposal: &BudgetRevisionProposal,
) -> Result<PreparedTopUp, String> {
    if !verified.allows_action(crate::parser::enrichment_reactivation::TOP_UP_ACTION) {
        return Err("ACCESS_DENIED: repository enrichment top-up scope is absent".into());
    }
    let graph = proposal.graph.as_str();
    let graph_fname = crate::persist::sanitize(graph);
    if graph_fname != graph {
        return Err("CONFLICT: repository enrichment top-up requires canonical graph name".into());
    }
    let (backend, multi, graph_type) = {
        let current = state.read().await;
        let entry = current
            .registry
            .get(graph)
            .ok_or("ACCESS_DENIED: repository enrichment graph is unavailable")?;
        if !crate::server::access::principal_may_access(
            &current.isolation,
            verified.agent_id(),
            graph,
            entry.graph_type,
            entry.owner.as_deref(),
            eg_types::acl::AccessCheck::Write,
        ) {
            return Err("ACCESS_DENIED: repository enrichment graph Write grant is absent".into());
        }
        (
            current
                .persistence
                .clone()
                .ok_or("CONFLICT: repository enrichment persistence is unavailable")?,
            current
                .multi_raft
                .clone()
                .ok_or("CONFLICT: repository enrichment top-up requires multi-Raft")?,
            entry.graph_type,
        )
    };
    let routed = multi
        .handle_for_graph(graph)
        .await
        .ok_or("STALE_ROUTE: repository enrichment graph group is unavailable")?;
    if !routed.placed || routed.handle.current_leader().await != Some(routed.handle.node_id) {
        return Err(
            "STALE_ROUTE: repository enrichment top-up requires placed local leader".into(),
        );
    }
    let retained = backend
        .read_enrichment_policy_revision(&graph_fname, &proposal.source_envelope)
        .await?
        .ok_or("CONFLICT: repository enrichment source policy is absent")?;
    // A legacy revision has no committed ceiling. No request or environment
    // value can fill it in after the source commit.
    if retained.max_total_units == 0 {
        return Err("ACCESS_DENIED: repository enrichment source has no retained ceiling".into());
    }
    let prior = backend
        .read_mutation_batch(&graph_fname, &proposal.new_source_envelope)
        .await?;
    let committed_at_ms;
    let transition = if let Some(record) = prior {
        let bytes = record
            .result_msgpack
            .as_deref()
            .ok_or("CONFLICT: repository enrichment prior receipt is absent")?;
        let original: EnrichmentTopUpTransition = eg_types::msgpack::decode_bounded(
            bytes,
            eg_types::msgpack::MsgpackLimits::new(16 * 1024 * 1024, 200_000, 96),
        )
        .map_err(|_| "CONFLICT: repository enrichment prior receipt is invalid")?;
        original.validate(graph, record.committed_at_ms)?;
        original.validate_parent_fence(
            routed.epoch,
            Some(routed.group_id),
            Some(&record),
            record.committed_at_ms,
        )?;
        let fresh_revision = {
            let current = state.read().await;
            let entry = current
                .registry
                .get(graph)
                .ok_or("ACCESS_DENIED: repository enrichment graph is unavailable")?;
            super::grant::bind_top_up_identity(
                verified,
                &current.isolation,
                entry.graph_type,
                entry.owner.as_deref(),
                &retained,
                proposal,
                &original.replacement_budget,
            )?
        };
        if fresh_revision != original.revision {
            return Err("IDEMPOTENCY_CONFLICT: repository enrichment retry changed".into());
        }
        committed_at_ms = record.committed_at_ms;
        original
    } else {
        let budget = backend
            .read_enrichment_budget_checkpoint(&graph_fname, &proposal.source_envelope)
            .await?
            .ok_or("CONFLICT: repository enrichment source budget is absent")?;
        let park = backend
            .read_enrichment_budget_park(&graph_fname)
            .await?
            .ok_or("CONFLICT: repository enrichment park is absent")?;
        let parent_batch_id = if retained.sequence == 0 {
            backend
                .read_change_envelope(&graph_fname, &proposal.source_envelope)
                .await?
                .ok_or("CONFLICT: repository enrichment source envelope is absent")?
                .envelope
                .mutation
                .batch_id
        } else {
            proposal.source_envelope.clone()
        };
        let old_delivery = exact_pending_record(
            backend.as_ref(),
            &graph_fname,
            &parent_batch_id,
            &proposal.source_envelope,
        )
        .await?;
        let snapshot =
            crate::server::dispatch::decode_pending_enrichment_intent(&old_delivery.intent)?;
        let plan = plan_reactivation(
            &snapshot,
            &budget,
            &park,
            proposal,
            retained.max_total_units,
        )?;
        let revision = {
            let current = state.read().await;
            let entry = current
                .registry
                .get(graph)
                .ok_or("ACCESS_DENIED: repository enrichment graph is unavailable")?;
            super::grant::bind_top_up_identity(
                verified,
                &current.isolation,
                entry.graph_type,
                entry.owner.as_deref(),
                &retained,
                proposal,
                &plan.checkpoint,
            )?
        };
        let version = backend
            .read_mutation_graph_version(&graph_fname)
            .await?
            .ok_or("CONFLICT: repository enrichment graph version is absent")?;
        committed_at_ms = crate::server::dispatch::authoritative_now_ms();
        let replacement_intent =
            crate::server::dispatch::enrichment_intent_for_snapshot(plan.snapshot)?;
        let replacement_batch =
            eg_types::MutationBatch::repository_enrichment_top_up(RepositoryEnrichmentTopUp {
                old_delivery: &old_delivery,
                policy_sequence: revision.sequence,
                revision_idempotency_key: revision.idempotency_key.as_deref().unwrap_or_default(),
                replacement_intent,
                replacement_batch_id: &proposal.new_source_envelope,
                serving_principal: crate::mutation_apply::ENGINE_LEDGER_PRINCIPAL,
                graph_version: version,
                placement_epoch: routed.epoch,
                fencing_token: routed.group_id,
                created_at_ms: committed_at_ms,
            })?;
        EnrichmentTopUpTransition {
            old_delivery,
            consumer: "repository-enrichment-v1".into(),
            replacement_batch,
            expected_budget: budget,
            expected_park: park,
            replacement_budget: plan.checkpoint,
            revision,
        }
    };
    transition.validate(graph, committed_at_ms)?;
    Ok(PreparedTopUp {
        transition,
        routed,
        graph_type,
        committed_at_ms,
    })
}

async fn exact_pending_record(
    backend: &dyn PersistenceBackend,
    graph: &str,
    batch_id: &str,
    source_envelope: &str,
) -> Result<MutationOutboxRecord, String> {
    let records = backend.read_mutation_outbox(graph, batch_id).await?;
    let mut matching = records.into_iter().filter(|record| {
        record.intent.topic == "repository.enrichment.pending"
            && record.intent.key == source_envelope
    });
    let record = matching
        .next()
        .ok_or("CONFLICT: repository enrichment source outbox row is absent")?;
    if matching.next().is_some()
        || record.commit_sequence.is_none()
        || record
            .identity
            .scope()
            .graph_name()
            .is_none_or(|name| name.as_str() != graph)
    {
        return Err("CONFLICT: repository enrichment source outbox identity changed".into());
    }
    record.validate()?;
    Ok(record)
}

/// Private entry point; no served Method maps here until the full acceptance
/// matrix passes. The authenticated transport nonce is consumed by Raft's
/// replicated request and never derived from the proposal.
#[allow(dead_code)]
pub(crate) async fn propose_top_up(
    state: &Arc<RwLock<ServerState>>,
    verified: &VerifiedRequestContext,
    request_id: u64,
    proposal: &BudgetRevisionProposal,
) -> Result<crate::mutation_batch::MutationBatchCommit, String> {
    let prepared = prepare(state, verified, proposal).await?;
    let attempt_nonce = verified
        .attempt_nonce()
        .ok_or("ACCESS_DENIED: repository enrichment top-up needs transport nonce")?;
    let access = crate::server::access::CarrierAuthority::from_verified(verified)?;
    let mutation = RaftMutationContext::from_verified_request(
        prepared.transition.replacement_batch.batch_id.clone(),
        request_id,
        Some(attempt_nonce),
        access.tenant_scope(),
        access.actor_scope().to_string(),
        false,
        RaftMutationTiming {
            placement_epoch: prepared.routed.epoch,
            fencing_token: Some(prepared.routed.group_id),
            created_at_ms: prepared.committed_at_ms,
        },
    )?;
    let graph = proposal.graph.as_str();
    let command = crate::raft::ReplicatedMutation::enrichment_top_up(
        &prepared.transition,
        graph,
        prepared.committed_at_ms,
        &state.read().await.auth_secret,
    )?;
    require_current_local_leader(state, graph, &prepared.routed).await?;
    let response = prepared
        .routed
        .handle
        .client_write(crate::raft::RaftRequest {
            graph_fname: crate::persist::sanitize(graph),
            graph_name: graph.to_string(),
            graph_type: prepared.graph_type,
            command,
            committed_at_ms: prepared.committed_at_ms,
            mutation,
        })
        .await?;
    if !response.applied {
        return Err("CONFLICT: repository enrichment top-up was not applied".into());
    }
    if let Some(error) = response.native_error {
        return Err(error);
    }
    let committed = response
        .native_commit
        .ok_or("CONFLICT: repository enrichment top-up lacks durable receipt")?;
    committed.validate()?;
    Ok(committed)
}

#[cfg(test)]
mod tests {
    use super::same_placement;

    #[test]
    fn top_up_preflight_refuses_changed_or_unplaced_route() {
        let prepared = (true, 4, 8);
        assert!(same_placement(prepared, (true, 4, 8)));
        assert!(!same_placement(prepared, (true, 4, 9)));
        assert!(!same_placement(prepared, (true, 5, 8)));
        assert!(!same_placement(prepared, (false, 4, 8)));
        assert!(!same_placement((false, 4, 8), (true, 4, 8)));
    }
}
