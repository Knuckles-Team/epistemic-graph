//! Exact-graph RBAC grant for the in-process repository enrichment consumer.

use crate::acl::{Grant, GrantEffect, RbacAction, ResourceSelector, Role};
use crate::isolation::{AgentIdentity, AgentRole, IsolationLayer};

pub(super) const SERVICE_ACTOR: &str = "service:repository-enrichment-v1";

fn role_for(graph: &str) -> String {
    format!("repository-enrichment:{graph}")
}

/// Grant only the graph named by a verified, held enrichment outbox lease.
/// Idempotent across retries and restart; never consumes an unbootstrapped
/// identity policy.
pub(super) fn ensure(isolation: &mut IsolationLayer, graph: &str) -> Result<(), String> {
    if isolation.identity_bootstrap_pending() {
        return Err("REPOSITORY_ENRICHMENT_POLICY_UNBOOTSTRAPPED".into());
    }
    let role = role_for(graph);
    for action in [RbacAction::Read, RbacAction::Write] {
        let grant = Grant {
            role: role.clone(),
            resource: ResourceSelector::Graph(graph.to_string()),
            action,
            effect: GrantEffect::Allow,
        };
        if !isolation.rbac().grants().contains(&grant) {
            isolation.try_add_role(Role::new(role.clone()))?;
            isolation.try_add_grant(grant)?;
        }
    }
    let mut identity = isolation
        .get_identity(SERVICE_ACTOR)
        .unwrap_or_else(|| AgentIdentity {
            agent_id: SERVICE_ACTOR.to_string(),
            role: AgentRole::Agent,
            teams: Vec::new(),
            roles: Vec::new(),
        });
    if identity.roles.contains(&role) {
        return Ok(());
    }
    identity.roles.push(role);
    isolation.try_register_agent(identity)
}

/// Bind a proposed top-up record to the authenticated request, exact graph
/// Write RBAC, and the committed source policy before a Raft proposal. The
/// caller must fetch `retained` from persistence; only the follower's atomic
/// compare and swap makes that read authoritative at commit time.
#[allow(dead_code)]
pub(super) fn bind_top_up_identity(
    verified: &crate::server::authority_context::VerifiedRequestContext,
    isolation: &IsolationLayer,
    graph_type: crate::protocol::GraphType,
    graph_owner: Option<&str>,
    retained: &crate::redb_store::enrichment_budget::RepositoryEnrichmentPolicyRevision,
    proposal: &crate::parser::enrichment_reactivation::BudgetRevisionProposal,
    replacement: &eg_types::native_control::EnrichmentBudgetCheckpoint,
) -> Result<crate::redb_store::enrichment_budget::RepositoryEnrichmentPolicyRevision, String> {
    use crate::parser::enrichment_reactivation::TOP_UP_ACTION;
    if !verified.allows_action(TOP_UP_ACTION)
        || !crate::server::access::principal_may_access(
            isolation,
            verified.agent_id(),
            &proposal.graph,
            graph_type,
            graph_owner,
            eg_types::acl::AccessCheck::Write,
        )
        || verified.tenant() != proposal.tenant_id
        || verified.principal_persistence_id() != proposal.caller_subject
        || verified.idempotency_key() != proposal.idempotency_key
        || proposal.verified_action != TOP_UP_ACTION
        || proposal.next_policy_sequence
            != proposal
                .expected_policy_sequence
                .checked_add(1)
                .ok_or("CONFLICT: repository enrichment policy sequence overflow")?
        || replacement.tenant_id != proposal.tenant_id
        || replacement.source_envelope != proposal.new_source_envelope
        || replacement.total_budget_units != proposal.replacement_total_units
        || retained.schema_version != 1
        || retained.tenant_id != proposal.tenant_id
        || retained.graph != proposal.graph
        || retained.repository_id != proposal.repository_id
        || retained.source_envelope != proposal.source_envelope
        || retained.snapshot_digest != proposal.source_snapshot_digest
        || retained.policy_digest != proposal.prior_policy_digest
        || retained.sequence != proposal.expected_policy_sequence
        || retained.max_total_units == 0
        || retained.total_budget_units >= proposal.replacement_total_units
        || retained.max_total_units < proposal.replacement_total_units
    {
        return Err("ACCESS_DENIED: repository enrichment top-up caller is not verified".into());
    }
    Ok(
        crate::redb_store::enrichment_budget::RepositoryEnrichmentPolicyRevision {
            schema_version: 1,
            tenant_id: proposal.tenant_id.clone(),
            graph: proposal.graph.clone(),
            repository_id: proposal.repository_id.clone(),
            source_envelope: proposal.new_source_envelope.clone(),
            snapshot_digest: replacement.snapshot_digest.clone(),
            policy_digest: proposal.replacement_policy_digest.clone(),
            total_budget_units: replacement.total_budget_units,
            max_total_units: retained.max_total_units,
            sequence: proposal.next_policy_sequence,
            prior_source_envelope: Some(proposal.source_envelope.clone()),
            prior_snapshot_digest: Some(proposal.source_snapshot_digest.clone()),
            prior_policy_digest: Some(proposal.prior_policy_digest.clone()),
            caller_subject: Some(verified.principal_persistence_id()),
            verified_action: Some(TOP_UP_ACTION.into()),
            idempotency_key: Some(verified.idempotency_key().into()),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::isolation::AccessLevel;
    use crate::protocol::GraphType;

    #[test]
    fn top_up_identity_requires_exact_verified_scope_subject_and_key() {
        use crate::parser::enrichment_reactivation::{BudgetRevisionProposal, TOP_UP_ACTION};
        use crate::server::authority_context::VerifiedRequestContext;
        let verified = VerifiedRequestContext::verified_for_test_with_scopes(
            "operator",
            "tenant-a",
            &[TOP_UP_ACTION],
        );
        let proposal = BudgetRevisionProposal {
            tenant_id: "tenant-a".into(),
            graph: "graph-a".into(),
            repository_id: "repo".into(),
            source_envelope: "source-one".into(),
            source_snapshot_digest: "a".repeat(64),
            prior_policy_digest: "b".repeat(64),
            replacement_policy_digest: "c".repeat(64),
            replacement_total_units: 14,
            new_source_envelope: "source-two".into(),
            expected_policy_sequence: 0,
            next_policy_sequence: 1,
            caller_subject: verified.principal_persistence_id(),
            verified_action: TOP_UP_ACTION.into(),
            idempotency_key: verified.idempotency_key().into(),
        };
        let checkpoint = eg_types::native_control::EnrichmentBudgetCheckpoint {
            schema_version: 1,
            tenant_id: "tenant-a".into(),
            snapshot_digest: "d".repeat(64),
            source_envelope: "source-two".into(),
            next_index: 1,
            page_number: 1,
            reserved_units: 6,
            spent_units: 0,
            remaining_units: 8,
            total_budget_units: 14,
            last_page_key: String::new(),
        };
        let retained = crate::redb_store::enrichment_budget::RepositoryEnrichmentPolicyRevision {
            schema_version: 1,
            tenant_id: proposal.tenant_id.clone(),
            graph: proposal.graph.clone(),
            repository_id: proposal.repository_id.clone(),
            source_envelope: proposal.source_envelope.clone(),
            snapshot_digest: proposal.source_snapshot_digest.clone(),
            policy_digest: proposal.prior_policy_digest.clone(),
            total_budget_units: 10,
            max_total_units: 14,
            sequence: 0,
            prior_source_envelope: None,
            prior_snapshot_digest: None,
            prior_policy_digest: None,
            caller_subject: None,
            verified_action: None,
            idempotency_key: None,
        };
        let mut isolation = IsolationLayer::new();
        isolation
            .try_register_agent(AgentIdentity {
                agent_id: "operator".into(),
                role: AgentRole::Agent,
                teams: Vec::new(),
                roles: Vec::new(),
            })
            .unwrap();
        isolation.try_add_role(Role::new("top-up-writer")).unwrap();
        isolation
            .try_add_grant(Grant {
                role: "top-up-writer".into(),
                resource: ResourceSelector::Graph("graph-a".into()),
                action: RbacAction::Write,
                effect: GrantEffect::Allow,
            })
            .unwrap();
        let mut identity = isolation.get_identity("operator").unwrap();
        identity.roles.push("top-up-writer".into());
        isolation.try_register_agent(identity).unwrap();
        let bind =
            |context: &VerifiedRequestContext,
             proposed: &BudgetRevisionProposal,
             policy: &crate::redb_store::enrichment_budget::RepositoryEnrichmentPolicyRevision,
             rules: &IsolationLayer| {
                bind_top_up_identity(
                    context,
                    rules,
                    GraphType::Global,
                    None,
                    policy,
                    proposed,
                    &checkpoint,
                )
            };
        let row = bind(&verified, &proposal, &retained, &isolation).unwrap();
        assert_eq!(
            row.caller_subject.as_deref(),
            Some(verified.principal_persistence_id().as_str())
        );
        let no_grant = VerifiedRequestContext::verified_for_test_with_scopes(
            "operator",
            "tenant-a",
            &["kg:write"],
        );
        assert!(bind(&no_grant, &proposal, &retained, &isolation).is_err());
        let mut forged = proposal.clone();
        forged.caller_subject = "attacker".into();
        assert!(bind(&verified, &forged, &retained, &isolation).is_err());
        forged = proposal.clone();
        forged.idempotency_key = "another-request".into();
        assert!(bind(&verified, &forged, &retained, &isolation).is_err());
        let mut revoked = IsolationLayer::new();
        revoked
            .try_register_agent(AgentIdentity {
                agent_id: "operator".into(),
                role: AgentRole::Agent,
                teams: Vec::new(),
                roles: Vec::new(),
            })
            .unwrap();
        assert!(bind(&verified, &proposal, &retained, &revoked).is_err());
        ensure(&mut revoked, "graph-b").unwrap();
        assert!(bind(&verified, &proposal, &retained, &revoked).is_err());
        let mut old = retained.clone();
        old.max_total_units = 0;
        assert!(bind(&verified, &proposal, &old, &isolation).is_err());
        old.max_total_units = 13;
        assert!(bind(&verified, &proposal, &old, &isolation).is_err());
        let mut legacy = serde_json::to_value(&retained).unwrap();
        legacy.as_object_mut().unwrap().remove("max_total_units");
        let legacy_bytes = rmp_serde::to_vec_named(&legacy).unwrap();
        let legacy: crate::redb_store::enrichment_budget::RepositoryEnrichmentPolicyRevision =
            rmp_serde::from_slice(&legacy_bytes).unwrap();
        assert_eq!(legacy.max_total_units, 0);
        assert!(bind(&verified, &proposal, &legacy, &isolation).is_err());
    }

    #[test]
    fn service_grant_is_idempotent_and_exact_graph() {
        let mut isolation = crate::server::state::ServerState::test_isolation("admin");
        ensure(&mut isolation, "code-a").unwrap();
        ensure(&mut isolation, "code-a").unwrap();
        assert!(isolation.check_access(
            SERVICE_ACTOR,
            "code-a",
            GraphType::Global,
            None,
            AccessLevel::Write
        ));
        assert!(!isolation.check_access(
            SERVICE_ACTOR,
            "code-b",
            GraphType::Global,
            None,
            AccessLevel::Write
        ));
        assert_eq!(
            isolation.get_identity(SERVICE_ACTOR).unwrap().roles,
            ["repository-enrichment:code-a"]
        );
    }
}
