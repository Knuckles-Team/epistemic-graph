//! RF-020 `kg-delegate` admission adapter.
//!
//! This handler validates the authenticated outer context and the retained
//! Agent Library revision, then lowers the request to the existing native
//! `Method::SubmitWorkItem`.  The caller sends that method through the ordinary
//! WorkItem admission route and maps its committed `SubmitWorkItemResult` with
//! [`result_from_submit`].  The native command log and outbox remain the sole
//! admission/result authority.

use std::collections::BTreeMap;
#[cfg(feature = "redb")]
use std::sync::Arc;

use eg_types::agent_library::AgentLibraryEntry;
#[cfg(feature = "redb")]
use eg_types::delegation::AgentLibraryEntryRef;
use eg_types::delegation::{KgDelegateDecision, KgDelegateRequest, KgDelegateResult};
use eg_types::epistemic_operations::RequestContext;
#[cfg(feature = "redb")]
use eg_types::mutation_batch::MutationBatchStatus;
use eg_types::native_control::{
    NativeControlSchemaVersion, SubmitWorkItemRequest, SubmitWorkItemResult,
};
use eg_types::protocol::Method;
#[cfg(feature = "redb")]
use eg_types::protocol::{Response, ResultPayload};
use serde_json::json;

use crate::server::auth::VerifiedRequestContext;

#[cfg(feature = "redb")]
use crate::server::persistence::PersistenceBackend;

#[cfg(feature = "redb")]
use crate::graph::GraphCore;

#[cfg(feature = "redb")]
use crate::server::persistence::agent_library::AgentLibraryStore;

#[path = "delegation/admission.rs"]
mod admission;
#[cfg(feature = "redb")]
#[path = "delegation/validation.rs"]
mod validation;

pub(crate) use admission::*;
#[cfg(all(test, feature = "redb", feature = "raft"))]
pub(crate) use validation::provenance_refs;
#[cfg(all(test, feature = "redb"))]
use validation::unprefixed_digest;
#[cfg(feature = "redb")]
pub(crate) use validation::{
    bind_request, decode_submit_result, lower_work_item, placement_authority,
    replay_request_matches, result_from_submit, retained_agent, retained_graph,
    validate_request_binding,
};

/// Shared identity comparison for request-boundary and delegation validation.
///
/// This remains available in slim builds because the native WorkItem request
/// boundary uses the same authenticated-authority comparison even when the
/// redb-backed delegation adapter is not compiled.
pub(crate) fn context_matches_verified_authority(
    context: &RequestContext,
    verified_context: &VerifiedRequestContext,
) -> bool {
    context.tenant_id == verified_context.tenant()
        && context.agent_id == verified_context.agent_id()
        && context.audience == verified_context.claims().audience
        && context.policy_version == verified_context.claims().policy_version
}

// Every admission under test resolves a retained definition through the
// `redb`-backed Agent Library owner.
#[cfg(all(test, feature = "redb"))]
mod tests {
    use super::*;
    use eg_types::agent_library::{AgentLibraryEntryDraft, AgentLibraryLifecycle};
    use eg_types::delegation::{KgDelegateSchemaVersion, MAX_DELEGATION_IN_FLIGHT};
    use eg_types::epistemic_operations::{
        RequestContext, RequestContextAuthenticationMethod, RequestContextSchemaVersion,
    };

    fn digest(byte: char) -> String {
        byte.to_string().repeat(64)
    }

    fn prefixed_digest(byte: char) -> String {
        format!("sha256:{}", digest(byte))
    }

    fn context(tenant: &str) -> RequestContext {
        context_with_scopes(tenant, &["work:delegate"])
    }

    /// A request context asking for exactly `scopes`.
    ///
    /// `validate_request_context` rejects a context that requests ANY scope the
    /// verified carrier does not hold, and it runs BEFORE the `work:delegate`
    /// gate. So a fixture that keeps `work:delegate` in the request context
    /// while handing `bind_request` a caller without it is testing forged-scope
    /// rejection, not the delegate-policy gate -- see
    /// `missing_delegate_policy_is_rejected_before_library_lookup`.
    fn context_with_scopes(tenant: &str, scopes: &[&str]) -> RequestContext {
        RequestContext {
            schema_version: RequestContextSchemaVersion::V2,
            request_id: "request:1".into(),
            subject_id: "subject:1".into(),
            tenant_id: tenant.into(),
            agent_id: "agent:1".into(),
            scopes: scopes.iter().map(|scope| (*scope).to_string()).collect(),
            audience: "epistemic-graph".into(),
            authentication_method: RequestContextAuthenticationMethod::LocalProcess,
            policy_version: "policy-test".into(),
            graph: tenant.into(),
            placement_epoch: Some(1),
            trace_id: "trace:context".into(),
            issued_at_ms: 1,
            expires_at_ms: 2,
        }
    }

    fn agent_entry() -> AgentLibraryEntry {
        agent_entry_for("agent:1")
    }

    fn agent_entry_for(agent_id: &str) -> AgentLibraryEntry {
        AgentLibraryEntry::create(
            AgentLibraryEntryDraft {
                agent_id: agent_id.into(),
                package_id: "package:1".into(),
                version: "1.0.0".into(),
                role: "worker".into(),
                role_digest: prefixed_digest('a'),
                system_prompt: eg_types::agent_component::ComponentDependency {
                    component_id: "prompt:1".into(),
                    kind: eg_types::agent_component::AgentComponentKind::SystemPrompt,
                    definition_digest: prefixed_digest('b'),
                },
                tools: vec![eg_types::agent_component::ComponentDependency {
                    component_id: "tool:1".into(),
                    kind: eg_types::agent_component::AgentComponentKind::Tool,
                    definition_digest: prefixed_digest('c'),
                }],
                skills: vec![eg_types::agent_component::ComponentDependency {
                    component_id: "skill:1".into(),
                    kind: eg_types::agent_component::AgentComponentKind::Skill,
                    definition_digest: prefixed_digest('d'),
                }],
                model_profile: eg_types::agent_component::ComponentDependency {
                    component_id: "model-profile:1".into(),
                    kind: eg_types::agent_component::AgentComponentKind::ModelProfile,
                    definition_digest: prefixed_digest('e'),
                },
                model_identity: "model:1".into(),
                ontologies: vec![eg_types::agent_component::ComponentDependency {
                    component_id: "ontology:1".into(),
                    kind: eg_types::agent_component::AgentComponentKind::Ontology,
                    definition_digest: prefixed_digest('f'),
                }],
                tenant_id: "tenant:1".into(),
                actor_scope: format!("tenant:1/{agent_id}"),
                purpose_id: "delegation.execute".into(),
                policy_digest: prefixed_digest('0'),
                source_revision: "library-source:7".into(),
                source_revision_digest: prefixed_digest('1'),
                runtime: Default::default(),
                instantiated_from: None,
            },
            7,
            AgentLibraryLifecycle::Published,
            1,
            1,
        )
        .unwrap()
    }

    fn request(tenant: &str, entry: &AgentLibraryEntry) -> KgDelegateRequest {
        request_with_context(context(tenant), entry)
    }

    fn request_with_context(
        context: RequestContext,
        entry: &AgentLibraryEntry,
    ) -> KgDelegateRequest {
        KgDelegateRequest {
            schema_version: KgDelegateSchemaVersion::V2,
            context,
            delegation_id: "delegation:1".into(),
            run_id: "run:1".into(),
            trace_id: "trace:run:1".into(),
            target: eg_types::delegation::DelegationTarget::Agent {
                entry: AgentLibraryEntryRef::from_entry(entry),
            },
            input_ref: "cas:input:1".into(),
            command_digest: digest('2'),
            capability_digest: unprefixed_digest(
                "tool_surface_digest",
                &entry.tool_surface_digest(),
            )
            .unwrap(),
            catalog_digest: eg_capabilities::CONTRACT_CATALOG_DIGEST.to_string(),
            policy_digest: entry.policy_digest.clone(),
            model_digest: Some(
                unprefixed_digest("model_profile_digest", entry.model_profile_digest()).unwrap(),
            ),
            idempotency_key: "delegate-idempotency:1".into(),
            kind: "agent.execute".into(),
            actor_scope: entry.actor_scope.clone(),
            purpose: entry.purpose_id.clone(),
            work_item_id: Some("workitem:1".into()),
            priority: 10,
            max_attempts: 3,
            deadline_unix: Some(2_000.0),
            max_tenant_in_flight: 10,
        }
    }

    fn verified() -> VerifiedRequestContext {
        VerifiedRequestContext::verified_for_test_with_scopes(
            "agent:1",
            "tenant:1",
            &["work:delegate"],
        )
    }

    #[test]
    fn bind_uses_authenticated_outer_tenant_and_pinned_entry() {
        let entry = agent_entry();
        let bound = bind_request(
            request("tenant:1", &entry),
            &verified(),
            &RetainedTarget::Agent(Box::new(entry.clone())),
            "tenant:1",
        )
        .unwrap();
        assert_eq!(bound.work_item.context.tenant_id, "tenant:1");
        assert_eq!(bound.work_item.provenance_refs.len(), 4);
        // The delegation's capability currency IS the retained entry's tool-surface
        // digest in unprefixed form -- a real content digest over the pinned
        // tools, not a fixture constant. `execution_digest_mismatch_is_rejected
        // _before_lowering` proves any other value is refused, so asserting the
        // derived value here is what binds the lowered WorkItem to the entry.
        assert_eq!(
            bound.work_item.metadata["capability_digest"],
            json!(unprefixed_digest("tool_surface_digest", &entry.tool_surface_digest()).unwrap())
        );
        assert_eq!(
            bound.work_item.metadata["agent_model_profile_digest"],
            json!(entry.model_profile_digest())
        );
        assert_eq!(
            bound.work_item.metadata["agent_skill_set_digest"],
            json!(entry.skill_set_digest())
        );
        assert!(matches!(bound.method(), Method::SubmitWorkItem { .. }));
    }

    #[test]
    fn execution_digest_mismatch_is_rejected_before_lowering() {
        let entry = agent_entry();
        for (field, value) in [
            ("model_digest", digest('9')),
            ("capability_digest", digest('9')),
            ("catalog_digest", digest('9')),
        ] {
            let mut request = request("tenant:1", &entry);
            match field {
                "model_digest" => request.model_digest = Some(value),
                "capability_digest" => request.capability_digest = value,
                "catalog_digest" => request.catalog_digest = value,
                _ => unreachable!(),
            }
            let error = bind_request(
                request,
                &verified(),
                &RetainedTarget::Agent(Box::new(entry.clone())),
                "tenant:1",
            )
            .unwrap_err();
            assert!(error.contains("digest"), "{field}: {error}");
        }
    }

    #[test]
    fn caller_may_delegate_to_another_retained_agent_in_same_tenant() {
        let entry = agent_entry_for("agent:2");
        let bound = bind_request(
            request("tenant:1", &entry),
            &verified(),
            &RetainedTarget::Agent(Box::new(entry.clone())),
            "tenant:1",
        )
        .unwrap();
        assert_eq!(bound.work_item.context.agent_id, "agent:1");
        assert_eq!(bound.work_item.metadata["agent_id"], json!("agent:2"));
    }

    #[test]
    fn missing_delegate_policy_is_rejected_before_library_lookup() {
        let entry = agent_entry();
        let caller =
            VerifiedRequestContext::verified_for_test_with_scopes("agent:1", "tenant:1", &[]);
        // The request must not ASK for `work:delegate` either, or the earlier
        // forged-scope check fires first and the delegate-policy gate this test
        // names is never reached.
        let request = request_with_context(context_with_scopes("tenant:1", &[]), &entry);
        let error = bind_request(
            request,
            &caller,
            &RetainedTarget::Agent(Box::new(entry.clone())),
            "tenant:1",
        )
        .unwrap_err();
        assert!(error.contains("work:delegate"), "{error}");
    }

    #[test]
    fn forged_nested_tenant_is_rejected() {
        let entry = agent_entry();
        let error = bind_request(
            request("tenant:other", &entry),
            &verified(),
            &RetainedTarget::Agent(Box::new(entry.clone())),
            "tenant:other",
        )
        .unwrap_err();
        assert!(error.contains("authenticated outer authority"));
    }

    #[test]
    fn stale_agent_revision_is_rejected_before_lowering() {
        let entry = agent_entry();
        let mut retained = entry.clone();
        retained.entry_revision += 1;
        let error = bind_request(
            request("tenant:1", &entry),
            &verified(),
            &RetainedTarget::Agent(Box::new(retained.clone())),
            "tenant:1",
        )
        .unwrap_err();
        assert!(error.contains("retained revision/digest"));
    }

    #[test]
    fn stale_policy_digest_is_rejected_before_lowering() {
        let entry = agent_entry();
        let mut request = request("tenant:1", &entry);
        request.policy_digest = prefixed_digest('9');
        let error = bind_request(
            request,
            &verified(),
            &RetainedTarget::Agent(Box::new(entry.clone())),
            "tenant:1",
        )
        .unwrap_err();
        assert!(error.contains("policy digest"));
    }

    #[test]
    fn retired_entry_is_rejected_before_lowering() {
        let entry = agent_entry();
        let retired = entry.retire(8, 2).unwrap();
        let request = request("tenant:1", &retired);
        let error = bind_request(
            request,
            &verified(),
            &RetainedTarget::Agent(Box::new(retired.clone())),
            "tenant:1",
        )
        .unwrap_err();
        assert!(error.contains("retired"));
    }

    #[test]
    fn native_result_maps_only_matching_admission() {
        let entry = agent_entry();
        let bound = bind_request(
            request("tenant:1", &entry),
            &verified(),
            &RetainedTarget::Agent(Box::new(entry.clone())),
            "tenant:1",
        )
        .unwrap();
        let result = SubmitWorkItemResult {
            schema_version: NativeControlSchemaVersion::V1,
            work_item_id: "workitem:1".into(),
            status: "ready".into(),
            created: true,
            replayed: false,
            command_sequence: 1,
            idempotency_key: "delegate-idempotency:1".into(),
            dependency_count: 0,
            admitted_count: 1,
            max_tenant_in_flight: MAX_DELEGATION_IN_FLIGHT,
            outbox_id: "outbox:1".into(),
            command_digest: digest('2'),
            provenance_refs: bound.work_item.provenance_refs.clone(),
            changed_work_item_ids: vec!["workitem:1".into()],
        };
        let mapped = result_from_submit(&bound, result).unwrap();
        assert_eq!(mapped.decision, KgDelegateDecision::Accepted);
        assert_eq!(mapped.outbox_id, "outbox:1");
    }

    #[test]
    fn native_replay_maps_to_replayed_result() {
        let entry = agent_entry();
        let bound = bind_request(
            request("tenant:1", &entry),
            &verified(),
            &RetainedTarget::Agent(Box::new(entry.clone())),
            "tenant:1",
        )
        .unwrap();
        let result = SubmitWorkItemResult {
            schema_version: NativeControlSchemaVersion::V1,
            work_item_id: "workitem:1".into(),
            status: "ready".into(),
            created: false,
            replayed: true,
            command_sequence: 1,
            idempotency_key: "delegate-idempotency:1".into(),
            dependency_count: 0,
            admitted_count: 1,
            max_tenant_in_flight: 10,
            outbox_id: "outbox:1".into(),
            command_digest: digest('2'),
            provenance_refs: bound.work_item.provenance_refs.clone(),
            changed_work_item_ids: vec!["workitem:1".into()],
        };
        assert_eq!(
            result_from_submit(&bound, result).unwrap().decision,
            KgDelegateDecision::Replayed
        );
    }

    // ---- L3 end to end: publish a NESTED graph, then delegate it ----
    //
    // The graph-delegation admission branch had no test of any kind: this
    // module constructed `RetainedTarget::Graph` zero times, so "L3 is now
    // runnable" was an untested claim, and `retained_graph` plus the
    // shape-digest/ceiling/capability checks were all unexercised. These go
    // through the REAL store so the retained entry is one admission actually
    // minted -- a hand-built entry would not prove the two halves agree.

    #[cfg(feature = "redb")]
    fn graph_test_store() -> (tempfile::TempDir, AgentLibraryStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = AgentLibraryStore::open(dir.path().to_str().unwrap()).unwrap();
        (dir, store)
    }

    #[cfg(feature = "redb")]
    fn graph_context(
        store: &AgentLibraryStore,
        key: &str,
        nonce: u8,
        expected_revision: u64,
    ) -> eg_types::agent_library::AgentLibraryMutationContext {
        eg_types::agent_library::AgentLibraryMutationContext {
            request_id: u64::from(nonce),
            principal: store.owner_principal().to_string(),
            caller_principal: format!("principal:sha256:{}", "a".repeat(64)),
            attempt_nonce: eg_types::contract::Nonce::from_bytes([nonce; 32]),
            tenant_id: "tenant-a".to_string(),
            actor_scope: "action-scope:a".to_string(),
            purpose_id: "agent-graph:publish".to_string(),
            policy_revision: "policy-v1".to_string(),
            policy_digest:
                crate::server::persistence::agent_library::current_agent_library_policy_digest()
                    .unwrap(),
            policy_decision_id: "agent-graph:decision:policy-v1".to_string(),
            idempotency_key: key.to_string(),
            expected_revision: Some(expected_revision),
            trace_id: None,
            created_at_ms: 10,
        }
    }

    /// Seed the schema component a graph fixture pins and return the pin that
    /// resolves it.
    ///
    /// A graph publish RESOLVES every component its shape pins, so the record
    /// has to exist in this tenant at exactly this revision.
    #[cfg(feature = "redb")]
    fn graph_component(
        store: &AgentLibraryStore,
        id: &str,
    ) -> eg_types::agent_component::ComponentDependency {
        crate::server::persistence::agent_component::seed_component_for_test(
            store,
            "tenant-a",
            id,
            eg_types::agent_component::AgentComponentKind::Schema,
            1,
        )
    }

    /// The child: one agent, then end, producing `contract:report`.
    #[cfg(feature = "redb")]
    fn child_graph_shape(store: &AgentLibraryStore) -> eg_types::agent_graph::AgentGraphShape {
        use eg_types::agent_graph::{AgentGraphEdge, AgentGraphNode, AgentGraphNodeKind};
        eg_types::agent_graph::AgentGraphShape {
            entry_node: "work".into(),
            nodes: vec![
                AgentGraphNode {
                    node_id: "work".into(),
                    kind: AgentGraphNodeKind::Agent {
                        agent_id: "agent:work".into(),
                        // Seeding an agent also seeds the five components it is
                        // assembled from, starting at this index -- so it is
                        // spaced clear of the schema seed above.
                        definition_digest:
                            crate::server::persistence::agent_library::seed_agent_for_test(
                                store,
                                "tenant-a",
                                "agent:work",
                                10,
                            ),
                    },
                    deps_contract: None,
                    output_contract: Some(graph_component(store, "contract:report")),
                },
                AgentGraphNode {
                    node_id: "done".into(),
                    kind: AgentGraphNodeKind::End,
                    deps_contract: None,
                    output_contract: None,
                },
            ],
            edges: vec![AgentGraphEdge {
                from: "work".into(),
                to: "done".into(),
                condition: None,
            }],
            max_iterations: 4,
        }
    }

    /// The parent: one step that RUNS the child graph, pinned by shape digest.
    #[cfg(feature = "redb")]
    fn parent_graph_shape(
        store: &AgentLibraryStore,
        child_shape_digest: &str,
    ) -> eg_types::agent_graph::AgentGraphShape {
        use eg_types::agent_graph::{AgentGraphEdge, AgentGraphNode, AgentGraphNodeKind};
        eg_types::agent_graph::AgentGraphShape {
            entry_node: "team".into(),
            nodes: vec![
                AgentGraphNode {
                    node_id: "team".into(),
                    kind: AgentGraphNodeKind::Graph {
                        graph_id: "graph:child".into(),
                        shape_digest: child_shape_digest.into(),
                    },
                    deps_contract: None,
                    output_contract: Some(graph_component(store, "contract:report")),
                },
                AgentGraphNode {
                    node_id: "done".into(),
                    kind: AgentGraphNodeKind::End,
                    deps_contract: None,
                    output_contract: None,
                },
            ],
            edges: vec![AgentGraphEdge {
                from: "team".into(),
                to: "done".into(),
                condition: None,
            }],
            max_iterations: 3,
        }
    }

    #[cfg(feature = "redb")]
    fn publish_graph_shape(
        store: &AgentLibraryStore,
        graph_id: &str,
        shape: eg_types::agent_graph::AgentGraphShape,
        key: &str,
        nonce: u8,
    ) -> eg_types::agent_graph::AgentGraphEntry {
        store
            .publish_graph(eg_types::agent_graph::AgentGraphPublishRequest {
                context: graph_context(store, key, nonce, 0),
                graph: eg_types::agent_graph::AgentGraphDraft {
                    graph_id: graph_id.to_string(),
                    version: "1.0.0".to_string(),
                    shape,
                    tenant_id: "tenant-a".to_string(),
                    actor_scope: "action-scope:a".to_string(),
                    purpose_id: "agent-graph:publish".to_string(),
                    policy_digest:
                        crate::server::persistence::agent_library::current_agent_library_policy_digest()
                            .unwrap(),
                    synthesis_evidence: None,
                },
            })
            .expect("publishes")
            .result
            .graph
    }

    /// Publish child + parent and return the RETAINED parent, resolved the way
    /// admission resolves it.
    #[cfg(feature = "redb")]
    fn nested_graph(store: &AgentLibraryStore) -> eg_types::agent_graph::AgentGraphEntry {
        let child = publish_graph_shape(
            store,
            "graph:child",
            child_graph_shape(store),
            "key-child",
            1,
        );
        let parent = publish_graph_shape(
            store,
            "graph:parent",
            parent_graph_shape(store, &child.shape_digest),
            "key-parent",
            2,
        );
        let retained = retained_graph(store, "tenant-a", "graph:parent", parent.entry_revision)
            .expect("the published parent is retained");
        assert_eq!(retained, parent);
        retained
    }

    #[cfg(feature = "redb")]
    fn graph_request(graph: &eg_types::agent_graph::AgentGraphEntry) -> KgDelegateRequest {
        KgDelegateRequest {
            schema_version: KgDelegateSchemaVersion::V2,
            context: context("tenant-a"),
            delegation_id: "delegation:1".into(),
            run_id: "run:1".into(),
            trace_id: "trace:run:1".into(),
            target: eg_types::delegation::DelegationTarget::Graph {
                graph: Box::new(eg_types::delegation::AgentGraphEntryRef::from_entry(graph)),
            },
            input_ref: "cas:input:1".into(),
            command_digest: digest('2'),
            // A graph's capability binding is its SHAPE digest -- what will run
            // -- not the record digest the reference pins.
            capability_digest: unprefixed_digest("shape_digest", &graph.shape_digest).unwrap(),
            catalog_digest: eg_capabilities::CONTRACT_CATALOG_DIGEST.to_string(),
            policy_digest: graph.policy_digest.clone(),
            // A graph has one model per agent node, so it pins none.
            model_digest: None,
            idempotency_key: "delegate-idempotency:1".into(),
            kind: "agent.execute".into(),
            actor_scope: graph.actor_scope.clone(),
            purpose: graph.purpose_id.clone(),
            work_item_id: Some("workitem:1".into()),
            priority: 10,
            max_attempts: 3,
            deadline_unix: Some(2_000.0),
            max_tenant_in_flight: 10,
        }
    }

    #[cfg(feature = "redb")]
    fn graph_verified() -> VerifiedRequestContext {
        VerifiedRequestContext::verified_for_test_with_scopes(
            "agent:1",
            "tenant-a",
            &["work:delegate"],
        )
    }

    #[cfg(feature = "redb")]
    #[test]
    fn a_published_nested_graph_is_delegated_end_to_end() {
        let (_dir, store) = graph_test_store();
        let graph = nested_graph(&store);
        // The parent runs the child: 3 iterations x the child's 4.
        assert_eq!(graph.composed_work_ceiling, 12);

        let bound = bind_request(
            graph_request(&graph),
            &graph_verified(),
            &RetainedTarget::Graph(Box::new(graph.clone())),
            "tenant-a",
        )
        .expect("a retained nested graph is admissible");

        assert_eq!(
            bound.work_item.metadata["target_kind"],
            json!("agent_graph")
        );
        assert_eq!(
            bound.work_item.metadata["agent_graph_shape_digest"],
            json!(graph.shape_digest)
        );
        assert_eq!(bound.work_item.metadata["agent_graph_node_count"], json!(2));
        // The work item's single model digest is the RECORD digest the
        // delegation pins, unprefixed -- the honest answer for a target that
        // has one model per agent node rather than one of its own.
        assert_eq!(
            bound.work_item.model_digest,
            graph.definition_digest.strip_prefix("sha256:").unwrap()
        );
        assert!(bound
            .work_item
            .provenance_refs
            .iter()
            .any(|reference| reference.starts_with("agent-graph:")));
    }

    #[cfg(feature = "redb")]
    #[test]
    fn a_graph_delegation_that_raises_its_admitted_ceiling_is_refused() {
        // The escape this field exists to close: a caller declaring a ceiling
        // the composition check never admitted would fan out past the bound
        // publish enforced.
        let (_dir, store) = graph_test_store();
        let graph = nested_graph(&store);
        let mut request = graph_request(&graph);
        let eg_types::delegation::DelegationTarget::Graph { graph: reference } =
            &mut request.target
        else {
            panic!("the fixture builds a graph target");
        };
        reference.composed_work_ceiling = 1_000_000;
        let error = bind_request(
            request,
            &graph_verified(),
            &RetainedTarget::Graph(Box::new(graph.clone())),
            "tenant-a",
        )
        .expect_err("a self-raised ceiling must be refused");
        assert!(
            error.contains("composed_work_ceiling is not the admitted ceiling"),
            "got: {error}"
        );
    }

    #[cfg(feature = "redb")]
    #[test]
    fn a_graph_delegation_pinning_the_wrong_record_digest_is_refused() {
        // Each of these three is a DISTINCT refusal on the graph arm, and each
        // is asserted by its own message: an `error.contains("digest")` would
        // pass on any of them, and on several unrelated ones.
        let (_dir, store) = graph_test_store();
        let graph = nested_graph(&store);

        let mut wrong_digest = graph_request(&graph);
        if let eg_types::delegation::DelegationTarget::Graph { graph: reference } =
            &mut wrong_digest.target
        {
            reference.definition_digest = prefixed_digest('9');
        }
        let error = bind_request(
            wrong_digest,
            &graph_verified(),
            &RetainedTarget::Graph(Box::new(graph.clone())),
            "tenant-a",
        )
        .expect_err("a mispinned record must be refused");
        assert!(
            error.contains("is not the retained revision/definition digest"),
            "got: {error}"
        );

        let mut wrong_capability = graph_request(&graph);
        wrong_capability.capability_digest = digest('9');
        let error = bind_request(
            wrong_capability,
            &graph_verified(),
            &RetainedTarget::Graph(Box::new(graph.clone())),
            "tenant-a",
        )
        .expect_err("a capability digest that is not the shape must be refused");
        assert!(
            error.contains("capability digest does not match the retained graph shape"),
            "got: {error}"
        );

        // A graph reference resolved against an AGENT is the kind-mismatch arm.
        let entry = agent_entry();
        let error = bind_request(
            graph_request(&graph),
            &graph_verified(),
            &RetainedTarget::Agent(Box::new(entry)),
            "tenant-a",
        )
        .expect_err("a target-kind mismatch must be refused");
        assert!(
            error.contains("resolved a different target kind than the request named")
                || error.contains("outside authenticated tenant"),
            "got: {error}"
        );
    }

    #[cfg(feature = "redb")]
    #[test]
    fn a_graph_delegation_pinning_the_shape_digest_would_not_even_validate() {
        // The regression this reference shape closes. The graph reference once
        // pinned `shape_digest` and validated it UNPREFIXED, while every stored
        // graph digest is `sha256:<hex>`: a prefixed value failed validation and
        // an unprefixed one failed the comparison, so the graph arm could never
        // admit anything. Nothing caught it because nothing ever built a graph
        // target end to end.
        let (_dir, store) = graph_test_store();
        let graph = nested_graph(&store);
        let mut unprefixed = graph_request(&graph);
        if let eg_types::delegation::DelegationTarget::Graph { graph: reference } =
            &mut unprefixed.target
        {
            reference.definition_digest = graph
                .definition_digest
                .strip_prefix("sha256:")
                .unwrap()
                .to_string();
        }
        let error = unprefixed
            .validate()
            .expect_err("an unprefixed digest is not the stored form");
        assert!(error.contains("sha256: digest form"), "got: {error}");
    }

    #[cfg(feature = "raft")]
    #[test]
    fn delegated_admission_uses_the_existing_work_item_native_command() {
        let entry = agent_entry();
        let bound = bind_request(
            request("tenant:1", &entry),
            &verified(),
            &RetainedTarget::Agent(Box::new(entry.clone())),
            "tenant:1",
        )
        .unwrap();
        let command = crate::raft::NativeMutationCommand::from_public_method(
            bound.method(),
            "delegation-native-route-test",
        )
        .expect("lowered SubmitWorkItem has a bounded native command");

        assert!(matches!(
            command,
            crate::raft::NativeMutationCommand::WorkItem { .. }
        ));
        assert!(crate::raft::NATIVE_CONSENSUS_METHODS.contains(&"SubmitWorkItem"));
        let reopened = command
            .open_public_method("delegation-native-route-test")
            .expect("native command authenticates its sealed method")
            .expect("WorkItem command carries a public method");
        assert!(matches!(reopened, Method::SubmitWorkItem { .. }));
    }
}
