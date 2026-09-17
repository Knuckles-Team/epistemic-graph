// Existing Agent Graph lifecycle, replay, and admission tests.

pub(super) use super::super::agent_fixtures::{
    ledger_record, mutation_context as context, open_agent_store as open_store,
};
use super::super::agent_revision::{decode_status_result, validate_status_record};
use super::*;
use eg_types::agent_component::ComponentDependency;
use eg_types::agent_graph::{
    AgentGraphDraft, AgentGraphEdge, AgentGraphNode, AgentGraphNodeKind, AgentGraphShape,
};
use eg_types::agent_library::AgentLibraryLifecycle;
use eg_types::mutation_batch::MutationBatchStatus;

pub(super) fn digest(byte: char) -> String {
    format!("sha256:{}", byte.to_string().repeat(64))
}

/// The seeding nonce index each pinned record owns.
///
/// Fixed here rather than taken from a counter for two reasons: two
/// fixtures that seed the same record must agree, and no two records may
/// share a seeding nonce -- a reused attempt nonce is
/// `REPLAY_NONCE_CONSUMED`, not a silent no-op. The agent indices are
/// spaced by ten because seeding an agent also seeds the five components it
/// is assembled from, starting at its own index.
pub(super) fn seed_index(record_id: &str) -> u8 {
    match record_id {
        "contract:findings" => 0,
        "contract:report" => 1,
        "contract:mismatch" => 2,
        "evidence:run-17" => 3,
        "agent:research" => 10,
        "agent:write" => 20,
        "agent:work" => 30,
        other => panic!("no seeding nonce reserved for '{other}'"),
    }
}

/// Seed a schema component and return the pin that resolves it.
///
/// A graph publish now RESOLVES every component its shape pins, so a
/// fixture can no longer invent a digest: the record has to exist, in this
/// tenant, at exactly this revision.
pub(super) fn component(
    store: &AgentLibraryStore,
    tenant_id: &str,
    reference: &str,
) -> ComponentDependency {
    super::super::agent_component::seed_component_for_test(
        store,
        tenant_id,
        reference,
        eg_types::agent_component::AgentComponentKind::Schema,
        seed_index(reference),
    )
}

/// Seed an agent and return the node kind that runs it, pinned to its real
/// `definition_digest`.
pub(super) fn agent_node(
    store: &AgentLibraryStore,
    tenant_id: &str,
    agent_id: &str,
) -> AgentGraphNodeKind {
    AgentGraphNodeKind::Agent {
        agent_id: agent_id.to_string(),
        definition_digest: super::super::agent_library::seed_agent_for_test(
            store,
            tenant_id,
            agent_id,
            seed_index(agent_id),
        ),
    }
}

/// The terminal node every fixture shape ends on.
fn end_node() -> AgentGraphNode {
    AgentGraphNode {
        node_id: "done".into(),
        kind: AgentGraphNodeKind::End,
        deps_contract: None,
        output_contract: None,
    }
}

/// An unconditional edge.
fn edge(from: &str, to: &str) -> AgentGraphEdge {
    AgentGraphEdge {
        from: from.into(),
        to: to.into(),
        condition: None,
    }
}

/// research -> write -> end, contracts agreeing across each edge.
pub(super) fn shape(store: &AgentLibraryStore, tenant_id: &str) -> AgentGraphShape {
    let findings = component(store, tenant_id, "contract:findings");
    AgentGraphShape {
        entry_node: "research".into(),
        nodes: vec![
            AgentGraphNode {
                node_id: "research".into(),
                kind: agent_node(store, tenant_id, "agent:research"),
                deps_contract: None,
                output_contract: Some(findings.clone()),
            },
            AgentGraphNode {
                node_id: "write".into(),
                kind: agent_node(store, tenant_id, "agent:write"),
                deps_contract: Some(findings),
                output_contract: Some(component(store, tenant_id, "contract:report")),
            },
            end_node(),
        ],
        edges: vec![edge("research", "write"), edge("write", "done")],
        max_iterations: 10,
    }
}

pub(super) fn draft(store: &AgentLibraryStore, tenant_id: &str, graph_id: &str) -> AgentGraphDraft {
    AgentGraphDraft {
        graph_id: graph_id.to_string(),
        version: "1.0.0".to_string(),
        shape: shape(store, tenant_id),
        tenant_id: tenant_id.to_string(),
        actor_scope: "action-scope:a".to_string(),
        purpose_id: "agent-graph:publish".to_string(),
        policy_digest: super::super::agent_library::current_agent_library_policy_digest().unwrap(),
        synthesis_evidence: None,
    }
}

/// Publish `graph` under a context in its own tenant.
pub(super) fn try_publish(
    store: &AgentLibraryStore,
    key: &str,
    nonce: u8,
    expected_revision: u64,
    graph: AgentGraphDraft,
) -> Result<AgentGraphWriteResult, String> {
    store.publish_graph(AgentGraphPublishRequest {
        context: context(
            store,
            &graph.tenant_id.clone(),
            key,
            nonce,
            expected_revision,
            "agent-graph:publish",
        ),
        graph,
    })
}

/// One step, then end, producing `contract:report`.
fn single_step_shape(
    store: &AgentLibraryStore,
    tenant_id: &str,
    step: (&str, AgentGraphNodeKind),
    max_iterations: u32,
) -> AgentGraphShape {
    let (node_id, kind) = step;
    AgentGraphShape {
        entry_node: node_id.into(),
        nodes: vec![
            AgentGraphNode {
                node_id: node_id.into(),
                kind,
                deps_contract: None,
                output_contract: Some(component(store, tenant_id, "contract:report")),
            },
            end_node(),
        ],
        edges: vec![edge(node_id, "done")],
        max_iterations,
    }
}

/// The child: one agent, then end, producing `contract:report`.
pub(crate) fn child_shape(
    store: &AgentLibraryStore,
    tenant_id: &str,
    max_iterations: u32,
) -> AgentGraphShape {
    let agent = agent_node(store, tenant_id, "agent:work");
    single_step_shape(store, tenant_id, ("work", agent), max_iterations)
}

/// A parent whose single step runs the named child graph, pinned by shape.
pub(crate) fn parent_shape(
    store: &AgentLibraryStore,
    tenant_id: &str,
    child: (&str, &str),
    max_iterations: u32,
) -> AgentGraphShape {
    let (graph_id, shape_digest) = child;
    let kind = AgentGraphNodeKind::Graph {
        graph_id: graph_id.into(),
        shape_digest: shape_digest.into(),
    };
    single_step_shape(store, tenant_id, ("team", kind), max_iterations)
}

/// Publish `shape` as `graph_id` in `tenant-a` and return the committed revision.
pub(crate) fn publish_shape(
    store: &AgentLibraryStore,
    graph_id: &str,
    shape: AgentGraphShape,
    key: &str,
    nonce: u8,
) -> AgentGraphEntry {
    let mut graph = draft(store, "tenant-a", graph_id);
    graph.shape = shape;
    try_publish(store, key, nonce, 0, graph)
        .expect("publishes")
        .result
        .graph
}

#[test]
fn a_published_graph_is_durable_and_reads_back() {
    let (_dir, store) = open_store();
    let published =
        try_publish(&store, "key-1", 1, 0, draft(&store, "tenant-a", "graph-a")).unwrap();
    assert!(!published.replayed);
    assert_eq!(published.result.graph.entry_revision, 1);
    assert_eq!(
        published.result.graph.shape_digest,
        shape(&store, "tenant-a").shape_digest()
    );

    let current = store.current_graph("tenant-a", "graph-a").unwrap().unwrap();
    assert_eq!(current, published.result.graph);
    assert_eq!(
        store.graph_revisions("tenant-a", "graph-a").unwrap().len(),
        1
    );
}

#[test]
fn a_byte_identical_retry_replays_rather_than_publishing_twice() {
    // The property the whole replay/nonce/receipt path exists for: a
    // transport retry must return the SAME committed result, not a second
    // revision.
    let (_dir, store) = open_store();
    let first = try_publish(&store, "key-1", 1, 0, draft(&store, "tenant-a", "graph-a")).unwrap();

    let mut retry_context = context(&store, "tenant-a", "key-1", 2, 0, "agent-graph:publish");
    retry_context.created_at_ms = 99;
    let replayed = store
        .publish_graph(AgentGraphPublishRequest {
            context: retry_context,
            graph: draft(&store, "tenant-a", "graph-a"),
        })
        .unwrap();
    assert!(replayed.replayed, "a retry must replay, not re-publish");
    assert_eq!(replayed.result, first.result);
    assert_eq!(
        store.graph_revisions("tenant-a", "graph-a").unwrap().len(),
        1,
        "a replay must not append a second revision"
    );
}

#[test]
fn a_replayed_fresh_nonce_is_consumed_before_returning() {
    let (_dir, store) = open_store();
    try_publish(&store, "key-1", 1, 0, draft(&store, "tenant-a", "graph-a")).unwrap();
    let replayed =
        try_publish(&store, "key-1", 2, 0, draft(&store, "tenant-a", "graph-a")).unwrap();
    assert!(replayed.replayed);

    let error =
        try_publish(&store, "key-2", 2, 1, draft(&store, "tenant-a", "graph-b")).unwrap_err();
    assert!(error.contains("REPLAY_NONCE_CONSUMED"), "got: {error}");
}

#[test]
fn graph_status_reads_publish_and_retire_domain_envelopes() {
    let (_dir, store) = open_store();
    let status = |context, kind| {
        store
            .graph_status(AgentGraphStatusRequest {
                context,
                graph_id: "graph-status".to_string(),
                kind,
            })
            .unwrap()
            .expect("durable status")
    };
    let publish_context = context(
        &store,
        "tenant-a",
        "status-publish",
        1,
        0,
        "agent-graph:publish",
    );
    let published = store
        .publish_graph(AgentGraphPublishRequest {
            context: publish_context.clone(),
            graph: draft(&store, "tenant-a", "graph-status"),
        })
        .unwrap();
    let publish_status = status(publish_context, AgentGraphMutationKind::Publish);
    assert!(publish_status.replayed);
    assert_eq!(publish_status.result, published.result);

    let retire_context = context(
        &store,
        "tenant-a",
        "status-retire",
        2,
        1,
        "agent-graph:retire",
    );
    let retired = store
        .retire_graph(AgentGraphRetireRequest {
            context: retire_context.clone(),
            graph_id: "graph-status".to_string(),
        })
        .unwrap();
    let retire_status = status(retire_context, AgentGraphMutationKind::Retire);
    assert!(retire_status.replayed);
    assert_eq!(retire_status.result, retired.result);
    assert_eq!(
        retire_status.result.graph.lifecycle,
        AgentLibraryLifecycle::Retired
    );
}

#[test]
fn graph_status_rejects_an_unwrapped_result() {
    let (_dir, store) = open_store();
    let published = try_publish(
        &store,
        "status-invalid",
        1,
        0,
        draft(&store, "tenant-a", "graph-invalid"),
    )
    .unwrap();
    let raw_result =
        eg_storage::encode_bounded(&published.result, "unwrapped agent graph status result")
            .unwrap();
    let error = decode_status_result::<GraphLayer>(&raw_result).unwrap_err();
    assert!(!error.is_empty(), "an unwrapped result must fail closed");
}

#[test]
fn graph_status_rejects_redirected_identity_state_version_and_kind() {
    let (_dir, store) = open_store();
    let context_a = context(
        &store,
        "tenant-a",
        "status-bindings",
        1,
        0,
        "agent-graph:publish",
    );
    let published = store
        .publish_graph(AgentGraphPublishRequest {
            context: context_a.clone(),
            graph: draft(&store, "tenant-a", "graph-bindings"),
        })
        .unwrap();
    let owner_a = store.scope_handle("tenant-a").unwrap();
    let record = ledger_record(&store, "tenant-a", "status-bindings");
    let refused = |record: &eg_types::MutationBatchRecord,
                   graph_id: &str,
                   kind: AgentGraphMutationKind,
                   committed: &AgentGraphCommittedResult| {
        validate_status_record::<GraphLayer>(
            record,
            owner_a.identity(),
            &context_a,
            (graph_id, kind),
            committed,
        )
        .is_err()
    };

    let mut wrong_key = published.result.clone();
    wrong_key.batch_id = "agent-library/v1/redirected".to_string();
    assert!(refused(
        &record,
        "graph-bindings",
        AgentGraphMutationKind::Publish,
        &wrong_key
    ));

    let context_b = context(
        &store,
        "tenant-b",
        "status-other-tenant",
        2,
        0,
        "agent-graph:publish",
    );
    let other_tenant = store
        .publish_graph(AgentGraphPublishRequest {
            context: context_b,
            graph: draft(&store, "tenant-b", "graph-other-tenant"),
        })
        .unwrap();
    let mut wrong_tenant = other_tenant.result.clone();
    wrong_tenant.batch_id = super::super::agent_library::batch_id("status-bindings").unwrap();
    assert!(refused(
        &record,
        "graph-other-tenant",
        AgentGraphMutationKind::Publish,
        &wrong_tenant
    ));

    let mut stale_version = published.result.clone();
    stale_version.committed_version = 0;
    assert!(refused(
        &record,
        "graph-bindings",
        AgentGraphMutationKind::Publish,
        &stale_version
    ));

    let mut uncommitted = record.clone();
    uncommitted.status = MutationBatchStatus::Prepared;
    assert!(refused(
        &uncommitted,
        "graph-bindings",
        AgentGraphMutationKind::Publish,
        &published.result
    ));

    assert!(refused(
        &record,
        "graph-bindings",
        AgentGraphMutationKind::Retire,
        &published.result
    ));
}

#[test]
fn a_retry_that_corrects_the_synthesis_evidence_is_not_a_replay() {
    // The publish replay identity used to be minted from `shape_digest`
    // alone, so a retry of a timed-out publish that CORRECTED the synthesis
    // evidence resolved as a replay of the uncorrected commit: the
    // correction was silently dropped and the caller was told it worked.
    // Keyed on the draft digest it is a different operation, and reusing
    // the key for it is a NAMED refusal instead.
    let (_dir, store) = open_store();
    try_publish(&store, "key-1", 1, 0, draft(&store, "tenant-a", "graph-a")).unwrap();

    let mut corrected = draft(&store, "tenant-a", "graph-a");
    corrected.synthesis_evidence = Some(component(&store, "tenant-a", "evidence:run-17"));
    assert_eq!(
        corrected.shape.shape_digest(),
        draft(&store, "tenant-a", "graph-a").shape.shape_digest(),
        "the shape is untouched, which is what made this a false replay"
    );
    let mut retry = context(&store, "tenant-a", "key-1", 2, 0, "agent-graph:publish");
    retry.created_at_ms = 99;
    let error = store
        .publish_graph(AgentGraphPublishRequest {
            context: retry,
            graph: corrected,
        })
        .unwrap_err();
    assert!(error.contains("IDEMPOTENCY_CONFLICT"), "got: {error}");
    assert_eq!(
        store.graph_revisions("tenant-a", "graph-a").unwrap()[0].synthesis_evidence,
        None,
        "and the committed revision is untouched"
    );
}

#[test]
fn the_admitted_ceiling_is_persisted_on_the_revision() {
    // `kg-delegate` checks a caller's declared `composed_work_ceiling`
    // against this field. It was computed inside the write transaction and
    // then discarded (`let _ = composition;`), under a comment claiming the
    // outbox headers recorded it -- they did not, and neither did anything
    // else, so delegation had nothing to compare against.
    let (_dir, store) = open_store();
    let published =
        try_publish(&store, "key-1", 1, 0, draft(&store, "tenant-a", "graph-a")).unwrap();
    // No child graphs, so the composed ceiling is the shape's own bound.
    assert_eq!(
        published.result.graph.composed_work_ceiling,
        u64::from(shape(&store, "tenant-a").max_iterations)
    );
    let current = store.current_graph("tenant-a", "graph-a").unwrap().unwrap();
    assert_eq!(
        current.composed_work_ceiling, published.result.graph.composed_work_ceiling,
        "it must survive the round trip through redb"
    );
    current
        .validate()
        .expect("the persisted row re-derives its own digest");
}

#[test]
fn reusing_an_attempt_nonce_is_refused() {
    let (_dir, store) = open_store();
    try_publish(&store, "key-1", 7, 0, draft(&store, "tenant-a", "graph-a")).unwrap();
    // Same nonce, DIFFERENT idempotency key: the nonce is single-use, so
    // this is a replayed attempt aimed at a different operation.
    let error =
        try_publish(&store, "key-2", 7, 1, draft(&store, "tenant-a", "graph-b")).unwrap_err();
    assert!(error.contains("REPLAY_NONCE_CONSUMED"), "got: {error}");
}

#[test]
fn a_stale_expected_revision_is_refused() {
    let (_dir, store) = open_store();
    try_publish(&store, "key-1", 1, 0, draft(&store, "tenant-a", "graph-a")).unwrap();
    // Head is 1; publishing against 0 again is a lost update.
    let error =
        try_publish(&store, "key-2", 2, 0, draft(&store, "tenant-a", "graph-a")).unwrap_err();
    assert!(!error.is_empty());
    assert_eq!(
        store.graph_revisions("tenant-a", "graph-a").unwrap().len(),
        1,
        "a refused write must leave no revision behind"
    );
}

#[test]
fn a_retired_graph_is_a_tombstone_that_cannot_be_resurrected() {
    let (_dir, store) = open_store();
    try_publish(&store, "key-1", 1, 0, draft(&store, "tenant-a", "graph-a")).unwrap();
    let retired = store
        .retire_graph(AgentGraphRetireRequest {
            context: context(&store, "tenant-a", "key-2", 2, 1, "agent-graph:retire"),
            graph_id: "graph-a".to_string(),
        })
        .unwrap();
    assert_eq!(
        retired.result.graph.lifecycle,
        AgentLibraryLifecycle::Retired
    );
    assert_eq!(retired.result.graph.entry_revision, 2);

    let error =
        try_publish(&store, "key-3", 3, 2, draft(&store, "tenant-a", "graph-a")).unwrap_err();
    assert_eq!(error, "retired agent graphs cannot be resurrected");
}

#[test]
fn graphs_are_scoped_to_their_tenant() {
    let (_dir, store) = open_store();
    try_publish(&store, "key-1", 1, 0, draft(&store, "tenant-a", "graph-a")).unwrap();
    assert!(store
        .current_graph("tenant-b", "graph-a")
        .unwrap()
        .is_none());
}

#[test]
fn a_graph_and_an_entry_of_the_same_id_do_not_collide() {
    // Both record families live in ONE owner file. If they shared a key
    // space, publishing a graph would overwrite the agent it composes.
    let (_dir, store) = open_store();
    try_publish(&store, "key-1", 1, 0, draft(&store, "tenant-a", "same-id")).unwrap();
    assert_eq!(
        super::super::agent_fixtures::layers_holding(&store, "tenant-a", "same-id"),
        [false, false, true, false],
        "only the graph layer holds the id"
    );
}

#[test]
fn an_unsound_shape_never_reaches_the_ledger() {
    let (_dir, store) = open_store();
    let mut broken = draft(&store, "tenant-a", "graph-a");
    // The consumer requires an input its producer does not produce.
    broken.shape.nodes[1].deps_contract = Some(component(&store, "tenant-a", "contract:mismatch"));
    let error = try_publish(&store, "key-1", 1, 0, broken).unwrap_err();
    assert!(error.contains("contracts agree"), "got: {error}");
    assert!(store
        .current_graph("tenant-a", "graph-a")
        .unwrap()
        .is_none());
}

// ---- reference resolution: the pins a shape makes to L1, L2 and
// ---- templates ----
//
// Every one of these publishes today by construction -- the fixtures seed
// what they pin. What none of them could do before is FAIL: a shape's
// `Agent`, `Template` and component pins were checked for well-formed text
// and a well-formed `sha256:<hex>` and nothing else, so 64 invented hex
// characters published a graph claiming an agent it was never composed
// against, and `shape_digest` then attested to the claim.

#[test]
fn a_graph_pinning_an_agent_that_does_not_exist_is_refused() {
    let (_dir, store) = open_store();
    let mut graph = draft(&store, "tenant-a", "graph-a");
    let AgentGraphNodeKind::Agent {
        definition_digest, ..
    } = graph.shape.nodes[0].kind.clone()
    else {
        panic!("the fixture's entry node runs an agent");
    };
    // A real digest, a name nothing carries: resolution is by (id, digest),
    // so BOTH halves have to resolve or a leaked digest is a grant.
    graph.shape.nodes[0].kind = AgentGraphNodeKind::Agent {
        agent_id: "agent:ghost".into(),
        definition_digest,
    };
    let error = try_publish(&store, "key-1", 1, 0, graph)
        .expect_err("an unresolvable agent pin must be refused");
    assert!(
        error.contains("which does not exist in this tenant"),
        "got: {error}"
    );
    assert!(store
        .current_graph("tenant-a", "graph-a")
        .unwrap()
        .is_none());
}

#[test]
fn a_graph_pinning_an_invented_agent_digest_is_refused() {
    let (_dir, store) = open_store();
    let mut graph = draft(&store, "tenant-a", "graph-a");
    graph.shape.nodes[0].kind = AgentGraphNodeKind::Agent {
        agent_id: "agent:research".into(),
        definition_digest: digest('7'),
    };
    let error = try_publish(&store, "key-1", 1, 0, graph)
        .expect_err("a digest no revision carries must be refused");
    assert!(error.contains("that was never published"), "got: {error}");
}

#[test]
fn a_graph_pinning_another_tenants_agent_is_refused() {
    // The pin resolves under the PUBLISHER's tenant, so another tenant's
    // agent is simply not found -- a caller who learns a digest must not be
    // able to run an agent it was never granted.
    let (_dir, store) = open_store();
    let foreign =
        super::super::agent_library::seed_agent_for_test(&store, "tenant-b", "agent:foreign", 40);
    let mut graph = draft(&store, "tenant-a", "graph-a");
    graph.shape.nodes[0].kind = AgentGraphNodeKind::Agent {
        agent_id: "agent:foreign".into(),
        definition_digest: foreign,
    };
    let error = try_publish(&store, "key-1", 1, 0, graph)
        .expect_err("a cross-tenant agent pin must be refused");
    assert!(
        error.contains("which does not exist in this tenant"),
        "got: {error}"
    );
}

#[test]
fn a_graph_pinning_a_retired_agent_is_refused_while_old_pins_still_resolve() {
    // The retained-but-not-buildable rule composition already applies to
    // child graphs, applied to the agents a shape runs.
    let (_dir, store) = open_store();
    try_publish(&store, "key-1", 1, 0, draft(&store, "tenant-a", "graph-a"))
        .expect("the first graph publishes while its agents are live");
    store
        .retire(eg_types::agent_library::AgentLibraryRetireRequest {
            context: context(
                &store,
                "tenant-a",
                "key-retire",
                2,
                1,
                "agent-library:retire",
            ),
            agent_id: "agent:research".to_string(),
        })
        .expect("the agent retires");
    let error = try_publish(&store, "key-2", 3, 0, draft(&store, "tenant-a", "graph-b"))
        .expect_err("nothing new may be built on a withdrawn agent");
    assert!(error.contains("which is retired"), "got: {error}");
    // The graph published before the retirement is untouched, and the agent
    // stays readable, because that graph still has to resolve it.
    assert!(store
        .current_graph("tenant-a", "graph-a")
        .unwrap()
        .is_some());
    assert!(store
        .current("tenant-a", "agent:research")
        .unwrap()
        .is_some());
}

#[test]
fn a_graph_pinning_synthesis_evidence_that_does_not_exist_is_refused() {
    // The L3 -> L1 edge. RF-ADR-008 says the evidence is what makes a
    // synthesized shape auditable; it only is if the record it names exists.
    let (_dir, store) = open_store();
    let mut graph = draft(&store, "tenant-a", "graph-a");
    graph.synthesis_evidence = Some(ComponentDependency {
        component_id: "evidence:never-published".into(),
        kind: eg_types::agent_component::AgentComponentKind::Schema,
        definition_digest: digest('7'),
    });
    let error = try_publish(&store, "key-1", 1, 0, graph)
        .expect_err("an unresolvable component pin must be refused");
    assert!(
        error.contains("which does not exist in this tenant"),
        "got: {error}"
    );
}

#[test]
fn a_graph_pinning_a_component_under_the_wrong_kind_is_refused() {
    // A shape's data contracts must be SCHEMA components. Structural
    // validation checks the kind the pin DECLARES; only resolution can
    // check the kind the named record actually has.
    let (_dir, store) = open_store();
    let model = super::super::agent_component::seed_component_for_test(
        &store,
        "tenant-a",
        "model-profile:mislabelled",
        eg_types::agent_component::AgentComponentKind::ModelProfile,
        50,
    );
    let mut graph = draft(&store, "tenant-a", "graph-a");
    graph.synthesis_evidence = Some(ComponentDependency {
        component_id: model.component_id,
        kind: eg_types::agent_component::AgentComponentKind::Schema,
        definition_digest: model.definition_digest,
    });
    let error = try_publish(&store, "key-1", 1, 0, graph)
        .expect_err("a pin whose kind does not match the record must be refused");
    assert!(error.contains("but it is a model_profile"), "got: {error}");
}
