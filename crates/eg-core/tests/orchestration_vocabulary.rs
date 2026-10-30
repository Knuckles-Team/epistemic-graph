//! Agent Library component projection (EG-ORCH-VOCAB-R003.6, R004.4, R005.5).

use std::collections::BTreeMap;

use eg_core::library_projection::{member_id, project_component, ProjectionOp};
use eg_types::agent_component::{
    AgentComponentEntry, AgentComponentFacts, AgentComponentKind, AgentComponentMutationKind,
    AgentComponentOutboxEvent, ComponentProvenance, ModalityFacts,
};
use eg_types::agent_library::AgentLibraryLifecycle;

fn entry(
    id: &str,
    kind: AgentComponentKind,
    facts: AgentComponentFacts,
    attributes: &[(&str, &str)],
    capabilities: &[&str],
) -> AgentComponentEntry {
    AgentComponentEntry {
        schema_version: eg_types::agent_component::AGENT_COMPONENT_SCHEMA_VERSION,
        component_id: id.into(),
        kind,
        version: "1".into(),
        content_digest: "sha256:c".into(),
        content_ref: None,
        facts,
        provenance: ComponentProvenance::Native,
        summary: "s".into(),
        classification: Vec::new(),
        requires: Vec::new(),
        declared_capabilities: capabilities.iter().map(|c| c.to_string()).collect(),
        required_capabilities: Vec::new(),
        declared_required_capabilities: Vec::new(),
        attributes: attributes
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .chain([("visibility".to_string(), "commons".to_string())])
            .collect::<BTreeMap<_, _>>(),
        tenant_id: "t1".into(),
        actor_scope: "a".into(),
        purpose_id: "p".into(),
        policy_digest: "sha256:p".into(),
        source_revision: "r1".into(),
        source_revision_digest: "sha256:r".into(),
        entry_revision: 1,
        lifecycle: AgentLibraryLifecycle::Published,
        definition_digest: "sha256:d".into(),
        created_at_ms: 1,
        updated_at_ms: 1,
    }
}

fn event(entry: AgentComponentEntry) -> AgentComponentOutboxEvent {
    AgentComponentOutboxEvent {
        schema_version: eg_types::agent_component::AGENT_COMPONENT_SCHEMA_VERSION,
        kind: AgentComponentMutationKind::Publish,
        component: entry,
        performing_actor: "actor".into(),
        action_actor_scope: "scope".into(),
    }
}

fn has_edge(ops: &[ProjectionOp], source: &str, target: &str, rel: &str) -> bool {
    ops.iter().any(|op| {
        matches!(op, ProjectionOp::Edge { source: s, target: t, relationship: r, .. }
            if s == source && t == target && r == rel)
    })
}

fn node_type(ops: &[ProjectionOp], id: &str) -> Option<String> {
    ops.iter().find_map(|op| match op {
        ProjectionOp::Node {
            id: i, node_type, stub: false, ..
        } if i == id => Some(node_type.clone()),
        _ => None,
    })
}

// spec: EG-ORCH-VOCAB-R003.6
#[test]
fn a2a_card_component_projects_as_library_member_with_claim_capabilities() {
    let card = entry(
        "card-1",
        AgentComponentKind::A2aAgentCard,
        AgentComponentFacts::Opaque,
        &[("agent_kind", "verifier"), ("instance_of_template", "tpl-1")],
        &["eg:capability/retrieval"],
    );
    let ops = project_component(&event(card));
    let member = member_id("card-1");
    assert_eq!(node_type(&ops, &member).as_deref(), Some("A2AAgentCard"));
    assert!(has_edge(&ops, &member, "agent-library", "memberOfLibrary"));
    assert!(has_edge(&ops, &member, "agentKind:verifier", "hasAgentKind"));
    assert!(has_edge(&ops, &member, &member_id("tpl-1"), "instanceOfTemplate"));
    let claim = ops.iter().any(|op| {
        matches!(op, ProjectionOp::Edge { relationship, target, props, .. }
            if relationship == "providesCapability"
                && target == "eg:capability/retrieval"
                && props.get("strength").and_then(|v| v.as_str()) == Some("claim"))
    });
    assert!(claim, "{ops:?}");
}

// spec: EG-ORCH-VOCAB-R004.4
#[test]
fn guardrail_rule_component_projects_applies_to_task_class() {
    let rule = entry(
        "rule-1",
        AgentComponentKind::GuardrailRule,
        AgentComponentFacts::Opaque,
        &[("applies_to_class", "eg:task/research"), ("source_generation", "gen-7")],
        &[],
    );
    let ops = project_component(&event(rule));
    let member = member_id("rule-1");
    assert_eq!(node_type(&ops, &member).as_deref(), Some("BestPractice"));
    assert!(has_edge(&ops, &member, "eg:task/research", "appliesToTaskClass"));
    let provenance = ops.iter().any(|op| {
        matches!(op, ProjectionOp::Node { id, props, .. }
            if *id == member && props.get("source_generation").and_then(|v| v.as_str()) == Some("gen-7"))
    });
    assert!(provenance, "the source generation is the rule's provenance");
}

// spec: EG-ORCH-VOCAB-R005.5
#[test]
fn model_profile_component_projects_with_context_window() {
    let model = entry(
        "model-1",
        AgentComponentKind::ModelProfile,
        AgentComponentFacts::ModelProfile {
            provider: "p".into(),
            model_identity: "m".into(),
            context_window_tokens: 128_000,
            max_output_tokens: 8_000,
            supports_tools: true,
            supports_structured_output: true,
            supports_vision: false,
            input_capabilities: None,
            modalities: ModalityFacts::default(),
            cost: None,
            latency_declared: None,
            latency_observed_ref: None,
            shell_proficiency: None,
            stopping_behaviour: None,
        },
        &[],
        &[],
    );
    let ops = project_component(&event(model));
    let member = member_id("model-1");
    assert_eq!(node_type(&ops, &member).as_deref(), Some("ModelProfile"));
    assert!(ops.iter().any(|op| matches!(op, ProjectionOp::Node { id, props, .. }
        if *id == member && props.get("contextWindow").and_then(|v| v.as_u64()) == Some(128_000))));
}

#[test]
fn retired_member_is_removed_and_other_kinds_project_nothing() {
    let mut card = entry("card-2", AgentComponentKind::A2aAgentCard, AgentComponentFacts::Opaque, &[], &[]);
    card.lifecycle = AgentLibraryLifecycle::Retired;
    let mut retire = event(card);
    retire.kind = AgentComponentMutationKind::Retire;
    assert_eq!(
        project_component(&retire),
        vec![ProjectionOp::RemoveNode { id: member_id("card-2") }]
    );
    let tool = entry("tool-1", AgentComponentKind::Skill, AgentComponentFacts::Opaque, &[], &[]);
    assert!(project_component(&event(tool)).is_empty());
}

#[test]
fn tenant_private_components_are_not_projected_to_commons() {
    let mut card = entry("card-private", AgentComponentKind::A2aAgentCard, AgentComponentFacts::Opaque, &[], &["eg:capability/retrieval"]);
    card.attributes.remove("visibility");
    assert!(project_component(&event(card.clone())).is_empty());
    card.attributes.insert("visibility".into(), "tenant".into());
    assert!(project_component(&event(card)).is_empty());
}

fn critique_loop(lifecycle: &str) -> eg_types::agent_graph::AgentGraphEntry {
    let agent = |id: &str| {
        serde_json::json!({"node_id": id, "kind": {"agent": {"agent_id": id, "definition_digest": "sha256:a"}}})
    };
    let slot = |id: &str, role: &str| {
        serde_json::json!({"node_id": id, "role": role, "min_width": 1, "max_width": 1, "max_rounds": 1})
    };
    serde_json::from_value(serde_json::json!({
        "schema_version": eg_types::agent_graph::AGENT_GRAPH_ENTRY_SCHEMA_VERSION,
        "graph_id": "critique", "version": "1",
        "shape": {
            "entry_node": "writer",
            "nodes": [agent("writer"), agent("checker")],
            "edges": [
                {"from": "writer", "to": "checker"},
                {"from": "checker", "to": "writer"}
            ],
            "max_iterations": 4
        },
        "tenant_id": "t1", "actor_scope": "a", "purpose_id": "p", "policy_digest": "sha256:p",
        "visibility": "commons",
        "topology": {
            "class_iri": "swarm:CritiqueLoop", "depth": 1,
            "slots": [slot("writer", "child"), slot("checker", "verifier")],
            "stop": {"verifier_pass": {"max_rounds": 3}}
        },
        "composed_work_ceiling": 4, "entry_revision": 1, "lifecycle": lifecycle,
        "shape_digest": "sha256:s", "definition_digest": "sha256:d",
        "created_at_ms": 1, "updated_at_ms": 1
    }))
    .expect("a critique loop entry")
}

#[test]
fn template_projection_emits_feeds_edges() {
    use eg_core::library_projection::{project_graph_template, slot_id, template_id};
    let ops = project_graph_template(&critique_loop("published"));
    let (writer, checker) = (slot_id("critique", "writer"), slot_id("critique", "checker"));
    assert!(has_edge(&ops, &template_id("critique"), &writer, "hasSlot"));
    assert!(has_edge(&ops, &checker, "agentKind:verifier", "requiresAgentKind"));
    assert!(has_edge(&ops, &writer, &checker, "feeds"));
    assert!(has_edge(&ops, &checker, &writer, "loopsBackTo"));
    assert!(!has_edge(&ops, &checker, &writer, "feeds"), "the loop-back is not a forward edge");
    let mut private = critique_loop("published");
    private.visibility = None;
    assert!(project_graph_template(&private).is_empty(), "tenant-private templates stay out of commons");
    let retired = project_graph_template(&critique_loop("retired"));
    assert!(retired.iter().all(|op| matches!(op, ProjectionOp::RemoveNode { .. })));
}
