//! Agent Library component facts as graph operations
//! (EG-ORCH-VOCAB-R003.6, R004.4, R005.5).
//!
//! One committed component revision becomes the nodes and typed edges the
//! orchestration vocabulary names: `:memberOfLibrary`, `:hasAgentKind`,
//! `:instanceOfTemplate`, `a :A2AAgentCard` with `:providesCapability`,
//! `a :BestPractice` with `:appliesToTaskClass`, and `a :ModelProfile` with
//! `:contextWindow`. The function is pure; the server projection worker
//! applies the operations to the shared `__commons__` graph.

use eg_types::agent_component::{
    AgentComponentFacts, AgentComponentKind, AgentComponentOutboxEvent,
};
use eg_types::agent_library::AgentLibraryLifecycle;
use serde_json::{json, Map, Value};

/// One graph operation of a projection.
#[derive(Debug, Clone, PartialEq)]
pub enum ProjectionOp {
    /// Write a node. A `stub` is written only when the id is absent, so a
    /// reference never overwrites the real node.
    Node {
        id: String,
        node_type: String,
        props: Map<String, Value>,
        stub: bool,
    },
    /// Write a typed edge; `relationship` is the canonical edge-type field.
    Edge {
        source: String,
        target: String,
        relationship: String,
        props: Map<String, Value>,
    },
    /// Remove a node and its edges.
    RemoveNode { id: String },
}

/// The component attribute that opts a component into the shared commons
/// graph. `__commons__` holds tenant-free facts, so a component without
/// `visibility = commons` is tenant-private and never projected.
pub const VISIBILITY_ATTRIBUTE: &str = "visibility";
pub const COMMONS_VISIBILITY: &str = "commons";

/// The graph id of the shared library.
pub fn library_id() -> String {
    "agent-library".to_string()
}

/// The graph id of a commons-visible component.
pub fn member_id(component_id: &str) -> String {
    format!("agent-library:{component_id}")
}

fn node(id: String, node_type: &str, props: Value, stub: bool) -> ProjectionOp {
    let Value::Object(props) = props else {
        unreachable!("projection properties are objects")
    };
    ProjectionOp::Node {
        id,
        node_type: node_type.to_string(),
        props,
        stub,
    }
}

fn edge(source: &str, target: &str, relationship: &str, props: Value) -> ProjectionOp {
    let Value::Object(props) = props else {
        unreachable!("projection properties are objects")
    };
    ProjectionOp::Edge {
        source: source.to_string(),
        target: target.to_string(),
        relationship: relationship.to_string(),
        props,
    }
}

/// The member class a component projects as, or `None` when the component is
/// not a library member in the orchestration vocabulary.
fn member_class(entry: &eg_types::agent_component::AgentComponentEntry) -> Option<&'static str> {
    match entry.kind {
        AgentComponentKind::A2aAgentCard => Some("A2AAgentCard"),
        AgentComponentKind::ModelProfile => Some("ModelProfile"),
        AgentComponentKind::GuardrailRule => Some("BestPractice"),
        _ => match entry.attributes.get("library_member").map(String::as_str) {
            Some("agent_template") => Some("AgentTemplate"),
            Some("agent") => Some("Agent"),
            _ => None,
        },
    }
}

/// The operations one committed component revision projects to.
pub fn project_component(event: &AgentComponentOutboxEvent) -> Vec<ProjectionOp> {
    let entry = &event.component;
    let member = member_id(&entry.component_id);
    if entry.attributes.get(VISIBILITY_ATTRIBUTE).map(String::as_str) != Some(COMMONS_VISIBILITY) {
        return Vec::new();
    }
    let Some(class) = member_class(entry) else {
        return Vec::new();
    };
    if entry.lifecycle != AgentLibraryLifecycle::Published {
        return vec![ProjectionOp::RemoveNode { id: member }];
    }
    let mut props = json!({
        "component_id": entry.component_id,
        "component_kind": entry.kind.as_str(),
        "version": entry.version,
        "definition_digest": entry.definition_digest,
        "source_revision": entry.source_revision,
    });
    if let AgentComponentFacts::ModelProfile {
        context_window_tokens,
        ..
    } = &entry.facts
    {
        props["contextWindow"] = json!(context_window_tokens);
    }
    if class == "BestPractice" {
        // The source generation is the rule's provenance.
        if let Some(generation) = entry.attributes.get("source_generation") {
            props["source_generation"] = json!(generation);
        }
    }
    let mut ops = vec![node(member.clone(), class, props, false)];
    let library = library_id();
    if class != "BestPractice" && class != "ModelProfile" {
        ops.push(node(library.clone(), "AgentLibrary", json!({}), true));
        ops.push(edge(&member, &library, "memberOfLibrary", json!({})));
    }
    if let Some(kind) = entry.attributes.get("agent_kind") {
        let kind_id = format!("agentKind:{kind}");
        ops.push(node(kind_id.clone(), "AgentKind", json!({ "name": kind }), true));
        ops.push(edge(&member, &kind_id, "hasAgentKind", json!({})));
    }
    if let Some(template) = entry.attributes.get("instance_of_template") {
        let template_id = member_id(template);
        ops.push(node(template_id.clone(), "AgentTemplate", json!({ "component_id": template }), true));
        ops.push(edge(&member, &template_id, "instanceOfTemplate", json!({})));
    }
    if class == "A2AAgentCard" {
        for capability in &entry.declared_capabilities {
            ops.push(node(capability.clone(), "Capability", json!({}), true));
            // A card's capabilities are its own claims (EG-DECISION-ENGINE-R126.1).
            ops.push(edge(&member, capability, "providesCapability", json!({ "strength": "claim" })));
        }
    }
    if class == "BestPractice" {
        if let Some(task_class) = entry.attributes.get("applies_to_class") {
            ops.push(node(task_class.clone(), "TaskClass", json!({}), true));
            ops.push(edge(&member, task_class, "appliesToTaskClass", json!({})));
        }
    }
    ops
}
