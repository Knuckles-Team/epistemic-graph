//! The refusals the shared protocol builds, pinned per layer to the exact text
//! each layer's own copy produced before the protocol was written once.

use eg_types::contract::Digest256;

use super::super::agent_component::ComponentLayer;
use super::super::agent_fixtures::{mutation_context, open_agent_store};
use super::super::agent_graph::GraphLayer;
use super::super::agent_template::TemplateLayer;
use super::*;

fn refusals<L: RevisionLayer>(
    store: &AgentLibraryStore,
    context: &AgentLibraryMutationContext,
) -> [String; 3] {
    let mut unrevised = context.clone();
    unrevised.expected_revision = None;
    let conflict = ReplayResolution::Conflict {
        recorded: Digest256::from_bytes([1; 32]),
        proposed: Digest256::from_bytes([2; 32]),
    };
    [
        require_context_tenant::<L>(context, "tenant-other").unwrap_err(),
        revision_scope(store, &unrevised, L::NOUN)
            .err()
            .expect("a write without an expected revision is refused"),
        recorded_receipt(conflict, L::NOUN).unwrap_err(),
    ]
}

#[test]
fn every_layer_refuses_in_the_words_its_own_copy_used() {
    let (_dir, store) = open_agent_store();
    let context = mutation_context(&store, "tenant-a", "key-1", 1, 0, "agent-graph:publish");
    assert_eq!(
        refusals::<ComponentLayer>(&store, &context),
        [
            "agent component publish context tenant does not match the graph's tenant",
            "agent component writes require an explicit expected_revision",
            "IDEMPOTENCY_CONFLICT: key was already used by a different agent component mutation",
        ]
    );
    assert_eq!(
        refusals::<GraphLayer>(&store, &context),
        [
            "agent graph publish context tenant does not match the graph's tenant",
            "agent graph writes require an explicit expected_revision",
            "IDEMPOTENCY_CONFLICT: key was already used by a different agent graph mutation",
        ]
    );
    assert_eq!(
        refusals::<TemplateLayer>(&store, &context),
        [
            "agent template publish context tenant does not match the template's tenant",
            "agent template writes require an explicit expected_revision",
            "IDEMPOTENCY_CONFLICT: key was already used by a different agent template mutation",
        ]
    );
}

#[test]
fn every_layer_names_its_events_slugs_and_headers_as_before() {
    let names = |slug: &str, operation: &str, id_header: &str| {
        (
            format!("{}_publish", slug.replace('-', "_")),
            format!("{operation}-retire"),
            id_header.to_string(),
        )
    };
    assert_eq!(
        names(
            ComponentLayer::SLUG,
            ComponentLayer::OPERATION,
            ComponentLayer::ID_HEADER
        ),
        (
            "agent_component_publish".to_string(),
            "component-retire".to_string(),
            "component_id".to_string()
        )
    );
    assert_eq!(
        names(
            GraphLayer::SLUG,
            GraphLayer::OPERATION,
            GraphLayer::ID_HEADER
        ),
        (
            "agent_graph_publish".to_string(),
            "graph-retire".to_string(),
            "graph_id".to_string()
        )
    );
    assert_eq!(
        names(
            TemplateLayer::SLUG,
            TemplateLayer::OPERATION,
            TemplateLayer::ID_HEADER
        ),
        (
            "agent_template_publish".to_string(),
            "template-retire".to_string(),
            "template_id".to_string()
        )
    );
}
