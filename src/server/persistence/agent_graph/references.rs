// Existing Agent Graph cross-record and composition tests.

use super::tests::{agent_node, component, context, digest, draft, open_store, shape};
use super::*;
use eg_types::agent_graph::{AgentGraphEdge, AgentGraphNode, AgentGraphNodeKind, AgentGraphShape};

#[test]
fn a_graph_instantiating_a_template_resolves_it_and_refuses_a_ghost() {
    let (_dir, store) = open_store();
    let template_digest = super::super::agent_pin_resolution::seed_template_for_test(
        &store,
        "tenant-a",
        "template:researcher",
        60,
    );
    let mut graph = draft(&store, "tenant-a", "graph-a");
    graph.shape.nodes[0].kind = AgentGraphNodeKind::Template {
        template_id: "template:researcher".into(),
        definition_digest: template_digest.clone(),
        bindings: BTreeMap::new(),
    };
    store
        .publish_graph(AgentGraphPublishRequest {
            context: context(&store, "tenant-a", "key-1", 1, 0, "agent-graph:publish"),
            graph: graph.clone(),
        })
        .expect("a template node pinning a real template publishes");

    // And the same shape naming a template nothing carries does not.
    let mut ghost = draft(&store, "tenant-a", "graph-b");
    ghost.shape.nodes[0].kind = AgentGraphNodeKind::Template {
        template_id: "template:ghost".into(),
        definition_digest: template_digest,
        bindings: BTreeMap::new(),
    };
    let error = store
        .publish_graph(AgentGraphPublishRequest {
            context: context(&store, "tenant-a", "key-2", 2, 0, "agent-graph:publish"),
            graph: ghost,
        })
        .expect_err("an unresolvable template pin must be refused");
    assert!(
        error.contains("which does not exist in this tenant"),
        "got: {error}"
    );
}

// ---- composition: a graph whose node is another GRAPH ----
//
// Nothing below this line was covered before. No store test published a
// shape containing an `AgentGraphNodeKind::Graph` node, so
// `resolve_composed_graph` -- the one reference edge this hierarchy
// actually resolved -- had no coverage at all, and neither did the composed
// ceiling that admission stamps from it.

/// The child: one agent, then end, producing `contract:report`.
fn child_shape(store: &AgentLibraryStore, tenant_id: &str, max_iterations: u32) -> AgentGraphShape {
    AgentGraphShape {
        entry_node: "work".into(),
        nodes: vec![
            AgentGraphNode {
                node_id: "work".into(),
                kind: agent_node(store, tenant_id, "agent:work"),
                deps_contract: None,
                output_contract: Some(component(store, tenant_id, "contract:report")),
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
        max_iterations,
    }
}

/// A parent whose single step runs the named child graph, pinned by shape.
fn parent_shape(
    store: &AgentLibraryStore,
    tenant_id: &str,
    child_id: &str,
    child_shape_digest: &str,
    max_iterations: u32,
) -> AgentGraphShape {
    AgentGraphShape {
        entry_node: "team".into(),
        nodes: vec![
            AgentGraphNode {
                node_id: "team".into(),
                kind: AgentGraphNodeKind::Graph {
                    graph_id: child_id.into(),
                    shape_digest: child_shape_digest.into(),
                },
                deps_contract: None,
                output_contract: Some(component(store, tenant_id, "contract:report")),
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
        max_iterations,
    }
}

fn publish_shape(
    store: &AgentLibraryStore,
    graph_id: &str,
    shape: AgentGraphShape,
    key: &str,
    nonce: u8,
) -> AgentGraphEntry {
    let mut graph = draft(store, "tenant-a", graph_id);
    graph.shape = shape;
    store
        .publish_graph(AgentGraphPublishRequest {
            context: context(store, "tenant-a", key, nonce, 0, "agent-graph:publish"),
            graph,
        })
        .expect("publishes")
        .result
        .graph
}

#[test]
fn a_nested_graph_publishes_and_its_composed_ceiling_is_the_product() {
    let (_dir, store) = open_store();
    let child = publish_shape(
        &store,
        "graph:child",
        child_shape(&store, "tenant-a", 4),
        "key-child",
        1,
    );
    assert_eq!(child.composed_work_ceiling, 4);

    let parent = publish_shape(
        &store,
        "graph:parent",
        parent_shape(&store, "tenant-a", "graph:child", &child.shape_digest, 3),
        "key-parent",
        2,
    );
    // The product, not either level: 3 x 4. This is the number
    // `kg-delegate` holds a caller's declared ceiling to.
    assert_eq!(parent.composed_work_ceiling, 12);
    let reread = store
        .current_graph("tenant-a", "graph:parent")
        .unwrap()
        .unwrap();
    assert_eq!(reread, parent);
    reread
        .validate()
        .expect("the persisted row re-derives its own digest");
}

#[test]
fn a_nested_graph_pinning_a_shape_no_revision_carries_is_refused() {
    // The whole point of resolving a composition inside the write
    // transaction: a pinned child is a claim, and an unresolvable claim must
    // not become a durable revision.
    let (_dir, store) = open_store();
    publish_shape(
        &store,
        "graph:child",
        child_shape(&store, "tenant-a", 4),
        "key-child",
        1,
    );
    let mut graph = draft(&store, "tenant-a", "graph:parent");
    graph.shape = parent_shape(&store, "tenant-a", "graph:child", &digest('7'), 3);
    let error = store
        .publish_graph(AgentGraphPublishRequest {
            context: context(
                &store,
                "tenant-a",
                "key-parent",
                2,
                0,
                "agent-graph:publish",
            ),
            graph,
        })
        .expect_err("an unresolvable pin must be refused");
    assert!(
        error.contains("no retained revision of that graph matches the pinned shape digest"),
        "got: {error}"
    );
    assert!(store
        .current_graph("tenant-a", "graph:parent")
        .unwrap()
        .is_none());
}

#[test]
fn composing_a_child_from_another_tenant_is_refused() {
    // Resolution is by (id, digest) alone, so a caller who learns a digest
    // must not be able to execute a graph it was never granted.
    let (_dir, store) = open_store();
    let mut child = draft(&store, "tenant-b", "graph:child");
    child.shape = child_shape(&store, "tenant-b", 4);
    let child = store
        .publish_graph(AgentGraphPublishRequest {
            context: context(&store, "tenant-b", "key-child", 1, 0, "agent-graph:publish"),
            graph: child,
        })
        .unwrap()
        .result
        .graph;

    let mut graph = draft(&store, "tenant-a", "graph:parent");
    graph.shape = parent_shape(&store, "tenant-a", "graph:child", &child.shape_digest, 3);
    let error = store
        .publish_graph(AgentGraphPublishRequest {
            context: context(
                &store,
                "tenant-a",
                "key-parent",
                2,
                0,
                "agent-graph:publish",
            ),
            graph,
        })
        .expect_err("a cross-tenant composition must be refused");
    assert!(
        error.contains("no such graph in this tenant"),
        "got: {error}"
    );
}

#[test]
fn composing_a_retired_child_is_refused_while_it_stays_resolvable() {
    let (_dir, store) = open_store();
    let child = publish_shape(
        &store,
        "graph:child",
        child_shape(&store, "tenant-a", 4),
        "key-child",
        1,
    );
    store
        .retire_graph(eg_types::agent_graph::AgentGraphRetireRequest {
            context: context(&store, "tenant-a", "key-retire", 2, 1, "agent-graph:retire"),
            graph_id: "graph:child".to_string(),
        })
        .unwrap();
    let mut graph = draft(&store, "tenant-a", "graph:parent");
    graph.shape = parent_shape(&store, "tenant-a", "graph:child", &child.shape_digest, 3);
    let error = store
        .publish_graph(AgentGraphPublishRequest {
            context: context(
                &store,
                "tenant-a",
                "key-parent",
                3,
                0,
                "agent-graph:publish",
            ),
            graph,
        })
        .expect_err("nothing new may be built on a withdrawn graph");
    assert!(error.contains("which is retired"), "got: {error}");
    // The retired revision is still RESOLVABLE by id -- a parent published
    // before the retirement keeps working.
    assert!(store
        .current_graph("tenant-a", "graph:child")
        .unwrap()
        .is_some());
}

#[test]
fn a_three_level_composition_stamps_the_product_across_every_level() {
    // Two levels top out at 1000 x 1000 with two legal shapes, so the
    // danger the ceiling exists for -- the PRODUCT across levels, each of
    // which looks modest on its own -- is only reachable at depth >= 2.
    let (_dir, store) = open_store();
    let leaf = publish_shape(
        &store,
        "graph:leaf",
        child_shape(&store, "tenant-a", 5),
        "key-leaf",
        1,
    );
    let mid = publish_shape(
        &store,
        "graph:mid",
        parent_shape(&store, "tenant-a", "graph:leaf", &leaf.shape_digest, 7),
        "key-mid",
        2,
    );
    assert_eq!(mid.composed_work_ceiling, 35);
    let root = publish_shape(
        &store,
        "graph:root",
        parent_shape(&store, "tenant-a", "graph:mid", &mid.shape_digest, 11),
        "key-root",
        3,
    );
    assert_eq!(
        root.composed_work_ceiling, 385,
        "11 x 7 x 5 -- not 11, not 77, and not the deepest level alone"
    );
}

#[test]
fn the_store_reopens_after_a_graph_commit() {
    // Recovery validates every owner row against its parent batch on open.
    // A graph write that recovery cannot re-derive would make the whole
    // owner -- entries included -- unopenable.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap();
    {
        let store = AgentLibraryStore::open(path).unwrap();
        store
            .publish_graph(AgentGraphPublishRequest {
                context: context(&store, "tenant-a", "key-1", 1, 0, "agent-graph:publish"),
                graph: draft(&store, "tenant-a", "graph-a"),
            })
            .unwrap();
    }
    let reopened = AgentLibraryStore::open(path).expect("owner reopens after a graph commit");
    let current = reopened
        .current_graph("tenant-a", "graph-a")
        .unwrap()
        .expect("the graph survives a reopen");
    assert_eq!(current.entry_revision, 1);
    assert_eq!(
        current.shape_digest,
        shape(&reopened, "tenant-a").shape_digest()
    );
}
