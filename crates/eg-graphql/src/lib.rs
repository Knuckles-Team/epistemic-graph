//! eg-graphql — a native GraphQL surface over the engine's property-graph
//! (CONCEPT:KG-2.235 reads; CONCEPT:EG-019 writes), all pure-Rust (NO async-graphql):
//!   * [`Schema::from_view`] — derives a GraphQL schema by introspecting a live
//!     `GraphView` (node labels → object types, properties → scalar fields, edge
//!     relationship types → object fields). The schema tracks the real graph with no
//!     hand-maintained SDL; [`Schema::to_mutation_sdl`] renders the matching write
//!     vocabulary.
//!   * [`execute`] — compiles a parsed GraphQL QUERY to scans + BFS over the SAME
//!     `GraphView` the Cypher / unified executor reads, returning GraphQL-shaped
//!     `{"data": …}` JSON. It reuses the engine's own label-index + edge primitives, so
//!     a GraphQL query returns the SAME nodes/fields as the equivalent Cypher query
//!     (proven by the `graphql_equals_cypher` test below).
//!   * [`execute_mutation`] — maps a GraphQL MUTATION (`createNode`/`updateNode`/
//!     `deleteNode`/`addEdge`/`removeEdge`) onto eg-core's native write ops over the
//!     live `GraphCore`, then shapes the affected node with the same selection-resolver
//!     the query path uses (CONCEPT:EG-019).
//!   * [`subscribe`] / [`subscribe_versioned`] — a poll over the current matches for a
//!     GraphQL SUBSCRIPTION (a query that streams); the OCC `version()` lets a watcher
//!     re-render only on change. A full push transport is a documented deferral
//!     (see `crate::subscription`).
//!
//! ## Why a hand-written parser (async-graphql evaluated, rejected)
//! `async-graphql` is the standard Rust GraphQL crate, but it pulls ~80+ transitive
//! crates plus a proc-macro schema-derive — too heavy for the Pi tier, and it wants a
//! STATIC `#[derive(SimpleObject)]` schema whereas ours is DERIVED FROM the graph at
//! runtime. The read subset here (one query op; root node-type fields with
//! `first`/`limit` and property-equality args; scalar + nested-edge selection) is small
//! enough that a hand-written tokenizer + recursive-descent parser + resolver keeps
//! the surface simpler AND Pi-excludable. The facade gates the whole crate behind
//! `graphql` (node/cluster/full, OUT of pi/default), so a Pi build links none of it.
//!
//! ## Deferred (documented)
//! A push subscription transport (a `tokio::sync::broadcast` change-stream — see
//! `crate::subscription`), fragments, variables, directives, interfaces/unions, and
//! relay-style connection pagination. A parse error names the unsupported construct.

pub mod mutation;
pub mod parser;
pub mod resolver;
pub mod schema;
pub mod subscription;

pub use mutation::execute as execute_mutation;
pub use parser::{parse, parse_operation, GqlError, Mutation, Operation, Query, Subscription};
pub use resolver::{execute, execute_query};
pub use schema::Schema;
pub use subscription::{poll as subscribe, poll_versioned as subscribe_versioned};

#[cfg(test)]
mod tests {
    use super::*;
    use eg_core::graph::GraphCore;
    use serde_json::{json, Value};

    fn pbytes(v: Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    /// The live `GraphCore` behind the fixture (writes operate on this).
    fn core_fixture() -> GraphCore {
        let core = GraphCore::new();
        core.add_node(
            "alice".into(),
            pbytes(json!({"type":"Person","name":"Alice","age":30})),
        );
        core.add_node(
            "bob".into(),
            pbytes(json!({"type":"Person","name":"Bob","age":25})),
        );
        core.add_node(
            "carol".into(),
            pbytes(json!({"type":"Person","name":"Carol","age":40})),
        );
        core.add_node("d1".into(), pbytes(json!({"type":"Doc","title":"Graphs"})));
        core.add_edge(
            "alice".into(),
            "bob".into(),
            pbytes(json!({"relationship":"KNOWS"})),
        )
        .unwrap();
        core.add_edge(
            "bob".into(),
            "carol".into(),
            pbytes(json!({"relationship":"KNOWS"})),
        )
        .unwrap();
        core
    }

    /// alice/bob/carol People + a Doc; alice-KNOWS->bob, bob-KNOWS->carol. The SAME
    /// shape eg-query/cypher's fixture uses, so the equivalence comparison is honest.
    fn fixture() -> eg_core::graph::GraphView {
        let core = GraphCore::new();
        core.add_node(
            "alice".into(),
            pbytes(json!({"type":"Person","name":"Alice","age":30})),
        );
        core.add_node(
            "bob".into(),
            pbytes(json!({"type":"Person","name":"Bob","age":25})),
        );
        core.add_node(
            "carol".into(),
            pbytes(json!({"type":"Person","name":"Carol","age":40})),
        );
        core.add_node("d1".into(), pbytes(json!({"type":"Doc","title":"Graphs"})));
        core.add_edge(
            "alice".into(),
            "bob".into(),
            pbytes(json!({"relationship":"KNOWS"})),
        )
        .unwrap();
        core.add_edge(
            "bob".into(),
            "carol".into(),
            pbytes(json!({"relationship":"KNOWS"})),
        )
        .unwrap();
        core.analysis_snapshot()
    }

    /// Schema-from-graph: node labels become types, properties + edge rels become
    /// fields.
    #[test]
    fn schema_derived_from_graph() {
        let view = fixture();
        let s = Schema::from_view(&view);
        assert!(s.has_type("Person") && s.has_type("Doc"));
        let person = &s.types["Person"];
        assert!(person.scalar_fields.contains("name"));
        assert!(person.scalar_fields.contains("age"));
        assert!(person.edge_fields.contains("KNOWS"));
        // SDL renders the Query root + the type fields.
        let sdl = s.to_sdl();
        assert!(sdl.contains("type Query"));
        assert!(sdl.contains("type Person"));
    }

    /// A nested GraphQL query returns the selected scalar + the traversed edge target.
    #[test]
    fn nested_query_resolves() {
        let view = fixture();
        let res = execute(
            &view,
            r#"{ Person(name: "Alice") { name knows: KNOWS { name } } }"#,
        )
        .unwrap();
        let people = &res["data"]["Person"];
        assert_eq!(people.as_array().unwrap().len(), 1);
        assert_eq!(people[0]["name"], json!("Alice"));
        // alice KNOWS bob.
        let knows = &people[0]["knows"];
        assert_eq!(knows.as_array().unwrap().len(), 1);
        assert_eq!(knows[0]["name"], json!("Bob"));
    }

    /// `first` caps the root rows.
    #[test]
    fn first_limits_rows() {
        let view = fixture();
        let res = execute(&view, "{ Person(first: 2) { name } }").unwrap();
        assert_eq!(res["data"]["Person"].as_array().unwrap().len(), 2);
    }

    /// An unknown root type is a clear error (it must be a node label).
    #[test]
    fn unknown_type_errors() {
        let view = fixture();
        let e = execute(&view, "{ Widget { id } }").unwrap_err();
        assert!(e.contains("no node type `Widget`"), "got {e}");
    }

    /// THE equivalence proof (CONCEPT:KG-2.235): a GraphQL query returns the SAME node
    /// set + the SAME field values as the EQUIVALENT Cypher query, run through the
    /// engine's real Cypher executor (eg-query). Two surfaces, one substrate.
    #[test]
    fn graphql_equals_cypher() {
        let view = fixture();

        // ── (a) all Person names — GraphQL vs `MATCH (p:Person) RETURN p.name` ──
        let gql = execute(&view, "{ Person { name } }").unwrap();
        let mut gql_names: Vec<String> = gql["data"]["Person"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n["name"].as_str().unwrap().to_string())
            .collect();
        gql_names.sort();

        let cy = eg_query::exec_cypher(&view, "MATCH (p:Person) RETURN p.name").unwrap();
        let mut cy_names: Vec<String> = cy
            .rows
            .iter()
            .map(|blob| {
                let cells: Vec<Value> = rmp_serde::from_slice(blob).unwrap();
                cells[0].as_str().unwrap().to_string()
            })
            .collect();
        cy_names.sort();

        assert_eq!(
            gql_names, cy_names,
            "GraphQL Person.name set must equal the Cypher result"
        );
        assert_eq!(gql_names, vec!["Alice", "Bob", "Carol"]);

        // ── (b) the KNOWS traversal from Alice — GraphQL nested edge vs
        //     `MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name` ──
        let gql2 = execute(&view, r#"{ Person(name: "Alice") { KNOWS { name } } }"#).unwrap();
        let gql2_targets: Vec<String> = gql2["data"]["Person"][0]["KNOWS"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n["name"].as_str().unwrap().to_string())
            .collect();

        let cy2 = eg_query::exec_cypher(
            &view,
            "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = 'Alice' RETURN b.name",
        )
        .unwrap();
        let cy2_targets: Vec<String> = cy2
            .rows
            .iter()
            .map(|blob| {
                let cells: Vec<Value> = rmp_serde::from_slice(blob).unwrap();
                cells[0].as_str().unwrap().to_string()
            })
            .collect();

        assert_eq!(
            gql2_targets, cy2_targets,
            "GraphQL KNOWS traversal must equal the Cypher traversal"
        );
        assert_eq!(gql2_targets, vec!["Bob"]);
    }

    // ── writes (CONCEPT:EG-019) ───────────────────────────────────────────────────

    /// THE core write→read proof: a `createNode` mutation makes a node a subsequent
    /// query sees, with the values supplied in `props`.
    #[test]
    fn mutation_create_then_query_sees_it() {
        let core = core_fixture();
        let res = execute_mutation(
            &core,
            r#"mutation {
                createNode(label: "Person", id: "dave", props: {name: "Dave", age: 50}) {
                    id
                    name
                }
            }"#,
        )
        .unwrap();
        assert_eq!(res["data"]["createNode"]["id"], json!("dave"));
        assert_eq!(res["data"]["createNode"]["name"], json!("Dave"));

        // A fresh query over the post-write snapshot sees Dave.
        let view = core.analysis_snapshot();
        let q = execute(&view, r#"{ Person(name: "Dave") { name age } }"#).unwrap();
        let people = q["data"]["Person"].as_array().unwrap();
        assert_eq!(people.len(), 1);
        assert_eq!(people[0]["name"], json!("Dave"));
        assert_eq!(people[0]["age"], json!(50));
    }

    /// `updateNode` merges props; the query then reflects the new value.
    #[test]
    fn mutation_update_round_trips() {
        let core = core_fixture();
        let res = execute_mutation(
            &core,
            r#"mutation { updateNode(id: "alice", props: {age: 31, city: "NYC"}) { id age city } }"#,
        )
        .unwrap();
        assert_eq!(res["data"]["updateNode"]["age"], json!(31));
        assert_eq!(res["data"]["updateNode"]["city"], json!("NYC"));

        let view = core.analysis_snapshot();
        let q = execute(&view, r#"{ Person(name: "Alice") { age city name } }"#).unwrap();
        assert_eq!(q["data"]["Person"][0]["age"], json!(31));
        assert_eq!(q["data"]["Person"][0]["city"], json!("NYC"));
        // untouched fields survive the merge.
        assert_eq!(q["data"]["Person"][0]["name"], json!("Alice"));
    }

    /// `updateNode` on a missing node is a clear error.
    #[test]
    fn mutation_update_missing_node_errors() {
        let core = core_fixture();
        let e = execute_mutation(&core, r#"mutation { updateNode(id: "ghost", props: {x: 1}) { id } }"#)
            .unwrap_err();
        assert!(e.contains("not found"), "got {e}");
    }

    /// `deleteNode` removes the node; the query no longer sees it.
    #[test]
    fn mutation_delete_round_trips() {
        let core = core_fixture();
        let res =
            execute_mutation(&core, r#"mutation { deleteNode(id: "carol") }"#).unwrap();
        assert_eq!(res["data"]["deleteNode"]["deleted"], json!(true));
        assert_eq!(res["data"]["deleteNode"]["id"], json!("carol"));

        let view = core.analysis_snapshot();
        let q = execute(&view, "{ Person { name } }").unwrap();
        let names: Vec<&str> = q["data"]["Person"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["name"].as_str().unwrap())
            .collect();
        assert!(!names.contains(&"Carol"), "Carol should be gone, got {names:?}");
    }

    /// `addEdge` creates a relationship the query traversal then follows.
    #[test]
    fn mutation_add_edge_round_trips() {
        let core = core_fixture();
        // alice -KNOWS-> carol is NOT in the fixture; add it.
        let res = execute_mutation(
            &core,
            r#"mutation { addEdge(from: "alice", to: "carol", type: "KNOWS", props: {since: 2020}) }"#,
        )
        .unwrap();
        assert_eq!(res["data"]["addEdge"]["added"], json!(true));
        assert_eq!(res["data"]["addEdge"]["type"], json!("KNOWS"));

        let view = core.analysis_snapshot();
        let q = execute(&view, r#"{ Person(name: "Alice") { KNOWS { name } } }"#).unwrap();
        let mut targets: Vec<&str> = q["data"]["Person"][0]["KNOWS"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n["name"].as_str().unwrap())
            .collect();
        targets.sort();
        assert_eq!(targets, vec!["Bob", "Carol"]);
    }

    /// `removeEdge` drops the relationship; the traversal no longer follows it.
    #[test]
    fn mutation_remove_edge_round_trips() {
        let core = core_fixture();
        execute_mutation(&core, r#"mutation { removeEdge(from: "alice", to: "bob") }"#).unwrap();

        let view = core.analysis_snapshot();
        let q = execute(&view, r#"{ Person(name: "Alice") { KNOWS { name } } }"#).unwrap();
        assert_eq!(q["data"]["Person"][0]["KNOWS"].as_array().unwrap().len(), 0);
    }

    /// `addEdge` to a missing endpoint surfaces the engine's error.
    #[test]
    fn mutation_add_edge_missing_endpoint_errors() {
        let core = core_fixture();
        let e = execute_mutation(
            &core,
            r#"mutation { addEdge(from: "alice", to: "ghost", type: "KNOWS") }"#,
        )
        .unwrap_err();
        assert!(e.contains("not found"), "got {e}");
    }

    /// A subscription POLL returns the current matches (same shape as a query) and the
    /// versioned poll advances after a write.
    #[test]
    fn subscription_polls_current_matches() {
        let core = core_fixture();
        let view = core.analysis_snapshot();
        let res = subscribe(&view, "subscription { Person { name } }").unwrap();
        assert_eq!(res["data"]["Person"].as_array().unwrap().len(), 3);

        // versioned poll: version advances once a mutation lands, so a watcher knows to
        // re-render.
        let (_d0, v0) = subscribe_versioned(&core, "subscription { Person { name } }").unwrap();
        execute_mutation(
            &core,
            r#"mutation { createNode(label: "Person", id: "eve", props: {name: "Eve"}) { id } }"#,
        )
        .unwrap();
        let (d1, v1) = subscribe_versioned(&core, "subscription { Person { name } }").unwrap();
        assert!(v1 > v0, "version must advance after a write ({v0} -> {v1})");
        assert_eq!(d1["data"]["Person"].as_array().unwrap().len(), 4);
    }

    /// The mutation SDL renders the write vocabulary, tied back to the graph's labels.
    #[test]
    fn mutation_sdl_renders() {
        let view = fixture();
        let sdl = Schema::from_view(&view).to_mutation_sdl();
        assert!(sdl.contains("type Mutation"));
        assert!(sdl.contains("createNode(label: String!"));
        assert!(sdl.contains("addEdge(from: ID!"));
        // the per-graph label comment ties generic ops to real types.
        assert!(sdl.contains("Person"));
    }
}
