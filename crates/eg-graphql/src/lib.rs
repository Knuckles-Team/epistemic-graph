//! eg-graphql — a native GraphQL **read** surface over the engine's property-graph
//! (CONCEPT:KG-2.235, Lane M query-language polish).
//!
//! It does two things, both pure-Rust (NO async-graphql):
//!   * [`Schema::from_view`] — derives a GraphQL schema by introspecting a live
//!     `GraphView` (node labels → object types, properties → scalar fields, edge
//!     relationship types → object fields). The schema tracks the real graph with no
//!     hand-maintained SDL.
//!   * [`execute`] — compiles a parsed GraphQL query to scans + BFS over the SAME
//!     `GraphView` the Cypher / unified executor reads, returning GraphQL-shaped
//!     `{"data": …}` JSON. It reuses the engine's own label-index + edge primitives, so
//!     a GraphQL query returns the SAME nodes/fields as the equivalent Cypher query
//!     (proven by the `graphql_equals_cypher` test below).
//!
//! ## Why a hand-written parser (async-graphql evaluated, rejected)
//! `async-graphql` is the standard Rust GraphQL crate, but it pulls ~80+ transitive
//! crates plus a proc-macro schema-derive — too heavy for the Pi tier, and it wants a
//! STATIC `#[derive(SimpleObject)]` schema whereas ours is DERIVED FROM the graph at
//! runtime. The read subset here (one query op; root node-type fields; `first`/`limit`
//! + property-equality args; scalar + nested-edge selection) is small enough that a
//! hand-written tokenizer + recursive-descent parser + a resolver is simpler AND keeps
//! the surface Pi-excludable. The facade gates the whole crate behind `graphql`
//! (folded into node/cluster/full, OUT of pi/default), so a Pi build links none of it.
//!
//! ## Deferred (documented)
//! Mutations, subscriptions, fragments, variables, directives, interfaces/unions, and
//! relay-style connection pagination. A parse error names the unsupported construct.

pub mod parser;
pub mod resolver;
pub mod schema;

pub use parser::{parse, GqlError, Query};
pub use resolver::{execute, execute_query};
pub use schema::Schema;

#[cfg(test)]
mod tests {
    use super::*;
    use eg_core::graph::GraphCore;
    use serde_json::{json, Value};

    fn pbytes(v: Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
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
}
