//! W2-parity — SPARQL 1.1 UPDATE executed over the native property-graph write ops
//! (CONCEPT:EG-KG.query.named-graph-support).
//!
//! This is the REAL update path that replaces the naive `INSERT DATA` string-split shim
//! (`graph_ops.rs`). It wires `spargebra::Update` / `GraphUpdateOperation` to the
//! engine's `add_node` / `add_edge` / `remove_*` primitives via the SAME RDF ⇄
//! property-graph mapping the loader uses ([`crate::mapping`]):
//!
//!   * a triple with a **resource object** → a typed edge `s --p--> o`
//!     (`{relationship:p}`);
//!   * a triple with a **literal object** → a typed property cell on `s`;
//!   * `rdf:type` → folded into the canonical node `node_type` label AND kept as an explicit edge.
//!
//! Crucially the inserts are **incremental + merge-aware**: a single literal insert
//! merges into the subject's existing property blob instead of overwriting it (the
//! loader writes whole-node blobs, which is wrong for one-triple updates). Deletes are
//! surgical: a literal delete drops one property key; a resource delete removes the one
//! matching typed edge and preserves any others between the same pair.
//!
//! Named-graph routing goes through the [`GraphStore`] trait so the executor stays
//! decoupled from the engine registry: the server wires a registry-backed store, while
//! tests use the in-memory [`MapStore`]. `DELETE/INSERT … WHERE` evaluates its WHERE over
//! a [`Dataset`] of the store's graph snapshots (named-graph aware), then instantiates
//! the delete/insert quad patterns per solution.

mod engine;
mod parse;
mod store;
mod terms;
mod triples;
mod where_ops;

pub use eg_types::rdf_report::UpdateReport;
pub use engine::{execute, execute_str, UpdateError};
pub use parse::{insert_data_triples, parse_update, referenced_named_graphs};
pub use store::{GraphStore, MapStore};
pub use triples::{insert_triples, remove_triples};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::guard::{GuardRejection, WriteGuard};
    use crate::sparql::{execute, Dataset, Projection, QueryOutcome};
    use oxrdf::{Graph, Triple};

    struct TestWriteGuard;

    impl WriteGuard for TestWriteGuard {
        fn check_graph(
            &self,
            _graph: Option<&str>,
            _base: &Graph,
            _additions: &[Triple],
            _removals: &[Triple],
        ) -> Result<(), GuardRejection> {
            Ok(())
        }
    }

    fn execute_str(
        update_str: &str,
        store: &dyn GraphStore,
        projection: &Projection,
    ) -> Result<UpdateReport, UpdateError> {
        super::execute_str(update_str, store, projection, &TestWriteGuard)
    }

    fn count(store: &MapStore, graph: Option<&str>, query: &str) -> usize {
        let view = store.core_of(graph).analysis_snapshot();
        match execute(
            &Dataset::new(&view, Vec::new()),
            query,
            &Projection::raw(),
            None,
        )
        .unwrap()
        {
            QueryOutcome::Solutions(r) => r.solutions.len(),
            _ => panic!("expected solutions"),
        }
    }

    /// INSERT DATA then SELECT sees the triple; DELETE DATA removes it.
    #[test]
    fn insert_then_select_then_delete() {
        let store = MapStore::new();
        execute_str(
            "PREFIX ex: <http://ex/> INSERT DATA { ex:a ex:knows ex:b . ex:a ex:name \"A\" }",
            &store,
            &Projection::raw(),
        )
        .unwrap();
        assert_eq!(
            count(
                &store,
                None,
                "SELECT ?o WHERE { <http://ex/a> <http://ex/knows> ?o }"
            ),
            1,
            "edge visible after INSERT DATA"
        );
        assert_eq!(
            count(
                &store,
                None,
                "SELECT ?n WHERE { <http://ex/a> <http://ex/name> ?n }"
            ),
            1,
            "literal visible after INSERT DATA"
        );

        execute_str(
            "PREFIX ex: <http://ex/> DELETE DATA { ex:a ex:knows ex:b }",
            &store,
            &Projection::raw(),
        )
        .unwrap();
        assert_eq!(
            count(
                &store,
                None,
                "SELECT ?o WHERE { <http://ex/a> <http://ex/knows> ?o }"
            ),
            0,
            "edge gone after DELETE DATA"
        );
        // The literal (a different triple) is untouched — incremental delete is surgical.
        assert_eq!(
            count(
                &store,
                None,
                "SELECT ?n WHERE { <http://ex/a> <http://ex/name> ?n }"
            ),
            1,
            "the name literal survives the edge delete"
        );
    }

    /// DELETE/INSERT … WHERE rewrites: rename every `ex:name "A"` to `"B"`.
    #[test]
    fn delete_insert_where_rewrites() {
        let store = MapStore::new();
        execute_str(
            "PREFIX ex: <http://ex/> INSERT DATA { ex:a ex:name \"A\" . ex:b ex:name \"A\" }",
            &store,
            &Projection::raw(),
        )
        .unwrap();
        execute_str(
            "PREFIX ex: <http://ex/>
             DELETE { ?s ex:name \"A\" } INSERT { ?s ex:name \"B\" }
             WHERE  { ?s ex:name \"A\" }",
            &store,
            &Projection::raw(),
        )
        .unwrap();
        assert_eq!(
            count(
                &store,
                None,
                "SELECT ?s WHERE { ?s <http://ex/name> \"A\" }"
            ),
            0,
            "no A left"
        );
        assert_eq!(
            count(
                &store,
                None,
                "SELECT ?s WHERE { ?s <http://ex/name> \"B\" }"
            ),
            2,
            "both renamed to B"
        );
    }

    /// Named-graph isolation: a triple inserted into GRAPH g1 is NOT in the default graph
    /// nor in g2, and a CLEAR of g1 leaves g2 intact.
    #[test]
    fn named_graph_isolation() {
        let store = MapStore::new();
        execute_str(
            "PREFIX ex: <http://ex/>
             INSERT DATA {
               GRAPH <http://g/1> { ex:a ex:p ex:b }
               GRAPH <http://g/2> { ex:c ex:p ex:d }
             }",
            &store,
            &Projection::raw(),
        )
        .unwrap();
        assert_eq!(
            count(&store, Some("http://g/1"), "SELECT ?s WHERE { ?s ?p ?o }"),
            1,
            "g1 has its triple"
        );
        assert_eq!(
            count(&store, Some("http://g/2"), "SELECT ?s WHERE { ?s ?p ?o }"),
            1,
            "g2 has its triple"
        );
        assert_eq!(
            count(&store, None, "SELECT ?s WHERE { ?s ?p ?o }"),
            0,
            "default graph stays empty"
        );

        execute_str("CLEAR GRAPH <http://g/1>", &store, &Projection::raw()).unwrap();
        assert_eq!(
            count(&store, Some("http://g/1"), "SELECT ?s WHERE { ?s ?p ?o }"),
            0,
            "g1 cleared"
        );
        assert_eq!(
            count(&store, Some("http://g/2"), "SELECT ?s WHERE { ?s ?p ?o }"),
            1,
            "g2 untouched by CLEAR g1"
        );
    }

    /// ADD merges src into dst WITHOUT clearing dst (CONCEPT:EG-KG.query.sparql-add-copy-move). Both graphs' triples
    /// survive in dst; the source is left intact.
    #[test]
    fn add_merges_into_destination() {
        let store = MapStore::new();
        execute_str(
            "PREFIX ex: <http://ex/>
             INSERT DATA {
               GRAPH <http://g/1> { ex:a ex:p ex:b . ex:a ex:name \"A\" }
               GRAPH <http://g/2> { ex:c ex:p ex:d }
             }",
            &store,
            &Projection::raw(),
        )
        .unwrap();
        execute_str(
            "ADD <http://g/1> TO <http://g/2>",
            &store,
            &Projection::raw(),
        )
        .unwrap();
        // dst keeps its own triple AND gains src's edge + literal (merge, no clear).
        assert_eq!(
            count(
                &store,
                Some("http://g/2"),
                "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
            ),
            3,
            "g2 has its own + both of g1's triples"
        );
        // Source unchanged.
        assert_eq!(
            count(
                &store,
                Some("http://g/1"),
                "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
            ),
            2,
            "g1 source left intact by ADD"
        );
    }

    /// COPY replaces dst with src (CONCEPT:EG-KG.query.sparql-add-copy-move): dst's prior content is dropped first,
    /// then src is copied in; src stays intact.
    #[test]
    fn copy_replaces_destination() {
        let store = MapStore::new();
        execute_str(
            "PREFIX ex: <http://ex/>
             INSERT DATA {
               GRAPH <http://g/1> { ex:a ex:p ex:b }
               GRAPH <http://g/2> { ex:c ex:p ex:d . ex:c ex:q ex:e }
             }",
            &store,
            &Projection::raw(),
        )
        .unwrap();
        execute_str(
            "COPY <http://g/1> TO <http://g/2>",
            &store,
            &Projection::raw(),
        )
        .unwrap();
        // dst == src exactly (its two prior triples are gone).
        assert_eq!(
            count(
                &store,
                Some("http://g/2"),
                "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
            ),
            1,
            "g2 replaced by g1's single triple"
        );
        assert_eq!(
            count(
                &store,
                Some("http://g/2"),
                "SELECT ?s WHERE { <http://ex/a> <http://ex/p> <http://ex/b> }"
            ),
            1,
            "g2 now holds g1's triple"
        );
        assert_eq!(
            count(
                &store,
                Some("http://g/1"),
                "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
            ),
            1,
            "g1 source unchanged by COPY"
        );
    }

    /// MOVE replaces dst with src AND empties src (CONCEPT:EG-KG.query.sparql-add-copy-move).
    #[test]
    fn move_replaces_and_empties_source() {
        let store = MapStore::new();
        execute_str(
            "PREFIX ex: <http://ex/>
             INSERT DATA {
               GRAPH <http://g/1> { ex:a ex:p ex:b . ex:a ex:name \"A\" }
               GRAPH <http://g/2> { ex:c ex:p ex:d }
             }",
            &store,
            &Projection::raw(),
        )
        .unwrap();
        execute_str(
            "MOVE <http://g/1> TO <http://g/2>",
            &store,
            &Projection::raw(),
        )
        .unwrap();
        assert_eq!(
            count(
                &store,
                Some("http://g/2"),
                "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
            ),
            2,
            "g2 replaced by g1's two triples"
        );
        assert_eq!(
            count(
                &store,
                Some("http://g/1"),
                "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
            ),
            0,
            "g1 source emptied by MOVE"
        );
    }

    /// SILENT swallows a missing-source: `MOVE SILENT` from a graph that was never written
    /// is a no-op that returns Ok and leaves the destination empty.
    #[test]
    fn move_silent_missing_source_is_noop() {
        let store = MapStore::new();
        let r = execute_str(
            "MOVE SILENT <http://g/missing> TO <http://g/dst>",
            &store,
            &Projection::raw(),
        );
        assert!(r.is_ok(), "SILENT missing-source move does not error");
        assert_eq!(
            count(
                &store,
                Some("http://g/dst"),
                "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
            ),
            0,
            "destination stays empty"
        );
    }

    /// A cross-named-graph query: with both graphs in the dataset, `GRAPH ?g {}` ranges
    /// over them independently (graph A's triple is not seen under graph B's name).
    #[test]
    fn named_graph_query_scoping() {
        let store = MapStore::new();
        execute_str(
            "PREFIX ex: <http://ex/>
             INSERT DATA {
               GRAPH <http://g/1> { ex:a ex:p ex:b }
               GRAPH <http://g/2> { ex:c ex:p ex:d }
             }",
            &store,
            &Projection::raw(),
        )
        .unwrap();
        // Build a dataset over both named graphs and ask which graph holds ex:a.
        let v1 = store.core_of(Some("http://g/1")).analysis_snapshot();
        let v2 = store.core_of(Some("http://g/2")).analysis_snapshot();
        let default = eg_core::graph::GraphView::default();
        let ds = Dataset::new(
            &default,
            vec![
                ("http://g/1".to_string(), &v1),
                ("http://g/2".to_string(), &v2),
            ],
        );
        let out = crate::sparql::execute(
            &ds,
            "SELECT ?g WHERE { GRAPH ?g { <http://ex/a> <http://ex/p> <http://ex/b> } }",
            &Projection::raw(),
            None,
        )
        .unwrap();
        let QueryOutcome::Solutions(r) = out else {
            panic!("expected solutions")
        };
        let graphs: Vec<String> = r
            .solutions
            .iter()
            .filter_map(|s| s.get("g").map(|b| b.as_str().to_string()))
            .collect();
        assert_eq!(
            graphs,
            vec!["<http://g/1>".to_string()],
            "only g1 holds ex:a"
        );
    }
}
