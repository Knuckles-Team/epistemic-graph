//! Native RDF/SPARQL handler (CONCEPT:EG-KG.ontology.kg-native-rdf-sparql / KG-2.218, features `rdf`/`sparql`).
//!
//! Owns the `// ── RDF/SPARQL ──` protocol section — `AddTriples` / `GetRdf`
//! (feature `rdf`) and `Sparql` (feature `sparql`). These are GRAPH-SCOPED ops: the
//! RDF dataset maps onto the SAME property-graph the rest of the engine uses (a
//! resource object ⇒ a typed edge, a literal object ⇒ a typed property cell,
//! `rdf:type` ⇒ the engine `type` label, a named graph ⇒ the target registry graph).
//! So they route through the normal `dispatch_graph_op` chain like Sql/Cypher.
//!
//! * `AddTriples` is a DURABLE MUTATION (it writes nodes + edges). The mutation
//!   gateway runs it against an isolated graph image, commits that complete image
//!   through `MutationBatch`, then publishes it to RAM.
//! * `GetRdf` serializes the graph back OUT to N-Triples (read-only).
//! * `Sparql` evaluates a SPARQL 1.1 SELECT over an off-lock GraphView snapshot
//!   (read-only), same idiom as the SQL/Cypher handlers.
//!
//! Multi-valued literals live inside the authoritative node blob, so RDF has no
//! secondary persistence or read authority.

#![allow(clippy::result_large_err)]

mod dispatch;
#[cfg(feature = "obda")]
mod obda;
mod reasoning;
#[cfg(feature = "sparql")]
mod sparql;
mod triples;
#[cfg(any(feature = "shacl", feature = "shex"))]
mod validation;

pub(crate) use dispatch::try_handle;
#[cfg(feature = "owl")]
pub(crate) use reasoning::try_handle_distributed;

// ── RunRules dispatch wiring (CONCEPT:EG-KG.ontology.eg-runtime-swrl-datalog / EG-023) ────────────────────────────
#[cfg(all(test, feature = "rdf"))]
mod run_rules_dispatch_tests {
    use crate::protocol::{Method, Request, Response, ResultPayload};
    use crate::server::auth::{
        build_shared_test_request, dispatch_test_on_heap as dispatch_on_heap,
    };
    use crate::server::state::ServerState;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    const SECRET: &str = "run-rules-test-secret";
    const TEST_AGENT: &str = "unit-test-agent";

    fn state() -> Arc<RwLock<ServerState>> {
        let mut state = ServerState::new_for_test(SECRET, ServerState::test_isolation(TEST_AGENT));
        state.persist_dir = Some(
            crate::server::sql_tables::test_persist_dir()
                .to_string_lossy()
                .into_owned(),
        );
        Arc::new(RwLock::new(state))
    }

    fn req(id: u64, method: Method) -> Request {
        build_shared_test_request(SECRET, id, "__commons__", TEST_AGENT, method)
    }

    /// A `RunRules` dispatched over the wire returns the DERIVED facts (CONCEPT:EG-KG.query.mirrors-pgwire):
    /// the `grandparent` entailment from the two `parent` ABox triples + the SWRL rule.
    #[tokio::test]
    async fn run_rules_returns_inferred_facts_via_dispatch() {
        let state = state();
        let resp: Response = dispatch_on_heap(
            &state,
            req(
                1,
                Method::RunRules {
                    ontology_ttl: "@prefix ex: <http://ex/> .\nex:alice ex:parent ex:bob .\nex:bob ex:parent ex:carol .\n".into(),
                    rules: vec![
                        "parent(?x,?y) ^ parent(?y,?z) -> grandparent(?x,?z) @0.8".into(),
                    ],
                    query_predicate: Some("grandparent".into()),
                    min_confidence: 0.0,
                    derived_only: true,
                },
            ),
        )
        .await;
        assert!(resp.error.is_none(), "RunRules failed: {:?}", resp.error);
        let bytes = match &resp.result {
            Some(ResultPayload::Raw(b)) => b.clone(),
            other => panic!("expected Raw, got {other:?}"),
        };
        let out: eg_rdf::rules::RuleReasonResponse =
            rmp_serde::from_slice(&bytes).expect("RuleReasonResponse");
        assert!(out.consistent);
        assert_eq!(out.registered_rules.len(), 1);
        assert_eq!(
            out.facts.len(),
            1,
            "only the grandparent fact: {:?}",
            out.facts
        );
        assert_eq!(out.facts[0].predicate, "grandparent");
        assert!(out.facts[0].derived, "the returned fact is an inference");
    }
}

/// W4.11 — external OBDA source SQL-generation + row-level predicate pushdown
/// (CONCEPT:EG-KG.query.obda-predicate-pushdown). Proves the pushdown "via the query plan"
/// (the rendered SQL) and end-to-end over a MOCK external source — no live database.
#[cfg(all(test, feature = "obda"))]
mod obda_pushdown_tests {
    use super::obda::{render_obda_select, ObdaSqlDialect, ObdaSqlExecutor, SqlObdaSource};
    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex};

    fn needed(cols: &[&str]) -> BTreeSet<String> {
        cols.iter().map(|c| c.to_string()).collect()
    }

    /// A mock [`ObdaSqlExecutor`] recording every rendered SQL string it is handed, and
    /// returning a fixed "external table" — the acceptance-shaped mock (no live DB).
    struct MockSqlExecutor {
        seen: Mutex<Vec<String>>,
        rows: Vec<eg_rdf::obda::ForeignRow>,
    }
    impl ObdaSqlExecutor for MockSqlExecutor {
        fn run_select(&self, sql: &str) -> Result<Vec<eg_rdf::obda::ForeignRow>, String> {
            self.seen.lock().unwrap().push(sql.to_string());
            Ok(self.rows.clone())
        }
    }

    fn row(id: &str, name: &str, age: &str) -> eg_rdf::obda::ForeignRow {
        [("id", id), ("name", name), ("age", age)]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// CONCEPT:EG-KG.query.obda-predicate-pushdown — a numeric filter renders a native-typed
    /// pushed `WHERE`, with every projected column cast to text (Postgres).
    #[test]
    fn render_obda_select_pushes_numeric_where_postgres() {
        let filters = vec![eg_rdf::obda::ObdaFilter {
            column: "age".into(),
            op: eg_rdf::obda::ObdaCompare::Gt,
            value: "28".into(),
            numeric: true,
        }];
        let sql = render_obda_select(
            "people",
            &needed(&["name", "age"]),
            &filters,
            ObdaSqlDialect::Postgres,
        )
        .unwrap();
        assert!(sql.contains("FROM \"people\""), "{sql}");
        assert!(sql.contains("\"age\"::text AS \"age\""), "{sql}");
        assert!(
            sql.contains("WHERE \"age\" > 28"),
            "the numeric FILTER must be pushed to SQL: {sql}"
        );
    }

    /// A MySQL render uses backtick quoting + `CAST(... AS CHAR)`.
    #[test]
    fn render_obda_select_mysql_dialect() {
        let filters = vec![eg_rdf::obda::ObdaFilter {
            column: "age".into(),
            op: eg_rdf::obda::ObdaCompare::Ge,
            value: "18".into(),
            numeric: true,
        }];
        let sql =
            render_obda_select("t", &needed(&["age"]), &filters, ObdaSqlDialect::MySql).unwrap();
        assert!(sql.contains("FROM `t`"), "{sql}");
        assert!(sql.contains("CAST(`age` AS CHAR) AS `age`"), "{sql}");
        assert!(sql.contains("WHERE `age` >= 18"), "{sql}");
    }

    /// A string-equality filter is single-quoted and injection-escaped (doubled quote).
    #[test]
    fn render_obda_select_escapes_string_literal() {
        let filters = vec![eg_rdf::obda::ObdaFilter {
            column: "name".into(),
            op: eg_rdf::obda::ObdaCompare::Eq,
            value: "O'Brien".into(),
            numeric: false,
        }];
        let sql = render_obda_select(
            "people",
            &needed(&["name"]),
            &filters,
            ObdaSqlDialect::Postgres,
        )
        .unwrap();
        assert!(
            sql.contains("WHERE \"name\" = 'O''Brien'"),
            "the string literal must be safely escaped: {sql}"
        );
    }

    /// An invalid identifier (injection attempt) is rejected, not quoted through.
    #[test]
    fn render_obda_select_rejects_bad_identifier() {
        let err = render_obda_select(
            "people; DROP TABLE users",
            &needed(&["name"]),
            &[],
            ObdaSqlDialect::Postgres,
        )
        .unwrap_err();
        assert!(err.contains("invalid SQL identifier"), "{err}");
    }

    /// CONCEPT:EG-KG.query.obda-predicate-pushdown — ACCEPTANCE: SPARQL over a virtual EXTERNAL
    /// table (a mock SQL source) pushes the `FILTER (?age > 28)` down into a real `WHERE`
    /// (asserted via the rendered SQL — "the query plan"), and the SPARQL answer is correct
    /// (the evaluator re-filters over the materialized view, so Bob(25) is excluded even though
    /// the mock returned him).
    #[test]
    fn sparql_over_external_mock_source_pushes_where() {
        let mock = Arc::new(MockSqlExecutor {
            seen: Mutex::new(Vec::new()),
            rows: vec![
                row("1", "Alice", "30"),
                row("2", "Bob", "25"),
                row("3", "Carol", "40"),
            ],
        });
        let src = SqlObdaSource {
            table: "people".into(),
            dialect: ObdaSqlDialect::Postgres,
            executor: mock.clone(),
        };
        let mut reg = eg_rdf::obda::ObdaSourceRegistry::new();
        reg.register("people", Arc::new(src));

        let vg = eg_rdf::obda::VirtualGraph::new().with_map(
            eg_rdf::obda::TriplesMap::new("people", "http://example.org/person/{id}")
                .add_column("http://example.org/name", "name")
                .add_typed_column(
                    "http://example.org/age",
                    "age",
                    "http://www.w3.org/2001/XMLSchema#integer",
                ),
        );
        let proj = eg_rdf::sparql::Projection::raw();
        let outcome = eg_rdf::obda::run_outcome_virtual(
            &vg,
            &reg,
            r#"PREFIX ex: <http://example.org/>
               SELECT ?name WHERE { ?p ex:name ?name ; ex:age ?age . FILTER (?age > 28) }"#,
            &proj,
        )
        .unwrap();

        // The FILTER reached the external DB as a pushed-down WHERE.
        let seen = mock.seen.lock().unwrap();
        assert!(
            seen.iter()
                .any(|s| s.contains("WHERE") && s.contains("\"age\" > 28")),
            "the FILTER must be pushed into the external SQL, saw: {seen:?}"
        );

        // ...and the answer is correct (Bob excluded by the evaluator's re-filter).
        match outcome {
            eg_rdf::sparql::QueryOutcome::Solutions(res) => {
                let mut names: Vec<String> = res
                    .solutions
                    .iter()
                    .filter_map(|s| s.get("name").map(|b| b.as_str().to_string()))
                    .collect();
                names.sort();
                assert_eq!(names, vec!["Alice", "Carol"]);
            }
            other => panic!("expected solutions, got {other:?}"),
        }
    }
}
