
    use super::*;
    use crate::mapping::{load_triples, parse_turtle, IriStore};

    fn select(view: &GraphView, query: &str) -> Result<SparqlResult, String> {
        select_projected(view, query, &Projection::raw())
    }

    fn select_projected(
        view: &GraphView,
        query: &str,
        projection: &Projection,
    ) -> Result<SparqlResult, String> {
        Ok(query_graph(view, query, projection)?.into_table())
    }

    fn query_graph(
        view: &GraphView,
        query: &str,
        projection: &Projection,
    ) -> Result<QueryOutcome, String> {
        execute(&Dataset::new(view, Vec::new()), query, projection, None)
    }

    fn query_dataset(
        dataset: &Dataset,
        query: &str,
        projection: &Projection,
    ) -> Result<QueryOutcome, String> {
        execute(dataset, query, projection, None)
    }

    fn query_dataset_service(
        dataset: &Dataset,
        query: &str,
        projection: &Projection,
        service: Option<&dyn RemoteSparql>,
    ) -> Result<QueryOutcome, String> {
        execute(dataset, query, projection, service)
    }

    /// Three people, ages and `knows` edges: the SPARQL fixture the proof
    /// tests (`sparql::proof::tests`) share.
    pub(super) fn loaded_view() -> GraphView {
        let ttl = r#"
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:alice a ex:Person ; ex:name "Alice" ; ex:age "30"^^xsd:integer ; ex:knows ex:bob .
ex:bob   a ex:Person ; ex:name "Bob"   ; ex:age "25"^^xsd:integer .
ex:carol a ex:Person ; ex:name "Carol" ; ex:age "40"^^xsd:integer ; ex:knows ex:alice .
"#;
        view_of_turtle(ttl)
    }

    /// A snapshot of graph `g` loaded from a Turtle document.
    /// W2: a ≥2-pattern BGP + a FILTER returns the right solutions.
    #[test]
    fn bgp_two_patterns_plus_filter() {
        let view = loaded_view();
        let res = select(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name WHERE {
              ?p a ex:Person .
              ?p ex:name ?name .
              ?p ex:age ?age .
              ?p ex:knows ?other .
              FILTER (?age > 28)
            }"#,
        )
        .unwrap();
        assert!(res.vars.contains(&"name".to_string()));
        let mut names: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("name").map(|b| b.as_str().to_string()))
            .collect();
        names.sort();
        // alice (30, knows bob) and carol (40, knows alice) qualify; bob (25) is
        // filtered out AND has no ex:knows anyway.
        assert_eq!(names, vec!["Alice", "Carol"], "got {names:?}");
    }

    /// W2: OPTIONAL left-join returns an unbound variable for the no-match case.
    #[test]
    fn optional_left_join() {
        let view = loaded_view();
        let res = select(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name ?other WHERE {
              ?p a ex:Person .
              ?p ex:name ?name .
              OPTIONAL { ?p ex:knows ?other }
            }"#,
        )
        .unwrap();
        let mut rows: Vec<(String, Option<String>)> = res
            .solutions
            .iter()
            .map(|s| {
                (
                    s.get("name").unwrap().as_str().to_string(),
                    s.get("other").map(|b| b.as_str().to_string()),
                )
            })
            .collect();
        rows.sort();
        assert_eq!(rows.len(), 3, "got {rows:?}");
        let bob = rows.iter().find(|(n, _)| n == "Bob").unwrap();
        assert_eq!(bob.1, None, "bob has no OPTIONAL knows-target");
        let alice = rows.iter().find(|(n, _)| n == "Alice").unwrap();
        assert!(alice.1.is_some(), "alice DOES know someone");
    }

    /// W2: UNION merges two graph patterns' solutions.
    #[test]
    fn union_merges_branches() {
        let view = loaded_view();
        let res = select(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name WHERE {
              { ?p ex:name "Alice" . ?p ex:name ?name . }
              UNION
              { ?p ex:name "Bob" . ?p ex:name ?name . }
            }"#,
        )
        .unwrap();
        let mut names: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("name").map(|b| b.as_str().to_string()))
            .collect();
        names.sort();
        names.dedup();
        assert_eq!(names, vec!["Alice", "Bob"], "got {names:?}");
    }

    /// W2: a fixed-length sequence property path `ex:knows/ex:name`.
    #[test]
    fn sequence_property_path() {
        let view = loaded_view();
        // carol knows alice; alice's name is "Alice".
        let res = select(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?n WHERE { ex:carol ex:knows/ex:name ?n . }"#,
        )
        .unwrap();
        let names: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("n").map(|b| b.as_str().to_string()))
            .collect();
        assert_eq!(names, vec!["Alice"], "got {names:?}");
    }

    /// `to_rows` projects unbound OPTIONAL variables as None (wire shape).
    #[test]
    fn to_rows_projects_unbound_as_none() {
        let view = loaded_view();
        let res = select(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name ?other WHERE {
              ?p ex:name ?name .
              OPTIONAL { ?p ex:knows ?other }
            }"#,
        )
        .unwrap();
        let (cols, rows) = res.to_rows();
        assert!(cols.contains(&"name".to_string()) && cols.contains(&"other".to_string()));
        let other_idx = cols.iter().position(|c| c == "other").unwrap();
        assert!(
            rows.iter().any(|r| r[other_idx].is_none()),
            "bob's ?other must be None"
        );
    }

    // ── CONCEPT:EG-KG.query.sparql-completeness — SPARQL completeness ──────────────────────────────

    /// Pull the single aggregate cell from a 1-row, 1-projected-var result.
    fn agg_cell(res: &SparqlResult, var: &str) -> String {
        assert_eq!(res.solutions.len(), 1, "expected ONE group, got {res:?}");
        res.solutions[0].get(var).unwrap().as_str().to_string()
    }

    /// COUNT(*) over a BGP — the whole result is one group.
    #[test]
    fn aggregate_count_all() {
        let view = loaded_view();
        let res = select(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT (COUNT(*) AS ?n) WHERE { ?p a ex:Person . }"#,
        )
        .unwrap();
        // alice, bob, carol.
        assert_eq!(agg_cell(&res, "n"), "3");
    }

    /// SUM(?age) over the three people = 30+25+40 = 95.
    #[test]
    fn aggregate_sum() {
        let view = loaded_view();
        let res = select(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT (SUM(?age) AS ?total) WHERE { ?p ex:age ?age . }"#,
        )
        .unwrap();
        assert_eq!(agg_cell(&res, "total"), "95");
    }

    /// GROUP BY a constant property + COUNT — every Person shares the same rdf:type,
    /// so a GROUP BY ?type yields one group of 3.
    #[test]
    fn aggregate_group_by_count() {
        let view = loaded_view();
        let res = select(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT ?type (COUNT(?p) AS ?n) WHERE { ?p rdf:type ?type . }
            GROUP BY ?type"#,
        )
        .unwrap();
        // one group (ex:Person), count 3.
        assert_eq!(res.solutions.len(), 1, "one group");
        assert_eq!(
            res.solutions[0].get("n").unwrap().as_str(),
            "3",
            "got {res:?}"
        );
    }

    /// `p+` transitive closure: carol knows alice, alice knows bob ⇒
    /// `ex:carol ex:knows+ ?who` reaches BOTH alice and bob.
    #[test]
    fn property_path_one_or_more() {
        let view = loaded_view();
        let res = select(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?who WHERE { ex:carol ex:knows+ ?who . }"#,
        )
        .unwrap();
        let mut who: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("who").map(|b| b.as_str().to_string()))
            .collect();
        who.sort();
        assert_eq!(
            who,
            vec![
                "<http://example.org/alice>".to_string(),
                "<http://example.org/bob>".to_string()
            ],
            "got {who:?}"
        );
    }

    /// `^p` inverse path: `ex:alice ^ex:knows ?who` ⇒ whoever knows alice = carol.
    #[test]
    fn property_path_inverse() {
        let view = loaded_view();
        let res = select(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?who WHERE { ex:alice ^ex:knows ?who . }"#,
        )
        .unwrap();
        let who: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("who").map(|b| b.as_str().to_string()))
            .collect();
        assert_eq!(
            who,
            vec!["<http://example.org/carol>".to_string()],
            "got {who:?}"
        );
    }

    /// `a|b` alternative path — match either of two predicates (here `knows`
    /// alternated with itself, so the result equals the plain `knows` pairs).
    #[test]
    fn property_path_alternative() {
        let view = loaded_view();
        let res = select(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?who WHERE { ex:carol (ex:knows|ex:knows) ?who . }"#,
        )
        .unwrap();
        let who: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("who").map(|b| b.as_str().to_string()))
            .collect();
        assert_eq!(
            who,
            vec!["<http://example.org/alice>".to_string()],
            "got {who:?}"
        );
    }

    /// A default graph is not implicitly exposed as a named graph.
    #[test]
    fn graph_named_form_requires_explicit_named_member() {
        let view = loaded_view();
        let res = select(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?g ?name WHERE { GRAPH ?g { ex:alice ex:name ?name . } }"#,
        )
        .unwrap();
        assert!(res.solutions.is_empty(), "got {res:?}");
    }

    // ── CONCEPT:EG-KG.ontology.lpg-rdf-projection-vocabulary — LPG→RDF projection vocabulary ────────────────────────

    /// The AU namespace + CamelCase projection — exactly what agent-utilities passes.
    const AU_NS: &str = "http://agent-utilities.dev/ontology#";
    fn au_proj() -> Projection {
        Projection::from_wire(AU_NS, "camel")
    }

    /// A NATIVE property-graph view (the `add_node`/`add_edge` shape AU writes, NOT the
    /// `AddTriples` RDF shape): node `type`/`name` are bare scalars, the edge is typed
    /// `knows`. `alice` is an `agent`, `bob` a `world_model` (a multi-word type, to
    /// exercise CamelCase). `alice knows bob`.
    fn native_view() -> GraphView {
        let core = eg_core::graph::GraphCore::new();
        let mut txn = core.txn();
        txn.add_node(
            "alice".into(),
            rmp_serde::to_vec_named(&serde_json::json!({"type":"agent","name":"Alice"})).unwrap(),
        );
        txn.add_node(
            "bob".into(),
            rmp_serde::to_vec_named(&serde_json::json!({"type":"world_model","name":"Bob"}))
                .unwrap(),
        );
        txn.add_edge(
            "alice".into(),
            "bob".into(),
            rmp_serde::to_vec_named(&serde_json::json!({"relationship":"knows"})).unwrap(),
        )
        .unwrap();
        drop(txn);
        core.analysis_snapshot()
    }

    /// `?s rdf:type au:Agent` resolves natively over the LPG — the original failure.
    /// `agent`→`au:Agent` (CamelCase), so only alice matches; `world_model`→
    /// `au:WorldModel`, so bob matches that class instead.
    #[test]
    fn projection_rdf_type_by_class() {
        let view = native_view();
        let proj = au_proj();
        let res = select_projected(
            &view,
            "PREFIX au: <http://agent-utilities.dev/ontology#>\
             SELECT ?s WHERE { ?s a au:Agent }",
            &proj,
        )
        .unwrap();
        let subs: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("s").map(|b| b.as_str().to_string()))
            .collect();
        assert_eq!(
            subs,
            vec![format!("<{AU_NS}alice>")],
            "only the agent-typed node is an au:Agent; got {subs:?}"
        );

        // The multi-word type CamelCases: bob is an au:WorldModel.
        let res2 = select_projected(
            &view,
            "PREFIX au: <http://agent-utilities.dev/ontology#>\
             SELECT ?s WHERE { ?s a au:WorldModel }",
            &proj,
        )
        .unwrap();
        let subs2: Vec<String> = res2
            .solutions
            .iter()
            .filter_map(|s| s.get("s").map(|b| b.as_str().to_string()))
            .collect();
        assert_eq!(subs2, vec![format!("<{AU_NS}bob>")], "got {subs2:?}");
    }

    /// A by-property literal query projects the property key under `au:` and matches the
    /// native scalar value.
    #[test]
    fn projection_by_property_literal() {
        let view = native_view();
        let res = select_projected(
            &view,
            "PREFIX au: <http://agent-utilities.dev/ontology#>\
             SELECT ?s WHERE { ?s au:name \"Alice\" }",
            &au_proj(),
        )
        .unwrap();
        let subs: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("s").map(|b| b.as_str().to_string()))
            .collect();
        assert_eq!(subs, vec![format!("<{AU_NS}alice>")], "got {subs:?}");
    }

    /// The typed edge projects to `au:knows` between `au:`-namespaced node IRIs.
    #[test]
    fn projection_edge() {
        let view = native_view();
        let res = select_projected(
            &view,
            "PREFIX au: <http://agent-utilities.dev/ontology#>\
             SELECT ?o WHERE { au:alice au:knows ?o }",
            &au_proj(),
        )
        .unwrap();
        let objs: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("o").map(|b| b.as_str().to_string()))
            .collect();
        assert_eq!(objs, vec![format!("<{AU_NS}bob>")], "got {objs:?}");
    }

    /// The full `?s ?p ?o` projection emits EXACTLY the AU-vocabulary triples that AU's
    /// rdflib `_build_rdf_graph` materializes from the same LPG: a CamelCased `rdf:type`
    /// per node under `au:`, each scalar property under `au:`, and the typed edge under
    /// `au:` between `au:` node IRIs. This is the engine==rdflib vocabulary contract.
    #[test]
    fn projection_full_triple_set_matches_au_convention() {
        let view = native_view();
        let res =
            select_projected(&view, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }", &au_proj()).unwrap();
        let triples: std::collections::HashSet<(String, String, String)> = res
            .solutions
            .iter()
            .map(|sol| {
                (
                    sol.get("s").unwrap().as_str().to_string(),
                    sol.get("p").unwrap().as_str().to_string(),
                    sol.get("o").unwrap().as_str().to_string(),
                )
            })
            .collect();
        let rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type".to_string();
        let expected: std::collections::HashSet<(String, String, String)> = [
            (
                format!("<{AU_NS}alice>"),
                rdf_type.clone(),
                format!("<{AU_NS}Agent>"),
            ),
            (
                format!("<{AU_NS}bob>"),
                rdf_type,
                format!("<{AU_NS}WorldModel>"),
            ),
            (
                format!("<{AU_NS}alice>"),
                format!("{AU_NS}name"),
                "Alice".into(),
            ),
            (
                format!("<{AU_NS}bob>"),
                format!("{AU_NS}name"),
                "Bob".into(),
            ),
            (
                format!("<{AU_NS}alice>"),
                format!("{AU_NS}knows"),
                format!("<{AU_NS}bob>"),
            ),
        ]
        .into_iter()
        .collect();
        assert_eq!(
            triples, expected,
            "projected triple set must match AU's vocabulary"
        );
    }

    /// The IDENTITY projection (the default) does NOT synthesize `rdf:type` from the
    /// node `type` field (it stays edge-sourced) and emits keys verbatim — so existing
    /// callers are byte-for-byte unchanged. Here a native LPG under the raw projection
    /// yields the bare-id `knows` edge + bare scalar literals, and NO `rdf:type`.
    #[test]
    fn identity_projection_unchanged() {
        let view = native_view();
        let res = select(&view, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }").unwrap();
        let preds: std::collections::HashSet<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("p").map(|b| b.as_str().to_string()))
            .collect();
        assert!(
            preds.contains("knows"),
            "raw edge predicate is bare; got {preds:?}"
        );
        assert!(
            preds.contains("name"),
            "raw scalar literal predicate is bare"
        );
        assert!(
            !preds.iter().any(|p| p.contains("rdf-syntax-ns#type")),
            "identity projection synthesizes NO rdf:type; got {preds:?}"
        );
    }

    // ── CONCEPT:EG-KG.query.named-graph-support — ASK / CONSTRUCT / DESCRIBE / named graphs ──────────────

    /// ASK returns true when the pattern matches, false when it does not.
    #[test]
    fn ask_true_and_false() {
        let view = loaded_view();
        let t = query_graph(
            &view,
            "PREFIX ex: <http://example.org/> ASK { ex:alice ex:name \"Alice\" }",
            &Projection::raw(),
        )
        .unwrap();
        assert!(matches!(t, QueryOutcome::Boolean(true)), "got {t:?}");
        let f = query_graph(
            &view,
            "PREFIX ex: <http://example.org/> ASK { ex:alice ex:name \"Zelda\" }",
            &Projection::raw(),
        )
        .unwrap();
        assert!(matches!(f, QueryOutcome::Boolean(false)), "got {f:?}");
    }

    /// CONSTRUCT instantiates its template — re-predicate `ex:knows` as `ex:friend`.
    #[test]
    fn construct_returns_expected_triples() {
        let view = loaded_view();
        let out = query_graph(
            &view,
            r#"PREFIX ex: <http://example.org/>
               CONSTRUCT { ?a ex:friend ?b } WHERE { ?a ex:knows ?b }"#,
            &Projection::raw(),
        )
        .unwrap();
        let QueryOutcome::Graph(triples) = out else {
            panic!("expected a graph")
        };
        // carol knows alice; alice knows bob ⇒ two friend triples.
        let mut got: Vec<String> = triples
            .iter()
            .map(|t| {
                format!(
                    "{} {}",
                    t.subject,
                    match &t.object {
                        oxrdf::Term::NamedNode(n) => n.as_str().to_string(),
                        other => other.to_string(),
                    }
                )
            })
            .collect();
        got.sort();
        assert!(triples
            .iter()
            .all(|t| t.predicate.as_str() == "http://example.org/friend"));
        assert_eq!(
            got,
            vec![
                "<http://example.org/alice> http://example.org/bob".to_string(),
                "<http://example.org/carol> http://example.org/alice".to_string(),
            ],
            "got {got:?}"
        );
    }

    /// DESCRIBE returns the triples about the resource (subject- and object-position).
    #[test]
    fn describe_returns_resource_triples() {
        let view = loaded_view();
        let out = query_graph(
            &view,
            "PREFIX ex: <http://example.org/> DESCRIBE ex:alice",
            &Projection::raw(),
        )
        .unwrap();
        let QueryOutcome::Graph(triples) = out else {
            panic!("expected a graph")
        };
        let alice = "<http://example.org/alice>";
        // Every described triple must mention alice in subject or object position.
        assert!(!triples.is_empty(), "alice has a description");
        assert!(
            triples
                .iter()
                .all(|t| t.subject.to_string() == alice || t.object.to_string() == alice),
            "every DESCRIBE triple touches alice; got {triples:?}"
        );
        // Her own properties (name) are in subject position …
        assert!(
            triples.iter().any(|t| t.subject.to_string() == alice
                && t.predicate.as_str() == "http://example.org/name"),
            "alice's name is described"
        );
        // … and carol-knows-alice is in object position (CBD object side).
        assert!(
            triples.iter().any(|t| t.object.to_string() == alice
                && t.predicate.as_str() == "http://example.org/knows"),
            "the inbound knows edge is described"
        );
    }

    /// Query-side named-graph isolation: a triple in graph A is not visible when the
    /// `GRAPH <B>` form scopes to graph B.
    #[test]
    fn named_graph_query_isolation() {
        let core_a = eg_core::graph::GraphCore::new();
        let mut iris = IriStore::default();
        load_triples(
            &core_a,
            &mut iris,
            "a",
            parse_turtle("@prefix ex: <http://ex/> . ex:a ex:p ex:b .").unwrap(),
        )
        .unwrap();
        let core_b = eg_core::graph::GraphCore::new();
        load_triples(
            &core_b,
            &mut iris,
            "b",
            parse_turtle("@prefix ex: <http://ex/> . ex:c ex:p ex:d .").unwrap(),
        )
        .unwrap();
        let va = core_a.analysis_snapshot();
        let vb = core_b.analysis_snapshot();
        let default = GraphView::default();
        let ds = Dataset::new(
            &default,
            vec![
                ("http://g/a".to_string(), &va),
                ("http://g/b".to_string(), &vb),
            ],
        );
        // ex:a is in graph A only — scoping to B yields nothing.
        let in_b = query_dataset(
            &ds,
            "SELECT ?o WHERE { GRAPH <http://g/b> { <http://ex/a> <http://ex/p> ?o } }",
            &Projection::raw(),
        )
        .unwrap();
        let QueryOutcome::Solutions(rb) = in_b else {
            panic!()
        };
        assert!(rb.solutions.is_empty(), "ex:a not visible in graph B");
        // Scoping to A finds it.
        let in_a = query_dataset(
            &ds,
            "SELECT ?o WHERE { GRAPH <http://g/a> { <http://ex/a> <http://ex/p> ?o } }",
            &Projection::raw(),
        )
        .unwrap();
        let QueryOutcome::Solutions(ra) = in_a else {
            panic!()
        };
        assert_eq!(ra.solutions.len(), 1, "ex:a visible in graph A");
    }

    // ── CONCEPT:EG-KG.ontology.sub-select — sub-SELECT ─────────────────────────────────────────────

    /// EG-051: a sub-SELECT that BINDS more vars than it projects must restrict each
    /// inner solution to the projected set, so an inner-only var can't leak and corrupt
    /// the outer join. Here the inner binds `?friend`/`?name` (the FRIEND's name) but
    /// projects only `?p`; the outer re-binds `?name` to the PERSON's own name. If the
    /// inner `?name` leaked, every join would mismatch and the result would be EMPTY.
    #[test]
    fn sub_select_restricts_to_projected_vars() {
        let view = loaded_view();
        let res = select(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name WHERE {
              { SELECT ?p WHERE { ?p ex:knows ?friend . ?friend ex:name ?name } }
              ?p ex:name ?name .
            }"#,
        )
        .unwrap();
        let mut names: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("name").map(|b| b.as_str().to_string()))
            .collect();
        names.sort();
        // alice (knows bob) and carol (knows alice) have an ex:knows; the join keys on
        // the projected ?p only, so each binds its OWN name.
        assert_eq!(names, vec!["Alice", "Carol"], "got {res:?}");
    }

    /// EG-051: a sub-SELECT computing `COUNT(*) AS ?n` projects + surfaces the scalar.
    #[test]
    fn sub_select_count_star() {
        let view = loaded_view();
        let res = select(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?n WHERE {
              { SELECT (COUNT(*) AS ?n) WHERE { ?p a ex:Person } }
            }"#,
        )
        .unwrap();
        assert_eq!(res.solutions.len(), 1, "one aggregate row");
        assert_eq!(
            res.solutions[0].get("n").unwrap().as_str(),
            "3",
            "got {res:?}"
        );
    }

    /// EG-051 regression: a plain top-level SELECT is byte-for-byte unchanged.
    #[test]
    fn sub_select_top_level_unchanged() {
        let view = loaded_view();
        let res = select(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name WHERE { ?p ex:name ?name }"#,
        )
        .unwrap();
        let mut names: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("name").map(|b| b.as_str().to_string()))
            .collect();
        names.sort();
        assert_eq!(names, vec!["Alice", "Bob", "Carol"], "got {res:?}");
    }
