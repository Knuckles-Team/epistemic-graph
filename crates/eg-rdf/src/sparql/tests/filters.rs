    // ── CONCEPT:EG-KG.ontology.rich-filter — rich FILTER ────────────────────────────────────────────

    fn filtered_names(view: &GraphView, filter: &str) -> Vec<String> {
        let q = format!(
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name WHERE {{
              ?p a ex:Person . ?p ex:name ?name . ?p ex:age ?age .
              FILTER ({filter})
            }}"#
        );
        let res = select(view, &q).unwrap();
        let mut names: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("name").map(|b| b.as_str().to_string()))
            .collect();
        names.sort();
        names
    }

    /// EG-053: REGEX (with the case-insensitive `i` flag).
    #[test]
    fn filter_regex() {
        let view = loaded_view();
        assert_eq!(
            filtered_names(&view, r#"REGEX(?name, "^a", "i")"#),
            vec!["Alice"]
        );
    }

    /// EG-053: arithmetic inside a comparison (`?age + 5 > 40`).
    #[test]
    fn filter_arithmetic_comparison() {
        let view = loaded_view();
        // ages 30/25/40 → 35/30/45; only carol (45) clears 40.
        assert_eq!(filtered_names(&view, "?age + 5 > 40"), vec!["Carol"]);
    }

    /// EG-053: `IN` membership (numeric-aware via the term compare).
    #[test]
    fn filter_in() {
        let view = loaded_view();
        let mut got = filtered_names(&view, r#"?name IN ("Alice", "Bob")"#);
        got.sort();
        assert_eq!(got, vec!["Alice", "Bob"]);
    }

    /// EG-053: `NOT IN` parses to `Not(In(…))` and excludes the listed members.
    #[test]
    fn filter_not_in() {
        let view = loaded_view();
        assert_eq!(
            filtered_names(&view, r#"?name NOT IN ("Alice", "Bob")"#),
            vec!["Carol"]
        );
    }

    /// EG-053: string built-ins — CONTAINS and UCASE.
    #[test]
    fn filter_string_functions() {
        let view = loaded_view();
        assert_eq!(
            filtered_names(&view, r#"CONTAINS(?name, "li")"#),
            vec!["Alice"]
        );
        assert_eq!(
            filtered_names(&view, r#"UCASE(?name) = "BOB""#),
            vec!["Bob"]
        );
        assert_eq!(filtered_names(&view, "STRLEN(?name) = 3"), vec!["Bob"]);
    }

    /// The boolean built-ins in FILTER position: the string relations, LANGMATCHES,
    /// and the unary term-type tests, plus an unsupported function (which yields no
    /// boolean, so the FILTER drops every row).
    #[test]
    fn filter_boolean_builtins() {
        let view = loaded_view();
        let everyone = vec!["Alice", "Bob", "Carol"];
        assert_eq!(
            filtered_names(&view, r#"STRSTARTS(?name, "Ca")"#),
            vec!["Carol"]
        );
        assert_eq!(
            filtered_names(&view, r#"STRENDS(?name, "ob")"#),
            vec!["Bob"]
        );
        assert_eq!(
            filtered_names(&view, r#"CONTAINS(?name, "o")"#),
            vec!["Bob", "Carol"]
        );
        assert_eq!(
            filtered_names(&view, r#"LANGMATCHES("en-US", "en")"#),
            everyone
        );
        assert_eq!(filtered_names(&view, r#"LANGMATCHES("en", "*")"#), everyone);
        assert!(filtered_names(&view, r#"LANGMATCHES("fr", "en")"#).is_empty());
        assert_eq!(filtered_names(&view, "isIRI(?p)"), everyone);
        assert!(filtered_names(&view, "isIRI(?name)").is_empty());
        assert!(filtered_names(&view, "isBlank(?p)").is_empty());
        assert_eq!(filtered_names(&view, "isLiteral(?name)"), everyone);
        assert!(filtered_names(&view, "isLiteral(?p)").is_empty());
        assert_eq!(filtered_names(&view, "isNumeric(?age)"), everyone);
        assert!(filtered_names(&view, "isNumeric(?name)").is_empty());
        assert_eq!(filtered_names(&view, "isURI(?p)"), everyone);
        assert!(filtered_names(&view, r#"UCASE(?name)"#).is_empty());
    }

    /// `isBlank` accepts a blank-node subject and `isIRI` rejects it.
    #[test]
    fn filter_is_blank_matches_a_blank_node_subject() {
        let view = view_of_turtle(
            r#"
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:alice a ex:Person ; ex:name "Alice" ; ex:age "30"^^xsd:integer .
[] a ex:Person ; ex:name "Anon" ; ex:age "20"^^xsd:integer .
"#,
        );
        assert_eq!(filtered_names(&view, "isBlank(?p)"), vec!["Anon"]);
        assert_eq!(filtered_names(&view, "isIRI(?p)"), vec!["Alice"]);
        assert!(filtered_names(&view, "isLiteral(?p)").is_empty());
    }

    // ── CONCEPT:EG-KG.ontology.minus — MINUS ──────────────────────────────────────────────────

    /// EG-055: MINUS removes every left solution compatible with a right solution.
    /// alice/carol HAVE an ex:knows ⇒ removed; bob does not ⇒ kept.
    #[test]
    fn minus_set_difference() {
        let view = loaded_view();
        let res = select(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name WHERE {
              ?p a ex:Person . ?p ex:name ?name .
              MINUS { ?p ex:knows ?o }
            }"#,
        )
        .unwrap();
        let names: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("name").map(|b| b.as_str().to_string()))
            .collect();
        assert_eq!(names, vec!["Bob"], "got {res:?}");
    }

    // ── CONCEPT:EG-KG.ontology.negated-property-set — negated property set `!p` ──────────────────────────────

    /// EG-056: `!ex:knows` matches every resource edge whose predicate is NOT ex:knows.
    #[test]
    fn negated_property_set() {
        let ttl = r#"
@prefix ex: <http://example.org/> .
ex:alice ex:knows ex:bob ; ex:likes ex:carol .
"#;
        let view = view_of_turtle(ttl);
        let res = select(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?s ?o WHERE { ?s !ex:knows ?o }"#,
        )
        .unwrap();
        let pairs: Vec<(String, String)> = res
            .solutions
            .iter()
            .map(|s| {
                (
                    s.get("s").unwrap().as_str().to_string(),
                    s.get("o").unwrap().as_str().to_string(),
                )
            })
            .collect();
        // only the ex:likes edge (alice→carol) survives; ex:knows is excluded.
        assert_eq!(pairs.len(), 1, "got {pairs:?}");
        assert!(
            pairs[0].0.contains("alice") && pairs[0].1.contains("carol"),
            "got {pairs:?}"
        );
    }

    // ── CONCEPT:EG-KG.ontology.order-by-values-exists — ORDER BY / VALUES / EXISTS ─────────────────────────────

    fn ordered_names(view: &GraphView, clause: &str) -> Vec<String> {
        let q = format!(
            "PREFIX ex: <http://example.org/> \
             SELECT ?name WHERE {{ ?p ex:name ?name ; ex:age ?age }} {clause}"
        );
        select(view, &q)
            .unwrap()
            .solutions
            .iter()
            .map(|s| s.get("name").unwrap().as_str().to_string())
            .collect()
    }

    /// EG-125: ORDER BY on a STRING var, ascending and descending.
    #[test]
    fn order_by_string_asc_desc() {
        let view = loaded_view();
        assert_eq!(
            ordered_names(&view, "ORDER BY ?name"),
            vec!["Alice", "Bob", "Carol"]
        );
        assert_eq!(
            ordered_names(&view, "ORDER BY DESC(?name)"),
            vec!["Carol", "Bob", "Alice"]
        );
    }

    /// EG-125: ORDER BY on a NUMERIC var sorts numerically (not lexically), asc + desc.
    #[test]
    fn order_by_numeric_asc_desc() {
        let view = loaded_view();
        // ages: Alice 30, Bob 25, Carol 40.
        assert_eq!(
            ordered_names(&view, "ORDER BY ?age"),
            vec!["Bob", "Alice", "Carol"]
        );
        assert_eq!(
            ordered_names(&view, "ORDER BY DESC(?age)"),
            vec!["Carol", "Alice", "Bob"]
        );
    }

    /// EG-125: an inline VALUES table joins with the surrounding BGP, restricting the
    /// result to the enumerated resources.
    #[test]
    fn values_join_restricts() {
        let view = loaded_view();
        let res = select(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name WHERE {
              VALUES ?p { ex:alice ex:carol }
              ?p ex:name ?name .
            }"#,
        )
        .unwrap();
        let mut names: Vec<String> = res
            .solutions
            .iter()
            .map(|s| s.get("name").unwrap().as_str().to_string())
            .collect();
        names.sort();
        assert_eq!(names, vec!["Alice", "Carol"], "VALUES restricts to ?p set");
    }

    /// EG-125: FILTER EXISTS keeps only solutions whose sub-pattern has a match; NOT
    /// EXISTS keeps only those WITHOUT. Alice/Carol have ex:knows; Bob does not.
    #[test]
    fn filter_exists_and_not_exists() {
        let view = loaded_view();
        let names = |clause: &str| -> Vec<String> {
            let q = format!(
                "PREFIX ex: <http://example.org/> \
                 SELECT ?name WHERE {{ ?p ex:name ?name . FILTER {clause} }}"
            );
            let mut v: Vec<String> = select(&view, &q)
                .unwrap()
                .solutions
                .iter()
                .map(|s| s.get("name").unwrap().as_str().to_string())
                .collect();
            v.sort();
            v
        };
        assert_eq!(
            names("EXISTS { ?p ex:knows ?o }"),
            vec!["Alice", "Carol"],
            "EXISTS keeps the knowers"
        );
        assert_eq!(
            names("NOT EXISTS { ?p ex:knows ?o }"),
            vec!["Bob"],
            "NOT EXISTS keeps the non-knowers"
        );
    }

    // ── CONCEPT:EG-KG.ontology.completing-eg-order-by — SPARQL algebra completeness (ORDER BY spec / VALUES / MINUS) ──

    /// A fixture with a repeated primary sort key (`ex:dept`) so multi-key ORDER BY and
    /// top-k tie-breaking are exercised.
    fn ranked_view() -> GraphView {
        let ttl = r#"
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:a ex:dept "Eng"   ; ex:name "Zoe" ; ex:rank "2"^^xsd:integer .
ex:b ex:dept "Eng"   ; ex:name "Amy" ; ex:rank "1"^^xsd:integer .
ex:c ex:dept "Sales" ; ex:name "Bob" ; ex:rank "1"^^xsd:integer .
"#;
        view_of_turtle(ttl)
    }

    fn ordered_col(view: &GraphView, q: &str, col: &str) -> Vec<String> {
        select(view, q)
            .unwrap()
            .solutions
            .iter()
            .map(|s| {
                s.get(col)
                    .map(|b| b.as_str().to_string())
                    .unwrap_or_default()
            })
            .collect()
    }

    /// EG-135: the ORDER BY term-type total order — unbound < blank node < IRI < literal —
    /// with typed value comparison WITHIN a kind. This is the correctness gap the EG-125
    /// arm left: bound values used to compare by lexical string regardless of kind.
    #[test]
    fn order_by_term_type_precedence_eg135() {
        use std::cmp::Ordering;
        let unbound: Option<Binding> = None;
        let blank = Some(Binding::Node("_:b1".to_string()));
        let iri = Some(Binding::Node("<http://ex/x>".to_string()));
        let lit = Some(Binding::Literal("Alice".to_string()));
        // Cross-kind precedence (each strictly less than the next kind).
        assert_eq!(cmp_binding(&unbound, &blank), Ordering::Less);
        assert_eq!(cmp_binding(&blank, &iri), Ordering::Less);
        assert_eq!(cmp_binding(&iri, &lit), Ordering::Less);
        assert_eq!(cmp_binding(&unbound, &lit), Ordering::Less);
        assert_eq!(cmp_binding(&lit, &unbound), Ordering::Greater);
        // Within literals: NUMERIC compare (not lexical — "9" must sort before "10").
        let nine = Some(Binding::Literal("9".to_string()));
        let ten = Some(Binding::Literal("10".to_string()));
        assert_eq!(cmp_binding(&nine, &ten), Ordering::Less);
        // Within literals: xsd:dateTime ISO-8601 lexicals sort chronologically.
        let early = Some(Binding::Literal("2020-01-01T00:00:00".to_string()));
        let late = Some(Binding::Literal("2021-06-15T12:00:00".to_string()));
        assert_eq!(cmp_binding(&early, &late), Ordering::Less);
        // Same-kind nodes order by term id, IRIs among themselves.
        let iri_a = Some(Binding::Node("<http://ex/a>".to_string()));
        let iri_b = Some(Binding::Node("<http://ex/b>".to_string()));
        assert_eq!(cmp_binding(&iri_a, &iri_b), Ordering::Less);
    }

    /// EG-135: multi-key ORDER BY (`?dept ASC, DESC(?rank)`) yields the exact top-level
    /// ROW ORDER — Eng before Sales, and within Eng the higher rank first.
    #[test]
    fn order_by_multikey_asc_desc_eg135() {
        let view = ranked_view();
        let q = "PREFIX ex: <http://example.org/> \
                 SELECT ?name WHERE { ?p ex:dept ?dept ; ex:name ?name ; ex:rank ?rank } \
                 ORDER BY ?dept DESC(?rank)";
        // Eng{rank2=Zoe, rank1=Amy} then Sales{Bob}.
        assert_eq!(ordered_col(&view, q, "name"), vec!["Zoe", "Amy", "Bob"]);
    }

    /// EG-135: `ORDER BY … LIMIT k` returns the correct top-k in order (the sort must run
    /// BEFORE the slice). ORDER BY ?rank then ?name, LIMIT 2 ⇒ the two lowest ranks,
    /// name-tie-broken: Amy(1), Bob(1) — Zoe(2) is cut.
    #[test]
    fn order_by_limit_topk_eg135() {
        let view = ranked_view();
        let q = "PREFIX ex: <http://example.org/> \
                 SELECT ?name WHERE { ?p ex:name ?name ; ex:rank ?rank } \
                 ORDER BY ?rank ?name LIMIT 2";
        assert_eq!(ordered_col(&view, q, "name"), vec!["Amy", "Bob"]);
    }

    /// EG-135: a `VALUES (?name ?tier)` table JOINED with a BGP both RESTRICTS (Carol,
    /// absent from the table, is dropped) and EXTENDS (binds the new `?tier` column).
    #[test]
    fn values_join_extends_eg135() {
        let view = loaded_view();
        let res = select(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name ?tier WHERE {
              ?p ex:name ?name .
              VALUES (?name ?tier) { ("Alice" "gold") ("Bob" "silver") }
            }"#,
        )
        .unwrap();
        let mut rows: Vec<(String, String)> = res
            .solutions
            .iter()
            .map(|s| {
                (
                    s.get("name").unwrap().as_str().to_string(),
                    s.get("tier").unwrap().as_str().to_string(),
                )
            })
            .collect();
        rows.sort();
        // Carol has no VALUES row ⇒ dropped; Alice/Bob gain their tier.
        assert_eq!(
            rows,
            vec![
                ("Alice".to_string(), "gold".to_string()),
                ("Bob".to_string(), "silver".to_string())
            ],
            "VALUES join restricts to the table AND binds ?tier"
        );
    }

    /// EG-135: the DEFINITIVE MINUS-vs-NOT-EXISTS distinction — a right pattern sharing NO
    /// variable with the left. TRUE MINUS removes nothing when the domains are disjoint
    /// (so all rows survive), whereas FILTER NOT EXISTS evaluates the pattern's mere
    /// existence and, since `?x ex:knows ?y` HAS matches, removes EVERY row. Proving they
    /// differ confirms MINUS is real set-difference, not a NOT-EXISTS rewrite.
    #[test]
    fn minus_vs_not_exists_distinction_eg135() {
        let view = loaded_view();
        let names = |where_clause: &str| -> Vec<String> {
            let q = format!(
                "PREFIX ex: <http://example.org/> \
                 SELECT ?name WHERE {{ ?p ex:name ?name . {where_clause} }}"
            );
            let mut v: Vec<String> = select(&view, &q)
                .unwrap()
                .solutions
                .iter()
                .map(|s| s.get("name").unwrap().as_str().to_string())
                .collect();
            v.sort();
            v
        };
        // Disjoint-domain MINUS deletes nothing → all three survive.
        assert_eq!(
            names("MINUS { ?x ex:knows ?y }"),
            vec!["Alice", "Bob", "Carol"],
            "disjoint-domain MINUS must remove nothing"
        );
        // NOT EXISTS on the same disjoint pattern → the pattern HAS matches, so it removes
        // everything. This is the behavior MINUS deliberately does NOT share.
        assert_eq!(
            names("FILTER NOT EXISTS { ?x ex:knows ?y }"),
            Vec::<String>::new(),
            "NOT EXISTS on a matching disjoint pattern must remove all rows"
        );
    }

    // ── CONCEPT:EG-KG.ontology.from-from-named — FROM / FROM NAMED ──────────────────────────────────────

    /// EG-054: a `FROM <g>` clause scopes the default graph to that graph, so a plain
    /// (non-GRAPH) BGP only sees `g`'s triples — not the whole registered dataset.
    #[test]
    fn from_scopes_default_graph() {
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
        // A default graph that contains BOTH edges — FROM must narrow away from it.
        let both = merge_views([&va, &vb].into_iter());
        let ds = Dataset::new(
            &both,
            vec![
                ("http://g/a".to_string(), &va),
                ("http://g/b".to_string(), &vb),
            ],
        );
        let subjects = |q: &str| -> Vec<String> {
            let QueryOutcome::Solutions(r) = query_dataset(&ds, q, &Projection::raw()).unwrap()
            else {
                panic!()
            };
            let mut v: Vec<String> = r
                .solutions
                .iter()
                .map(|s| s.get("s").unwrap().as_str().to_string())
                .collect();
            v.sort();
            v
        };
        // No FROM: both edges visible in the default graph.
        assert_eq!(subjects("SELECT ?s WHERE { ?s ?p ?o }").len(), 2);
        // FROM <g/a>: only graph A's subject (ex:a) is visible.
        let from_a = {
            let QueryOutcome::Solutions(r) = query_dataset(
                &ds,
                "SELECT ?s FROM <http://g/a> WHERE { ?s ?p ?o }",
                &Projection::raw(),
            )
            .unwrap() else {
                panic!()
            };
            r.solutions
                .iter()
                .map(|s| s.get("s").unwrap().as_str().to_string())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            from_a.len(),
            1,
            "FROM <g/a> restricts to one edge: {from_a:?}"
        );
        assert!(from_a[0].contains("<http://ex/a>"), "got {from_a:?}");
    }

    // ── CONCEPT:EG-KG.query.sparql-service-federation-client — SPARQL SERVICE federation ──────────────────────────────

    /// A mock [`RemoteSparql`] returning a fixed canned outcome, standing in for the
    /// facade's `ureq` client so the SERVICE algebra + SILENT/join semantics test in
    /// pure `eg-rdf` (no HTTP).
    struct MockService(Result<SparqlResult, String>);
    impl RemoteSparql for MockService {
        fn select(&self, _endpoint: &str, _query: &str) -> Result<SparqlResult, String> {
            self.0.clone()
        }
    }

    /// (a) SERVICE solutions JOIN a local BGP on the shared variable `?name`.
    #[test]
    fn service_joins_local_bgp() {
        let view = loaded_view();
        let ds = Dataset::new(&view, Vec::new());
        let mut row = Solution::new();
        row.insert("name".to_string(), Binding::Literal("Alice".to_string()));
        row.insert("score".to_string(), Binding::Literal("100".to_string()));
        let svc = MockService(Ok(SparqlResult {
            vars: vec!["name".to_string(), "score".to_string()],
            solutions: vec![row],
        }));
        let q = r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name ?score WHERE {
              ?p ex:name ?name .
              SERVICE <http://remote/e> { ?name ex:score ?score }
            }"#;
        let QueryOutcome::Solutions(r) =
            query_dataset_service(&ds, q, &Projection::raw(), Some(&svc)).unwrap()
        else {
            panic!()
        };
        // Only Alice has a remote score → the join keeps exactly one solution.
        assert_eq!(r.solutions.len(), 1, "got {:?}", r.solutions);
        assert_eq!(r.solutions[0].get("name").unwrap().as_str(), "Alice");
        assert_eq!(r.solutions[0].get("score").unwrap().as_str(), "100");
    }

    /// (b) SILENT swallows a remote error to ONE empty solution → the local side passes
    /// through (the three people bound by the BGP survive the join unchanged).
    #[test]
    fn service_silent_swallows_error() {
        let view = loaded_view();
        let ds = Dataset::new(&view, Vec::new());
        let svc = MockService(Err("remote down".to_string()));
        let q = r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name WHERE {
              ?p ex:name ?name .
              SERVICE SILENT <http://remote/e> { ?name ex:score ?score }
            }"#;
        let QueryOutcome::Solutions(r) =
            query_dataset_service(&ds, q, &Projection::raw(), Some(&svc)).unwrap()
        else {
            panic!()
        };
        assert_eq!(
            r.solutions.len(),
            3,
            "SILENT pass-through: {:?}",
            r.solutions
        );
    }

    /// (c) A non-SILENT remote error propagates; a `None` client (fail-closed) errors too.
    #[test]
    fn service_error_propagates_without_silent() {
        let view = loaded_view();
        let ds = Dataset::new(&view, Vec::new());
        let svc = MockService(Err("remote down".to_string()));
        let q = r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name WHERE {
              ?p ex:name ?name .
              SERVICE <http://remote/e> { ?name ex:score ?score }
            }"#;
        assert!(
            query_dataset_service(&ds, q, &Projection::raw(), Some(&svc)).is_err(),
            "non-SILENT SERVICE error must propagate"
        );
        assert!(
            query_dataset_service(&ds, q, &Projection::raw(), None).is_err(),
            "no client bound is fail-closed"
        );
    }

    /// (d) The generated remote query round-trips through the parser, projecting the
    /// inner pattern's in-scope variables.
    #[test]
    fn service_remote_query_round_trips() {
        fn find_service_inner(p: &GraphPattern) -> Option<GraphPattern> {
            match p {
                GraphPattern::Service { inner, .. } => Some((**inner).clone()),
                GraphPattern::Join { left, right } => {
                    find_service_inner(left).or_else(|| find_service_inner(right))
                }
                GraphPattern::Project { inner, .. }
                | GraphPattern::Filter { inner, .. }
                | GraphPattern::Distinct { inner }
                | GraphPattern::Slice { inner, .. } => find_service_inner(inner),
                _ => None,
            }
        }
        let Query::Select { pattern, .. } = parse_query(
            "PREFIX ex: <http://example.org/> SELECT * WHERE { SERVICE <http://r/e> { ?s ex:p ?o } }",
        )
        .unwrap() else {
            panic!()
        };
        let inner = find_service_inner(&pattern).expect("a SERVICE node");
        let remote = service::build_service_query(&inner);
        assert!(
            parse_query(&remote).is_ok(),
            "generated remote query must parse: {remote}"
        );
        assert!(
            remote.contains("?s") && remote.contains("?o"),
            "projects in-scope vars: {remote}"
        );
    }
