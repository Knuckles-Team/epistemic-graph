    // ── EG-127: SPARQL 1.1 builtin-function library completion ──────────────

    /// Evaluate a scalar expression via `BIND(<expr> AS ?x)` over an empty BGP.
    fn scalar(view: &GraphView, expr: &str) -> String {
        let q = format!("SELECT ?x WHERE {{ BIND({expr} AS ?x) }}");
        let res = select(view, &q).unwrap_or_else(|e| panic!("run {expr}: {e}"));
        res.solutions
            .first()
            .and_then(|s| s.get("x"))
            .unwrap_or_else(|| panic!("no ?x for {expr}"))
            .as_str()
            .to_string()
    }

    /// EG-127: hash built-ins against the canonical `"abc"` test vectors.
    #[cfg(feature = "sparql-hash")]
    #[test]
    fn eg127_hash_builtins_known_vectors() {
        let v = loaded_view();
        assert_eq!(
            scalar(&v, r#"MD5("abc")"#),
            "900150983cd24fb0d6963f7d28e17f72"
        );
        assert_eq!(
            scalar(&v, r#"SHA1("abc")"#),
            "a9993e364706816aba3e25717850c26c9cd0d89d"
        );
        assert_eq!(
            scalar(&v, r#"SHA256("abc")"#),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            scalar(&v, r#"SHA384("abc")"#),
            "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed\
8086072ba1e7cc2358baeca134c825a7"
        );
        assert_eq!(
            scalar(&v, r#"SHA512("abc")"#),
            "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a\
2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
        );
    }

    /// EG-127: term constructors — UUID()/STRUUID() and STRDT().
    #[test]
    fn eg127_term_constructors() {
        let v = loaded_view();
        assert_eq!(scalar(&v, "STRLEN(STRUUID())"), "36");
        assert!(
            scalar(&v, "STR(UUID())").starts_with("urn:uuid:"),
            "UUID() is a urn:uuid: IRI"
        );
        // STRDT keeps the lexical value; BNODE(str) yields a labelled blank node.
        assert_eq!(
            scalar(
                &v,
                r#"STRDT("42", <http://www.w3.org/2001/XMLSchema#integer>)"#
            ),
            "42"
        );
        assert_eq!(scalar(&v, r#"STR(BNODE("tag1"))"#), "_:tag1");
    }

    /// EG-127: date-time accessors over an xsd:dateTime lexical + NOW().
    #[test]
    fn eg127_datetime_accessors() {
        let v = loaded_view();
        let dt = r#""2024-03-15T10:30:45+01:00""#;
        assert_eq!(scalar(&v, &format!("YEAR({dt})")), "2024");
        assert_eq!(scalar(&v, &format!("MONTH({dt})")), "3");
        assert_eq!(scalar(&v, &format!("DAY({dt})")), "15");
        assert_eq!(scalar(&v, &format!("HOURS({dt})")), "10");
        assert_eq!(scalar(&v, &format!("MINUTES({dt})")), "30");
        assert_eq!(scalar(&v, &format!("SECONDS({dt})")), "45");
        assert_eq!(scalar(&v, &format!("TZ({dt})")), "+01:00");
        assert_eq!(scalar(&v, &format!("TIMEZONE({dt})")), "PT1H");
        // NOW() is a well-formed xsd:dateTime whose year is in the plausible range.
        let now_year: i64 = scalar(&v, "YEAR(NOW())").parse().unwrap();
        assert!(now_year >= 2024, "NOW() year = {now_year}");
    }

    /// EG-127: REPLACE (regex-backed) + STRBEFORE/STRAFTER.
    #[test]
    fn eg127_string_extras() {
        let v = loaded_view();
        assert_eq!(scalar(&v, r#"REPLACE("abcabc", "b", "X")"#), "aXcaXc");
        assert_eq!(scalar(&v, r#"REPLACE("Foo", "o", "0", "i")"#), "F00");
        assert_eq!(scalar(&v, r#"STRBEFORE("hello@world", "@")"#), "hello");
        assert_eq!(scalar(&v, r#"STRAFTER("hello@world", "@")"#), "world");
        // Separator not found ⇒ empty string.
        assert_eq!(scalar(&v, r#"STRBEFORE("abc", "z")"#), "");
        assert_eq!(scalar(&v, r#"ENCODE_FOR_URI("a b/c")"#), "a%20b%2Fc");
    }

    /// EG-127: numeric built-ins ABS/CEIL/FLOOR/ROUND (half-toward-+inf).
    #[test]
    fn eg127_numeric_builtins() {
        let v = loaded_view();
        assert_eq!(scalar(&v, "ABS(-5)"), "5");
        assert_eq!(scalar(&v, "CEIL(1.2)"), "2");
        assert_eq!(scalar(&v, "FLOOR(1.8)"), "1");
        assert_eq!(scalar(&v, "ROUND(2.5)"), "3");
        assert_eq!(scalar(&v, "ROUND(-2.5)"), "-2");
        // RAND() is a double in [0,1).
        let r: f64 = scalar(&v, "RAND()").parse().unwrap();
        assert!((0.0..1.0).contains(&r), "RAND() = {r}");
    }

    // ── EG-130: SPARQL-star (RDF 1.2) — construct + project a quoted triple ──

    /// EG-130: TRIPLE() constructs a first-class quoted-triple term that binds to `?t`;
    /// isTRIPLE tests it and SUBJECT/PREDICATE/OBJECT project its components.
    #[cfg(feature = "sparql-star")]
    #[test]
    fn eg130_sparql_star_accessors() {
        let v = loaded_view();
        let t = "TRIPLE(<http://example.org/a>, <http://example.org/b>, <http://example.org/c>)";
        // The raw binding is the canonical quoted-triple term (STR() would strip the
        // outer angle brackets, so we read the binding directly here).
        assert!(
            scalar(&v, t).starts_with("<<"),
            "TRIPLE() yields a quoted-triple term"
        );
        assert_eq!(scalar(&v, &format!("isTRIPLE({t})")), "true");
        assert_eq!(scalar(&v, "isTRIPLE(<http://example.org/a>)"), "false");
        assert_eq!(
            scalar(&v, &format!("STR(SUBJECT({t}))")),
            "http://example.org/a"
        );
        assert_eq!(
            scalar(&v, &format!("STR(PREDICATE({t}))")),
            "http://example.org/b"
        );
        assert_eq!(
            scalar(&v, &format!("STR(OBJECT({t}))")),
            "http://example.org/c"
        );
    }

    // ── EG-261: GeoSPARQL baseline over a full SPARQL query ─────────────────────

    /// Load two features, each with the canonical `geo:hasGeometry`/`geo:asWKT` shape.
    #[cfg(feature = "geosparql")]
    fn geo_view() -> GraphView {
        let ttl = r#"
@prefix geo: <http://www.opengis.net/ont/geosparql#> .
@prefix ex:  <http://example.org/> .
ex:cityA geo:hasGeometry ex:gA .
ex:gA    geo:asWKT "POINT(1 1)"^^geo:wktLiteral .
ex:cityB geo:hasGeometry ex:gB .
ex:gB    geo:asWKT "POINT(5 5)"^^geo:wktLiteral .
"#;
        view_of_turtle(ttl)
    }

    /// EG-261: the `?feature geo:hasGeometry ?g . ?g geo:asWKT ?wkt` resolution pattern
    /// composes with a `geof:sfWithin` FILTER over a `geo:wktLiteral` constant — only the
    /// feature whose point lies inside the polygon survives.
    #[cfg(feature = "geosparql")]
    #[test]
    fn eg261_hasgeometry_aswkt_sfwithin_full_query() {
        let view = geo_view();
        let res = select(
            &view,
            r#"
            PREFIX geo:  <http://www.opengis.net/ont/geosparql#>
            PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
            SELECT ?f WHERE {
              ?f geo:hasGeometry ?g .
              ?g geo:asWKT ?wkt .
              FILTER(geof:sfWithin(?wkt, "POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))"^^geo:wktLiteral))
            }"#,
        )
        .unwrap();
        let feats: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("f").map(|b| b.as_str().to_string()))
            .collect();
        assert_eq!(
            feats,
            vec!["<http://example.org/cityA>"],
            "only cityA's POINT(1 1) is within the polygon; got {feats:?}"
        );
    }

    /// Load three polygon features (container + strictly-interior + boundary-tangential),
    /// each with the canonical `geo:asWKT` shape, for the RCC8 query test (CONCEPT:EG-KG.ontology.concept-7).
    #[cfg(feature = "geosparql")]
    fn rcc8_view() -> GraphView {
        let ttl = r#"
@prefix geo: <http://www.opengis.net/ont/geosparql#> .
@prefix ex:  <http://example.org/> .
ex:big   geo:asWKT "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"^^geo:wktLiteral .
ex:inner geo:asWKT "POLYGON((2 2, 4 2, 4 4, 2 4, 2 2))"^^geo:wktLiteral .
ex:edge  geo:asWKT "POLYGON((0 0, 5 0, 5 5, 0 5, 0 0))"^^geo:wktLiteral .
"#;
        view_of_turtle(ttl)
    }

    /// EG-155: a full SPARQL `FILTER(geof:rcc8ntpp(?pw, ?bw))` query end-to-end — the new
    /// RCC8 relation dispatches through the shared `geof:` boolean-function hook, admitting
    /// only the strictly-interior region (NTPP) and rejecting both the boundary-tangential
    /// region (that is TPP) and the container itself.
    #[cfg(feature = "geosparql")]
    #[test]
    fn eg155_sparql_filter_rcc8ntpp_full_query() {
        let view = rcc8_view();
        let res = select(
            &view,
            r#"
            PREFIX geo:  <http://www.opengis.net/ont/geosparql#>
            PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
            SELECT ?part WHERE {
              ?part geo:asWKT ?pw .
              <http://example.org/big> geo:asWKT ?bw .
              FILTER(geof:rcc8ntpp(?pw, ?bw))
            }"#,
        )
        .unwrap();
        let parts: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("part").map(|b| b.as_str().to_string()))
            .collect();
        assert_eq!(
            parts,
            vec!["<http://example.org/inner>"],
            "only the strictly-interior region is a non-tangential proper part; got {parts:?}"
        );
    }

    /// EG-261: `geof:distance` is usable as a projected value expression in a full query
    /// (the 3-4-5 planar triangle → 5).
    #[cfg(feature = "geosparql")]
    #[test]
    fn eg261_distance_value_in_query() {
        let view = geo_view();
        let d = scalar(
            &view,
            r#"<http://www.opengis.net/def/function/geosparql/distance>("POINT(0 0)"^^<http://www.opengis.net/ont/geosparql#wktLiteral>, "POINT(3 4)"^^<http://www.opengis.net/ont/geosparql#wktLiteral>, "")"#,
        );
        assert_eq!(d, "5", "planar distance of a 3-4-5 triangle; got {d}");
    }
