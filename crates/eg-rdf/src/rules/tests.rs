use super::*;
use crate::mapping::parse_turtle;

fn c(s: &str) -> String {
    format!("<http://ex/{s}>")
}

/// A custom rule `parent(x,y) ∧ parent(y,z) → grandparent(x,z)` fires and the
/// inferred fact carries propagated confidence (rule_conf × body-fact confs).
#[test]
fn custom_grandparent_rule_with_confidence() {
    let ttl = r#"
@prefix ex: <http://ex/> .
ex:alice ex:parent ex:bob .
ex:bob   ex:parent ex:carol .
"#;
    let triples = parse_turtle(ttl).unwrap();
    let ont = crate::owl::parse_ontology(&triples);
    let mut rs = RuleSet::new();
    let name = rs
        .add_str("gp: parent(?x,?y) ^ parent(?y,?z) -> grandparent(?x,?z) @0.9")
        .unwrap();
    assert_eq!(name, "gp");
    assert_eq!(rs.names(), vec!["gp".to_string()]);

    let res = reason_triples(&triples, &ont, &rs);
    // grandparent(alice, carol) inferred. The rule predicate is bare `grandparent`;
    // the parent facts come from the graph as canonical IRIs, so the head IRIs are
    // the bound individuals.
    let gp = res
        .facts
        .iter()
        .find(|(p, a, _)| p == "grandparent" && a == &vec![c("alice"), c("carol")]);
    assert!(
        gp.is_some(),
        "grandparent(alice,carol) must be derived; facts={:?}",
        res.facts
    );
    // Confidence = rule 0.9 × parent(1.0) × parent(1.0) = 0.9.
    let conf = gp.unwrap().2;
    assert!(
        (conf - 0.9).abs() < 1e-9,
        "grandparent conf 0.9, got {conf}"
    );

    // Removing the rule retracts it from the set.
    assert!(rs.remove("gp"));
    assert!(rs.is_empty());
}

/// Confidence PRODUCT: a low-confidence parent fact drags the grandparent down.
#[test]
fn custom_rule_confidence_product() {
    // bob->carol asserted at 0.5 via eg:confidence on the edge subject is not how
    // facts carry conf here (facts default 1.0); instead exercise rule_conf product
    // across a two-hop chain where the rule itself is 0.5.
    let ttl = r#"
@prefix ex: <http://ex/> .
ex:alice ex:parent ex:bob .
ex:bob   ex:parent ex:carol .
"#;
    let triples = parse_turtle(ttl).unwrap();
    let ont = crate::owl::parse_ontology(&triples);
    let mut rs = RuleSet::new();
    rs.add_str("parent(?x,?y) ^ parent(?y,?z) -> grandparent(?x,?z) @0.5")
        .unwrap();
    let res = reason_triples(&triples, &ont, &rs);
    let conf = res
        .fact_confidence("grandparent", &[&c("alice"), &c("carol")])
        .unwrap();
    assert!((conf - 0.5).abs() < 1e-9, "got {conf}");
}

/// FunctionalProperty merges: `hasFather` functional + two values ⇒ they are
/// `owl:sameAs`, and facts of the merged individuals unify (congruence).
#[test]
fn functional_property_merges_individuals() {
    let ttl = r#"
@prefix ex:  <http://ex/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
ex:hasFather a owl:FunctionalProperty .
ex:bob ex:hasFather ex:john .
ex:bob ex:hasFather ex:johnny .
ex:john ex:livesIn ex:paris .
"#;
    let triples = parse_turtle(ttl).unwrap();
    let ont = crate::owl::parse_ontology(&triples);
    let res = reason_triples(&triples, &ont, &RuleSet::new());
    // john and johnny are merged (lexicographic root = john).
    assert!(
        res.same_as.iter().any(|(a, b)| {
            (a == &c("john") && b == &c("johnny")) || (a == &c("johnny") && b == &c("john"))
        }),
        "functional hasFather must derive john owl:sameAs johnny; same_as={:?}",
        res.same_as
    );
    // Congruence: livesIn(john, paris) is now livesIn(<root>, paris) and both
    // names resolve — the representative carries the edge.
    let root = if c("john") <= c("johnny") {
        c("john")
    } else {
        c("johnny")
    };
    assert!(
        res.holds("<http://ex/livesIn>", &[&root, &c("paris")]),
        "merged individual keeps the livesIn edge; facts={:?}",
        res.facts
    );
    assert!(res.consistent);
}

/// InverseFunctionalProperty (a key): same SSN ⇒ same person.
#[test]
fn inverse_functional_property_is_a_key() {
    let ttl = r#"
@prefix ex:  <http://ex/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
ex:ssn a owl:InverseFunctionalProperty .
ex:alice ex:ssn ex:s123 .
ex:alicia ex:ssn ex:s123 .
"#;
    let triples = parse_turtle(ttl).unwrap();
    let ont = crate::owl::parse_ontology(&triples);
    let res = reason_triples(&triples, &ont, &RuleSet::new());
    assert!(
        res.same_as
            .iter()
            .any(|(a, b)| (a, b) == (&c("alice"), &c("alicia"))
                || (a, b) == (&c("alicia"), &c("alice"))),
        "same SSN ⇒ alice owl:sameAs alicia; {:?}",
        res.same_as
    );
}

/// `owl:sameAs` over an `owl:differentFrom` pair is an instance-level clash.
#[test]
fn sameas_over_differentfrom_is_inconsistent() {
    let ttl = r#"
@prefix ex:  <http://ex/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
ex:hasFather a owl:FunctionalProperty .
ex:bob ex:hasFather ex:john .
ex:bob ex:hasFather ex:johnny .
ex:john owl:differentFrom ex:johnny .
"#;
    let triples = parse_turtle(ttl).unwrap();
    let ont = crate::owl::parse_ontology(&triples);
    let res = reason_triples(&triples, &ont, &RuleSet::new());
    assert!(
        !res.consistent,
        "functional merge of a differentFrom pair must be inconsistent"
    );
    assert!(!res.conflicts.is_empty());
}

/// Built-in OWL-RL: subPropertyOf + domain feed a custom rule in the same fixpoint.
#[test]
fn builtin_rl_chains_into_custom_rule() {
    let ttl = r#"
@prefix ex:   <http://ex/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
ex:hasDad rdfs:subPropertyOf ex:parent .
ex:alice ex:hasDad ex:bob .
ex:bob   ex:parent ex:carol .
"#;
    let triples = parse_turtle(ttl).unwrap();
    let ont = crate::owl::parse_ontology(&triples);
    let mut rs = RuleSet::new();
    rs.add_str("parent(?x,?y) ^ parent(?y,?z) -> grandparent(?x,?z)")
        .unwrap();
    let res = reason_triples(&triples, &ont, &rs);
    // hasDad ⊑ parent lifts alice->bob into a parent edge; then the custom rule fires.
    assert!(
        res.holds("grandparent", &[&c("alice"), &c("carol")]),
        "subPropertyOf must feed the custom grandparent rule; facts={:?}",
        res.facts
    );
}

/// The Datalog `:-` form parses equivalently to the `->` form.
#[test]
fn datalog_arrow_form_parses() {
    let r = Rule::parse("grandparent(?x,?z) :- parent(?x,?y), parent(?y,?z)").unwrap();
    assert_eq!(r.head.len(), 1);
    assert_eq!(r.head[0].pred, "grandparent");
    assert_eq!(r.body.len(), 2);
}

/// An unsafe rule (head var not bound by the body) is rejected.
#[test]
fn unsafe_rule_rejected() {
    assert!(Rule::parse("foo(?x) -> bar(?x, ?y)").is_err());
}

/// SWRL built-in atoms parse as body atoms WITHOUT disturbing the existing DSL
/// (CONCEPT:EG-KG.ontology.concept-3): a plain bare-predicate Datalog rule parses unchanged, and a
/// `swrlb:` built-in atom keeps its predicate intact (and a bare numeric arg becomes
/// a constant, not a variable).
#[test]
fn swrl_builtin_atom_parses_and_plain_dsl_unaffected() {
    let plain = Rule::parse("grandparent(?x,?z) :- parent(?x,?y), parent(?y,?z)").unwrap();
    assert_eq!(plain.body.len(), 2);
    assert_eq!(plain.head[0].pred, "grandparent");

    let r = Rule::parse("Senior(?p) :- age(?p,?a), swrlb:greaterThan(?a, 65)").unwrap();
    assert_eq!(r.body.len(), 2);
    assert_eq!(r.body[1].pred, "swrlb:greaterThan");
    // The bare `65` is a constant; `?a` and `?p` are variables.
    assert_eq!(r.body[1].args[1], RTerm::Const("65".into()));
    assert_eq!(r.body[1].args[0], RTerm::Var("a".into()));
}

/// A SWRL comparison built-in filters bindings (CONCEPT:EG-KG.ontology.concept-3): only individuals
/// whose age ≥ 18 are classified `Adult`.
#[test]
fn swrl_comparison_builtin_filters() {
    let ttl = r#"
@prefix ex: <http://ex/> .
ex:alice ex:age 30 .
ex:bob   ex:age 12 .
"#;
    let triples = parse_turtle(ttl).unwrap();
    let ont = crate::owl::parse_ontology(&triples);
    let mut rs = RuleSet::new();
    rs.add_str("Adult(?p) :- age(?p, ?a), swrlb:greaterThanOrEqual(?a, 18)")
        .unwrap();
    let res = reason_triples(&triples, &ont, &rs);
    assert!(
        res.holds("Adult", &[&c("alice")]),
        "alice (30) ≥ 18 ⇒ Adult; facts={:?}",
        res.facts
    );
    assert!(
        !res.holds("Adult", &[&c("bob")]),
        "bob (12) < 18 ⇒ filtered out; facts={:?}",
        res.facts
    );
}

/// A SWRL math built-in binds its result variable (CONCEPT:EG-KG.ontology.concept-3): `?n = ?a + 1`.
#[test]
fn swrl_math_builtin_binds_result() {
    let ttl = r#"
@prefix ex: <http://ex/> .
ex:alice ex:age 30 .
"#;
    let triples = parse_turtle(ttl).unwrap();
    let ont = crate::owl::parse_ontology(&triples);
    let mut rs = RuleSet::new();
    rs.add_str("age(?p, ?a) ^ swrlb:add(?n, ?a, 1) -> nextAge(?p, ?n)")
        .unwrap();
    let res = reason_triples(&triples, &ont, &rs);
    assert!(
        res.holds("nextAge", &[&c("alice"), "31"]),
        "30 + 1 = 31 binds ?n ⇒ nextAge(alice, 31); facts={:?}",
        res.facts
    );
}

/// A SWRL string built-in binds its result variable (CONCEPT:EG-KG.ontology.concept-3): upper-casing a
/// data value, and a `stringConcat` producer.
#[test]
fn swrl_string_builtin_binds_result() {
    let ttl = r#"
@prefix ex: <http://ex/> .
ex:alice ex:firstName "alice" .
"#;
    let triples = parse_turtle(ttl).unwrap();
    let ont = crate::owl::parse_ontology(&triples);
    let mut rs = RuleSet::new();
    rs.add_str(r#"firstName(?p, ?n) ^ swrlb:upperCase(?u, ?n) -> upperName(?p, ?u)"#)
        .unwrap();
    rs.add_str(r#"firstName(?p, ?n) ^ swrlb:stringConcat(?g, "Ms ", ?n) -> greeting(?p, ?g)"#)
        .unwrap();
    let res = reason_triples(&triples, &ont, &rs);
    assert!(
        res.holds("upperName", &[&c("alice"), "ALICE"]),
        "upperCase(alice) = ALICE; facts={:?}",
        res.facts
    );
    assert!(
        res.holds("greeting", &[&c("alice"), "Ms alice"]),
        "stringConcat(\"Ms \", alice) = \"Ms alice\"; facts={:?}",
        res.facts
    );
}

/// The server-op request path round-trips Turtle + rules into a filtered response.
#[test]
fn run_rule_reasoning_request_path() {
    let req = RuleReasonRequest {
        ontology_ttl: r#"
@prefix ex: <http://ex/> .
ex:alice ex:parent ex:bob .
ex:bob   ex:parent ex:carol .
"#
        .into(),
        rules: vec!["parent(?x,?y) ^ parent(?y,?z) -> grandparent(?x,?z) @0.8".into()],
        query_predicate: Some("grandparent".into()),
        min_confidence: 0.0,
        derived_only: true,
    };
    let resp = run_rule_reasoning(&req).unwrap();
    assert_eq!(resp.registered_rules.len(), 1);
    assert!(resp.consistent);
    assert_eq!(
        resp.facts.len(),
        1,
        "only grandparent facts: {:?}",
        resp.facts
    );
    let f = &resp.facts[0];
    assert_eq!(f.predicate, "grandparent");
    assert!(f.derived);
    assert!((f.confidence - 0.8).abs() < 1e-9);
}
