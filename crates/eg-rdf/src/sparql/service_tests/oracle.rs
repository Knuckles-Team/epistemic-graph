//! FILTER pushdown against an INDEPENDENT endpoint. The endpoint here does not use this
//! crate's evaluator: it holds real RDF terms — IRIs, blank nodes, language-tagged and
//! datatyped literals — and evaluates a pushed filter with RDF term semantics (term
//! identity for `sameTerm`, value equality with type errors for `=`, three-valued `&&`
//! and `||`). Its answers then cross the same lossy boundary the HTTP client applies: a
//! literal comes back as its lexical form only.
//!
//! Each case runs one query twice — against the endpoint as it is, and against the same
//! endpoint refusing every filter, which forces the full-fetch path — and requires the
//! same rows. A filter whose pushed evaluation would lose a row therefore fails here.

use std::collections::HashMap;
use std::sync::Mutex;

use oxrdf::vocab::xsd;
use oxrdf::{BlankNode, Literal, NamedNode, Term};
use spargebra::algebra::{Expression, Function, GraphPattern};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use spargebra::Query;

use super::super::{
    execute, parse_query, view_of_turtle, Binding, Dataset, Projection, RemoteSparql, Solution,
    SparqlResult,
};

/// One solution of the endpoint: variables bound to real terms.
type Row = HashMap<String, Term>;

// ── term-level filter semantics (`None` is a SPARQL error) ──────────────────────────

fn value(expr: &Expression, row: &Row) -> Option<Term> {
    match expr {
        Expression::Variable(variable) => row.get(variable.as_str()).cloned(),
        Expression::NamedNode(node) => Some(Term::NamedNode(node.clone())),
        Expression::Literal(literal) => Some(Term::Literal(literal.clone())),
        _ => None,
    }
}

/// The value of a literal of a numeric datatype.
fn numeric(term: &Term) -> Option<f64> {
    let Term::Literal(literal) = term else {
        return None;
    };
    [xsd::INTEGER, xsd::DECIMAL, xsd::DOUBLE, xsd::FLOAT]
        .contains(&literal.datatype())
        .then(|| literal.value().parse().ok())
        .flatten()
}

fn is_plain_string(term: &Term) -> bool {
    matches!(term, Term::Literal(literal) if literal.datatype() == xsd::STRING)
}

/// RDFterm-equal with the numeric value comparison: identical terms are equal, two
/// numbers compare by value, two different plain strings are unequal, and any other
/// pair of different literals is a type error. A literal never equals an IRI.
fn rdf_equal(a: &Term, b: &Term) -> Option<bool> {
    if a == b {
        return Some(true);
    }
    if let (Some(x), Some(y)) = (numeric(a), numeric(b)) {
        return Some(x == y);
    }
    let both_literals = a.is_literal() && b.is_literal();
    let comparable = is_plain_string(a) && is_plain_string(b);
    (!both_literals || comparable).then_some(false)
}

fn and(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    }
}

fn or(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    and(a.map(|v| !v), b.map(|v| !v)).map(|v| !v)
}

/// `IN`: true when one candidate is equal, an error when none is and one comparison
/// raised an error.
fn among(term: &Term, list: &[Expression], row: &Row) -> Option<bool> {
    list.iter()
        .map(|candidate| rdf_equal(term, &value(candidate, row)?))
        .fold(Some(false), or)
}

fn kind_test(function: &Function, term: &Term) -> Option<bool> {
    match function {
        Function::IsIri => Some(term.is_named_node()),
        Function::IsBlank => Some(term.is_blank_node()),
        Function::IsLiteral => Some(term.is_literal()),
        _ => None,
    }
}

fn relation(expr: &Expression, row: &Row) -> Option<bool> {
    match expr {
        Expression::SameTerm(a, b) => Some(value(a, row)? == value(b, row)?),
        Expression::Equal(a, b) => rdf_equal(&value(a, row)?, &value(b, row)?),
        Expression::Greater(a, b) => Some(numeric(&value(a, row)?)? > numeric(&value(b, row)?)?),
        Expression::In(a, list) => among(&value(a, row)?, list, row),
        _ => None,
    }
}

fn truth(expr: &Expression, row: &Row) -> Option<bool> {
    match expr {
        Expression::Bound(variable) => Some(row.contains_key(variable.as_str())),
        Expression::And(a, b) => and(truth(a, row), truth(b, row)),
        Expression::Or(a, b) => or(truth(a, row), truth(b, row)),
        Expression::Not(a) => truth(a, row).map(|v| !v),
        Expression::FunctionCall(function, arguments) => {
            kind_test(function, &value(arguments.first()?, row)?)
        }
        other => relation(other, row),
    }
}

// ── the endpoint ────────────────────────────────────────────────────────────────────

struct Triple {
    subject: NamedNode,
    predicate: NamedNode,
    object: Term,
}

fn unify(pattern: &TermPattern, term: &Term, row: &mut Row) -> bool {
    match pattern {
        TermPattern::Variable(variable) => {
            let bound = row
                .entry(variable.as_str().to_string())
                .or_insert_with(|| term.clone());
            *bound == *term
        }
        TermPattern::NamedNode(node) => *term == Term::NamedNode(node.clone()),
        TermPattern::Literal(literal) => *term == Term::Literal(literal.clone()),
        _ => false,
    }
}

fn matched(pattern: &TriplePattern, triple: &Triple, mut row: Row) -> Option<Row> {
    let predicate = match &pattern.predicate {
        NamedNodePattern::NamedNode(node) => *node == triple.predicate,
        NamedNodePattern::Variable(_) => false,
    };
    let subject = Term::NamedNode(triple.subject.clone());
    (predicate
        && unify(&pattern.subject, &subject, &mut row)
        && unify(&pattern.object, &triple.object, &mut row))
    .then_some(row)
}

/// What the HTTP client keeps of a term: its kind, and for a literal its lexical form —
/// no language tag, no datatype.
fn erased(term: &Term) -> Option<Binding> {
    match term {
        Term::NamedNode(node) => Some(Binding::Node(format!("<{}>", node.as_str()))),
        Term::BlankNode(node) => Some(Binding::Node(format!("_:{}", node.as_str()))),
        Term::Literal(literal) => Some(Binding::Literal(literal.value().to_string())),
        #[cfg(feature = "sparql-star")]
        Term::Triple(_) => None,
    }
}

struct TermEndpoint {
    triples: Vec<Triple>,
    /// Refuse a query carrying a filter: the caller falls back to the full fetch.
    refuse_filters: bool,
    sent: Mutex<Vec<String>>,
}

impl TermEndpoint {
    fn bgp(&self, patterns: &[TriplePattern]) -> Vec<Row> {
        patterns.iter().fold(vec![Row::new()], |rows, pattern| {
            rows.iter()
                .flat_map(|row| {
                    self.triples
                        .iter()
                        .filter_map(|triple| matched(pattern, triple, row.clone()))
                })
                .collect()
        })
    }

    fn rows(&self, pattern: &GraphPattern) -> Result<Vec<Row>, String> {
        match pattern {
            GraphPattern::Project { inner, .. } => self.rows(inner),
            GraphPattern::Bgp { patterns } => Ok(self.bgp(patterns)),
            GraphPattern::Filter { expr, inner } if !self.refuse_filters => {
                let mut rows = self.rows(inner)?;
                rows.retain(|row| truth(expr, row) == Some(true));
                Ok(rows)
            }
            _ => Err("this endpoint does not evaluate that pattern".into()),
        }
    }
}

impl RemoteSparql for TermEndpoint {
    fn select(&self, _endpoint: &str, query: &str) -> Result<SparqlResult, String> {
        self.sent.lock().unwrap().push(query.to_string());
        let Query::Select { pattern, .. } = parse_query(query)? else {
            return Err("a SELECT query".into());
        };
        let erase = |row: Row| -> Solution {
            row.iter()
                .filter_map(|(name, term)| Some((name.clone(), erased(term)?)))
                .collect()
        };
        Ok(SparqlResult {
            vars: Vec::new(),
            solutions: self.rows(&pattern)?.into_iter().map(erase).collect(),
        })
    }
}

// ── the fixture: every kind of term an object can be ────────────────────────────────

fn named(iri: &str) -> NamedNode {
    NamedNode::new(iri).expect("a valid IRI")
}

/// `<urn:s:<name>> <urn:label> <object>` for each entry.
fn labelled(objects: Vec<(&str, Term)>) -> Vec<Triple> {
    objects
        .into_iter()
        .map(|(name, object)| Triple {
            subject: named(&format!("urn:s:{name}")),
            predicate: named("urn:label"),
            object,
        })
        .collect()
}

fn terms() -> Vec<Triple> {
    let typed =
        |lexical: &str, datatype| -> Term { Literal::new_typed_literal(lexical, datatype).into() };
    let tagged = |lexical: &str, tag: &str| -> Term {
        Literal::new_language_tagged_literal(lexical, tag)
            .expect("a valid language tag")
            .into()
    };
    labelled(vec![
        ("hello-en", tagged("hello", "en")),
        ("hello-fr", tagged("hello", "fr")),
        ("hello", Literal::new_simple_literal("hello").into()),
        ("one-string", Literal::new_simple_literal("1").into()),
        ("one-integer", typed("1", xsd::INTEGER)),
        ("padded-integer", typed("01", xsd::INTEGER)),
        ("one-decimal", typed("1.0", xsd::DECIMAL)),
        ("two-integer", typed("2", xsd::INTEGER)),
        ("iri", named("urn:x").into()),
        (
            "iri-spelled-out",
            Literal::new_simple_literal("urn:x").into(),
        ),
        (
            "blank",
            BlankNode::new("b1").expect("a blank node id").into(),
        ),
    ])
}

/// The subjects `filter` keeps, and whether the filter reached the endpoint.
fn subjects(filter: &str, refuse_filters: bool) -> (Vec<String>, bool) {
    let endpoint = TermEndpoint {
        triples: terms(),
        refuse_filters,
        sent: Mutex::new(Vec::new()),
    };
    let local = view_of_turtle("<urn:local> <urn:p> <urn:o> .");
    let query = format!(
        "SELECT ?s WHERE {{ SERVICE <http://remote/e> {{ ?s <urn:label> ?o }} FILTER ({filter}) }}"
    );
    let dataset = Dataset::new(&local, Vec::new());
    let table = execute(&dataset, &query, &Projection::raw(), Some(&endpoint))
        .expect("the query runs")
        .into_table();
    let mut kept: Vec<String> = table
        .solutions
        .iter()
        .filter_map(|row| row.get("s"))
        .map(|subject| subject.as_str().replace("urn:s:", ""))
        .collect();
    kept.sort();
    let sent = endpoint.sent.lock().unwrap();
    let answered = sent.iter().any(|query| query.contains("FILTER"));
    (kept, answered && !refuse_filters)
}

/// `filter` returns the same subjects whether or not the endpoint evaluates filters, and
/// those subjects are `expected`. Returns whether the filter was pushed.
fn same_on_both_paths(filter: &str, expected: &[&str]) -> bool {
    let (fetched, _) = subjects(filter, true);
    let (optimized, pushed) = subjects(filter, false);
    let mut expected: Vec<String> = expected.iter().map(|name| format!("<{name}>")).collect();
    expected.sort();
    assert_eq!(fetched, expected, "{filter}: the full-fetch rows");
    assert_eq!(
        optimized, fetched,
        "{filter}: the optimized path lost or gained a row (pushed: {pushed})"
    );
    pushed
}

// ── the cases ───────────────────────────────────────────────────────────────────────

#[test]
fn the_endpoint_distinguishes_what_the_local_evaluator_cannot() {
    let endpoint = TermEndpoint {
        triples: terms(),
        refuse_filters: false,
        sent: Mutex::new(Vec::new()),
    };
    let kept = |filter: &str| -> usize {
        let query = format!("SELECT ?s WHERE {{ ?s <urn:label> ?o FILTER ({filter}) }}");
        endpoint.select("e", &query).unwrap().solutions.len()
    };
    assert_eq!(
        kept("sameTerm(?o, \"hello\"@fr)"),
        1,
        "only the @fr literal"
    );
    assert_eq!(
        kept("?o = 1"),
        3,
        "the three numbers; the string is an error"
    );
    assert_eq!(
        kept("?o = <urn:x>"),
        1,
        "the IRI, not the literal spelling it"
    );
    assert_eq!(kept("!bound(?nothing)"), 11);
}

#[test]
fn a_language_tag_comparison_keeps_the_rows_the_local_path_keeps() {
    let pushed = same_on_both_paths(
        "sameTerm(?o, \"hello\"@fr)",
        &["hello", "hello-en", "hello-fr"],
    );
    assert!(!pushed, "the tag is erased locally: the filter stays local");
}

#[test]
fn a_datatype_comparison_keeps_the_rows_the_local_path_keeps() {
    let numerals = ["one-decimal", "one-integer", "one-string", "padded-integer"];
    assert!(!same_on_both_paths("?o = 1", &numerals));
    assert!(!same_on_both_paths("?o IN (1, 7)", &numerals));
    assert!(!same_on_both_paths(
        "sameTerm(?o, \"1\")",
        &["one-integer", "one-string"]
    ));
    assert!(!same_on_both_paths("?o > 1", &["two-integer"]));
}

#[test]
fn a_bare_iri_comparison_keeps_the_literal_that_spells_the_iri() {
    let pushed = same_on_both_paths("?o = <urn:x>", &["iri", "iri-spelled-out"]);
    assert!(!pushed, "the brackets are stripped locally: it stays local");
    assert!(!same_on_both_paths(
        "?o IN (<urn:x>, <urn:y>)",
        &["iri", "iri-spelled-out"]
    ));
}

#[test]
fn a_negation_keeps_the_rows_the_local_path_keeps() {
    assert!(!same_on_both_paths("!isLiteral(?o)", &["blank", "iri"]));
    assert!(!same_on_both_paths(
        "?o != <urn:x>",
        &[
            "blank",
            "hello",
            "hello-en",
            "hello-fr",
            "one-decimal",
            "one-integer",
            "one-string",
            "padded-integer",
            "two-integer",
        ]
    ));
}

#[test]
fn the_sound_forms_are_pushed_and_return_the_same_rows() {
    for (filter, expected) in [
        ("isIRI(?o) && ?o = <urn:x>", vec!["iri"]),
        ("<urn:x> = ?o && isIRI(?o)", vec!["iri"]),
        ("isIRI(?o) && sameTerm(?o, <urn:x>)", vec!["iri"]),
        ("isIRI(?o) && ?o IN (<urn:y>, <urn:x>)", vec!["iri"]),
        ("isIRI(?o) && ?o = <urn:y>", vec![]),
        ("isIRI(?o)", vec!["iri"]),
        ("isBlank(?o)", vec!["blank"]),
        ("isBlank(?o) || isIRI(?o)", vec!["blank", "iri"]),
        ("isIRI(?s) && isIRI(?o) && bound(?o)", vec!["iri"]),
        (
            "(isIRI(?o) && ?o = <urn:x>) || isBlank(?o)",
            vec!["blank", "iri"],
        ),
    ] {
        assert!(
            same_on_both_paths(filter, &expected),
            "{filter} is sound and must still be pushed"
        );
    }
    let literals = same_on_both_paths(
        "isLiteral(?o) && bound(?s)",
        &[
            "hello",
            "hello-en",
            "hello-fr",
            "iri-spelled-out",
            "one-decimal",
            "one-integer",
            "one-string",
            "padded-integer",
            "two-integer",
        ],
    );
    assert!(literals);
}
