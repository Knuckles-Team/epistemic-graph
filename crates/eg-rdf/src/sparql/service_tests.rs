//! `SERVICE` delegation proofs. The endpoint here answers from its OWN graph — a different
//! dataset than the local one — through the same evaluator, records every query it was
//! sent, and can refuse a query by keyword or by position. So each test compares the rows
//! a query returns with what the endpoint was actually asked.

use std::sync::Mutex;
use std::time::Duration;

use eg_core::graph::GraphView;
use spargebra::algebra::{Expression, GraphPattern};
use spargebra::Query;

use super::service::{filter_is_pushable, service_values_rows};

mod oracle;
use super::{
    execute, parse_query, view_of_turtle, Binding, Dataset, Projection, RemoteSparql,
    ServiceBudget, Solution, SparqlResult, SERVICE_BUDGET_EXCEEDED,
};

const PREFIX: &str = "PREFIX ex: <http://example.org/>\n";

/// A remote endpoint over its own graph.
struct Endpoint {
    view: GraphView,
    queries: Mutex<Vec<String>>,
    /// A query containing any of these words is refused.
    refuse_words: &'static [&'static str],
    /// The request at this position (0-based) is refused.
    refuse_at: Option<usize>,
    /// Sleep this long before answering.
    delay: Duration,
    budget: ServiceBudget,
}

impl Endpoint {
    fn over(turtle: &str) -> Self {
        Self {
            view: view_of_turtle(turtle),
            queries: Mutex::new(Vec::new()),
            refuse_words: &[],
            refuse_at: None,
            delay: Duration::ZERO,
            budget: ServiceBudget::default(),
        }
    }

    fn refusing(mut self, words: &'static [&'static str]) -> Self {
        self.refuse_words = words;
        self
    }

    fn within(mut self, budget: ServiceBudget) -> Self {
        self.budget = budget;
        self
    }

    fn queries(&self) -> Vec<String> {
        self.queries.lock().unwrap().clone()
    }
}

impl RemoteSparql for Endpoint {
    fn select(&self, _endpoint: &str, query: &str) -> Result<SparqlResult, String> {
        let position = {
            let mut queries = self.queries.lock().unwrap();
            queries.push(query.to_string());
            queries.len() - 1
        };
        std::thread::sleep(self.delay);
        if self.refuse_at == Some(position) {
            return Err("request refused".into());
        }
        if let Some(word) = self.refuse_words.iter().find(|w| query.contains(**w)) {
            return Err(format!("{word} is not supported"));
        }
        let dataset = Dataset::new(&self.view, Vec::new());
        execute(&dataset, query, &Projection::raw(), None).map(super::QueryOutcome::into_table)
    }

    fn service_budget(&self) -> ServiceBudget {
        self.budget
    }
}

/// Run `query` (the `ex:` prefix is supplied) over the local graph `local` with `endpoint`
/// bound as the `SERVICE` client.
fn run(local: &GraphView, endpoint: &Endpoint, query: &str) -> Result<Vec<Solution>, String> {
    let dataset = Dataset::new(local, Vec::new());
    let text = format!("{PREFIX}{query}");
    execute(&dataset, &text, &Projection::raw(), Some(endpoint)).map(|o| o.into_table().solutions)
}

/// The sorted bindings of `var`; an unbound row contributes `-`.
fn column(rows: &[Solution], var: &str) -> Vec<String> {
    let mut out: Vec<String> = rows
        .iter()
        .map(|row| row.get(var).map_or("-", Binding::as_str).to_string())
        .collect();
    out.sort();
    out
}

fn iri(local_name: &str) -> String {
    format!("<http://example.org/{local_name}>")
}

// ── FILTER pushdown eligibility ────────────────────────────────────────────────────

/// Locally only `ex:a` carries the flag; the endpoint's dataset has no flag at all.
const FLAGGED_LOCALLY: &str = r#"
@prefix ex: <http://example.org/> .
ex:a <urn:localFlag> "yes" .
ex:b ex:name "B" .
"#;

const REMOTE_SCORES: &str = r#"
@prefix ex: <http://example.org/> .
ex:a ex:score 1 .
ex:b ex:score 2 .
"#;

#[test]
fn a_filter_reading_the_local_dataset_is_never_sent_to_the_endpoint() {
    let local = view_of_turtle(FLAGGED_LOCALLY);
    let endpoint = Endpoint::over(REMOTE_SCORES);
    let rows = run(
        &local,
        &endpoint,
        "SELECT ?s ?score WHERE {
            SERVICE <http://remote/e> { ?s ex:score ?score }
            FILTER EXISTS { ?s <urn:localFlag> ?x }
        }",
    )
    .unwrap();
    assert_eq!(
        column(&rows, "s"),
        vec![iri("a")],
        "the flag exists only locally: the row it selects must survive"
    );
    assert_eq!(column(&rows, "score"), vec!["1"]);
    let sent = endpoint.queries();
    assert_eq!(sent.len(), 1);
    assert!(!sent[0].contains("EXISTS"), "{}", sent[0]);
    assert!(!sent[0].contains("FILTER"), "{}", sent[0]);
}

#[test]
fn a_negated_local_exists_filter_is_never_sent_to_the_endpoint() {
    let local = view_of_turtle(FLAGGED_LOCALLY);
    let endpoint = Endpoint::over(REMOTE_SCORES);
    let rows = run(
        &local,
        &endpoint,
        "SELECT ?s WHERE {
            SERVICE <http://remote/e> { ?s ex:score ?score }
            FILTER NOT EXISTS { ?s <urn:localFlag> ?x }
        }",
    )
    .unwrap();
    assert_eq!(column(&rows, "s"), vec![iri("b")]);
    let sent = endpoint.queries();
    assert_eq!(sent.len(), 1);
    assert!(!sent[0].contains("EXISTS"), "{}", sent[0]);
}

#[test]
fn a_filter_over_the_service_rows_is_sent_and_reapplied_locally() {
    let local = view_of_turtle(FLAGGED_LOCALLY);
    let endpoint = Endpoint::over(REMOTE_SCORES);
    let rows = run(
        &local,
        &endpoint,
        "SELECT ?s ?score WHERE {
            SERVICE <http://remote/e> { ?s ex:score ?score }
            FILTER (isIRI(?s) && ?s = ex:b)
        }",
    )
    .unwrap();
    assert_eq!(column(&rows, "s"), vec![iri("b")]);
    let sent = endpoint.queries();
    assert_eq!(sent.len(), 1, "the endpoint filtered: one narrowed request");
    assert!(sent[0].contains("FILTER"), "{}", sent[0]);
}

/// The filter directly over the `SERVICE` clause of `query`, and that clause's pattern.
fn service_filter(query: &str) -> (Expression, GraphPattern) {
    fn find(pattern: &GraphPattern) -> Option<(Expression, GraphPattern)> {
        match pattern {
            GraphPattern::Filter { expr, inner } => match inner.as_ref() {
                GraphPattern::Service { inner, .. } => Some((expr.clone(), (**inner).clone())),
                other => find(other),
            },
            GraphPattern::Project { inner, .. } => find(inner),
            _ => None,
        }
    }
    let Query::Select { pattern, .. } = parse_query(&format!("{PREFIX}{query}")).unwrap() else {
        panic!("a SELECT query")
    };
    find(&pattern).expect("a FILTER directly over a SERVICE clause")
}

fn pushable(filter: &str) -> bool {
    let (expr, remote) = service_filter(&format!(
        "SELECT * WHERE {{ SERVICE <http://remote/e> {{ ?s ex:score ?score }} FILTER ({filter}) }}"
    ));
    filter_is_pushable(&expr, &remote)
}

/// Every filter of `filters` is pushable, or every one is not; `why` names the rule.
fn assert_pushable(filters: &[&str], expected: bool, why: &str) {
    for filter in filters {
        assert_eq!(pushable(filter), expected, "{filter}: {why}");
    }
}

#[test]
fn only_filters_the_endpoint_can_decide_from_the_service_rows_are_pushable() {
    let sound = [
        "bound(?score)",
        "isIRI(?s)",
        "isBlank(?s) || isIRI(?s)",
        "isLiteral(?score) && bound(?s)",
        "isIRI(?s) && ?s = ex:a",
        "ex:a = ?s && isIRI(?s)",
        "isIRI(?s) && sameTerm(?s, ex:a)",
        "isIRI(?s) && ?s IN (ex:a, ex:b) && isLiteral(?score)",
        "(isIRI(?s) && ?s = ex:a) || isBlank(?s)",
    ];
    assert_pushable(&sound, true, "true at the endpoint whenever true locally");
    let not_decidable_from_the_rows = [
        "EXISTS { ?s <urn:localFlag> ?x }",
        "NOT EXISTS { ?s <urn:localFlag> ?x }",
        "isIRI(?s) && EXISTS { ?s <urn:localFlag> ?x }",
        "coalesce(EXISTS { ?s ex:name ?n }, false)",
        "bound(?elsewhere)",
        "isIRI(?elsewhere)",
        "rand() < 0.5",
        "str(now()) > \"2000\"",
        "isIRI(iri(str(?score)))",
        "<http://example.org/extension>(?score)",
        "datatype(?score) = <http://www.w3.org/2001/XMLSchema#integer>",
    ];
    assert_pushable(
        &not_decidable_from_the_rows,
        false,
        "reads a dataset, is not deterministic, or names a variable the pattern lacks",
    );
    let compares_what_was_erased = [
        "?score > 1",
        "?score = 1",
        "?score != 1",
        "?score IN (1, 2)",
        "sameTerm(?score, \"1\"@en)",
        "?score + 1 = 2",
        "?s = ex:a",
        "sameTerm(?s, ex:a)",
        "?s IN (ex:a, ex:b)",
        "isIRI(?score) && ?s = ex:a",
        "isIRI(?s) || ?s = ex:a",
        "isIRI(?s) && ?s = \"a\"",
        "isIRI(?s) && ?s IN (ex:a, \"a\")",
        "isIRI(?s) && ?s = ?score",
        "isLiteral(?s) && ?s = ex:a",
        "strstarts(str(?s), \"http://example.org/\")",
        "contains(?score, \"1\")",
        "ucase(?score) = \"1\"",
        "strlen(?score) = 1",
        "isIRI(str(?s))",
        "isNumeric(?score)",
    ];
    assert_pushable(
        &compares_what_was_erased,
        false,
        "a language tag, a datatype, or the IRI/literal distinction is gone locally",
    );
    let negates = [
        "!bound(?score)",
        "!isIRI(?s)",
        "!(isIRI(?s) && ?s = ex:a)",
        "isIRI(?s) && ?s != ex:a",
        "isIRI(?s) && ?s NOT IN (ex:a)",
        "if(bound(?score), isIRI(?s), isBlank(?s))",
    ];
    assert_pushable(
        &negates,
        false,
        "an unbound or dropped binding makes the two sides disagree under negation",
    );
}

// ── SILENT and the fallback to the clause's own query ──────────────────────────────

#[test]
fn a_refused_filter_pushdown_falls_back_to_the_original_query_under_silent_too() {
    let local = view_of_turtle(FLAGGED_LOCALLY);
    for clause in ["SERVICE", "SERVICE SILENT"] {
        let endpoint = Endpoint::over(REMOTE_SCORES).refusing(&["FILTER"]);
        let rows = run(
            &local,
            &endpoint,
            &format!(
                "SELECT ?s ?score WHERE {{
                    {clause} <http://remote/e> {{ ?s ex:score ?score }}
                    FILTER (isIRI(?s) && ?s = ex:b)
                }}"
            ),
        )
        .unwrap();
        assert_eq!(
            column(&rows, "s"),
            vec![iri("b")],
            "{clause}: the original query's rows, filtered locally"
        );
        let sent = endpoint.queries();
        assert_eq!(sent.len(), 2, "{clause}: narrowed, then original");
        assert!(sent[0].contains("FILTER") && !sent[1].contains("FILTER"));
    }
}

#[test]
fn a_refused_limit_pushdown_falls_back_to_the_original_query_under_silent_too() {
    let local = view_of_turtle(FLAGGED_LOCALLY);
    for clause in ["SERVICE", "SERVICE SILENT"] {
        let endpoint = Endpoint::over(REMOTE_SCORES).refusing(&["LIMIT"]);
        let rows = run(
            &local,
            &endpoint,
            &format!(
                "SELECT ?s WHERE {{ {clause} <http://remote/e> {{ ?s ex:score ?score }} }} LIMIT 1"
            ),
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_ne!(
            column(&rows, "s"),
            vec!["-"],
            "{clause}: a row of the original query, not the SILENT empty solution"
        );
        let sent = endpoint.queries();
        assert_eq!(sent.len(), 2, "{clause}: narrowed, then original");
        assert!(sent[0].contains("LIMIT 1") && !sent[1].contains("LIMIT"));
    }
}

#[test]
fn silent_hushes_only_the_failure_of_the_original_query() {
    let local = view_of_turtle(FLAGGED_LOCALLY);
    let query = |clause: &str, filter: &str| {
        format!(
            "SELECT ?s WHERE {{
                {clause} <http://remote/e> {{ ?s ex:score ?score }}
                FILTER ({filter})
            }}"
        )
    };
    // A filter that is sent: narrowed, then original, and only then the SILENT empty
    // solution — which this filter rejects locally.
    let down = Endpoint::over(REMOTE_SCORES).refusing(&["SELECT"]);
    let rows = run(&local, &down, &query("SERVICE SILENT", "isIRI(?s)")).unwrap();
    assert!(rows.is_empty(), "{rows:?}");
    assert_eq!(down.queries().len(), 2, "narrowed, then original");

    // A filter that stays local: the original query alone, and its SILENT empty
    // solution kept by the local filter.
    let down = Endpoint::over(REMOTE_SCORES).refusing(&["SELECT"]);
    let rows = run(&local, &down, &query("SERVICE SILENT", "!bound(?score)")).unwrap();
    assert_eq!(column(&rows, "s"), vec!["-"], "the SILENT empty solution");
    assert_eq!(down.queries().len(), 1);

    let down = Endpoint::over(REMOTE_SCORES).refusing(&["SELECT"]);
    let error = run(&local, &down, &query("SERVICE", "isIRI(?s)")).unwrap_err();
    assert!(error.contains("SERVICE failed"), "{error}");
    assert_eq!(down.queries().len(), 2, "narrowed, then original");
}

#[test]
fn a_pushed_limit_reaches_the_endpoint() {
    let local = view_of_turtle(FLAGGED_LOCALLY);
    let endpoint = Endpoint::over(REMOTE_SCORES);
    let rows = run(
        &local,
        &endpoint,
        "SELECT ?score WHERE { SERVICE <http://remote/e> { ?s ex:score ?score } } LIMIT 1",
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    let sent = endpoint.queries();
    assert_eq!(sent.len(), 1);
    assert!(sent[0].contains("LIMIT 1"), "{}", sent[0]);
}

// ── the bind join and its budget ───────────────────────────────────────────────────

const JOIN: &str = "SELECT ?p ?score WHERE {
    ?p ex:name ?name .
    SERVICE <http://remote/e> { ?p ex:score ?score }
}";

/// `n` subjects `ex:s<i>`, each with one `predicate` value.
fn subjects(n: usize, predicate: &str) -> String {
    let mut turtle = String::from("@prefix ex: <http://example.org/> .\n");
    for i in 0..n {
        turtle.push_str(&format!("ex:s{i} ex:{predicate} \"{i}\" .\n"));
    }
    turtle
}

fn budget() -> ServiceBudget {
    ServiceBudget::default()
}

#[test]
fn a_bind_join_ships_the_distinct_local_keys_and_matches_the_full_fetch() {
    let local = view_of_turtle(&subjects(3, "name"));
    let shipped = Endpoint::over(&subjects(5, "score"));
    let fetched = Endpoint::over(&subjects(5, "score")).refusing(&["VALUES"]);
    let bound = run(&local, &shipped, JOIN).unwrap();
    let naive = run(&local, &fetched, JOIN).unwrap();
    assert_eq!(column(&bound, "p"), vec![iri("s0"), iri("s1"), iri("s2")]);
    assert_eq!(column(&bound, "p"), column(&naive, "p"));
    assert_eq!(column(&bound, "score"), column(&naive, "score"));

    let sent = shipped.queries();
    assert_eq!(sent.len(), 1, "one batch carried every key");
    assert!(sent[0].contains("VALUES"));
    for name in ["s0", "s1", "s2"] {
        assert!(sent[0].contains(&iri(name)), "{}", sent[0]);
    }
    let sent = fetched.queries();
    assert_eq!(sent.len(), 2, "the refused batch, then the original query");
    assert!(!sent[1].contains("VALUES"));
}

#[test]
fn service_values_rows_deduplicate_and_reject_blank_nodes() {
    let keys = vec!["name".to_string()];
    let rows: Vec<_> = (0..205)
        .map(|index| {
            let mut row = Solution::new();
            row.insert(
                "name".into(),
                Binding::Node(format!("<http://example.org/{index}>")),
            );
            row
        })
        .collect();
    let values = service_values_rows(&rows, &keys).unwrap();
    assert_eq!(values.len(), 205);
    assert_eq!(
        values.chunks(100).map(<[_]>::len).collect::<Vec<_>>(),
        [100, 100, 5]
    );
    let duplicated = [rows[0].clone(), rows[0].clone()];
    assert_eq!(service_values_rows(&duplicated, &keys).unwrap().len(), 1);
    let mut blank = Solution::new();
    blank.insert("name".into(), Binding::Node("_:local".into()));
    assert!(service_values_rows(&[blank], &keys).is_none());
    let mut typed_unknown = Solution::new();
    typed_unknown.insert("name".into(), Binding::Literal("42".into()));
    assert!(service_values_rows(&[typed_unknown], &keys).is_none());
}

/// 205 local keys are three `VALUES` batches (100 + 100 + 5).
fn join_205(endpoint: &Endpoint, query: &str) -> Result<Vec<Solution>, String> {
    run(&view_of_turtle(&subjects(205, "name")), endpoint, query)
}

fn refused(outcome: Result<Vec<Solution>, String>, dimension: &str) {
    let error = outcome.expect_err("the budget refuses the join");
    assert!(
        error.starts_with(&format!("{SERVICE_BUDGET_EXCEEDED}:{dimension}:")),
        "{error}"
    );
}

#[test]
fn a_bind_join_within_its_budget_sends_every_batch() {
    let endpoint = Endpoint::over(&subjects(205, "score"));
    let rows = join_205(&endpoint, JOIN).unwrap();
    assert_eq!(rows.len(), 205);
    assert_eq!(endpoint.queries().len(), 3);
}

#[test]
fn the_request_budget_bounds_the_whole_bind_join() {
    let endpoint = Endpoint::over(&subjects(205, "score")).within(ServiceBudget {
        max_requests: 2,
        ..budget()
    });
    refused(join_205(&endpoint, JOIN), "requests");
    assert_eq!(endpoint.queries().len(), 2, "the third batch is never sent");
}

#[test]
fn the_row_budget_bounds_the_whole_bind_join() {
    let endpoint = Endpoint::over(&subjects(205, "score")).within(ServiceBudget {
        max_rows: 150,
        ..budget()
    });
    refused(join_205(&endpoint, JOIN), "rows");
    assert_eq!(
        endpoint.queries().len(),
        2,
        "100 rows fit, 200 do not: nothing is sent after the refusal"
    );
}

#[test]
fn the_key_budget_sends_the_original_query_instead_of_batches() {
    let endpoint = Endpoint::over(&subjects(205, "score")).within(ServiceBudget {
        max_keys: 50,
        ..budget()
    });
    let rows = join_205(&endpoint, JOIN).unwrap();
    assert_eq!(rows.len(), 205);
    let sent = endpoint.queries();
    assert_eq!(sent.len(), 1);
    assert!(!sent[0].contains("VALUES"), "{}", sent[0]);
}

#[test]
fn the_wall_budget_stops_a_slow_bind_join_between_requests() {
    let mut endpoint = Endpoint::over(&subjects(205, "score")).within(ServiceBudget {
        max_wall: Duration::from_millis(200),
        ..budget()
    });
    endpoint.delay = Duration::from_millis(250);
    refused(join_205(&endpoint, JOIN), "wall_ms");
    assert_eq!(
        endpoint.queries().len(),
        1,
        "the first batch outlived the wall budget: no second request"
    );
}

/// The endpoint answers the first batch (100 rows), refuses the second, and answers the
/// fallback to the original query (205 rows): three requests and 305 rows in all.
fn refusing_the_second_batch(budget: ServiceBudget) -> Endpoint {
    let mut endpoint = Endpoint::over(&subjects(205, "score")).within(budget);
    endpoint.refuse_at = Some(1);
    endpoint
}

#[test]
fn the_budget_counts_the_refused_batch_the_answered_batch_and_the_fallback() {
    let exact = refusing_the_second_batch(ServiceBudget {
        max_requests: 3,
        max_rows: 305,
        ..budget()
    });
    let rows = join_205(&exact, JOIN).unwrap();
    assert_eq!(rows.len(), 205, "the fallback's rows, joined");
    assert_eq!(exact.queries().len(), 3);

    let one_request_short = refusing_the_second_batch(ServiceBudget {
        max_requests: 2,
        ..budget()
    });
    refused(join_205(&one_request_short, JOIN), "requests");
    assert_eq!(
        one_request_short.queries().len(),
        2,
        "the refused batch was a request: the fallback is not sent"
    );

    let one_row_short = refusing_the_second_batch(ServiceBudget {
        max_rows: 304,
        ..budget()
    });
    refused(join_205(&one_row_short, JOIN), "rows");
    assert_eq!(
        one_row_short.queries().len(),
        3,
        "the answered batch's 100 rows still count beside the fallback's 205"
    );
}

#[test]
fn silent_turns_a_budget_refusal_into_the_join_identity_without_another_request() {
    let endpoint = Endpoint::over(&subjects(205, "score")).within(ServiceBudget {
        max_requests: 2,
        ..budget()
    });
    let rows = join_205(&endpoint, &JOIN.replace("SERVICE", "SERVICE SILENT")).unwrap();
    assert_eq!(rows.len(), 205, "every local row passes through");
    assert_eq!(column(&rows, "score")[0], "-");
    assert_eq!(endpoint.queries().len(), 2);
}
