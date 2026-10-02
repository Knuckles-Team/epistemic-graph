//! `SERVICE` delegation (CONCEPT:EG-KG.query.sparql-service-federation-client): the remote
//! evaluation of one `SERVICE` clause, and the three ways the evaluator narrows what the
//! endpoint returns — a pushed `FILTER`, a pushed `LIMIT`, and a bind join that ships the
//! local join keys in `VALUES` batches.
//!
//! A narrowed request is an optimization of the clause's own query, never a replacement
//! for it:
//!
//!  * it is sent without `SILENT`; when the endpoint refuses it, the clause's own query is
//!    sent, and only that final attempt's failure becomes the `SILENT` join identity;
//!  * a `FILTER` is pushed only when the endpoint provably keeps every row the local
//!    filter keeps ([`filter_is_pushable`]) — a row the endpoint drops cannot be restored
//!    by filtering again, and the rows that come back have lost their language tags and
//!    datatypes, so almost every comparison stays local;
//!  * a bind join runs under one [`ServiceBudget`] covering every request it sends — the
//!    batches the endpoint answered, the batch it refused, and the fallback to the
//!    clause's own query.

use std::time::{Duration, Instant};

use spargebra::algebra::{Expression, Function, GraphPattern};
use spargebra::term::{GroundTerm, NamedNode, NamedNodePattern, Variable};
use spargebra::Query;

use super::{
    collect_vars, eval_pattern, hash_join, project_solutions, Binding, Ctx, Solution, SparqlResult,
};

/// A remote SPARQL endpoint the evaluator can delegate a `SERVICE` clause to
/// (CONCEPT:EG-KG.query.sparql-service-federation-client). This is the SEAM: `eg-rdf` owns the algebra + the SILENT / join
/// semantics but knows NOTHING about HTTP — the facade supplies a `ureq`-backed impl
/// (feature `sparql-service`), keeping the Pi/crate-DAG contract intact (no HTTP dep
/// enters this pure-Rust crate). `select` runs one remote SELECT and returns its
/// solution table; `Err` carries a human-readable failure (routed by SILENT).
pub trait RemoteSparql: Sync {
    /// Evaluate `query` (a complete SPARQL SELECT) against `endpoint`, returning its rows.
    fn select(&self, endpoint: &str, query: &str) -> Result<SparqlResult, String>;

    /// The bounds one bind join against this client's endpoints runs under.
    fn service_budget(&self) -> ServiceBudget {
        ServiceBudget::from_env()
    }
}

/// Error-code prefix of a bind join stopped by its budget:
/// `SERVICE_BUDGET_EXCEEDED:<dimension>: …`.
pub const SERVICE_BUDGET_EXCEEDED: &str = "SERVICE_BUDGET_EXCEEDED";

/// Distinct join keys one `VALUES` batch carries.
const BIND_BATCH_KEYS: usize = 100;

/// Bounds on one `SERVICE` bind join as a whole. The per-batch key cap bounds a single
/// request; this bounds the operation — every batch, and the fallback to the clause's
/// own query when the endpoint refuses a batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServiceBudget {
    /// Distinct join keys a bind join may ship; above it the clause's own query is sent.
    pub max_keys: usize,
    /// Requests the operation may send, refused batches and the fallback included.
    pub max_requests: usize,
    /// Rows the operation may receive, summed over every answered request.
    pub max_rows: usize,
    /// Wall-clock time after which no further request is sent.
    pub max_wall: Duration,
}

impl Default for ServiceBudget {
    fn default() -> Self {
        Self {
            max_keys: 10_000,
            max_requests: 256,
            max_rows: 250_000,
            max_wall: Duration::from_millis(60_000),
        }
    }
}

impl ServiceBudget {
    /// The defaults, each overridable by the federation limits
    /// `EPISTEMIC_GRAPH_FEDERATION_MAX_{BIND_KEYS,REQUESTS,ROWS,WALL_MS}`.
    pub fn from_env() -> Self {
        use eg_types::runtime_limit::positive_from_env;
        let d = Self::default();
        let wall_ms = u64::try_from(d.max_wall.as_millis()).unwrap_or(u64::MAX);
        Self {
            max_keys: positive_from_env("EPISTEMIC_GRAPH_FEDERATION_MAX_BIND_KEYS", d.max_keys),
            max_requests: positive_from_env(
                "EPISTEMIC_GRAPH_FEDERATION_MAX_REQUESTS",
                d.max_requests,
            ),
            max_rows: positive_from_env("EPISTEMIC_GRAPH_FEDERATION_MAX_ROWS", d.max_rows),
            max_wall: Duration::from_millis(positive_from_env(
                "EPISTEMIC_GRAPH_FEDERATION_MAX_WALL_MS",
                wall_ms,
            )),
        }
    }
}

/// What the endpoint said to one accounted request.
enum Answer {
    Rows(Vec<Solution>),
    Rejected(String),
}

/// What one bind join has spent. A budget refusal is the outer `Err` and ends the
/// operation: it is never answered by sending yet another request.
struct ServiceMeter {
    budget: ServiceBudget,
    requests: usize,
    rows: usize,
    started: Instant,
}

impl ServiceMeter {
    fn new(budget: ServiceBudget) -> Self {
        Self {
            budget,
            requests: 0,
            rows: 0,
            started: Instant::now(),
        }
    }

    fn refusal(dimension: &str, limit: impl std::fmt::Display) -> String {
        format!(
            "{SERVICE_BUDGET_EXCEEDED}:{dimension}: the SERVICE join exceeded its limit of {limit}"
        )
    }

    /// One request: counted before it is sent — so a refused request still counts — and
    /// its rows counted when it answers.
    fn request(&mut self, target: &Target<'_>, pattern: &GraphPattern) -> Result<Answer, String> {
        if self.requests >= self.budget.max_requests {
            return Err(Self::refusal("requests", self.budget.max_requests));
        }
        if self.started.elapsed() > self.budget.max_wall {
            return Err(Self::refusal("wall_ms", self.budget.max_wall.as_millis()));
        }
        self.requests += 1;
        let result = match target.select(pattern) {
            Ok(result) => result,
            Err(error) => return Ok(Answer::Rejected(error)),
        };
        self.rows = self.rows.saturating_add(result.len());
        if self.rows > self.budget.max_rows {
            return Err(Self::refusal("rows", self.budget.max_rows));
        }
        Ok(Answer::Rows(result))
    }

    /// Every batch's rows, or `None` as soon as the endpoint refuses one.
    fn run_batches(
        &mut self,
        target: &Target<'_>,
        batches: &[GraphPattern],
    ) -> Result<Option<Vec<Solution>>, String> {
        let mut rows = Vec::new();
        for batch in batches {
            match self.request(target, batch)? {
                Answer::Rows(more) => rows.extend(more),
                Answer::Rejected(_) => return Ok(None),
            }
        }
        Ok(Some(rows))
    }

    /// The remote side of `call`'s bind join over `local`: the key batches when the keys
    /// can be shipped within the key budget, and the clause's own query when they cannot
    /// or the endpoint refuses a batch — every request under this one meter.
    fn metered_join(
        &mut self,
        call: &ServiceCall<'_>,
        target: &Target<'_>,
        local: &[Solution],
    ) -> Result<Vec<Solution>, String> {
        let max_keys = self.budget.max_keys;
        let keys = call
            .bind_keys(local)
            .filter(|keys| keys.rows.len() <= max_keys);
        if let Some(keys) = keys {
            if let Some(rows) = self.run_batches(target, &call.batches(&keys))? {
                return Ok(rows);
            }
        }
        match self.request(target, call.pattern)? {
            Answer::Rows(rows) => Ok(rows),
            Answer::Rejected(error) => Err(error),
        }
    }
}

/// The distinct local join keys of a bind join, as `VALUES` rows over `variables`.
struct BindKeys {
    variables: Vec<Variable>,
    rows: Vec<Vec<Option<GroundTerm>>>,
}

/// The endpoint a clause resolved to and the client that reaches it.
struct Target<'c> {
    client: &'c dyn RemoteSparql,
    endpoint: &'c str,
}

impl Target<'_> {
    /// One request for `pattern`; a failure is the endpoint's, never hushed here.
    fn select(&self, pattern: &GraphPattern) -> Result<Vec<Solution>, String> {
        self.client
            .select(self.endpoint, &build_service_query(pattern))
            .map(|result| result.solutions)
            .map_err(|e| format!("eg-rdf SPARQL: SERVICE failed: {e}"))
    }
}

/// One `SERVICE [SILENT] <endpoint> { pattern }` clause.
#[derive(Clone, Copy)]
pub(super) struct ServiceCall<'q> {
    pub(super) name: &'q NamedNodePattern,
    pub(super) pattern: &'q GraphPattern,
    pub(super) silent: bool,
}

impl<'q> ServiceCall<'q> {
    /// The clause of a `GraphPattern::Service` node; `None` for any other pattern.
    pub(super) fn of(pattern: &'q GraphPattern) -> Option<Self> {
        match pattern {
            GraphPattern::Service {
                name,
                inner,
                silent,
            } => Some(Self {
                name,
                pattern: inner,
                silent: *silent,
            }),
            _ => None,
        }
    }

    /// Only a constant-IRI endpoint with a bound client can be reached: a variable
    /// endpoint is unsupported, and no client (feature off, empty allow-list) is
    /// fail-closed.
    fn target<'a>(&'a self, ctx: &Ctx<'a>) -> Result<Target<'a>, String> {
        let NamedNodePattern::NamedNode(endpoint) = self.name else {
            return Err("eg-rdf SPARQL: SERVICE with a variable endpoint is unsupported".into());
        };
        let client = ctx
            .service
            .ok_or("eg-rdf SPARQL: SERVICE requires a remote client; none bound")?;
        Ok(Target {
            client,
            endpoint: endpoint.as_str(),
        })
    }

    /// `SILENT` turns the clause's FINAL failure into one empty solution — the join
    /// identity, so the enclosing join passes the local side through unchanged.
    fn hushed(&self, outcome: Result<Vec<Solution>, String>) -> Result<Vec<Solution>, String> {
        match outcome {
            Err(_) if self.silent => Ok(vec![Solution::new()]),
            other => other,
        }
    }

    /// The clause's own query: every solution of its pattern at the endpoint.
    pub(super) fn eval(&self, ctx: &Ctx) -> Result<Vec<Solution>, String> {
        self.hushed(
            self.target(ctx)
                .and_then(|target| target.select(self.pattern)),
        )
    }

    /// Send `narrowed` without `SILENT`; when it cannot be sent or the endpoint refuses
    /// it, the clause's own query decides the outcome.
    fn eval_narrowed(&self, ctx: &Ctx, narrowed: &GraphPattern) -> Result<Vec<Solution>, String> {
        match self.target(ctx).and_then(|target| target.select(narrowed)) {
            Ok(rows) => Ok(rows),
            Err(_) => self.eval(ctx),
        }
    }

    /// The clause under an enclosing `FILTER`. The caller applies `expr` to the returned
    /// rows either way; it is also sent to the endpoint when [`Self::pushed_filter`]
    /// allows it.
    pub(super) fn eval_filtered(
        &self,
        ctx: &Ctx,
        expr: &Expression,
    ) -> Result<Vec<Solution>, String> {
        match self.pushed_filter(expr) {
            Some(narrowed) => self.eval_narrowed(ctx, &narrowed),
            None => self.eval(ctx),
        }
    }

    /// The clause's pattern narrowed by `expr`, when the endpoint provably keeps every row
    /// the local filter keeps ([`filter_is_pushable`]).
    fn pushed_filter(&self, expr: &Expression) -> Option<GraphPattern> {
        filter_is_pushable(expr, self.pattern).then(|| GraphPattern::Filter {
            expr: expr.clone(),
            inner: Box::new(self.pattern.clone()),
        })
    }

    /// The clause under an enclosing `LIMIT limit` with no offset.
    fn eval_limit(&self, ctx: &Ctx, limit: usize) -> Result<Vec<Solution>, String> {
        let narrowed = GraphPattern::Slice {
            inner: Box::new(self.pattern.clone()),
            start: 0,
            length: Some(limit),
        };
        self.eval_narrowed(ctx, &narrowed)
    }

    /// `local ⋈ SERVICE`: ship the distinct local join keys in `VALUES` batches, falling
    /// back to the clause's own query when the keys cannot be shipped or the endpoint
    /// refuses a batch. One budget covers every request of the operation.
    pub(super) fn eval_bind_join(
        &self,
        ctx: &Ctx,
        local: &[Solution],
    ) -> Result<Vec<Solution>, String> {
        if local.is_empty() {
            return Ok(Vec::new());
        }
        let remote = self.hushed(self.join_rows(ctx, local))?;
        Ok(hash_join(local, &remote))
    }

    /// The remote side of a bind join, metered as one operation.
    fn join_rows(&self, ctx: &Ctx, local: &[Solution]) -> Result<Vec<Solution>, String> {
        let target = self.target(ctx)?;
        ServiceMeter::new(target.client.service_budget()).metered_join(self, &target, local)
    }

    /// The distinct local join keys. `None` when they cannot be shipped: no shared
    /// variable, a row leaving a key unbound, or a key that is not an IRI.
    fn bind_keys(&self, local: &[Solution]) -> Option<BindKeys> {
        let mut keys: Vec<String> = collect_vars(self.pattern)
            .into_iter()
            .filter(|key| local.iter().any(|row| row.contains_key(key)))
            .collect();
        keys.sort();
        let variables: Vec<Variable> = keys
            .iter()
            .map(|key| Variable::new(key.as_str()).ok())
            .collect::<Option<_>>()?;
        if variables.is_empty() {
            return None;
        }
        let rows = service_values_rows(local, &keys)?;
        Some(BindKeys { variables, rows })
    }

    /// The pattern joined with each `VALUES` batch of `keys`.
    fn batches(&self, keys: &BindKeys) -> Vec<GraphPattern> {
        let batch = |bindings: &[Vec<Option<GroundTerm>>]| GraphPattern::Join {
            left: Box::new(self.pattern.clone()),
            right: Box::new(GraphPattern::Values {
                variables: keys.variables.clone(),
                bindings: bindings.to_vec(),
            }),
        };
        keys.rows.chunks(BIND_BATCH_KEYS).map(batch).collect()
    }
}

/// `LIMIT limit` (no offset) over `inner`: pushed when `inner` is a `SERVICE` clause, or
/// a projection of one; evaluated as written otherwise.
pub(super) fn eval_limited(
    ctx: &Ctx,
    inner: &GraphPattern,
    limit: usize,
) -> Result<Vec<Solution>, String> {
    if let Some(call) = ServiceCall::of(inner) {
        return call.eval_limit(ctx, limit);
    }
    if let GraphPattern::Project {
        inner: projected,
        variables,
    } = inner
    {
        if let Some(call) = ServiceCall::of(projected) {
            return Ok(project_solutions(call.eval_limit(ctx, limit)?, variables));
        }
    }
    eval_pattern(ctx, inner)
}

/// Convert only losslessly representable keys. The evaluator stores literal
/// lexical values without datatype/language metadata, so sending them as plain
/// literals would make typed/lang-tagged remote matches disappear. Blank nodes
/// cannot be sent as VALUES ground terms either. Both use full fetch.
pub(super) fn service_values_rows(
    local: &[Solution],
    keys: &[String],
) -> Option<Vec<Vec<Option<GroundTerm>>>> {
    let mut seen = std::collections::HashSet::new();
    let mut rows = Vec::new();
    for row in local {
        let terms: Vec<_> = keys
            .iter()
            .map(|key| match row.get(key)? {
                Binding::Node(node) => {
                    let iri = node.strip_prefix('<')?.strip_suffix('>')?;
                    Some(GroundTerm::NamedNode(NamedNode::new(iri).ok()?))
                }
                Binding::Literal(_) => None,
            })
            .collect::<Option<_>>()?;
        let signature: Vec<String> = terms.iter().map(ToString::to_string).collect();
        if seen.insert(signature) {
            rows.push(terms.into_iter().map(Some).collect());
        }
    }
    Some(rows)
}

/// Build the SPARQL SELECT text sent to a remote SERVICE endpoint (CONCEPT:EG-KG.query.sparql-service-federation-client): wrap
/// `inner` in a `SELECT` projecting its in-scope variables and render it with spargebra's
/// `Display` (which emits valid SPARQL 1.1). The projected vars are what the enclosing join
/// binds on, so the remote side returns exactly the columns the local pattern needs.
pub(super) fn build_service_query(inner: &GraphPattern) -> String {
    let mut variables: Vec<Variable> = Vec::new();
    inner.on_in_scope_variable(|v| {
        if !variables.contains(v) {
            variables.push(v.clone());
        }
    });
    let pattern = GraphPattern::Project {
        inner: Box::new(inner.clone()),
        variables,
    };
    Query::Select {
        dataset: None,
        pattern,
        base_iri: None,
    }
    .to_string()
}

// ── FILTER pushdown eligibility ────────────────────────────────────────────────────

/// May `expr`, the filter directly over a `SERVICE` clause whose pattern is `remote`, also
/// be evaluated by the endpoint?
///
/// # What has to hold
///
/// The local filter runs over the returned rows whether or not the filter was sent, so
/// sending it is sound exactly when the endpoint keeps every row the local filter keeps:
/// *local true ⇒ endpoint true*, for every row the pattern can produce. The two sides do
/// not evaluate the same thing. The endpoint evaluates RDF terms. The local side
/// evaluates what is left of them after the client boundary ([`RemoteSparql::select`]
/// returns [`Binding`]s):
///
///  * an IRI arrives as `Binding::Node("<iri>")`, a blank node as `Binding::Node("_:id")`;
///  * a literal arrives as `Binding::Literal(lexical form)` — its language tag and its
///    datatype are gone;
///  * a term the client cannot represent (a quoted triple) arrives as no binding at all.
///
/// On top of that the local operators are lexical: `=` and `IN` strip the angle brackets
/// of a node and compare strings, or compare as numbers when both sides parse as one;
/// `sameTerm`, `CONTAINS`, `STRSTARTS`, `STRENDS`, `STR`, `UCASE`, `LCASE` and `STRLEN`
/// work on the same strings; the ordering operators parse them as numbers.
///
/// # What that rules out
///
/// Anything the endpoint may answer `false` or raise a type error for while the local
/// side answers `true`:
///
///  * every comparison involving a literal — `"hello"@en` and `"hello"@fr` are the same
///    string here and different terms there; `"1"^^xsd:string = 1` is true here and a
///    type error there; so `=`, `sameTerm`, `<`/`<=`/`>`/`>=`, `IN`, arithmetic and the
///    string functions are never sent;
///  * a bare comparison with an IRI — the literal `"urn:x"` equals `<urn:x>` here once
///    the brackets are stripped, and never there;
///  * every negation — where the client drops a term, `!bound(?v)` is true here and false
///    there, and where a variable is unbound `!isIRI(?v)` is true here and an error there;
///  * `EXISTS` / `NOT EXISTS` (matched against the evaluating side's dataset), the
///    non-deterministic built-ins, `IRI`, extension functions, `IF`, `COALESCE`.
///
/// # What is left, and why it is sound
///
/// A filter is sent when it is built from the forms below with `&&` and `||` only.
///
///  * `bound(?v)`, `isIRI(?v)`, `isBlank(?v)`, `isLiteral(?v)` on a variable of `remote`.
///    Each is true here only for a binding of that kind — present, `Node("<…>")`,
///    `Node("_:…")`, `Literal` — and the boundary produces that binding only from a
///    bound term, an IRI, a blank node and a literal respectively.
///  * `?v = <iri>`, `<iri> = ?v`, `sameTerm` of the two, and `?v IN (<iri>, …)` with only
///    IRIs listed — but only as a conjunct beside `isIRI(?v)` for the same variable. The
///    conjunction is true here only when the binding is `Node("<x>")` and `x` is the
///    constant, so the term is that IRI and both conjuncts are true at the endpoint; the
///    guard is what excludes the literal `"urn:x"`.
///  * `a && b`: true here only when both are, so both are true there. `a || b`: true here
///    only when one is, so that one is true there, and a true operand decides a SPARQL
///    `||` even when the other raises an error.
///
/// The forms are an allow-list: an operator not named here is local-only.
pub(super) fn filter_is_pushable(expr: &Expression, remote: &GraphPattern) -> bool {
    holds_at_endpoint(expr, &collect_vars(remote))
}

/// `expr` is a disjunction of conjunctions of the sound forms.
fn holds_at_endpoint(expr: &Expression, scope: &[String]) -> bool {
    if let Expression::Or(a, b) = expr {
        return holds_at_endpoint(a, scope) && holds_at_endpoint(b, scope);
    }
    let conjuncts = conjuncts(expr);
    let guarded = iri_guarded(&conjuncts);
    conjuncts
        .iter()
        .all(|part| conjunct_holds(part, scope, &guarded))
}

/// The operands of a (possibly nested) `&&`; any other expression is its own conjunct.
fn conjuncts(expr: &Expression) -> Vec<&Expression> {
    match expr {
        Expression::And(a, b) => [conjuncts(a), conjuncts(b)].concat(),
        other => vec![other],
    }
}

/// The variables an `isIRI(?v)` conjunct guards.
fn iri_guarded<'e>(conjuncts: &[&'e Expression]) -> Vec<&'e str> {
    conjuncts
        .iter()
        .copied()
        .filter_map(|part| match part {
            Expression::FunctionCall(Function::IsIri, arguments) => variable_of(arguments),
            _ => None,
        })
        .collect()
}

/// The variable a one-argument call is applied to, when its argument is a bare variable.
fn variable_of(arguments: &[Expression]) -> Option<&str> {
    match arguments {
        [Expression::Variable(variable)] => Some(variable.as_str()),
        _ => None,
    }
}

/// One conjunct: a nested disjunction, a term test on a variable of the pattern, or an
/// IRI comparison of a variable the same conjunction guards with `isIRI`.
fn conjunct_holds(part: &Expression, scope: &[String], guarded: &[&str]) -> bool {
    match part {
        Expression::Or(..) => holds_at_endpoint(part, scope),
        Expression::Bound(variable) => in_scope(variable.as_str(), scope),
        Expression::FunctionCall(function, arguments) => {
            TERM_KIND_TESTS.contains(function)
                && variable_of(arguments).is_some_and(|name| in_scope(name, scope))
        }
        other => compared_with_iris(other).is_some_and(|name| guarded.contains(&name)),
    }
}

fn in_scope(name: &str, scope: &[String]) -> bool {
    scope.iter().any(|variable| variable == name)
}

/// The built-ins that test only the kind of a term, which survives the client boundary.
const TERM_KIND_TESTS: &[Function] = &[Function::IsIri, Function::IsBlank, Function::IsLiteral];

/// The variable of `?v = <iri>`, `<iri> = ?v`, `sameTerm` of the two, or
/// `?v IN (<iri>, …)`; `None` for every other expression.
fn compared_with_iris(expr: &Expression) -> Option<&str> {
    let is_iri = |operand: &Expression| matches!(operand, Expression::NamedNode(_));
    match expr {
        Expression::Equal(a, b) | Expression::SameTerm(a, b) => match (a.as_ref(), b.as_ref()) {
            (Expression::Variable(variable), iri) | (iri, Expression::Variable(variable)) => {
                is_iri(iri).then_some(variable.as_str())
            }
            _ => None,
        },
        Expression::In(operand, list) => match operand.as_ref() {
            Expression::Variable(variable) => list.iter().all(is_iri).then_some(variable.as_str()),
            _ => None,
        },
        _ => None,
    }
}
