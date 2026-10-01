//! `SERVICE` delegation (CONCEPT:EG-KG.query.sparql-service-federation-client): the remote
//! evaluation of one `SERVICE` clause, and the three ways the evaluator narrows what the
//! endpoint returns — a pushed `FILTER`, a pushed `LIMIT`, and a bind join that ships the
//! local join keys in `VALUES` batches.
//!
//! A narrowed request is an optimization of the clause's own query, never a replacement
//! for it:
//!
//!  * it is sent without `SILENT`; when the endpoint refuses it, the clause's own query is
//!    sent, and only that final attempt's failure becomes the `SILENT` join identity.

use spargebra::algebra::{Expression, GraphPattern};
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
}

/// Distinct join keys one `VALUES` batch carries.
const BIND_BATCH_KEYS: usize = 100;

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

/// Every batch's rows, or `None` as soon as the endpoint refuses one.
fn bind(target: &Target<'_>, batches: &[GraphPattern]) -> Option<Vec<Solution>> {
    let mut rows = Vec::new();
    for batch in batches {
        rows.extend(target.select(batch).ok()?);
    }
    Some(rows)
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

    /// The clause's pattern narrowed by `expr`.
    fn pushed_filter(&self, expr: &Expression) -> Option<GraphPattern> {
        Some(GraphPattern::Filter {
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
    /// refuses a batch.
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

    /// The remote side of a bind join: the key batches, else the clause's own query.
    fn join_rows(&self, ctx: &Ctx, local: &[Solution]) -> Result<Vec<Solution>, String> {
        let target = self.target(ctx)?;
        if let Some(keys) = self.bind_keys(local) {
            if let Some(rows) = bind(&target, &self.batches(&keys)) {
                return Ok(rows);
            }
        }
        target.select(self.pattern)
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
