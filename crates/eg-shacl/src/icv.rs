//! Integrity Constraint Validation — ICV (CONCEPT:EG-146).
//!
//! Stardog-style **closed-world** integrity constraints layered on the SHACL Core
//! engine (CONCEPT:EG-132). Where standard SHACL/OWL description-logic validation is
//! *open-world* (a missing required property is "unknown", never a failure), ICV
//! interprets the SAME SHACL shapes as **database integrity constraints** under the
//! **Closed-World Assumption (CWA)** and the **Unique-Name Assumption (UNA)**:
//!
//! * CWA — a fact NOT asserted (and, optionally, NOT OWL-inferred) is treated as
//!   FALSE. So a `sh:minCount 1` on a targeted node with zero asserted values is a
//!   hard **violation**, not an open-world "could exist elsewhere". (eg-shacl's SHACL
//!   engine already evaluates over the asserted graph — it is graph-closed — so ICV
//!   *formalizes* that as the intended default semantics and adds the constraint
//!   framing, the SPARQL witness, and the write guard on top.)
//! * UNA — two different IRIs denote two different things unless an explicit
//!   `owl:sameAs` says otherwise, so cardinality counts asserted distinct value nodes.
//!
//! ICV adds three things over plain [`crate::validate::validate`]:
//!
//! 1. [`validate_icv`] — run the shapes as closed-world constraints and return an
//!    [`IcvReport`]: per violation the focus node, the failing shape/constraint, the
//!    offending value(s), a human-readable message (reused from [`ValidationResult`]).
//! 2. An **explain witness** — for every violation, [`IcvViolation::witness`] is the
//!    SPARQL query a user can run against the data graph to SEE the offending data.
//!    This is the ICV differentiator: a constraint failure is *self-explaining*.
//! 3. A **guard mode** — [`check_write`] evaluates whether a proposed set of triple
//!    additions/removals WOULD introduce a new violation, returning accept/reject plus
//!    the introduced (and resolved / pre-existing) violations. This is the pure check
//!    that a constraint-enforced transaction sits on; the write-path enforcement wiring
//!    (a per-graph [`crate::policy::IcvPolicy`] + [`crate::policy::IcvPolicyRegistry`]
//!    implementing eg-rdf's `WriteGuard`, so `eg_rdf::update::execute_guarded` REJECTS a
//!    commit that would introduce a violation) is CONCEPT:EG-300 — see [`crate::policy`].
//!
//! Surpassing Stardog: ICV can run over the **OWL-reasoned view** — pass the
//! materialized OWL inferences to [`validate_icv_with_inferences`] so constraints are
//! checked against the *entailed* model, not just the asserted triples. Auto-wiring the
//! eg-rdf `owl` reasoner to materialize those inferences is a documented follow-up
//! (the reasoner lives behind eg-rdf's `owl` feature and needs the classification
//! pipeline); the pure closed-world check over a supplied reasoned graph is provided.

use std::collections::HashMap;

use eg_rdf::oxrdf::{Graph, Term, Triple};
use serde::{Deserialize, Serialize};

use crate::report::{Severity, ValidationResult};
use crate::shapes::{Constraint, NodeKind, Path, RangeKind, Shape, ShapesGraph};
use crate::validate::{graph_from_turtle, validate};
use crate::vocab;

/// One integrity-constraint violation (CONCEPT:EG-146): the underlying SHACL
/// [`ValidationResult`] plus the **explain witness** — a SPARQL query that returns the
/// offending data so the failure is self-demonstrating.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcvViolation {
    /// The SHACL result (focus node, path, value, source shape, constraint component,
    /// message, severity).
    #[serde(flatten)]
    pub result: ValidationResult,
    /// The SPARQL query demonstrating the violation over the data graph — the query a
    /// user could run to see exactly which data breaks the constraint.
    pub witness: String,
}

/// The ICV report (CONCEPT:EG-146): a closed-world integrity check outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcvReport {
    /// `true` iff there is no violation of severity `Violation` — i.e. the graph
    /// satisfies every integrity constraint under the closed-world reading.
    pub conforms: bool,
    /// Every reported violation, each carrying its explain witness.
    pub violations: Vec<IcvViolation>,
}

impl IcvReport {
    fn from_violations(violations: Vec<IcvViolation>) -> Self {
        let conforms = !violations
            .iter()
            .any(|v| v.result.severity == Severity::Violation);
        IcvReport {
            conforms,
            violations,
        }
    }
}

/// The accept/reject outcome of a guarded write (CONCEPT:EG-146).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteCheck {
    /// `true` iff the proposed change introduces NO new `Violation`-severity breach.
    pub accepted: bool,
    /// Violations the proposed change would INTRODUCE (present after, absent before).
    pub introduced: Vec<IcvViolation>,
    /// Violations the proposed change would RESOLVE (present before, absent after).
    pub resolved: Vec<IcvViolation>,
    /// Violations already present before the change and still present after.
    pub pre_existing: Vec<IcvViolation>,
}

/// Validate a data graph against a shapes graph as **closed-world integrity
/// constraints** (CONCEPT:EG-146). Returns an [`IcvReport`] with a SPARQL witness per
/// violation. Reuses the SHACL Core engine ([`crate::validate::validate`]) for
/// detection — so ICV and SHACL agree exactly on WHAT is a violation; ICV adds the
/// closed-world framing + the witness.
pub fn validate_icv(shapes_graph: &Graph, data_graph: &Graph) -> IcvReport {
    let report = validate(shapes_graph, data_graph);
    let shapes = ShapesGraph::new(shapes_graph);
    let registry = collect_shape_registry(&shapes);

    let violations = report
        .results
        .into_iter()
        .map(|result| {
            let witness = build_witness(&result, &registry);
            IcvViolation { result, witness }
        })
        .collect();
    IcvReport::from_violations(violations)
}

/// Validate over the **OWL-reasoned view** (CONCEPT:EG-146, the "surpass Stardog"
/// angle): the supplied `inferred` triples (e.g. the materialized output of eg-rdf's
/// `owl` EL⁺/RL reasoner) are merged into the data graph, then the closed-world ICV
/// check runs over the ENTAILED model. So an integrity constraint like `sh:class` is
/// satisfied by an inferred type, and a `sh:minCount` can be met by an inferred edge —
/// while a genuinely missing fact still fails closed-world.
///
/// Auto-materializing the inferences from an ontology is a documented follow-up (the
/// reasoner is behind eg-rdf's `owl` feature and needs the classification pipeline);
/// this is the pure check over a caller-supplied reasoned view.
pub fn validate_icv_with_inferences(
    shapes_graph: &Graph,
    data_graph: &Graph,
    inferred: &[Triple],
) -> IcvReport {
    let mut reasoned = data_graph.clone();
    for t in inferred {
        reasoned.insert(t);
    }
    validate_icv(shapes_graph, &reasoned)
}

/// Convenience: parse a shapes + data Turtle document and run the closed-world ICV
/// check (CONCEPT:EG-146). Returns a parse error string if either document fails.
pub fn validate_icv_turtle(shapes_ttl: &str, data_ttl: &str) -> Result<IcvReport, String> {
    let shapes = graph_from_turtle(shapes_ttl)?;
    let data = graph_from_turtle(data_ttl)?;
    Ok(validate_icv(&shapes, &data))
}

/// **Guard mode** (CONCEPT:EG-146): would applying `additions` and `removals` to
/// `base` introduce an integrity violation? Evaluates the closed-world ICV check on the
/// projected graph (`base − removals + additions`) and diffs it against `base`,
/// returning accept/reject plus the introduced / resolved / pre-existing violations.
///
/// This is the pure decision function a constraint-enforced transaction sits on: an
/// engine write path can call `check_write` on the staged change set and refuse the
/// commit when `accepted` is false. The actual write/commit-path wiring is CONCEPT:EG-300
/// — [`crate::policy::IcvPolicyRegistry`] implements eg-rdf's `WriteGuard` so
/// `eg_rdf::update::execute_guarded` enforces this per graph.
pub fn check_write(
    shapes_graph: &Graph,
    base: &Graph,
    additions: &[Triple],
    removals: &[Triple],
) -> WriteCheck {
    let before = validate_icv(shapes_graph, base);

    let mut projected = base.clone();
    for t in removals {
        projected.remove(t);
    }
    for t in additions {
        projected.insert(t);
    }
    let after = validate_icv(shapes_graph, &projected);

    let before_keys: HashMap<ViolationKey, ()> = before
        .violations
        .iter()
        .map(|v| (key(&v.result), ()))
        .collect();
    let after_keys: HashMap<ViolationKey, ()> = after
        .violations
        .iter()
        .map(|v| (key(&v.result), ()))
        .collect();

    let introduced: Vec<IcvViolation> = after
        .violations
        .iter()
        .filter(|v| !before_keys.contains_key(&key(&v.result)))
        .cloned()
        .collect();
    let resolved: Vec<IcvViolation> = before
        .violations
        .iter()
        .filter(|v| !after_keys.contains_key(&key(&v.result)))
        .cloned()
        .collect();
    let pre_existing: Vec<IcvViolation> = before
        .violations
        .iter()
        .filter(|v| after_keys.contains_key(&key(&v.result)))
        .cloned()
        .collect();

    let accepted = !introduced
        .iter()
        .any(|v| v.result.severity == Severity::Violation);

    WriteCheck {
        accepted,
        introduced,
        resolved,
        pre_existing,
    }
}

// ── Violation identity (for the write-guard diff) ───────────────────────────

type ViolationKey = (String, Option<String>, Option<String>, String, String);

fn key(r: &ValidationResult) -> ViolationKey {
    (
        r.focus_node.clone(),
        r.path.clone(),
        r.value.clone(),
        r.source_shape.clone(),
        r.constraint_component.clone(),
    )
}

// ── Shape registry (to recover a violation's constraint parameters) ─────────

/// Collect every shape reachable from the root shapes — the roots plus every shape
/// referenced by `sh:node`/`sh:property`/`sh:and`/`sh:or`/`sh:not`/`sh:xone` — keyed by
/// its N-Triples id string, so a [`ValidationResult`] (which carries `source_shape` as a
/// string) can be mapped back to its parsed [`Shape`] to build the witness.
fn collect_shape_registry(shapes: &ShapesGraph) -> HashMap<String, Shape> {
    let mut acc: HashMap<String, Shape> = HashMap::new();
    for root in shapes.root_shapes() {
        collect_shape(shapes, &root, &mut acc);
    }
    acc
}

fn collect_shape(shapes: &ShapesGraph, id: &Term, acc: &mut HashMap<String, Shape>) {
    let k = id.to_string();
    if acc.contains_key(&k) {
        return;
    }
    let shape = shapes.parse_shape(id);
    acc.insert(k, shape.clone());
    for c in &shape.constraints {
        match c {
            Constraint::Node(t) | Constraint::Not(t) | Constraint::Property(t) => {
                collect_shape(shapes, t, acc);
            }
            Constraint::And(v) | Constraint::Or(v) | Constraint::Xone(v) => {
                for t in v {
                    collect_shape(shapes, t, acc);
                }
            }
            _ => {}
        }
    }
}

/// Find the constraint on `shape` whose reported component IRI is `cc`.
fn constraint_for(shape: &Shape, cc: &str) -> Option<Constraint> {
    shape
        .constraints
        .iter()
        .find(|c| component_matches(c, cc))
        .cloned()
}

fn component_matches(c: &Constraint, cc: &str) -> bool {
    matches!(
        (c, cc),
        (Constraint::MinCount(_), vocab::CC_MIN_COUNT)
            | (Constraint::MaxCount(_), vocab::CC_MAX_COUNT)
            | (Constraint::Datatype(_), vocab::CC_DATATYPE)
            | (Constraint::Class(_), vocab::CC_CLASS)
            | (Constraint::NodeKind(_), vocab::CC_NODE_KIND)
            | (Constraint::MinLength(_), vocab::CC_MIN_LENGTH)
            | (Constraint::MaxLength(_), vocab::CC_MAX_LENGTH)
            | (Constraint::Pattern { .. }, vocab::CC_PATTERN)
            | (Constraint::LanguageIn(_), vocab::CC_LANGUAGE_IN)
            | (Constraint::In(_), vocab::CC_IN)
            | (Constraint::HasValue(_), vocab::CC_HAS_VALUE)
    ) || matches!(
        (c, cc),
        (
            Constraint::Range(RangeKind::MinInclusive, _),
            vocab::CC_MIN_INCLUSIVE
        ) | (
            Constraint::Range(RangeKind::MaxInclusive, _),
            vocab::CC_MAX_INCLUSIVE
        ) | (
            Constraint::Range(RangeKind::MinExclusive, _),
            vocab::CC_MIN_EXCLUSIVE
        ) | (
            Constraint::Range(RangeKind::MaxExclusive, _),
            vocab::CC_MAX_EXCLUSIVE
        )
    )
}

// ── Witness generation (the ICV differentiator) ─────────────────────────────

/// The predicate IRI of a property shape's `sh:path`, if it is a simple predicate path.
fn path_predicate(shape: &Shape) -> Option<String> {
    match &shape.path {
        Some(Path::Predicate(p)) => Some(p.as_str().to_string()),
        _ => None,
    }
}

/// The graph pattern that binds `?value` to a shape's value nodes for `focus`: the
/// objects of the `sh:path` predicate (property shape), or the focus node itself bound
/// via `VALUES` (node shape).
fn value_binding(focus: &str, pred: &Option<String>) -> String {
    match pred {
        Some(p) => format!("  {focus} <{p}> ?value .\n"),
        None => format!("  VALUES ?value {{ {focus} }}\n"),
    }
}

/// Build the SPARQL witness for a single violation. Reuses the parsed [`Shape`] (to
/// recover the constraint's parameters) so the witness returns exactly the offending
/// data. The core constraints get a precise query; shape-based/logical constraints get
/// a best-effort query that returns the offending value node(s).
fn build_witness(res: &ValidationResult, registry: &HashMap<String, Shape>) -> String {
    let focus = &res.focus_node;
    let shape = registry.get(&res.source_shape);
    let pred = shape.and_then(path_predicate);
    let constraint = shape.and_then(|s| constraint_for(s, &res.constraint_component));

    let header = |what: &str| {
        format!(
            "# CONCEPT:EG-146 ICV witness — {what}\n# focus: {focus}  shape: {}\n",
            res.source_shape
        )
    };

    match &constraint {
        Some(Constraint::MinCount(n)) => {
            // Closed-world: too FEW asserted values. The count query shows the shortfall.
            format!(
                "{}SELECT (COUNT(?value) AS ?count) WHERE {{\n{}}}\n# violation iff ?count < {n}",
                header(&format!("sh:minCount {n} — fewer than {n} value(s)")),
                value_binding(focus, &pred),
            )
        }
        Some(Constraint::MaxCount(n)) => format!(
            "{}SELECT ?value WHERE {{\n{}}}\n# violation iff more than {n} rows",
            header(&format!("sh:maxCount {n} — more than {n} values")),
            value_binding(focus, &pred),
        ),
        Some(Constraint::HasValue(v)) => {
            // Set-level: the required value must be asserted (closed-world).
            let vv = v.to_string();
            match &pred {
                Some(p) => format!(
                    "{}ASK {{ {focus} <{p}> {vv} }}\n# violation iff this ASK is false",
                    header(&format!("sh:hasValue {vv} — required value missing")),
                ),
                None => format!(
                    "{}ASK {{ FILTER(sameTerm({focus}, {vv})) }}\n# violation iff false",
                    header(&format!("sh:hasValue {vv} — required value missing")),
                ),
            }
        }
        Some(Constraint::Datatype(dt)) => filter_witness(
            &header(&format!(
                "sh:datatype <{}> — wrong literal datatype",
                dt.as_str()
            )),
            focus,
            &pred,
            &format!(
                "!isLiteral(?value) || datatype(?value) != <{}>",
                dt.as_str()
            ),
        ),
        Some(Constraint::Class(cls)) => format!(
            "{}SELECT ?value WHERE {{\n{}  FILTER NOT EXISTS {{ ?value a <{}> }}\n}}",
            header(&format!(
                "sh:class <{}> — value is not a (closed-world) instance",
                cls.as_str()
            )),
            value_binding(focus, &pred),
            cls.as_str(),
        ),
        Some(Constraint::NodeKind(k)) => filter_witness(
            &header("sh:nodeKind — wrong node kind"),
            focus,
            &pred,
            nodekind_offender_filter(*k),
        ),
        Some(Constraint::Range(kind, bound)) => {
            let op = match kind {
                RangeKind::MinInclusive => "<",
                RangeKind::MaxInclusive => ">",
                RangeKind::MinExclusive => "<=",
                RangeKind::MaxExclusive => ">=",
            };
            filter_witness(
                &header(&format!("sh:{kind:?} {bound} — value out of range")),
                focus,
                &pred,
                &format!("?value {op} {bound}"),
            )
        }
        Some(Constraint::MinLength(n)) => filter_witness(
            &header(&format!("sh:minLength {n} — value too short")),
            focus,
            &pred,
            &format!("STRLEN(STR(?value)) < {n}"),
        ),
        Some(Constraint::MaxLength(n)) => filter_witness(
            &header(&format!("sh:maxLength {n} — value too long")),
            focus,
            &pred,
            &format!("STRLEN(STR(?value)) > {n}"),
        ),
        Some(Constraint::Pattern { pattern, flags }) => {
            let flag_arg = match flags {
                Some(f) => format!(", \"{f}\""),
                None => String::new(),
            };
            filter_witness(
                &header(&format!("sh:pattern {pattern} — value does not match")),
                focus,
                &pred,
                &format!("!REGEX(STR(?value), \"{}\"{flag_arg})", escape(pattern)),
            )
        }
        Some(Constraint::In(list)) => {
            let items = list
                .iter()
                .map(|t| t.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            filter_witness(
                &header("sh:in — value not in the allowed set"),
                focus,
                &pred,
                &format!("?value NOT IN ({items})"),
            )
        }
        Some(Constraint::LanguageIn(langs)) => {
            let ors = langs
                .iter()
                .map(|l| format!("LANGMATCHES(LANG(?value), \"{}\")", escape(l)))
                .collect::<Vec<_>>()
                .join(" || ");
            let cond = if ors.is_empty() {
                "true".to_string()
            } else {
                format!("!({ors})")
            };
            filter_witness(
                &header("sh:languageIn — language tag not allowed"),
                focus,
                &pred,
                &cond,
            )
        }
        // Shape-based / logical / deferred: a best-effort witness that returns the
        // offending value node(s); the referenced shape names the sub-constraint.
        _ => {
            let val = res.value.clone().unwrap_or_else(|| focus.clone());
            format!(
                "{}SELECT ?value WHERE {{\n{}}}\n# offending value node: {val}\n# (does not satisfy component {})",
                header("value fails the referenced constraint/shape"),
                value_binding(focus, &pred),
                res.constraint_component,
            )
        }
    }
}

/// A `SELECT ?value … FILTER(<offender_cond>)` witness returning the value nodes for
/// which `offender_cond` is TRUE (i.e. the ones that break the constraint).
fn filter_witness(header: &str, focus: &str, pred: &Option<String>, offender_cond: &str) -> String {
    format!(
        "{header}SELECT ?value WHERE {{\n{}  FILTER ({offender_cond})\n}}",
        value_binding(focus, pred),
    )
}

/// The SPARQL FILTER condition that is TRUE for a value node of the WRONG node kind.
fn nodekind_offender_filter(k: NodeKind) -> &'static str {
    match k {
        NodeKind::Iri => "!isIRI(?value)",
        NodeKind::BlankNode => "!isBlank(?value)",
        NodeKind::Literal => "!isLiteral(?value)",
        NodeKind::BlankNodeOrIri => "isLiteral(?value)",
        NodeKind::BlankNodeOrLiteral => "isIRI(?value)",
        NodeKind::IriOrLiteral => "isBlank(?value)",
    }
}

/// Escape a string for embedding inside a double-quoted SPARQL literal.
fn escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}
