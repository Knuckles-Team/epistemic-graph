//! The ShEx Core validation engine (CONCEPT:EG-133).
//!
//! [`validate`] takes a parsed [`Schema`], an RDF data graph (`eg_rdf::oxrdf::Graph`) and
//! a [`ShapeMap`] (focus node → shape label) and produces a [`ShexReport`]. For each
//! shape-map entry it asks: does the focus node **conform** to the shape expression?
//!
//! * A [`NodeConstraint`] tests the node itself (nodeKind / datatype / string+numeric
//!   facets / value set).
//! * A [`Shape`] gathers the node's outgoing arcs and matches them against the shape's
//!   triple expression; a `closed` shape additionally rejects any leftover arc whose
//!   predicate is neither mentioned nor listed in `extra`.
//! * `EachOf` requires every branch; `OneOf` requires exactly one branch (alternation);
//!   `ShapeAnd`/`ShapeOr`/`ShapeNot` compose shape expressions; a `Ref` recurses into
//!   another labelled shape.
//!
//! The matcher uses the common-case greedy partition (each triple constraint consumes the
//! arcs on its predicate whose object satisfies its value expression). Full backtracking
//! partition across triple constraints that SHARE a predicate, and inverse (`^`) triple
//! constraints, are documented EG-133 follow-ups.

use eg_rdf::oxrdf::{Graph, NamedNode, NamedOrBlankNodeRef, Term};
use regex::RegexBuilder;

use crate::report::{NodeResult, ShexReport};
use crate::schema::{
    NodeConstraint, NodeKind, Schema, Shape, ShapeExpr, ShapeLabel, TripleExpr, ValueSetValue,
    START,
};

/// One (focus node, shape label) association to validate.
#[derive(Debug, Clone)]
pub struct ShapeMapEntry {
    pub node: Term,
    pub shape: ShapeLabel,
}

/// A ShEx shape map — the query (which nodes to test against which shapes).
#[derive(Debug, Clone, Default)]
pub struct ShapeMap {
    pub entries: Vec<ShapeMapEntry>,
}

impl ShapeMap {
    pub fn new() -> Self {
        ShapeMap::default()
    }

    /// Associate a focus node term with a shape label.
    pub fn add(&mut self, node: Term, shape: impl Into<ShapeLabel>) -> &mut Self {
        self.entries.push(ShapeMapEntry {
            node,
            shape: shape.into(),
        });
        self
    }

    /// Build a shape map from `(node_iri, shape_label)` string pairs — the ergonomic path
    /// when every focus node is an IRI. Use [`START`] as the label for the start shape.
    pub fn from_iri_pairs(pairs: &[(&str, &str)]) -> ShapeMap {
        let mut m = ShapeMap::new();
        for (node, shape) in pairs {
            m.add(Term::NamedNode(NamedNode::new_unchecked(*node)), *shape);
        }
        m
    }
}

/// Guard against pathological cyclic shape references.
const MAX_DEPTH: usize = 40;

/// Validate a shape map against a data graph using a parsed schema (CONCEPT:EG-133).
pub fn validate(schema: &Schema, data: &Graph, shape_map: &ShapeMap) -> ShexReport {
    let v = Validator { schema, data };
    let mut results = Vec::with_capacity(shape_map.entries.len());
    for entry in &shape_map.entries {
        let label = if entry.shape == START { START } else { &entry.shape };
        let outcome = v.conforms_to_label(&entry.node, &entry.shape, 0);
        results.push(NodeResult {
            node: entry.node.to_string(),
            shape: label.to_string(),
            conforms: outcome.is_ok(),
            reason: outcome.err(),
        });
    }
    ShexReport::from_results(results)
}

/// Convenience: parse a ShExJ schema + a Turtle data graph and validate a shape map given
/// as `(node_iri, shape_label)` string pairs.
pub fn validate_shexj(
    schema_json: &str,
    data_ttl: &str,
    pairs: &[(&str, &str)],
) -> Result<ShexReport, String> {
    let schema = Schema::from_shexj(schema_json)?;
    let data = graph_from_turtle(data_ttl)?;
    let map = ShapeMap::from_iri_pairs(pairs);
    Ok(validate(&schema, &data, &map))
}

/// Build an `oxrdf::Graph` from a Turtle document via eg-rdf's parser.
pub fn graph_from_turtle(ttl: &str) -> Result<Graph, String> {
    let triples = eg_rdf::mapping::parse_turtle(ttl)?;
    let mut g = Graph::new();
    for t in triples {
        g.insert(&t);
    }
    Ok(g)
}

struct Validator<'a> {
    schema: &'a Schema,
    data: &'a Graph,
}

/// A focus node's outgoing arc — a `(predicate, object)` pair.
struct Arc {
    predicate: String,
    object: Term,
}

impl Validator<'_> {
    /// Does `node` conform to the shape identified by `label`? `Ok(())` ⇒ conforms;
    /// `Err(reason)` ⇒ a failure explanation.
    fn conforms_to_label(&self, node: &Term, label: &str, depth: usize) -> Result<(), String> {
        let expr = self
            .schema
            .resolve(label)
            .ok_or_else(|| format!("unknown shape label `{label}`"))?;
        self.satisfies(node, expr, depth)
    }

    /// Does `node` satisfy the shape expression `expr`?
    fn satisfies(&self, node: &Term, expr: &ShapeExpr, depth: usize) -> Result<(), String> {
        if depth > MAX_DEPTH {
            return Ok(()); // cyclic-reference guard — assume conformance to break the loop
        }
        match expr {
            ShapeExpr::NodeConstraint(nc) => self.check_node_constraint(node, nc),
            ShapeExpr::Shape(shape) => self.match_shape(node, shape, depth),
            ShapeExpr::And(list) => {
                for e in list {
                    self.satisfies(node, e, depth)?;
                }
                Ok(())
            }
            ShapeExpr::Or(list) => {
                if list.iter().any(|e| self.satisfies(node, e, depth).is_ok()) {
                    Ok(())
                } else {
                    Err("node satisfies none of the ShapeOr branches".into())
                }
            }
            ShapeExpr::Not(inner) => match self.satisfies(node, inner, depth) {
                Ok(()) => Err("node satisfies a ShapeNot (negated) expression".into()),
                Err(_) => Ok(()),
            },
            ShapeExpr::Ref(label) => self.conforms_to_label(node, label, depth + 1),
        }
    }

    // ── Node constraints ─────────────────────────────────────────────────
    fn check_node_constraint(&self, node: &Term, nc: &NodeConstraint) -> Result<(), String> {
        if let Some(kind) = nc.node_kind {
            if !node_kind_ok(node, kind) {
                return Err(format!("node {node} has the wrong nodeKind"));
            }
        }
        if let Some(dt) = &nc.datatype {
            let ok = matches!(node, Term::Literal(l) if l.datatype().as_str() == dt.as_str());
            if !ok {
                return Err(format!("node {node} is not of datatype <{dt}>"));
            }
        }
        if let Some(values) = &nc.values {
            if !values.iter().any(|v| value_set_matches(node, v)) {
                return Err(format!("node {node} is not in the value set"));
            }
        }
        self.check_string_facets(node, nc)?;
        self.check_numeric_facets(node, nc)?;
        Ok(())
    }

    fn check_string_facets(&self, node: &Term, nc: &NodeConstraint) -> Result<(), String> {
        let f = &nc.string_facets;
        if f.length.is_none()
            && f.minlength.is_none()
            && f.maxlength.is_none()
            && f.pattern.is_none()
        {
            return Ok(());
        }
        let Some(s) = lexical(node) else {
            return Err(format!("node {node} has no lexical form for a string facet"));
        };
        let len = s.chars().count();
        if let Some(n) = f.length {
            if len != n {
                return Err(format!("node {node} length {len} != {n}"));
            }
        }
        if let Some(n) = f.minlength {
            if len < n {
                return Err(format!("node {node} shorter than minlength {n}"));
            }
        }
        if let Some(n) = f.maxlength {
            if len > n {
                return Err(format!("node {node} longer than maxlength {n}"));
            }
        }
        if let Some(pat) = &f.pattern {
            if !pattern_ok(&s, pat, f.flags.as_deref()) {
                return Err(format!("node {node} does not match pattern {pat}"));
            }
        }
        Ok(())
    }

    fn check_numeric_facets(&self, node: &Term, nc: &NodeConstraint) -> Result<(), String> {
        let f = &nc.numeric_facets;
        if f.mininclusive.is_none()
            && f.maxinclusive.is_none()
            && f.minexclusive.is_none()
            && f.maxexclusive.is_none()
        {
            return Ok(());
        }
        let Some(x) = lexical(node).and_then(|s| s.trim().parse::<f64>().ok()) else {
            return Err(format!("node {node} is not numeric for a numeric facet"));
        };
        if let Some(b) = f.mininclusive {
            if x < b {
                return Err(format!("node {node} < mininclusive {b}"));
            }
        }
        if let Some(b) = f.maxinclusive {
            if x > b {
                return Err(format!("node {node} > maxinclusive {b}"));
            }
        }
        if let Some(b) = f.minexclusive {
            if x <= b {
                return Err(format!("node {node} <= minexclusive {b}"));
            }
        }
        if let Some(b) = f.maxexclusive {
            if x >= b {
                return Err(format!("node {node} >= maxexclusive {b}"));
            }
        }
        Ok(())
    }

    // ── Shapes ─────────────────────────────────────────────────────────────
    fn match_shape(&self, node: &Term, shape: &Shape, depth: usize) -> Result<(), String> {
        let arcs = self.outgoing_arcs(node);
        let mut consumed = vec![false; arcs.len()];

        if let Some(expr) = &shape.expression {
            self.match_triple_expr(expr, &arcs, &mut consumed, depth)?;
        }

        if shape.closed {
            for (i, arc) in arcs.iter().enumerate() {
                if !consumed[i] && !shape.extra.iter().any(|p| p == &arc.predicate) {
                    return Err(format!(
                        "closed shape rejects extra arc <{}> on node {node}",
                        arc.predicate
                    ));
                }
            }
        }
        Ok(())
    }

    /// The outgoing arcs of a focus node (empty for a literal — it has no subject arcs).
    fn outgoing_arcs(&self, node: &Term) -> Vec<Arc> {
        let subject: NamedOrBlankNodeRef<'_> = match node {
            Term::NamedNode(n) => NamedOrBlankNodeRef::NamedNode(n.as_ref()),
            Term::BlankNode(b) => NamedOrBlankNodeRef::BlankNode(b.as_ref()),
            _ => return Vec::new(),
        };
        self.data
            .triples_for_subject(subject)
            .map(|t| Arc {
                predicate: t.predicate.as_str().to_string(),
                object: t.object.into_owned(),
            })
            .collect()
    }

    /// Match a triple expression against the node's arcs, marking consumed arcs.
    fn match_triple_expr(
        &self,
        expr: &TripleExpr,
        arcs: &[Arc],
        consumed: &mut [bool],
        depth: usize,
    ) -> Result<(), String> {
        match expr {
            TripleExpr::TripleConstraint {
                predicate,
                value_expr,
                min,
                max,
                inverse: _,
            } => {
                // Greedy: every not-yet-consumed arc on this predicate whose object
                // satisfies the value expression is matched.
                let mut matched = Vec::new();
                for (i, arc) in arcs.iter().enumerate() {
                    if consumed[i] || &arc.predicate != predicate {
                        continue;
                    }
                    let ok = match value_expr {
                        None => true,
                        Some(ve) => self.satisfies(&arc.object, ve, depth + 1).is_ok(),
                    };
                    if ok {
                        matched.push(i);
                    }
                }
                let count = matched.len() as i64;
                if count < *min {
                    return Err(format!(
                        "predicate <{predicate}> has {count} conforming value(s), need at least {min}"
                    ));
                }
                if *max != -1 && count > *max {
                    return Err(format!(
                        "predicate <{predicate}> has {count} conforming value(s), at most {max} allowed"
                    ));
                }
                for i in matched {
                    consumed[i] = true;
                }
                Ok(())
            }
            TripleExpr::EachOf(list) => {
                for e in list {
                    self.match_triple_expr(e, arcs, consumed, depth)?;
                }
                Ok(())
            }
            TripleExpr::OneOf(list) => {
                // Alternation: commit the first branch that matches on a trial partition.
                for branch in list {
                    let mut trial = consumed.to_vec();
                    if self.match_triple_expr(branch, arcs, &mut trial, depth).is_ok() {
                        consumed.copy_from_slice(&trial);
                        return Ok(());
                    }
                }
                Err("no branch of a OneOf triple expression matched".into())
            }
        }
    }
}

// ── Term-level helpers ─────────────────────────────────────────────────────

fn node_kind_ok(node: &Term, kind: NodeKind) -> bool {
    match kind {
        NodeKind::Iri => matches!(node, Term::NamedNode(_)),
        NodeKind::BNode => matches!(node, Term::BlankNode(_)),
        NodeKind::Literal => matches!(node, Term::Literal(_)),
        NodeKind::NonLiteral => matches!(node, Term::NamedNode(_) | Term::BlankNode(_)),
    }
}

fn value_set_matches(node: &Term, v: &ValueSetValue) -> bool {
    match v {
        ValueSetValue::Iri(iri) => matches!(node, Term::NamedNode(n) if n.as_str() == iri),
        ValueSetValue::Literal {
            value,
            datatype,
            language,
        } => match node {
            Term::Literal(l) => {
                if l.value() != value {
                    return false;
                }
                if let Some(dt) = datatype {
                    if l.datatype().as_str() != dt.as_str() {
                        return false;
                    }
                }
                if let Some(lang) = language {
                    if l.language() != Some(lang.as_str()) {
                        return false;
                    }
                }
                true
            }
            _ => false,
        },
    }
}

/// The lexical form of a term for string/numeric facets; `None` for a blank node.
fn lexical(t: &Term) -> Option<String> {
    match t {
        Term::Literal(l) => Some(l.value().to_string()),
        Term::NamedNode(n) => Some(n.as_str().to_string()),
        _ => None,
    }
}

fn pattern_ok(s: &str, pattern: &str, flags: Option<&str>) -> bool {
    let mut b = RegexBuilder::new(pattern);
    if let Some(f) = flags {
        b.case_insensitive(f.contains('i'));
        b.multi_line(f.contains('m'));
        b.dot_matches_new_line(f.contains('s'));
        b.ignore_whitespace(f.contains('x'));
    }
    match b.build() {
        Ok(re) => re.is_match(s),
        Err(_) => false,
    }
}
