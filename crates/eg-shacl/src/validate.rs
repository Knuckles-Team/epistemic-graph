//! The SHACL Core validation engine (CONCEPT:EG-KG.ontology.concept-6).
//!
//! [`validate`] takes a shapes graph and a data graph (both `eg_rdf::oxrdf::Graph`) and
//! produces a [`ValidationReport`]. The algorithm mirrors the SHACL Core spec:
//!
//! 1. Root shapes = shapes carrying a target. Each target selects **focus nodes** in the
//!    data graph.
//! 2. A shape's **value nodes** are the focus node itself (node shape) or the objects of
//!    its `sh:path` predicate (property shape).
//! 3. Cardinality (`sh:minCount`/`sh:maxCount`), `sh:hasValue`, and `sh:sparql` are
//!    checked over/against the FOCUS (set-level); `sh:closed` is checked per value node
//!    against ITS OWN outgoing properties; every other component is checked per value
//!    node.
//! 4. `sh:node`/`sh:and`/`sh:or`/`sh:not`/`sh:xone` recurse into referenced shapes;
//!    `sh:property` validates each value node against the referenced property shape and
//!    surfaces its results directly.
//!
//! `validate` is fallible: a `sh:sparql` constraint whose query fails to parse, or that
//! uses a construct this engine's [`crate::sparql`] evaluator does not support, aborts
//! the WHOLE validation run with `Err` rather than silently producing an incomplete or
//! wrong report (CONCEPT:EG-KG.ontology.concept-6 — no masking).

use std::cmp::Ordering;

use eg_rdf::oxrdf::{Graph, NamedNode, Term};
use regex::RegexBuilder;

use crate::report::{ValidationReport, ValidationResult};
use crate::shapes::{
    as_subject_ref, nn, Constraint, NodeKind, Path, RangeKind, Shape, ShapesGraph, Target,
};
use crate::sparql::{self, PreBindings};
use crate::vocab;

/// Validate a data graph against a shapes graph (CONCEPT:EG-KG.ontology.concept-6). `Err`
/// iff a `sh:sparql` constraint's query cannot be evaluated (see the module docs).
pub fn validate(shapes_graph: &Graph, data_graph: &Graph) -> Result<ValidationReport, String> {
    let v = Validator {
        shapes: ShapesGraph::new(shapes_graph),
        data: data_graph,
    };
    let mut results = Vec::new();
    for shape_term in v.shapes.root_shapes() {
        let shape = v.shapes.parse_shape(&shape_term);
        if shape.deactivated {
            continue;
        }
        for focus in v.focus_nodes(&shape) {
            v.validate_focus(&shape, &focus, &mut results, 0)?;
        }
    }
    Ok(ValidationReport::from_results(results))
}

struct Validator<'a> {
    shapes: ShapesGraph<'a>,
    data: &'a Graph,
}

/// Guard against pathological cyclic shape references.
const MAX_DEPTH: usize = 40;

impl Validator<'_> {
    /// Focus nodes selected by a shape's targets.
    fn focus_nodes(&self, shape: &Shape) -> Vec<Term> {
        let mut out: Vec<Term> = Vec::new();
        let push = |t: Term, out: &mut Vec<Term>| {
            if !out.iter().any(|x| x == &t) {
                out.push(t);
            }
        };
        for target in &shape.targets {
            match target {
                Target::Node(n) => push(n.clone(), &mut out),
                Target::Class(c) => {
                    for s in self
                        .data
                        .subjects_for_predicate_object(nn(vocab::RDF_TYPE), c.as_ref())
                    {
                        push(s.into_owned().into(), &mut out);
                    }
                }
                Target::SubjectsOf(p) => {
                    for t in self.data.triples_for_predicate(p.as_ref()) {
                        push(t.subject.into_owned().into(), &mut out);
                    }
                }
                Target::ObjectsOf(p) => {
                    for t in self.data.triples_for_predicate(p.as_ref()) {
                        push(t.object.into_owned(), &mut out);
                    }
                }
            }
        }
        out
    }

    /// The value nodes of `shape` for `focus`.
    fn value_nodes(&self, shape: &Shape, focus: &Term) -> Vec<Term> {
        match &shape.path {
            None => vec![focus.clone()],
            Some(Path::Predicate(p)) => match as_subject_ref(focus) {
                Some(s) => self
                    .data
                    .objects_for_subject_predicate(s, p.as_ref())
                    .map(|t| t.into_owned())
                    .collect(),
                None => Vec::new(),
            },
            // Complex path — unsupported (EG-132 follow-up); no value nodes, no results.
            Some(Path::Unsupported) => Vec::new(),
        }
    }

    /// The `sh:resultPath` string for a property shape, if any.
    fn path_str(shape: &Shape) -> Option<String> {
        match &shape.path {
            Some(Path::Predicate(p)) => Some(format!("<{}>", p.as_str())),
            _ => None,
        }
    }

    fn result(
        &self,
        shape: &Shape,
        focus: &Term,
        value: Option<&Term>,
        component: &str,
        default_msg: String,
    ) -> ValidationResult {
        ValidationResult {
            focus_node: focus.to_string(),
            path: Self::path_str(shape),
            value: value.map(|v| v.to_string()),
            source_shape: shape.id.to_string(),
            constraint_component: component.to_string(),
            message: Some(shape.message.clone().unwrap_or(default_msg)),
            severity: shape.severity,
        }
    }

    /// Validate every constraint of `shape` for one `focus` node.
    fn validate_focus(
        &self,
        shape: &Shape,
        focus: &Term,
        out: &mut Vec<ValidationResult>,
        depth: usize,
    ) -> Result<(), String> {
        if depth > MAX_DEPTH {
            return Ok(());
        }
        let values = self.value_nodes(shape, focus);
        for c in &shape.constraints {
            match c {
                // ── Set-level (over all value nodes, or over $this directly) ──
                Constraint::MinCount(n) => {
                    if values.len() < *n {
                        out.push(self.result(
                            shape,
                            focus,
                            None,
                            vocab::CC_MIN_COUNT,
                            format!("fewer than {n} values"),
                        ));
                    }
                }
                Constraint::MaxCount(n) => {
                    if values.len() > *n {
                        out.push(self.result(
                            shape,
                            focus,
                            None,
                            vocab::CC_MAX_COUNT,
                            format!("more than {n} values"),
                        ));
                    }
                }
                Constraint::HasValue(v) => {
                    if !values.iter().any(|x| x == v) {
                        out.push(self.result(
                            shape,
                            focus,
                            None,
                            vocab::CC_HAS_VALUE,
                            format!("missing required value {v}"),
                        ));
                    }
                }
                Constraint::Sparql(constraint_ref) => {
                    self.check_sparql(shape, focus, constraint_ref, out)?;
                }
                // ── Per value node ────────────────────────────────────────
                _ => {
                    for vn in &values {
                        self.check_value(shape, focus, vn, c, out, depth)?;
                    }
                }
            }
        }
        if shape.closed {
            let allowed = self.closed_allowed_predicates(shape);
            for vn in &values {
                self.check_closed(shape, focus, vn, &allowed, out);
            }
        }
        Ok(())
    }

    /// Check one per-value-node constraint against a single value node.
    fn check_value(
        &self,
        shape: &Shape,
        focus: &Term,
        vn: &Term,
        c: &Constraint,
        out: &mut Vec<ValidationResult>,
        depth: usize,
    ) -> Result<(), String> {
        match c {
            Constraint::Datatype(dt) => {
                let ok = matches!(vn, Term::Literal(l) if l.datatype().as_str() == dt.as_str());
                if !ok {
                    out.push(self.result(
                        shape,
                        focus,
                        Some(vn),
                        vocab::CC_DATATYPE,
                        format!("value is not of datatype <{}>", dt.as_str()),
                    ));
                }
            }
            Constraint::Class(cls) => {
                let ok = as_subject_ref(vn).is_some_and(|s| {
                    self.data
                        .objects_for_subject_predicate(s, nn(vocab::RDF_TYPE))
                        .any(|o| matches!(o, eg_rdf::oxrdf::TermRef::NamedNode(n) if n.as_str() == cls.as_str()))
                });
                if !ok {
                    out.push(self.result(
                        shape,
                        focus,
                        Some(vn),
                        vocab::CC_CLASS,
                        format!("value is not an instance of <{}>", cls.as_str()),
                    ));
                }
            }
            Constraint::NodeKind(k) => {
                if !node_kind_ok(vn, *k) {
                    out.push(self.result(
                        shape,
                        focus,
                        Some(vn),
                        vocab::CC_NODE_KIND,
                        "value has the wrong node kind".into(),
                    ));
                }
            }
            Constraint::Range(kind, bound) => {
                if !range_ok(vn, *kind, bound) {
                    let (cc, word) = match kind {
                        RangeKind::MinInclusive => (vocab::CC_MIN_INCLUSIVE, "< minInclusive"),
                        RangeKind::MaxInclusive => (vocab::CC_MAX_INCLUSIVE, "> maxInclusive"),
                        RangeKind::MinExclusive => (vocab::CC_MIN_EXCLUSIVE, "<= minExclusive"),
                        RangeKind::MaxExclusive => (vocab::CC_MAX_EXCLUSIVE, ">= maxExclusive"),
                    };
                    out.push(self.result(
                        shape,
                        focus,
                        Some(vn),
                        cc,
                        format!("value {word} {bound}"),
                    ));
                }
            }
            Constraint::MinLength(n) => {
                if !length_ok(vn, Some(*n), None) {
                    out.push(self.result(
                        shape,
                        focus,
                        Some(vn),
                        vocab::CC_MIN_LENGTH,
                        format!("value shorter than {n}"),
                    ));
                }
            }
            Constraint::MaxLength(n) => {
                if !length_ok(vn, None, Some(*n)) {
                    out.push(self.result(
                        shape,
                        focus,
                        Some(vn),
                        vocab::CC_MAX_LENGTH,
                        format!("value longer than {n}"),
                    ));
                }
            }
            Constraint::Pattern { pattern, flags } => {
                if !pattern_ok(vn, pattern, flags.as_deref()) {
                    out.push(self.result(
                        shape,
                        focus,
                        Some(vn),
                        vocab::CC_PATTERN,
                        format!("value does not match pattern {pattern}"),
                    ));
                }
            }
            Constraint::LanguageIn(langs) => {
                if !language_ok(vn, langs) {
                    out.push(self.result(
                        shape,
                        focus,
                        Some(vn),
                        vocab::CC_LANGUAGE_IN,
                        "value language not in the allowed set".into(),
                    ));
                }
            }
            Constraint::In(list) => {
                if !list.iter().any(|x| x == vn) {
                    out.push(self.result(
                        shape,
                        focus,
                        Some(vn),
                        vocab::CC_IN,
                        "value is not in the allowed set".into(),
                    ));
                }
            }
            // ── Shape-based (recursive) ──────────────────────────────────
            Constraint::Node(shape_ref) => {
                if !self.node_conforms(shape_ref, vn, depth)? {
                    out.push(self.result(
                        shape,
                        focus,
                        Some(vn),
                        vocab::CC_NODE,
                        format!("value does not conform to shape {shape_ref}"),
                    ));
                }
            }
            Constraint::Not(shape_ref) => {
                if self.node_conforms(shape_ref, vn, depth)? {
                    out.push(self.result(
                        shape,
                        focus,
                        Some(vn),
                        vocab::CC_NOT,
                        format!("value conforms to negated shape {shape_ref}"),
                    ));
                }
            }
            Constraint::And(list) => {
                let mut all = true;
                for s in list {
                    if !self.node_conforms(s, vn, depth)? {
                        all = false;
                    }
                }
                if !all {
                    out.push(self.result(
                        shape,
                        focus,
                        Some(vn),
                        vocab::CC_AND,
                        "value does not conform to all sh:and shapes".into(),
                    ));
                }
            }
            Constraint::Or(list) => {
                let mut any = false;
                for s in list {
                    if self.node_conforms(s, vn, depth)? {
                        any = true;
                    }
                }
                if !any {
                    out.push(self.result(
                        shape,
                        focus,
                        Some(vn),
                        vocab::CC_OR,
                        "value conforms to none of the sh:or shapes".into(),
                    ));
                }
            }
            Constraint::Xone(list) => {
                let mut n = 0usize;
                for s in list {
                    if self.node_conforms(s, vn, depth)? {
                        n += 1;
                    }
                }
                if n != 1 {
                    out.push(self.result(
                        shape,
                        focus,
                        Some(vn),
                        vocab::CC_XONE,
                        format!("value conforms to {n} sh:xone shapes (want exactly 1)"),
                    ));
                }
            }
            Constraint::Property(prop_ref) => {
                // Validate the value node against the referenced property shape and
                // surface its results directly (the SHACL sh:property semantics).
                let prop_shape = self.shapes.parse_shape(prop_ref);
                if !prop_shape.deactivated {
                    self.validate_focus(&prop_shape, vn, out, depth + 1)?;
                }
            }
            // Set-level constraints (incl. sh:sparql) never reach here.
            Constraint::MinCount(_)
            | Constraint::MaxCount(_)
            | Constraint::HasValue(_)
            | Constraint::Sparql(_) => {}
        }
        Ok(())
    }

    /// Whether `focus` conforms to the shape identified by `shape_ref` (no results of any
    /// severity). Used by the shape-based logical/`sh:node` components.
    fn node_conforms(&self, shape_ref: &Term, focus: &Term, depth: usize) -> Result<bool, String> {
        if depth > MAX_DEPTH {
            return Ok(true);
        }
        let shape = self.shapes.parse_shape(shape_ref);
        let mut tmp = Vec::new();
        self.validate_focus(&shape, focus, &mut tmp, depth + 1)?;
        Ok(tmp.is_empty())
    }

    // ── sh:sparql (CONCEPT:EG-KG.ontology.concept-6, W3C SHACL-SPARQL §3.5) ─────────────────────────

    /// Evaluate a `sh:sparql` constraint for one focus node: run its `sh:select`
    /// query with `$this` (and, for a property shape, `$PATH`) pre-bound, and turn
    /// every returned solution row into one [`ValidationResult`] — mirroring how
    /// `InvalidResource2`'s TWO offending labels become TWO results in the W3C
    /// SHACL-SPARQL test suite (`sparql/node/sparql-001`), not one.
    fn check_sparql(
        &self,
        shape: &Shape,
        focus: &Term,
        constraint_ref: &Term,
        out: &mut Vec<ValidationResult>,
    ) -> Result<(), String> {
        let sc = self
            .shapes
            .parse_sparql_constraint(constraint_ref)
            .ok_or_else(|| format!("sh:sparql: constraint {constraint_ref} has no sh:select"))?;
        let path_term = match &shape.path {
            Some(Path::Predicate(p)) => Some(Term::NamedNode(p.clone())),
            _ => None,
        };
        let pre = PreBindings {
            this: focus.clone(),
            path: path_term,
            shapes_graph: sparql::shapes_graph_sentinel(),
            current_shape: shape.id.clone(),
        };
        let rows = sparql::eval_select(
            &sc.select,
            &sc.prefixes,
            self.data,
            self.shapes.graph(),
            &pre,
        )?;
        for row in rows {
            let focus_out = row.get("this").cloned().unwrap_or_else(|| focus.clone());
            let path_out = row
                .get("path")
                .map(|t| t.to_string())
                .or_else(|| Self::path_str(shape));
            let value_out = row
                .get("value")
                .cloned()
                .unwrap_or_else(|| focus_out.clone());
            let message = row
                .get("message")
                .map(|t| lexical(t).unwrap_or_else(|| t.to_string()))
                .or_else(|| sc.message.clone())
                .or_else(|| shape.message.clone());
            out.push(ValidationResult {
                focus_node: focus_out.to_string(),
                path: path_out,
                value: Some(value_out.to_string()),
                source_shape: shape.id.to_string(),
                constraint_component: vocab::CC_SPARQL.to_string(),
                message,
                severity: shape.severity,
            });
        }
        Ok(())
    }

    // ── sh:closed (CONCEPT:EG-KG.ontology.concept-6, W3C SHACL Core §4.6.1) ──────────────────────

    /// Predicates a closed `shape`'s value nodes may carry without violating
    /// `sh:closed`: the paths of its own `sh:property` sub-shapes, plus
    /// `sh:ignoredProperties`.
    fn closed_allowed_predicates(&self, shape: &Shape) -> Vec<NamedNode> {
        let mut allowed = shape.ignored_properties.clone();
        for c in &shape.constraints {
            if let Constraint::Property(prop_ref) = c {
                let prop_shape = self.shapes.parse_shape(prop_ref);
                if let Some(Path::Predicate(p)) = prop_shape.path {
                    allowed.push(p);
                }
            }
        }
        allowed
    }

    /// One `ClosedConstraintComponent` violation per (predicate, value) pair
    /// asserted on `vn` whose predicate is not in `allowed` — a literal `vn` has no
    /// outgoing triples, so it trivially satisfies a closed shape.
    fn check_closed(
        &self,
        shape: &Shape,
        focus: &Term,
        vn: &Term,
        allowed: &[NamedNode],
        out: &mut Vec<ValidationResult>,
    ) {
        let Some(subject) = as_subject_ref(vn) else {
            return;
        };
        for t in self.data.triples_for_subject(subject) {
            if allowed.iter().any(|a| a.as_ref() == t.predicate) {
                continue;
            }
            out.push(ValidationResult {
                focus_node: focus.to_string(),
                path: Some(format!("<{}>", t.predicate.as_str())),
                value: Some(t.object.into_owned().to_string()),
                source_shape: shape.id.to_string(),
                constraint_component: vocab::CC_CLOSED.to_string(),
                message: Some(shape.message.clone().unwrap_or_else(|| {
                    format!(
                        "predicate <{}> is not allowed by this closed shape",
                        t.predicate.as_str()
                    )
                })),
                severity: shape.severity,
            });
        }
    }
}

/// SPARQL/XSD-style comparison for `sh:minInclusive` etc.: numeric if both lexical forms
/// parse as `f64`, else a lexical string compare. `None` if the value node has no lexical
/// form (a blank node).
fn cmp_value(vn: &Term, bound: &Term) -> Option<Ordering> {
    let a = lexical(vn)?;
    let b = lexical(bound)?;
    match (a.trim().parse::<f64>(), b.trim().parse::<f64>()) {
        (Ok(x), Ok(y)) => x.partial_cmp(&y),
        _ => Some(a.cmp(&b)),
    }
}

fn range_ok(vn: &Term, kind: RangeKind, bound: &Term) -> bool {
    let Some(ord) = cmp_value(vn, bound) else {
        return false; // incomparable (e.g. a blank-node value) ⇒ violation
    };
    match kind {
        RangeKind::MinInclusive => ord != Ordering::Less,
        RangeKind::MaxInclusive => ord != Ordering::Greater,
        RangeKind::MinExclusive => ord == Ordering::Greater,
        RangeKind::MaxExclusive => ord == Ordering::Less,
    }
}

/// The lexical form of a term for string/range constraints; `None` for a blank node.
fn lexical(t: &Term) -> Option<String> {
    match t {
        Term::Literal(l) => Some(l.value().to_string()),
        Term::NamedNode(n) => Some(n.as_str().to_string()),
        _ => None,
    }
}

fn length_ok(vn: &Term, min: Option<usize>, max: Option<usize>) -> bool {
    let Some(s) = lexical(vn) else {
        return false; // blank node ⇒ length undefined ⇒ violation
    };
    let len = s.chars().count();
    if let Some(m) = min {
        if len < m {
            return false;
        }
    }
    if let Some(m) = max {
        if len > m {
            return false;
        }
    }
    true
}

fn pattern_ok(vn: &Term, pattern: &str, flags: Option<&str>) -> bool {
    let Some(s) = lexical(vn) else {
        return false;
    };
    let mut b = RegexBuilder::new(pattern);
    if let Some(f) = flags {
        b.case_insensitive(f.contains('i'));
        b.multi_line(f.contains('m'));
        b.dot_matches_new_line(f.contains('s'));
        b.ignore_whitespace(f.contains('x'));
    }
    match b.build() {
        Ok(re) => re.is_match(&s),
        Err(_) => false, // a bad pattern can never match ⇒ violation
    }
}

fn language_ok(vn: &Term, langs: &[String]) -> bool {
    match vn {
        Term::Literal(l) => match l.language() {
            Some(tag) => langs.iter().any(|want| lang_range_matches(want, tag)),
            None => false,
        },
        _ => false,
    }
}

/// Basic-language-range match (`en` matches `en`, `en-US`), case-insensitive.
fn lang_range_matches(range: &str, tag: &str) -> bool {
    let range = range.to_ascii_lowercase();
    let tag = tag.to_ascii_lowercase();
    tag == range || tag.strip_prefix(&range).is_some_and(|r| r.starts_with('-'))
}

fn node_kind_ok(vn: &Term, kind: NodeKind) -> bool {
    let is_iri = matches!(vn, Term::NamedNode(_));
    let is_bnode = matches!(vn, Term::BlankNode(_));
    let is_lit = matches!(vn, Term::Literal(_));
    match kind {
        NodeKind::Iri => is_iri,
        NodeKind::BlankNode => is_bnode,
        NodeKind::Literal => is_lit,
        NodeKind::BlankNodeOrIri => is_bnode || is_iri,
        NodeKind::BlankNodeOrLiteral => is_bnode || is_lit,
        NodeKind::IriOrLiteral => is_iri || is_lit,
    }
}

/// Convenience: parse two Turtle documents (shapes + data) and validate. Returns an
/// error string if either document fails to parse, or [`validate`] itself errors.
pub fn validate_turtle(shapes_ttl: &str, data_ttl: &str) -> Result<ValidationReport, String> {
    let shapes = graph_from_turtle(shapes_ttl)?;
    let data = graph_from_turtle(data_ttl)?;
    validate(&shapes, &data)
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
