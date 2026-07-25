//! Shapes-graph model + parser (CONCEPT:EG-KG.ontology.concept-6).
//!
//! Reads an RDF shapes graph (`eg_rdf::oxrdf::Graph`) into an in-memory [`Shape`] model:
//! node/property shapes, their targets, an (optional) predicate `sh:path`, and the SHACL
//! Core constraint components. Complex property paths (sequence / inverse / alternative /
//! `*`+`?`) are recognised but recorded as [`Path::Unsupported`] — a documented EG-132
//! follow-up — so they never produce a false violation.

use eg_rdf::oxrdf::{Graph, NamedNode, NamedNodeRef, NamedOrBlankNodeRef, Term, TermRef};

use crate::report::Severity;
use crate::vocab;

/// `NamedNodeRef` for a constant IRI (all vocab IRIs are valid, so `_unchecked`).
pub(crate) fn nn(iri: &str) -> NamedNodeRef<'_> {
    NamedNodeRef::new_unchecked(iri)
}

/// Turn a `TermRef` into an owned `Term`.
fn own(t: TermRef<'_>) -> Term {
    t.into_owned()
}

/// A `Term` usable as a subject (IRI / blank node), else `None` for a literal.
pub(crate) fn as_subject_ref(t: &Term) -> Option<NamedOrBlankNodeRef<'_>> {
    match t {
        Term::NamedNode(n) => Some(NamedOrBlankNodeRef::NamedNode(n.as_ref())),
        Term::BlankNode(b) => Some(NamedOrBlankNodeRef::BlankNode(b.as_ref())),
        _ => None,
    }
}

/// SHACL node kinds (`sh:nodeKind`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    BlankNode,
    Iri,
    Literal,
    BlankNodeOrIri,
    BlankNodeOrLiteral,
    IriOrLiteral,
}

impl NodeKind {
    fn from_iri(iri: &str) -> Option<NodeKind> {
        Some(match iri {
            vocab::K_BLANK_NODE => NodeKind::BlankNode,
            vocab::K_IRI => NodeKind::Iri,
            vocab::K_LITERAL => NodeKind::Literal,
            vocab::K_BLANK_NODE_OR_IRI => NodeKind::BlankNodeOrIri,
            vocab::K_BLANK_NODE_OR_LITERAL => NodeKind::BlankNodeOrLiteral,
            vocab::K_IRI_OR_LITERAL => NodeKind::IriOrLiteral,
            _ => return None,
        })
    }
}

/// The bound of an `sh:minInclusive`/… range constraint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeKind {
    MinInclusive,
    MaxInclusive,
    MinExclusive,
    MaxExclusive,
}

/// A shape target.
#[derive(Debug, Clone)]
pub enum Target {
    /// `sh:targetClass` — instances (via `rdf:type`) of the class.
    Class(NamedNode),
    /// `sh:targetNode` — the node itself.
    Node(Term),
    /// `sh:targetSubjectsOf` — subjects of triples with the predicate.
    SubjectsOf(NamedNode),
    /// `sh:targetObjectsOf` — objects of triples with the predicate.
    ObjectsOf(NamedNode),
}

/// A property path. Only predicate paths are resolved; anything else is `Unsupported`.
#[derive(Debug, Clone)]
pub enum Path {
    /// A single predicate `sh:path <p>`.
    Predicate(NamedNode),
    /// A complex path (blank-node path expression) — EG-132 follow-up.
    Unsupported,
}

/// A SHACL Core constraint component.
#[derive(Debug, Clone)]
pub enum Constraint {
    MinCount(usize),
    MaxCount(usize),
    Datatype(NamedNode),
    Class(NamedNode),
    NodeKind(NodeKind),
    Range(RangeKind, Term),
    MinLength(usize),
    MaxLength(usize),
    Pattern {
        pattern: String,
        flags: Option<String>,
    },
    LanguageIn(Vec<String>),
    In(Vec<Term>),
    HasValue(Term),
    And(Vec<Term>),
    Or(Vec<Term>),
    Not(Term),
    Xone(Vec<Term>),
    /// `sh:node` — value nodes must conform to the referenced (node) shape.
    Node(Term),
    /// `sh:property` — value nodes are validated against the referenced property shape.
    Property(Term),
    /// `sh:sparql` — a SPARQL-based constraint (CONCEPT:EG-KG.ontology.concept-6, W3C SHACL-SPARQL §3.5): the
    /// referenced constraint node, resolved on demand via
    /// [`ShapesGraph::parse_sparql_constraint`] (its `sh:select`/`sh:message`/
    /// `sh:prefixes`) by the evaluator ([`crate::validate`]) and the ICV witness
    /// builder ([`crate::icv`]).
    Sparql(Term),
}

/// A resolved `sh:sparql` constraint descriptor (CONCEPT:EG-KG.ontology.concept-6, W3C SHACL-SPARQL §3.5.1):
/// the SELECT query text plus its local `sh:message` override and the prefix
/// declarations resolved from `sh:prefixes` (see [`ShapesGraph::parse_sparql_constraint`]).
#[derive(Debug, Clone)]
pub struct SparqlConstraint {
    /// The `sh:select` query text (a SPARQL 1.1 SELECT using `$this`/`?this`, and
    /// optionally `$PATH`/`?PATH`/`$shapesGraph`/`?shapesGraph`/`$currentShape`/
    /// `?currentShape` as pre-bound variables).
    pub select: String,
    /// `sh:message` on the constraint node itself — overrides the containing
    /// shape's own `sh:message` for results produced by this constraint, mirroring
    /// how [`Shape::message`] is used elsewhere.
    pub message: Option<String>,
    /// Resolved `prefix -> namespace` declarations reachable from `sh:prefixes`
    /// (each value transitively via `owl:imports`), prepended as `PREFIX` lines
    /// before the query is parsed.
    pub prefixes: Vec<(String, String)>,
}

/// A parsed shape (node or property).
#[derive(Debug, Clone)]
pub struct Shape {
    /// The shape node (IRI or blank node).
    pub id: Term,
    /// `sh:path`, present iff this is a property shape.
    pub path: Option<Path>,
    pub targets: Vec<Target>,
    pub constraints: Vec<Constraint>,
    pub message: Option<String>,
    pub severity: Severity,
    pub deactivated: bool,
    /// `sh:closed true` (CONCEPT:EG-KG.ontology.concept-6, W3C SHACL Core §4.6.1): this shape's value nodes may
    /// carry ONLY the properties named by this shape's own `sh:property` paths (see
    /// [`Constraint::Property`]) plus [`Shape::ignored_properties`] — any other
    /// asserted predicate is a `ClosedConstraintComponent` violation.
    pub closed: bool,
    /// `sh:ignoredProperties` — predicates a closed shape allows even though they are
    /// not the path of any of its own property shapes (e.g. `rdf:type`). Empty unless
    /// `closed` is set; meaningless otherwise.
    pub ignored_properties: Vec<NamedNode>,
}

/// A read-only view over a shapes graph that parses [`Shape`]s on demand.
pub struct ShapesGraph<'a> {
    graph: &'a Graph,
}

impl<'a> ShapesGraph<'a> {
    pub fn new(graph: &'a Graph) -> Self {
        ShapesGraph { graph }
    }

    /// The underlying shapes graph — used by [`crate::validate`] to evaluate
    /// `sh:sparql` constraints' `GRAPH $shapesGraph { … }` clauses against it.
    pub(crate) fn graph(&self) -> &'a Graph {
        self.graph
    }

    /// All ROOT shape terms — those with a target (`sh:targetClass`/`targetNode`/
    /// `targetSubjectsOf`/`targetObjectsOf`) — i.e. shapes that anchor validation.
    pub fn root_shapes(&self) -> Vec<Term> {
        let mut seen: Vec<Term> = Vec::new();
        for tp in [
            vocab::TARGET_CLASS,
            vocab::TARGET_NODE,
            vocab::TARGET_SUBJECTS_OF,
            vocab::TARGET_OBJECTS_OF,
        ] {
            for t in self.graph.triples_for_predicate(nn(tp)) {
                let s: Term = t.subject.into_owned().into();
                if !seen.iter().any(|x| x == &s) {
                    seen.push(s);
                }
            }
        }
        seen
    }

    /// Objects of `(subject, predicate)` as owned terms.
    fn objects(&self, subject: &Term, predicate: &str) -> Vec<Term> {
        match as_subject_ref(subject) {
            Some(s) => self
                .graph
                .objects_for_subject_predicate(s, nn(predicate))
                .map(own)
                .collect(),
            None => Vec::new(),
        }
    }

    /// The single object of `(subject, predicate)`, if any.
    fn object(&self, subject: &Term, predicate: &str) -> Option<Term> {
        self.objects(subject, predicate).into_iter().next()
    }

    /// A single non-negative integer object (for `sh:minCount` etc.).
    fn usize_object(&self, subject: &Term, predicate: &str) -> Option<usize> {
        match self.object(subject, predicate)? {
            Term::Literal(l) => l.value().trim().parse::<usize>().ok(),
            _ => None,
        }
    }

    /// A single string object (for `sh:pattern` / `sh:message` / `sh:flags`).
    fn string_object(&self, subject: &Term, predicate: &str) -> Option<String> {
        match self.object(subject, predicate)? {
            Term::Literal(l) => Some(l.value().to_string()),
            Term::NamedNode(n) => Some(n.as_str().to_string()),
            _ => None,
        }
    }

    /// Walk an RDF list (`rdf:first`/`rdf:rest`) from `head` into a term vector.
    fn rdf_list(&self, head: &Term) -> Vec<Term> {
        let mut out = Vec::new();
        let mut cur = head.clone();
        let mut guard = 0usize;
        loop {
            guard += 1;
            if guard > 100_000 {
                break; // cycle guard
            }
            if let Term::NamedNode(n) = &cur {
                if n.as_str() == vocab::RDF_NIL {
                    break;
                }
            }
            let Some(first) = self.object(&cur, vocab::RDF_FIRST) else {
                break;
            };
            out.push(first);
            match self.object(&cur, vocab::RDF_REST) {
                Some(rest) => cur = rest,
                None => break,
            }
        }
        out
    }

    /// Parse the `sh:path` of a shape (predicate path only; else `Unsupported`).
    fn parse_path(&self, subject: &Term) -> Option<Path> {
        match self.object(subject, vocab::PATH)? {
            Term::NamedNode(n) => Some(Path::Predicate(n)),
            _ => Some(Path::Unsupported),
        }
    }

    /// Parse a shape node into the [`Shape`] model.
    pub fn parse_shape(&self, id: &Term) -> Shape {
        let path = self.parse_path(id);
        let mut targets = Vec::new();
        for c in self.objects(id, vocab::TARGET_CLASS) {
            if let Term::NamedNode(n) = c {
                targets.push(Target::Class(n));
            }
        }
        for c in self.objects(id, vocab::TARGET_NODE) {
            targets.push(Target::Node(c));
        }
        for c in self.objects(id, vocab::TARGET_SUBJECTS_OF) {
            if let Term::NamedNode(n) = c {
                targets.push(Target::SubjectsOf(n));
            }
        }
        for c in self.objects(id, vocab::TARGET_OBJECTS_OF) {
            if let Term::NamedNode(n) = c {
                targets.push(Target::ObjectsOf(n));
            }
        }

        let mut constraints = Vec::new();
        if let Some(v) = self.usize_object(id, vocab::MIN_COUNT) {
            constraints.push(Constraint::MinCount(v));
        }
        if let Some(v) = self.usize_object(id, vocab::MAX_COUNT) {
            constraints.push(Constraint::MaxCount(v));
        }
        if let Some(Term::NamedNode(n)) = self.object(id, vocab::DATATYPE) {
            constraints.push(Constraint::Datatype(n));
        }
        if let Some(Term::NamedNode(n)) = self.object(id, vocab::CLASS) {
            constraints.push(Constraint::Class(n));
        }
        if let Some(Term::NamedNode(n)) = self.object(id, vocab::NODE_KIND) {
            if let Some(k) = NodeKind::from_iri(n.as_str()) {
                constraints.push(Constraint::NodeKind(k));
            }
        }
        for (pred, kind) in [
            (vocab::MIN_INCLUSIVE, RangeKind::MinInclusive),
            (vocab::MAX_INCLUSIVE, RangeKind::MaxInclusive),
            (vocab::MIN_EXCLUSIVE, RangeKind::MinExclusive),
            (vocab::MAX_EXCLUSIVE, RangeKind::MaxExclusive),
        ] {
            if let Some(v) = self.object(id, pred) {
                constraints.push(Constraint::Range(kind, v));
            }
        }
        if let Some(v) = self.usize_object(id, vocab::MIN_LENGTH) {
            constraints.push(Constraint::MinLength(v));
        }
        if let Some(v) = self.usize_object(id, vocab::MAX_LENGTH) {
            constraints.push(Constraint::MaxLength(v));
        }
        if let Some(p) = self.string_object(id, vocab::PATTERN) {
            constraints.push(Constraint::Pattern {
                pattern: p,
                flags: self.string_object(id, vocab::FLAGS),
            });
        }
        if let Some(head) = self.object(id, vocab::LANGUAGE_IN) {
            let langs = self
                .rdf_list(&head)
                .into_iter()
                .filter_map(|t| match t {
                    Term::Literal(l) => Some(l.value().to_string()),
                    _ => None,
                })
                .collect();
            constraints.push(Constraint::LanguageIn(langs));
        }
        if let Some(head) = self.object(id, vocab::IN) {
            constraints.push(Constraint::In(self.rdf_list(&head)));
        }
        if let Some(v) = self.object(id, vocab::HAS_VALUE) {
            constraints.push(Constraint::HasValue(v));
        }
        if let Some(head) = self.object(id, vocab::AND) {
            constraints.push(Constraint::And(self.rdf_list(&head)));
        }
        if let Some(head) = self.object(id, vocab::OR) {
            constraints.push(Constraint::Or(self.rdf_list(&head)));
        }
        if let Some(head) = self.object(id, vocab::XONE) {
            constraints.push(Constraint::Xone(self.rdf_list(&head)));
        }
        if let Some(v) = self.object(id, vocab::NOT) {
            constraints.push(Constraint::Not(v));
        }
        for v in self.objects(id, vocab::NODE) {
            constraints.push(Constraint::Node(v));
        }
        for v in self.objects(id, vocab::PROPERTY) {
            constraints.push(Constraint::Property(v));
        }
        for v in self.objects(id, vocab::SPARQL) {
            constraints.push(Constraint::Sparql(v));
        }

        let severity = match self.object(id, vocab::SEVERITY) {
            Some(Term::NamedNode(n)) if n.as_str() == vocab::WARNING => Severity::Warning,
            Some(Term::NamedNode(n)) if n.as_str() == vocab::INFO => Severity::Info,
            _ => Severity::Violation,
        };
        let deactivated = matches!(
            self.object(id, vocab::DEACTIVATED),
            Some(Term::Literal(ref l)) if l.value() == "true"
        );
        let closed = matches!(
            self.object(id, vocab::CLOSED),
            Some(Term::Literal(ref l)) if l.value() == "true"
        );
        let ignored_properties = match self.object(id, vocab::IGNORED_PROPERTIES) {
            Some(head) => self
                .rdf_list(&head)
                .into_iter()
                .filter_map(|t| match t {
                    Term::NamedNode(n) => Some(n),
                    _ => None,
                })
                .collect(),
            None => Vec::new(),
        };

        Shape {
            id: id.clone(),
            path,
            targets,
            constraints,
            message: self.string_object(id, vocab::MESSAGE),
            severity,
            deactivated,
            closed,
            ignored_properties,
        }
    }

    /// Resolve a `sh:sparql` constraint node into its [`SparqlConstraint`] descriptor
    /// (CONCEPT:EG-KG.ontology.concept-6, W3C SHACL-SPARQL §3.5.1): the `sh:select` text, an optional local
    /// `sh:message` override, and the prefix declarations reachable from `sh:prefixes`.
    pub fn parse_sparql_constraint(&self, id: &Term) -> Option<SparqlConstraint> {
        let select = self.string_object(id, vocab::SELECT)?;
        let message = self.string_object(id, vocab::MESSAGE);
        let mut prefixes = Vec::new();
        let mut seen_decls: Vec<Term> = Vec::new();
        for target in self.objects(id, vocab::PREFIXES) {
            self.collect_prefixes(&target, &mut prefixes, &mut seen_decls, 0);
        }
        Some(SparqlConstraint {
            select,
            message,
            prefixes,
        })
    }

    /// Collect `sh:declare [ sh:prefix "p" ; sh:namespace "iri" ]` entries reachable
    /// from `target`, following `owl:imports` transitively (W3C SHACL-SPARQL §3.5.1: "the
    /// object of `sh:prefixes` MAY be linked to a resource that has its own prefix
    /// declarations... e.g. via `owl:imports`"). `seen` guards against an
    /// `owl:imports` cycle; `depth` bounds pathological chains.
    fn collect_prefixes(
        &self,
        target: &Term,
        out: &mut Vec<(String, String)>,
        seen: &mut Vec<Term>,
        depth: usize,
    ) {
        if depth > 16 || seen.iter().any(|t| t == target) {
            return;
        }
        seen.push(target.clone());
        for decl in self.objects(target, vocab::DECLARE) {
            let prefix = self.string_object(&decl, vocab::PREFIX);
            let namespace = self.string_object(&decl, vocab::NAMESPACE);
            if let (Some(p), Some(ns)) = (prefix, namespace) {
                out.push((p, ns));
            }
        }
        for imported in self.objects(target, vocab::OWL_IMPORTS) {
            self.collect_prefixes(&imported, out, seen, depth + 1);
        }
    }
}
