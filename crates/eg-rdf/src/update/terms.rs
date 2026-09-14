use eg_core::graph::GraphCore;
use oxrdf::{Literal, NamedOrBlankNode, Term};
use spargebra::algebra::GraphTarget;
use spargebra::term::{
    GraphName, GraphNamePattern, GroundQuad, GroundTerm, GroundTermPattern, NamedNodePattern, Quad,
    TermPattern,
};

use crate::sparql::{Binding, Solution};

use super::store::GraphStore;
use super::triples::{delete_triple, insert_triple};
use super::UpdateReport;

// ── ground-data ops (INSERT DATA / DELETE DATA) ─────────────────────────────────

/// Write or remove one resolved `(graph, subject, predicate, object)`. A quad whose graph is
/// absent, or whose object has no representable term, is a no-op — the pre-existing contract.
pub(super) fn apply_resolved_quad(
    store: &dyn GraphStore,
    parts: Option<(Option<String>, String, String, ObjTerm)>,
    report: &mut UpdateReport,
    insert: bool,
) -> Result<(), String> {
    let Some((graph, s, p, obj)) = parts else {
        return Ok(());
    };
    let Some(core) = store.core(graph.as_deref()) else {
        return Ok(());
    };
    if insert {
        if insert_triple(&core, &s, &p, &obj)? {
            report.inserted += 1;
        }
    } else if delete_triple(&core, &s, &p, &obj) {
        report.deleted += 1;
    }
    Ok(())
}

/// A quad of ground data (INSERT DATA / DELETE DATA), resolved to what the store writes.
pub(super) trait GroundQuadData {
    fn resolved(&self) -> Option<(Option<String>, String, String, ObjTerm)>;
}

impl GroundQuadData for Quad {
    fn resolved(&self) -> Option<(Option<String>, String, String, ObjTerm)> {
        Some((
            graph_opt(&self.graph_name),
            nob_id(&self.subject),
            self.predicate.as_str().to_string(),
            obj_from_term(&self.object)?,
        ))
    }
}

impl GroundQuadData for GroundQuad {
    fn resolved(&self) -> Option<(Option<String>, String, String, ObjTerm)> {
        Some((
            graph_opt(&self.graph_name),
            format!("<{}>", self.subject.as_str()),
            self.predicate.as_str().to_string(),
            obj_from_ground(&self.object)?,
        ))
    }
}

/// Apply one per-graph store operation across every graph a SPARQL `GraphTarget` names.
/// `silent` swallows the store's error, per SPARQL 1.1 CLEAR/DROP SILENT.
pub(super) fn apply_to_target(
    store: &dyn GraphStore,
    target: &GraphTarget,
    silent: bool,
    op: &dyn Fn(Option<&str>) -> Result<(), String>,
) -> Result<(), String> {
    match target {
        GraphTarget::DefaultGraph => op(None),
        GraphTarget::NamedNode(n) => op(Some(n.as_str())),
        GraphTarget::NamedGraphs | GraphTarget::AllGraphs => {
            if matches!(target, GraphTarget::AllGraphs) {
                op(None)?;
            }
            for (name, _) in store.named() {
                op(Some(&name))?;
            }
            Ok(())
        }
    }
    .or_else(|e| if silent { Ok(()) } else { Err(e) })
}

// ── the typed object an update writes/removes ───────────────────────────────────

pub(super) enum ObjTerm {
    Resource(String),
    Literal(Literal),
}

pub(super) fn obj_from_term(t: &Term) -> Option<ObjTerm> {
    match t {
        Term::Literal(l) => Some(ObjTerm::Literal(l.clone())),
        Term::NamedNode(n) => Some(ObjTerm::Resource(format!("<{}>", n.as_str()))),
        Term::BlankNode(b) => Some(ObjTerm::Resource(format!("_:{}", b.as_str()))),
        #[allow(unreachable_patterns)]
        _ => None,
    }
}

fn obj_from_ground(t: &GroundTerm) -> Option<ObjTerm> {
    match t {
        GroundTerm::NamedNode(n) => Some(ObjTerm::Resource(format!("<{}>", n.as_str()))),
        GroundTerm::Literal(l) => Some(ObjTerm::Literal(l.clone())),
        #[allow(unreachable_patterns)]
        _ => None,
    }
}

fn nob_id(s: &NamedOrBlankNode) -> String {
    match s {
        NamedOrBlankNode::NamedNode(n) => format!("<{}>", n.as_str()),
        NamedOrBlankNode::BlankNode(b) => format!("_:{}", b.as_str()),
    }
}

fn graph_opt(g: &GraphName) -> Option<String> {
    match g {
        GraphName::DefaultGraph => None,
        GraphName::NamedNode(n) => Some(n.as_str().to_string()),
    }
}

// ── pattern instantiation (DELETE/INSERT WHERE) ─────────────────────────────────

/// A pattern position once spargebra's ground (DELETE) and non-ground (INSERT) term
/// families are collapsed: a formatted resource id, a literal, or a solution variable.
enum PatternTerm<'a> {
    Resource(String),
    Literal(&'a Literal),
    Variable(&'a str),
}

/// The projection both spargebra quad-pattern families share.
trait QuadPatternTerm {
    fn position(&self) -> Option<PatternTerm<'_>>;
}

impl QuadPatternTerm for TermPattern {
    fn position(&self) -> Option<PatternTerm<'_>> {
        match self {
            TermPattern::NamedNode(n) => Some(PatternTerm::Resource(format!("<{}>", n.as_str()))),
            TermPattern::BlankNode(b) => Some(PatternTerm::Resource(format!("_:{}", b.as_str()))),
            TermPattern::Literal(l) => Some(PatternTerm::Literal(l)),
            TermPattern::Variable(v) => Some(PatternTerm::Variable(v.as_str())),
            #[allow(unreachable_patterns)]
            _ => None,
        }
    }
}

impl QuadPatternTerm for GroundTermPattern {
    fn position(&self) -> Option<PatternTerm<'_>> {
        match self {
            GroundTermPattern::NamedNode(n) => {
                Some(PatternTerm::Resource(format!("<{}>", n.as_str())))
            }
            GroundTermPattern::Literal(l) => Some(PatternTerm::Literal(l)),
            GroundTermPattern::Variable(v) => Some(PatternTerm::Variable(v.as_str())),
            #[allow(unreachable_patterns)]
            _ => None,
        }
    }
}

/// A subject must denote a node: a literal (or a literal binding) has no node id.
fn subject_id(t: PatternTerm<'_>, sol: &Solution) -> Option<String> {
    match t {
        PatternTerm::Resource(id) => Some(id),
        PatternTerm::Literal(_) => None,
        PatternTerm::Variable(v) => binding_node_id(sol.get(v)?),
    }
}

fn object_term(t: PatternTerm<'_>, sol: &Solution) -> Option<ObjTerm> {
    match t {
        PatternTerm::Resource(id) => Some(ObjTerm::Resource(id)),
        PatternTerm::Literal(l) => Some(ObjTerm::Literal(l.clone())),
        PatternTerm::Variable(v) => binding_to_obj(sol.get(v)?),
    }
}

/// Instantiate one INSERT/DELETE quad pattern with a solution.
pub(super) fn instantiate<T: QuadPatternTerm>(
    graph: &GraphNamePattern,
    subject: &T,
    predicate: &NamedNodePattern,
    object: &T,
    sol: &Solution,
) -> Option<(Option<String>, String, String, ObjTerm)> {
    Some((
        resolve_graph_pattern(graph, sol)?,
        subject_id(subject.position()?, sol)?,
        pred_from_pattern(predicate, sol)?,
        object_term(object.position()?, sol)?,
    ))
}

/// Outer `Option` = resolvable; inner = `None` default / `Some(iri)` named.
fn resolve_graph_pattern(g: &GraphNamePattern, sol: &Solution) -> Option<Option<String>> {
    match g {
        GraphNamePattern::DefaultGraph => Some(None),
        GraphNamePattern::NamedNode(n) => Some(Some(n.as_str().to_string())),
        GraphNamePattern::Variable(v) => sol
            .get(v.as_str())
            .map(|b| Some(strip_iri(b.as_str()).to_string())),
    }
}

fn pred_from_pattern(p: &NamedNodePattern, sol: &Solution) -> Option<String> {
    match p {
        NamedNodePattern::NamedNode(n) => Some(n.as_str().to_string()),
        NamedNodePattern::Variable(v) => Some(strip_iri(sol.get(v.as_str())?.as_str()).to_string()),
    }
}

fn binding_node_id(b: &Binding) -> Option<String> {
    match b {
        Binding::Node(id) => Some(id.clone()),
        Binding::Literal(_) => None,
    }
}

fn binding_to_obj(b: &Binding) -> Option<ObjTerm> {
    match b {
        Binding::Node(id) => Some(ObjTerm::Resource(id.clone())),
        Binding::Literal(v) => Some(ObjTerm::Literal(Literal::new_simple_literal(v))),
    }
}

fn strip_iri(s: &str) -> &str {
    s.strip_prefix('<')
        .and_then(|x| x.strip_suffix('>'))
        .unwrap_or(s)
}
