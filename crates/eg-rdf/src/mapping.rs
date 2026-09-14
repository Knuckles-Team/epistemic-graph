//! W1 — Native RDF ⇄ property-graph mapping over `GraphCore` (CONCEPT:EG-KG.ontology.kg-native-rdf-sparql).
//!
//! THE MAPPING (verbatim and lossless):
//!
//! | RDF construct | engine representation |
//! |---|---|
//! | IRI / blank-node **subject or object** | a graph **node**, id = canonical term string (`<iri>` / `_:b`); the IRI is interned in [`IriStore`] so the node id is a stable handle. |
//! | triple with a **resource object** `(s,p,o)` | a **typed edge** `s --p--> o`, edge-blob `{ "relationship": p }` — the engine's canonical edge-relation field. |
//! | triple with a **literal object** `(s,p,"lit"^^dt@lang)` | a **node property** on `s`: `p -> {value, datatype, lang}` (a typed cell INSIDE the JSON property blob, so the xsd datatype + language tag survive). |
//! | `rdf:type` | folded into the node `"type"` property (lights up the engine's `type`-based label index) AND kept as an explicit typing edge so multi-typed resources round-trip. |
//! | **named graph** | a `GraphCore` in the multi-graph registry — a graph name IS a named graph. A `:NamedGraph` marker node records the container (see [`NAMED_GRAPH_MARKER`]). |
//!
//! **The one formerly lossy edge:** a node property map is key-unique, so a
//! subject with two different literals for the SAME predicate needs an auxiliary
//! representation. Extras are now retained under [`RDF_MULTI_VALUE_KEY`] in the
//! same authoritative node blob as the query-fast first value. Consequently the
//! full RDF dataset participates in the graph's MutationBatch transaction and has
//! one commit authority.
//!
//! Round-trip: parse Turtle/N-Triples → store into GraphCore → serialize back to
//! N-Triples, and the triple SET is equal (semantic equality — bnode labels and
//! triple order are not significant in RDF).

use std::collections::{BTreeMap, BTreeSet, HashMap};

use eg_core::graph::GraphCore;
use oxrdf::{BlankNode, GraphName, Literal, NamedNode, NamedOrBlankNode, Quad, Term, Triple};
use oxttl::{
    NQuadsParser, NQuadsSerializer, NTriplesParser, NTriplesSerializer, TriGParser, TriGSerializer,
    TurtleParser, TurtleSerializer,
};

/// The engine `type` of the marker node that records "this graph is an RDF named
/// graph" (the `:NamedGraph` node shape linking the RDF surface to the registry).
pub const NAMED_GRAPH_MARKER: &str = "NamedGraph";

/// Reserved node-property key holding `{ predicate_iri: [typed_literal, ...] }`
/// for second-and-later literal values. The first value stays at its ordinary
/// predicate key for property-graph query compatibility.
pub const RDF_MULTI_VALUE_KEY: &str = "__rdf_multivalue_literals";

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

// ── A18 TBox/ABox RLS distinction (CONCEPT:EG-KG.sharding.row-level-security) ──────────
//
// Row-level default-deny (`eg_core::isolation::can_see_row`) protects ABox rows -- a
// row ABOUT someone/something whose owner controls its visibility. An OWL axiom /
// class / property-definition node is SCHEMA (TBox), not a row about anyone, and is
// already protected at the correct granularity by GRAPH-level ACL. Applying row-level
// default-deny to it is a category error whose consequence was a graph's whole schema
// going invisible to every non-`System` actor once a caller had no way to tag a
// SPARQL-staged axiom's class nodes (`_visibility`/`_owner` are not valid absolute
// IRIs, so SPARQL UPDATE has no syntactic way to set them).
//
// This module structurally identifies TBox nodes -- the subject/object of a
// RECOGNIZED RDFS/OWL 2 schema predicate, or the subject of an explicit
// `rdf:type owl:Class`/... declaration -- and records them in
// `GraphCore::schema_refs` (BUG A3, 2026-08-12), a live reverse index
// (`iri -> count of live schema-defining triples`) `can_see_row`/`filter_view`
// consult by node id, DERIVED fresh on every read rather than a `_schema`
// property key cached on the node's own blob. The prior blob-stamp approach
// was write-once: nothing ever cleared it on axiom deletion, so an IRI once
// used as a schema term stayed schema-visible forever even after being
// repurposed as an ordinary ABox individual. Graph-level ACL (unaffected)
// still gates whether a caller reaches row filtering at all. Never a name
// convention on the node id.
//
// Deliberately NARROW: `owl:sameAs`/`owl:differentFrom` state facts ABOUT
// INDIVIDUALS (ABox, even though the reasoner processes them), and `owl:oneOf`'s list
// members can themselves be individuals -- none of these are included, so this can
// NEVER mark an ABox individual as schema-visible by association. Only the
// well-established two-node class/property axiom shapes below are recognized.

/// RDFS/OWL 2 schema predicates: a resource-object triple using one of these names an
/// axiom ABOUT a class/property (TBox), never a fact about an individual (ABox) -- see
/// the module-level A18 note above. `pub(crate)` so `crate::update` (the SPARQL UPDATE
/// write path) shares this ONE vocabulary rather than duplicating it.
pub(crate) const TBOX_SCHEMA_PREDICATES: &[&str] = &[
    "http://www.w3.org/2000/01/rdf-schema#subClassOf",
    "http://www.w3.org/2000/01/rdf-schema#subPropertyOf",
    "http://www.w3.org/2000/01/rdf-schema#domain",
    "http://www.w3.org/2000/01/rdf-schema#range",
    "http://www.w3.org/2002/07/owl#equivalentClass",
    "http://www.w3.org/2002/07/owl#equivalentProperty",
    "http://www.w3.org/2002/07/owl#disjointWith",
    "http://www.w3.org/2002/07/owl#propertyDisjointWith",
    "http://www.w3.org/2002/07/owl#inverseOf",
    "http://www.w3.org/2002/07/owl#onProperty",
    "http://www.w3.org/2002/07/owl#someValuesFrom",
    "http://www.w3.org/2002/07/owl#allValuesFrom",
    "http://www.w3.org/2002/07/owl#hasValue",
    "http://www.w3.org/2002/07/owl#intersectionOf",
    "http://www.w3.org/2002/07/owl#unionOf",
];

/// `rdf:type` OBJECTS that make the triple's SUBJECT a schema (TBox) declaration -- a
/// class/property/ontology/restriction definition -- rather than an ABox
/// instance-typing fact. An individual typed with an ORDINARY user class
/// (`ex:robot1 rdf:type ex:Robot`) is NOT matched here (`ex:Robot` is not in this
/// list), so ordinary instance data is unaffected; only an explicit declaration that
/// the SUBJECT ITSELF is a class/property/ontology resource is schema. `pub(crate)`,
/// shared with `crate::update` -- see [`TBOX_SCHEMA_PREDICATES`].
pub(crate) const TBOX_TYPE_OBJECTS: &[&str] = &[
    "http://www.w3.org/2002/07/owl#Class",
    "http://www.w3.org/2000/01/rdf-schema#Class",
    "http://www.w3.org/2002/07/owl#ObjectProperty",
    "http://www.w3.org/2002/07/owl#DatatypeProperty",
    "http://www.w3.org/2002/07/owl#AnnotationProperty",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property",
    "http://www.w3.org/2002/07/owl#Restriction",
    "http://www.w3.org/2002/07/owl#Ontology",
];

/// IRI interning: a string IRI ↔ a small integer handle. A production deployment
/// can back this with a redb table (`iri_id ↔ iri_string`) so node ids are compact
/// u64s; here the bidirectional map is in memory and the term string is the node
/// id, but the interner proves the handle indirection and dedups repeated IRIs.
#[derive(Default)]
pub struct IriStore {
    fwd: HashMap<String, u64>,
    rev: Vec<String>,
}

impl IriStore {
    pub fn intern(&mut self, iri: &str) -> u64 {
        if let Some(&id) = self.fwd.get(iri) {
            return id;
        }
        let id = self.rev.len() as u64;
        self.fwd.insert(iri.to_string(), id);
        self.rev.push(iri.to_string());
        id
    }
    pub fn resolve(&self, id: u64) -> Option<&str> {
        self.rev.get(id as usize).map(|s| s.as_str())
    }
    pub fn len(&self) -> usize {
        self.rev.len()
    }
    pub fn is_empty(&self) -> bool {
        self.rev.is_empty()
    }
}

/// A typed literal cell, stored inside the node property JSON so the xsd datatype
/// and language tag survive the property-graph blob.
pub fn literal_to_cell(lit: &Literal) -> serde_json::Value {
    let mut m = serde_json::Map::new();
    m.insert(
        "value".into(),
        serde_json::Value::String(lit.value().to_string()),
    );
    m.insert(
        "datatype".into(),
        serde_json::Value::String(lit.datatype().as_str().to_string()),
    );
    if let Some(lang) = lit.language() {
        m.insert("lang".into(), serde_json::Value::String(lang.to_string()));
    }
    serde_json::Value::Object(m)
}

/// Reconstruct an oxrdf [`Literal`] from a stored typed cell.
pub fn cell_to_literal(cell: &serde_json::Value) -> Option<Literal> {
    let obj = cell.as_object()?;
    let value = obj.get("value")?.as_str()?;
    let dt = obj.get("datatype").and_then(|d| d.as_str());
    let lang = obj.get("lang").and_then(|l| l.as_str());
    Some(match (lang, dt) {
        (Some(l), _) => Literal::new_language_tagged_literal(value, l).ok()?,
        (None, Some(d)) => Literal::new_typed_literal(value, NamedNode::new(d).ok()?),
        (None, None) => Literal::new_simple_literal(value),
    })
}

/// Canonical node id for a subject (IRI or blank node).
fn subject_id(s: &NamedOrBlankNode) -> String {
    match s {
        NamedOrBlankNode::NamedNode(n) => format!("<{}>", n.as_str()),
        NamedOrBlankNode::BlankNode(b) => format!("_:{}", b.as_str()),
        #[allow(unreachable_patterns)]
        _ => unreachable!("RDF-1.1 subjects are IRI or bnode"),
    }
}

/// Canonical node id for an IRI/bnode term (a resource object); `None` for a literal.
fn term_node_id(t: &Term) -> Option<String> {
    match t {
        Term::NamedNode(n) => Some(format!("<{}>", n.as_str())),
        Term::BlankNode(b) => Some(format!("_:{}", b.as_str())),
        Term::Literal(_) => None,
        #[allow(unreachable_patterns)]
        _ => None,
    }
}

mod canonical;
mod lowering;
mod parsing;
mod serialization;

pub use canonical::{quad_set_key, triple_set_key};
pub use eg_types::rdf_report::LoadReport;
pub use lowering::{load_triples, lower_triples, register_named_graph, LoweredTripleGraph};
#[cfg(feature = "json-ld")]
pub use parsing::parse_jsonld;
pub use parsing::{from_nquads, from_trig, parse_nquads, parse_ntriples, parse_trig, parse_turtle};
#[cfg(feature = "rdf-xml")]
pub use parsing::{from_rdfxml, parse_rdfxml};
#[cfg(feature = "json-ld")]
pub use serialization::to_jsonld;
#[cfg(feature = "rdf-xml")]
pub use serialization::to_rdfxml;
pub use serialization::{export_triples, to_nquads, to_ntriples, to_trig, to_turtle};

/// Extract the lexical value of a node property cell.
///
/// A typed RDF cell is the JSON object `{value, datatype, lang}` (the `AddTriples`
/// shape this module maps to and from); a native-LPG scalar is a bare string / number /
/// bool. Arrays, objects without `value`, and null yield `None`. The SPARQL read path
/// and the SPARQL Update write path must agree on this exactly, so it lives with the
/// cell shape rather than once per consumer.
#[cfg(feature = "sparql")]
pub(crate) fn cell_lexical(cell: &serde_json::Value) -> Option<String> {
    match cell {
        serde_json::Value::Object(m) => m.get("value").and_then(|v| v.as_str()).map(String::from),
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TTL: &str = r#"
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:alice a ex:Person ;
         ex:name "Alice" ;
         ex:age "30"^^xsd:integer ;
         ex:knows ex:bob .
ex:bob  a ex:Person ;
        ex:name "Bob"@en .
_:anon  ex:name "Anon" ;
        ex:knows ex:alice .
"#;

    /// W1 round-trip: Turtle IN → GraphCore → N-Triples OUT, semantically equal,
    /// with a blank node, an xsd:integer datatype, and an @en language tag.
    #[test]
    fn turtle_round_trips_through_property_graph() {
        let parsed = parse_turtle(TTL).expect("parse turtle");
        // alice: type, name, age, knows (4) + bob: type, name (2) + anon: name, knows (2)
        assert_eq!(parsed.len(), 8, "expected 8 triples, got {}", parsed.len());

        let core = GraphCore::new();
        let mut iris = IriStore::default();
        let report = load_triples(&core, &mut iris, "g", parsed.clone()).expect("load");
        assert_eq!(report.triples, 8);
        assert_eq!(report.multivalue, 0);
        assert!(iris.len() >= 4, "interned {} iris", iris.len());

        let exported = export_triples(&core, "g").expect("export");
        let in_key = triple_set_key(&parsed);
        let out_key = triple_set_key(&exported);
        assert_eq!(
            in_key, out_key,
            "triple set must round-trip.\n  IN:  {in_key:#?}\n  OUT: {out_key:#?}"
        );

        // Byte-level N-Triples sanity: serialize OUT then reparse to the same set.
        let nt = to_ntriples(&exported).expect("to nt");
        let reparsed = parse_ntriples(&nt).expect("reparse nt");
        assert_eq!(triple_set_key(&reparsed), out_key);
    }

    /// W1: the xsd:integer datatype and @en language tag survive the property blob.
    #[test]
    fn typed_literal_datatype_and_lang_survive() {
        let parsed = parse_turtle(TTL).unwrap();
        let core = GraphCore::new();
        let mut iris = IriStore::default();
        load_triples(&core, &mut iris, "g", parsed).unwrap();
        let exported = export_triples(&core, "g").unwrap();
        let age = exported
            .iter()
            .find(|t| t.predicate.as_str().ends_with("age"))
            .expect("age triple present");
        if let Term::Literal(l) = &age.object {
            assert_eq!(
                l.datatype().as_str(),
                "http://www.w3.org/2001/XMLSchema#integer",
                "xsd:integer datatype must survive"
            );
        } else {
            panic!("age object is not a literal");
        }
        let name_en = exported.iter().find(|t| {
            t.predicate.as_str().ends_with("name")
                && matches!(&t.object, Term::Literal(l) if l.language() == Some("en"))
        });
        assert!(name_en.is_some(), "@en language tag must survive");
    }

    /// A multi-valued predicate is lossless in the authoritative graph image.
    #[test]
    fn multivalue_without_store_is_embedded_losslessly() {
        let ttl = r#"
@prefix ex: <http://example.org/> .
ex:x ex:tag "a" , "b" , "c" .
"#;
        let parsed = parse_turtle(ttl).unwrap();
        assert_eq!(parsed.len(), 3);
        let core = GraphCore::new();
        let mut iris = IriStore::default();
        let report = load_triples(&core, &mut iris, "g", parsed).unwrap();
        assert_eq!(report.multivalue, 2, "two extras beyond the first tag");
        let exported = export_triples(&core, "g").unwrap();
        let tags: Vec<_> = exported
            .iter()
            .filter(|t| t.predicate.as_str().ends_with("tag"))
            .collect();
        assert_eq!(tags.len(), 3, "all values land in the authoritative blob");
    }

    #[test]
    fn named_graph_marker_is_registered_and_not_exported() {
        let core = GraphCore::new();
        register_named_graph(&core, "my:graph");
        assert!(core.has_node("__named_graph__:my:graph"));
        // The marker must not leak into exported RDF.
        let exported = export_triples(&core, "my:graph").unwrap();
        assert!(exported.is_empty(), "marker node must not export as RDF");
    }

    // ── EG-131: RDF serialization matrix round-trips ────────────────────────

    /// Reproject parsed quads back to triples for set-equality against the input.
    fn quads_as_triples(quads: &[Quad]) -> Vec<Triple> {
        quads
            .iter()
            .map(|q| Triple::new(q.subject.clone(), q.predicate.clone(), q.object.clone()))
            .collect()
    }

    /// EG-131: N-Quads serialize → parse round-trips the triple set AND the graph name.
    #[test]
    fn nquads_round_trips() {
        let triples = parse_turtle(TTL).unwrap();
        let g = "http://example.org/g";
        let nq = to_nquads(&triples, Some(g)).unwrap();
        let quads = parse_nquads(&nq).unwrap();
        assert_eq!(
            triple_set_key(&triples),
            triple_set_key(&quads_as_triples(&quads)),
            "N-Quads must round-trip the triple set"
        );
        assert!(
            quads
                .iter()
                .all(|q| matches!(&q.graph_name, GraphName::NamedNode(n) if n.as_str() == g)),
            "every quad must carry the named graph"
        );
    }

    /// EG-131: TriG serialize → parse round-trips the triple set AND the graph name.
    #[test]
    fn trig_round_trips() {
        let triples = parse_turtle(TTL).unwrap();
        let g = "http://example.org/g";
        let trig = to_trig(&triples, Some(g)).unwrap();
        let quads = parse_trig(&trig).unwrap();
        assert_eq!(
            triple_set_key(&triples),
            triple_set_key(&quads_as_triples(&quads)),
            "TriG must round-trip the triple set"
        );
        assert!(
            quads
                .iter()
                .all(|q| matches!(&q.graph_name, GraphName::NamedNode(n) if n.as_str() == g)),
            "every quad must carry the named graph"
        );
    }

    /// EG-131: RDF/XML serialize → parse round-trips the triple set (feature `rdf-xml`).
    #[cfg(feature = "rdf-xml")]
    #[test]
    fn rdfxml_round_trips() {
        let triples = parse_turtle(TTL).unwrap();
        let xml = to_rdfxml(&triples).unwrap();
        let reparsed = parse_rdfxml(&xml).unwrap();
        assert_eq!(
            triple_set_key(&triples),
            triple_set_key(&reparsed),
            "RDF/XML must round-trip the triple set"
        );
    }

    /// EG-131: JSON-LD serialize → parse round-trips the triple set (feature `json-ld`).
    #[cfg(feature = "json-ld")]
    #[test]
    fn jsonld_round_trips() {
        let triples = parse_turtle(TTL).unwrap();
        let jld = to_jsonld(&triples, None).unwrap();
        let quads = parse_jsonld(&jld).unwrap();
        assert_eq!(
            triple_set_key(&triples),
            triple_set_key(&quads_as_triples(&quads)),
            "JSON-LD must round-trip the triple set"
        );
    }

    // ── EG-137: RDF 1.1 concrete-syntax matrix via the `from_*` reader surface ──

    /// EG-137: N-Quads `to_nquads` → `from_nquads` round-trips the triple set AND the
    /// named graph term (the quad-carried graph is preserved).
    #[test]
    fn eg137_nquads_from_reader_round_trips_named_graph() {
        let triples = parse_turtle(TTL).unwrap();
        let g = "http://example.org/g137";
        let nq = to_nquads(&triples, Some(g)).unwrap();
        let quads = from_nquads(&nq).unwrap();
        assert_eq!(
            triple_set_key(&triples),
            triple_set_key(&quads_as_triples(&quads)),
        );
        assert!(quads
            .iter()
            .all(|q| matches!(&q.graph_name, GraphName::NamedNode(n) if n.as_str() == g)));
    }

    /// EG-137: TriG `to_trig` → `from_trig` round-trips the triple set AND the named graph.
    #[test]
    fn eg137_trig_from_reader_round_trips_named_graph() {
        let triples = parse_turtle(TTL).unwrap();
        let g = "http://example.org/g137";
        let trig = to_trig(&triples, Some(g)).unwrap();
        let quads = from_trig(&trig).unwrap();
        assert_eq!(
            triple_set_key(&triples),
            triple_set_key(&quads_as_triples(&quads)),
        );
        assert!(quads
            .iter()
            .all(|q| matches!(&q.graph_name, GraphName::NamedNode(n) if n.as_str() == g)));
    }

    /// EG-137: RDF/XML `to_rdfxml` → `from_rdfxml` round-trips the triple set (feature
    /// `rdf-xml`).
    #[cfg(feature = "rdf-xml")]
    #[test]
    fn eg137_rdfxml_from_reader_round_trips() {
        let triples = parse_turtle(TTL).unwrap();
        let xml = to_rdfxml(&triples).unwrap();
        let reparsed = from_rdfxml(&xml).unwrap();
        assert_eq!(triple_set_key(&triples), triple_set_key(&reparsed));
    }

    // ── EG-130: RDF-star / SPARQL-star (RDF 1.2) ────────────────────────────

    /// EG-130: a quoted triple `<< s p o >>` parses (RDF 1.2 reifying-triple-term form:
    /// a base triple + an `rdf:reifies` triple whose OBJECT is a first-class
    /// `Term::Triple`) and round-trips through N-Triples-star, set-equal.
    #[cfg(feature = "sparql-star")]
    #[test]
    fn eg130_quoted_triple_round_trips() {
        let ttl = "@prefix ex: <http://example.org/> .\nex:s ex:p << ex:a ex:b ex:c >> .";
        let parsed = parse_turtle(ttl).expect("parse quoted-triple term");
        assert!(
            parsed.iter().any(|t| matches!(&t.object, Term::Triple(_))),
            "a first-class quoted-triple term must be present"
        );
        let nt = to_ntriples(&parsed).unwrap();
        let reparsed = parse_ntriples(&nt).unwrap();
        assert_eq!(
            triple_set_key(&parsed),
            triple_set_key(&reparsed),
            "the quoted triple must round-trip through N-Triples-star"
        );
    }

    /// EG-130: the annotation syntax `{| p o |}` parses (RDF 1.2) and its quoted triple
    /// term round-trips.
    #[cfg(feature = "sparql-star")]
    #[test]
    fn eg130_annotation_syntax_round_trips() {
        let ttl = "@prefix ex: <http://example.org/> .\nex:a ex:b ex:c {| ex:certainty 0.9 |} .";
        let parsed = parse_turtle(ttl).expect("parse annotation syntax");
        assert!(
            parsed.iter().any(|t| matches!(&t.object, Term::Triple(_))),
            "annotation desugars to a quoted-triple term"
        );
        let reparsed = parse_ntriples(&to_ntriples(&parsed).unwrap()).unwrap();
        assert_eq!(triple_set_key(&parsed), triple_set_key(&reparsed));
    }
}
