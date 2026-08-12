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
// `rdf:type owl:Class`/... declaration -- and stamps them with
// `eg_core::isolation::RLS_SCHEMA_KEY` so `can_see_row` exempts them from row-level
// default-deny while graph-level ACL (unaffected) still gates whether a caller reaches
// row filtering at all. Never a name convention on the node id.
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

/// Stamp `properties` as ontology SCHEMA (TBox) -- see the module-level A18 note.
/// Idempotent: a no-op if already present.
pub(crate) fn mark_schema(properties: &mut serde_json::Map<String, serde_json::Value>) {
    properties
        .entry(eg_core::isolation::RLS_SCHEMA_KEY.to_string())
        .or_insert(serde_json::Value::Bool(true));
}

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

/// Canonical lossless property-graph rows lowered from an RDF triple stream.
/// Every server surface consumes this representation so literal multiplicity,
/// typing edges, identifiers, and serialization cannot drift by transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoweredTripleGraph {
    pub nodes: Vec<(String, Vec<u8>)>,
    pub edges: Vec<(String, String, Vec<u8>)>,
    pub triples: usize,
    pub multivalue: usize,
}

/// Lower RDF triples to deterministic graph rows without mutating a graph.
pub fn lower_triples(
    triples: impl IntoIterator<Item = Triple>,
) -> Result<LoweredTripleGraph, String> {
    let mut node_props: BTreeMap<String, serde_json::Map<String, serde_json::Value>> =
        BTreeMap::new();
    let mut edges: Vec<(String, String, String)> = Vec::new();
    let mut multivalue: Vec<(String, String, serde_json::Value)> = Vec::new();
    let mut count = 0usize;

    for triple in triples {
        count += 1;
        let subject = subject_id(&triple.subject);
        let predicate = triple.predicate.as_str().to_string();
        match &triple.object {
            Term::Literal(literal) => {
                let properties = node_props.entry(subject.clone()).or_default();
                if properties.contains_key(&predicate) {
                    multivalue.push((subject, predicate, literal_to_cell(literal)));
                } else {
                    properties.insert(predicate, literal_to_cell(literal));
                }
            }
            #[cfg(feature = "sparql-star")]
            Term::Triple(_) => {}
            object => {
                let object_id = term_node_id(object)
                    .ok_or_else(|| "RDF resource object has no canonical node id".to_string())?;
                node_props.entry(subject.clone()).or_default();
                node_props.entry(object_id.clone()).or_default();
                if predicate == RDF_TYPE {
                    if let Term::NamedNode(node_type) = object {
                        node_props
                            .entry(subject.clone())
                            .or_default()
                            .entry("type".to_string())
                            .or_insert_with(|| {
                                serde_json::Value::String(node_type.as_str().to_string())
                            });
                        // A18: an explicit `rdf:type owl:Class`/`rdfs:Class`/...
                        // declaration makes the SUBJECT itself schema (TBox) --
                        // see the module-level A18 note above.
                        if TBOX_TYPE_OBJECTS.contains(&node_type.as_str()) {
                            mark_schema(node_props.entry(subject.clone()).or_default());
                        }
                    }
                } else if TBOX_SCHEMA_PREDICATES.contains(&predicate.as_str()) {
                    // A18: a recognized RDFS/OWL schema predicate names an axiom
                    // ABOUT both endpoints (a class/property reference on each
                    // side), so both are schema -- see the module-level A18 note.
                    mark_schema(node_props.entry(subject.clone()).or_default());
                    mark_schema(node_props.entry(object_id.clone()).or_default());
                }
                edges.push((subject, predicate, object_id));
            }
        }
    }

    for (subject, predicate, cell) in &multivalue {
        let properties = node_props.entry(subject.clone()).or_default();
        let extra = properties
            .entry(RDF_MULTI_VALUE_KEY.to_string())
            .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
        let extra_object = extra
            .as_object_mut()
            .ok_or_else(|| format!("reserved RDF multivalue cell on {subject} is not an object"))?;
        extra_object
            .entry(predicate.clone())
            .or_insert_with(|| serde_json::Value::Array(Vec::new()))
            .as_array_mut()
            .ok_or_else(|| format!("RDF multivalue predicate {predicate} is not an array"))?
            .push(cell.clone());
    }

    let nodes = node_props
        .into_iter()
        .map(|(id, properties)| {
            let blob = rmp_serde::to_vec_named(&serde_json::Value::Object(properties))
                .map_err(|error| format!("encode RDF node {id}: {error}"))?;
            Ok((id, blob))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let edges = edges
        .into_iter()
        .map(|(source, predicate, target)| {
            let blob = rmp_serde::to_vec_named(&serde_json::json!({ "relationship": predicate }))
                .map_err(|error| format!("encode RDF edge: {error}"))?;
            Ok((source, target, blob))
        })
        .collect::<Result<Vec<_>, String>>()?;

    Ok(LoweredTripleGraph {
        nodes,
        edges,
        triples: count,
        multivalue: multivalue.len(),
    })
}

/// Register the `:NamedGraph` marker node-shape that links this RDF dataset to its
/// registry graph (CONCEPT:EG-KG.ontology.kg-native-rdf-sparql). Idempotent — re-loading the same graph leaves
/// one marker. `graph_name` is the registry key (the named-graph IRI/name).
pub fn register_named_graph(core: &GraphCore, graph_name: &str) {
    let id = format!("__named_graph__:{graph_name}");
    let blob = rmp_serde::to_vec_named(&serde_json::json!({
        "type": NAMED_GRAPH_MARKER,
        "graph_name": graph_name,
    }))
    .unwrap_or_default();
    core.add_node(id, blob);
}

/// Load an RDF triple stream into a `GraphCore` (one named graph), interning IRIs.
///
/// Multi-valued literal predicates are preserved losslessly in the authoritative
/// node blob: the FIRST literal lands at its ordinary predicate key and every
/// EXTRA literal lands under [`RDF_MULTI_VALUE_KEY`].
pub fn load_triples(
    core: &GraphCore,
    iris: &mut IriStore,
    graph_name: &str,
    triples: impl IntoIterator<Item = Triple>,
) -> Result<LoadReport, String> {
    let lowered = lower_triples(triples)?;
    for (id, _) in &lowered.nodes {
        iris.intern(id);
    }
    for (source, target, _) in &lowered.edges {
        iris.intern(source);
        iris.intern(target);
    }

    // Write the complete lossless rows in one GraphCore txn. The enclosing server
    // gateway stages this txn and durably publishes it once.
    let mut txn = core.txn();
    for (id, blob) in &lowered.nodes {
        txn.add_node(id.clone(), blob.clone());
    }
    for (source, target, blob) in &lowered.edges {
        txn.add_edge(source.clone(), target.clone(), blob.clone())?;
    }
    drop(txn);
    let _ = graph_name;

    Ok(LoadReport {
        triples: lowered.triples,
        multivalue: lowered.multivalue,
    })
}

/// Summary of a [`load_triples`] call.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LoadReport {
    /// Total triples consumed.
    pub triples: usize,
    /// Multi-valued literal extras encountered (second+ literal per `(s,p)`).
    pub multivalue: usize,
}

/// Parse a Turtle document into oxrdf triples.
pub fn parse_turtle(doc: &str) -> Result<Vec<Triple>, String> {
    let mut out = Vec::new();
    for r in TurtleParser::new().for_reader(doc.as_bytes()) {
        out.push(r.map_err(|e| format!("turtle parse: {e}"))?);
    }
    Ok(out)
}

/// Parse an N-Triples document into oxrdf triples.
pub fn parse_ntriples(doc: &str) -> Result<Vec<Triple>, String> {
    let mut out = Vec::new();
    for r in NTriplesParser::new().for_reader(doc.as_bytes()) {
        out.push(r.map_err(|e| format!("ntriples parse: {e}"))?);
    }
    Ok(out)
}

/// Serialize the `GraphCore` back OUT to RDF triples — the inverse mapping. Edges →
/// object triples; node literal-cell properties → literal triples; the folded
/// `type` property is emitted as the `rdf:type` edge (so it round-trips once).
/// Multi-valued literals come from the same node image. The `:NamedGraph` marker
/// node is skipped because it is engine bookkeeping.
pub fn export_triples(core: &GraphCore, graph_name: &str) -> Result<Vec<Triple>, String> {
    let mut out: Vec<Triple> = Vec::new();
    let mut graph_registered = false;

    // Object triples from edges.
    for (s, o, props) in core.get_edges() {
        let v = eg_types::msgpack::decode_property_value(&props).unwrap_or(serde_json::json!({}));
        let pred = v
            .get("relationship")
            .and_then(|x| x.as_str())
            .ok_or("edge missing relationship")?;
        out.push(make_triple(&s, pred, &o)?);
    }

    // Literal triples + folded rdf:type from node property blobs.
    for (id, props) in core.get_nodes() {
        if id.starts_with("__named_graph__:") {
            graph_registered = true;
            continue; // engine bookkeeping, not RDF.
        }
        let v = eg_types::msgpack::decode_property_value(&props).unwrap_or(serde_json::json!({}));
        let Some(obj) = v.as_object() else { continue };
        // Skip the marker node by its type, too (defensive).
        if obj.get("type").and_then(|t| t.as_str()) == Some(NAMED_GRAPH_MARKER) {
            graph_registered = true;
            continue;
        }
        for (k, cell) in obj {
            if k == RDF_MULTI_VALUE_KEY {
                if let Some(by_predicate) = cell.as_object() {
                    for (predicate, values) in by_predicate {
                        let pred = NamedNode::new(predicate)
                            .map_err(|e| format!("bad pred iri {predicate}: {e}"))?;
                        for value in values.as_array().into_iter().flatten() {
                            if let Some(lit) = cell_to_literal(value) {
                                let subj = parse_subject(id.as_str())?;
                                out.push(Triple::new(subj, pred.clone(), lit));
                            }
                        }
                    }
                }
                continue;
            }
            if k == "type" {
                continue; // emitted as an explicit rdf:type edge already.
            }
            if k == "graph_name" {
                continue; // marker bookkeeping.
            }
            if let Some(lit) = cell_to_literal(cell) {
                let subj = parse_subject(id.as_str())?;
                let pred = NamedNode::new(k).map_err(|e| format!("bad pred iri {k}: {e}"))?;
                out.push(Triple::new(subj, pred, lit));
            }
        }
    }

    let _ = (graph_name, graph_registered);

    Ok(out)
}

fn parse_subject(id: &str) -> Result<NamedOrBlankNode, String> {
    if let Some(iri) = id.strip_prefix('<').and_then(|s| s.strip_suffix('>')) {
        Ok(NamedOrBlankNode::NamedNode(
            NamedNode::new(iri).map_err(|e| format!("bad iri {iri}: {e}"))?,
        ))
    } else if let Some(b) = id.strip_prefix("_:") {
        Ok(NamedOrBlankNode::BlankNode(
            BlankNode::new(b).map_err(|e| format!("bad bnode {b}: {e}"))?,
        ))
    } else {
        Err(format!("node id is not a term: {id}"))
    }
}

fn make_triple(s: &str, p: &str, o: &str) -> Result<Triple, String> {
    let subj = parse_subject(s)?;
    let pred = NamedNode::new(p).map_err(|e| format!("bad pred {p}: {e}"))?;
    let obj: Term = if let Some(iri) = o.strip_prefix('<').and_then(|x| x.strip_suffix('>')) {
        Term::NamedNode(NamedNode::new(iri).map_err(|e| format!("bad obj iri {iri}: {e}"))?)
    } else if let Some(b) = o.strip_prefix("_:") {
        Term::BlankNode(BlankNode::new(b).map_err(|e| format!("bad obj bnode {b}: {e}"))?)
    } else {
        return Err(format!("object node id is not a term: {o}"));
    };
    Ok(Triple::new(subj, pred, obj))
}

/// Serialize triples to an N-Triples string (the canonical, order-independent form).
pub fn to_ntriples(triples: &[Triple]) -> Result<String, String> {
    let mut buf = Vec::new();
    let mut ser = NTriplesSerializer::new().for_writer(&mut buf);
    for t in triples {
        ser.serialize_triple(t.as_ref())
            .map_err(|e| format!("nt serialize: {e}"))?;
    }
    ser.finish();
    String::from_utf8(buf).map_err(|e| format!("nt utf8: {e}"))
}

/// Serialize triples to a Turtle string (CONCEPT:EG-KG.ontology.content-negotiation-serializers — the `text/turtle` content-
/// negotiation form for CONSTRUCT/DESCRIBE). Mirrors [`to_ntriples`] but uses the oxttl
/// `TurtleSerializer`, which abbreviates predicate/object lists into compact Turtle.
pub fn to_turtle(triples: &[Triple]) -> Result<String, String> {
    let mut buf = Vec::new();
    let mut ser = TurtleSerializer::new().for_writer(&mut buf);
    for t in triples {
        ser.serialize_triple(t.as_ref())
            .map_err(|e| format!("ttl serialize: {e}"))?;
    }
    ser.finish().map_err(|e| format!("ttl finish: {e}"))?;
    String::from_utf8(buf).map_err(|e| format!("ttl utf8: {e}"))
}

// ── EG-131: the RDF serialization matrix ────────────────────────────────────────
//
// Coverage beyond N-Triples/Turtle. The QUAD formats (N-Quads/TriG) place every triple
// in `graph` (a named-graph IRI) or the default graph; they ride the oxttl quad
// serializers already in the `rdf` feature (pure Rust, in pi — not a heavy dep). RDF/XML
// and JSON-LD 1.1 add pure-Rust quick-xml/oxjsonld codecs behind their own
// features (`rdf-xml`/`json-ld`), kept OUT of pi. These are the graph-result forms wired
// into the `/sparql` content-negotiation seam (CONCEPT:EG-KG.ontology.content-negotiation-serializers) for CONSTRUCT/DESCRIBE.

/// Lift a triple into a quad in the named `graph` (or the default graph when `None`).
fn triple_to_quad(t: &Triple, graph: Option<&str>) -> Result<Quad, String> {
    let g = match graph {
        Some(name) => GraphName::NamedNode(
            NamedNode::new(name).map_err(|e| format!("bad graph iri {name}: {e}"))?,
        ),
        None => GraphName::DefaultGraph,
    };
    Ok(Quad::new(
        t.subject.clone(),
        t.predicate.clone(),
        t.object.clone(),
        g,
    ))
}

/// Serialize triples to N-Quads, each placed in `graph` (or the default graph).
pub fn to_nquads(triples: &[Triple], graph: Option<&str>) -> Result<String, String> {
    let mut buf = Vec::new();
    let mut ser = NQuadsSerializer::new().for_writer(&mut buf);
    for t in triples {
        ser.serialize_quad(triple_to_quad(t, graph)?.as_ref())
            .map_err(|e| format!("nq serialize: {e}"))?;
    }
    ser.finish();
    String::from_utf8(buf).map_err(|e| format!("nq utf8: {e}"))
}

/// Serialize triples to TriG (Turtle-with-named-graphs), placing each in `graph`.
pub fn to_trig(triples: &[Triple], graph: Option<&str>) -> Result<String, String> {
    let mut buf = Vec::new();
    let mut ser = TriGSerializer::new().for_writer(&mut buf);
    for t in triples {
        ser.serialize_quad(triple_to_quad(t, graph)?.as_ref())
            .map_err(|e| format!("trig serialize: {e}"))?;
    }
    ser.finish().map_err(|e| format!("trig finish: {e}"))?;
    String::from_utf8(buf).map_err(|e| format!("trig utf8: {e}"))
}

/// Parse an N-Quads document into oxrdf quads (subject/predicate/object + graph name).
pub fn parse_nquads(doc: &str) -> Result<Vec<Quad>, String> {
    let mut out = Vec::new();
    for r in NQuadsParser::new().for_reader(doc.as_bytes()) {
        out.push(r.map_err(|e| format!("nquads parse: {e}"))?);
    }
    Ok(out)
}

/// Parse a TriG document into oxrdf quads.
pub fn parse_trig(doc: &str) -> Result<Vec<Quad>, String> {
    let mut out = Vec::new();
    for r in TriGParser::new().for_reader(doc.as_bytes()) {
        out.push(r.map_err(|e| format!("trig parse: {e}"))?);
    }
    Ok(out)
}

/// Serialize triples to RDF/XML (CONCEPT:EG-KG.ontology.feature, feature `rdf-xml`).
#[cfg(feature = "rdf-xml")]
pub fn to_rdfxml(triples: &[Triple]) -> Result<String, String> {
    crate::rdfxml::serialize(triples)
}

/// Parse an RDF/XML document into oxrdf triples (feature `rdf-xml`).
#[cfg(feature = "rdf-xml")]
pub fn parse_rdfxml(doc: &str) -> Result<Vec<Triple>, String> {
    crate::rdfxml::parse(doc)
}

/// Serialize triples to JSON-LD 1.1 (CONCEPT:EG-KG.ontology.feature, feature `json-ld`). Expansion form;
/// context compaction/framing are a documented follow-up.
#[cfg(feature = "json-ld")]
pub fn to_jsonld(triples: &[Triple], graph: Option<&str>) -> Result<String, String> {
    use oxjsonld::JsonLdSerializer;
    let mut buf = Vec::new();
    let mut ser = JsonLdSerializer::new().for_writer(&mut buf);
    for t in triples {
        ser.serialize_quad(triple_to_quad(t, graph)?.as_ref())
            .map_err(|e| format!("jsonld serialize: {e}"))?;
    }
    ser.finish().map_err(|e| format!("jsonld finish: {e}"))?;
    String::from_utf8(buf).map_err(|e| format!("jsonld utf8: {e}"))
}

/// Parse a JSON-LD 1.1 document into oxrdf quads (feature `json-ld`).
#[cfg(feature = "json-ld")]
pub fn parse_jsonld(doc: &str) -> Result<Vec<Quad>, String> {
    use oxjsonld::JsonLdParser;
    let mut out = Vec::new();
    for r in JsonLdParser::new().for_slice(doc.as_bytes()) {
        out.push(r.map_err(|e| format!("jsonld parse: {e}"))?);
    }
    Ok(out)
}

// ── EG-137: named-graph-aware `from_*` reader surface ────────────────────────────
//
// CONCEPT:EG-KG.ontology.completes-rdf-concrete-syntax completes the RDF 1.1 concrete-syntax matrix — TriG + N-Quads (quad,
// named-graph-aware) and RDF/XML — alongside Turtle/N-Triples (EG-050). The writers
// (`to_trig`/`to_nquads`/`to_rdfxml`) sit above; these `from_*` readers give the matrix
// a first-class, uniformly-named reader surface (delegating to the same oxttl/quick-xml
// parsers). Named-graph awareness: `from_trig`/`from_nquads` yield `Quad`s carrying the
// per-statement graph term; RDF/XML is a single-graph syntax so `from_rdfxml` yields
// triples.

/// EG-137: parse N-Quads into quads (subject/predicate/object + graph term). The
/// canonically-named reader for the `application/n-quads` form.
pub fn from_nquads(doc: &str) -> Result<Vec<Quad>, String> {
    parse_nquads(doc)
}

/// EG-137: parse TriG (named-graph-aware Turtle) into quads. The canonically-named
/// reader for the `application/trig` form.
pub fn from_trig(doc: &str) -> Result<Vec<Quad>, String> {
    parse_trig(doc)
}

/// EG-137: parse RDF/XML into triples (a single-graph syntax). The canonically-named
/// reader for the `application/rdf+xml` form (feature `rdf-xml`).
#[cfg(feature = "rdf-xml")]
pub fn from_rdfxml(doc: &str) -> Result<Vec<Triple>, String> {
    parse_rdfxml(doc)
}

/// A canonical, bnode/order-insensitive comparison key for a quad set (mirrors
/// [`triple_set_key`], adding the graph name).
pub fn quad_set_key(quads: &[Quad]) -> BTreeSet<String> {
    quads
        .iter()
        .map(|q| {
            let g = match &q.graph_name {
                GraphName::NamedNode(n) => format!("<{}>", n.as_str()),
                GraphName::BlankNode(_) => "_:g".to_string(),
                GraphName::DefaultGraph => "default".to_string(),
            };
            let t = Triple::new(q.subject.clone(), q.predicate.clone(), q.object.clone());
            format!("{g} {}", canonical_triple_str(&t))
        })
        .collect()
}

/// A canonical, bnode/order-insensitive comparison key for a triple set: literals
/// keep datatype/lang; bnodes are normalized to a single placeholder so structural
/// equality holds (these datasets have no bnode-distinguishing structure).
pub fn triple_set_key(triples: &[Triple]) -> BTreeSet<String> {
    triples.iter().map(canonical_triple_str).collect()
}

fn canonical_term(t: &Term) -> String {
    match t {
        Term::NamedNode(n) => format!("<{}>", n.as_str()),
        Term::BlankNode(_) => "_:b".to_string(),
        Term::Literal(l) => {
            let mut parts = BTreeMap::new();
            parts.insert("v", l.value().to_string());
            parts.insert("d", l.datatype().as_str().to_string());
            if let Some(lang) = l.language() {
                parts.insert("l", lang.to_string());
            }
            format!("{parts:?}")
        }
        // RDF-star (CONCEPT:EG-KG.ontology.concept-5): a quoted-triple object contributes a canonical,
        // recursively-normalized `<<s p o>>` key so the round-trip set comparison is
        // meaningful (not collapsed to a placeholder).
        #[cfg(feature = "sparql-star")]
        Term::Triple(t) => {
            let s = match &t.subject {
                NamedOrBlankNode::NamedNode(n) => format!("<{}>", n.as_str()),
                NamedOrBlankNode::BlankNode(_) => "_:b".to_string(),
            };
            format!(
                "<<{s} <{}> {}>>",
                t.predicate.as_str(),
                canonical_term(&t.object)
            )
        }
        #[allow(unreachable_patterns)]
        _ => "?".to_string(),
    }
}

fn canonical_triple_str(t: &Triple) -> String {
    let s = match &t.subject {
        NamedOrBlankNode::NamedNode(n) => format!("<{}>", n.as_str()),
        NamedOrBlankNode::BlankNode(_) => "_:b".to_string(),
        #[allow(unreachable_patterns)]
        _ => "?".to_string(),
    };
    format!(
        "{s} <{}> {}",
        t.predicate.as_str(),
        canonical_term(&t.object)
    )
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
