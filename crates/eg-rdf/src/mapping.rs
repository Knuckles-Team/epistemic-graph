//! W1 — Native RDF ⇄ property-graph mapping over `GraphCore` (CONCEPT:KG-2.217).
//!
//! THE MAPPING (verbatim — faithful for the common shape; the one lossy edge is
//! handled by the opt-in [`crate::quads`] redb table):
//!
//! | RDF construct | engine representation |
//! |---|---|
//! | IRI / blank-node **subject or object** | a graph **node**, id = canonical term string (`<iri>` / `_:b`); the IRI is interned in [`IriStore`] so the node id is a stable handle. |
//! | triple with a **resource object** `(s,p,o)` | a **typed edge** `s --p--> o`, edge-blob `{ "type": p }` — the engine's existing edge-relation convention (`rel_matches`/`GetTriples` already read `type`). |
//! | triple with a **literal object** `(s,p,"lit"^^dt@lang)` | a **node property** on `s`: `p -> {value, datatype, lang}` (a typed cell INSIDE the JSON property blob, so the xsd datatype + language tag survive). |
//! | `rdf:type` | folded into the node `"type"` property (lights up the engine's `type`-based label index) AND kept as an explicit typing edge so multi-typed resources round-trip. |
//! | **named graph** | a `GraphCore` in the multi-graph registry — a graph name IS a named graph. A `:NamedGraph` marker node records the container (see [`NAMED_GRAPH_MARKER`]). |
//!
//! **The one lossy edge (handled, not pretended-away):** a node property map is
//! key-unique, so a subject with two different literals for the SAME predicate
//! (`ex:x ex:tag "a", "b"`) would collapse to one. [`load_triples`] detects a
//! multi-valued literal predicate and, when a [`crate::quads::QuadStore`] is
//! supplied, stores the EXTRA values in the lossless redb `quads` table; the FIRST
//! value still lands in the property blob (the query-fast projection). With no
//! quad store the extras are dropped and reported, so the loss is never silent.
//!
//! Round-trip: parse Turtle/N-Triples → store into GraphCore → serialize back to
//! N-Triples, and the triple SET is equal (semantic equality — bnode labels and
//! triple order are not significant in RDF).

use std::collections::{BTreeMap, BTreeSet, HashMap};

use eg_core::graph::GraphCore;
use oxrdf::{BlankNode, Literal, NamedNode, Subject, Term, Triple};
use oxttl::{NTriplesParser, NTriplesSerializer, TurtleParser, TurtleSerializer};

/// The engine `type` of the marker node that records "this graph is an RDF named
/// graph" (the `:NamedGraph` node shape linking the RDF surface to the registry).
pub const NAMED_GRAPH_MARKER: &str = "NamedGraph";

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

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
fn subject_id(s: &Subject) -> String {
    match s {
        Subject::NamedNode(n) => format!("<{}>", n.as_str()),
        Subject::BlankNode(b) => format!("_:{}", b.as_str()),
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

/// Register the `:NamedGraph` marker node-shape that links this RDF dataset to its
/// registry graph (CONCEPT:KG-2.217). Idempotent — re-loading the same graph leaves
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
/// When `quads` is `Some`, multi-valued literal predicates (the lossy edge of the
/// property-graph mapping) are preserved losslessly: the FIRST literal for a
/// `(subject, predicate)` lands in the property blob, every EXTRA literal is
/// written to the redb quad table. With `quads = None` the extras are DROPPED and
/// their count is returned in [`LoadReport::dropped_multivalue`] so the loss is
/// never silent.
pub fn load_triples(
    core: &GraphCore,
    iris: &mut IriStore,
    graph_name: &str,
    triples: impl IntoIterator<Item = Triple>,
    #[cfg(feature = "rdf-redb")] quads: Option<&crate::quads::QuadStore>,
) -> Result<LoadReport, String> {
    // Accumulate per-subject literal props so a node is written once with all its
    // literal-valued predicates; collect object-edges separately.
    let mut node_props: HashMap<String, serde_json::Map<String, serde_json::Value>> =
        HashMap::new();
    let mut edges: Vec<(String, String, String)> = Vec::new();
    // (subject, predicate, literal-cell) extras beyond the first per (s,p): the
    // lossless cases that the key-unique property blob can't hold.
    let mut multivalue: Vec<(String, String, serde_json::Value)> = Vec::new();
    let mut n = 0usize;

    for t in triples {
        n += 1;
        let s_id = subject_id(&t.subject);
        iris.intern(&s_id);
        let pred = t.predicate.as_str().to_string();

        match &t.object {
            Term::Literal(lit) => {
                let entry = node_props.entry(s_id.clone()).or_default();
                if entry.contains_key(&pred) {
                    // SECOND+ literal for the same predicate — the lossy edge.
                    multivalue.push((s_id.clone(), pred, literal_to_cell(lit)));
                } else {
                    entry.insert(pred, literal_to_cell(lit));
                }
            }
            obj => {
                let o_id = term_node_id(obj).expect("non-literal object");
                iris.intern(&o_id);
                node_props.entry(s_id.clone()).or_default();
                node_props.entry(o_id.clone()).or_default();
                if pred == RDF_TYPE {
                    if let Term::NamedNode(nt) = obj {
                        let entry = node_props.entry(s_id.clone()).or_default();
                        entry
                            .entry("type".to_string())
                            .or_insert_with(|| serde_json::Value::String(nt.as_str().to_string()));
                    }
                }
                edges.push((s_id.clone(), pred, o_id));
            }
        }
    }

    // Write nodes (one blob each), then edges, in a single txn.
    let mut txn = core.txn();
    for (id, props) in &node_props {
        let blob = rmp_serde::to_vec_named(&serde_json::Value::Object(props.clone()))
            .map_err(|e| format!("encode node {id}: {e}"))?;
        txn.add_node(id.clone(), blob);
    }
    for (s, p, o) in &edges {
        let blob = rmp_serde::to_vec_named(&serde_json::json!({ "type": p }))
            .map_err(|e| format!("encode edge: {e}"))?;
        txn.add_edge(s.clone(), o.clone(), blob)?;
    }
    drop(txn);

    // Route the multi-valued literal extras to the lossless store when present;
    // otherwise they are dropped and counted (never silently lost).
    #[cfg(feature = "rdf-redb")]
    let dropped = match quads {
        Some(store) => {
            for (s, p, cell) in &multivalue {
                store
                    .put_literal(graph_name, s, p, cell)
                    .map_err(|e| format!("quad store put: {e}"))?;
            }
            0
        }
        None => multivalue.len(),
    };
    #[cfg(not(feature = "rdf-redb"))]
    let dropped = {
        let _ = graph_name;
        multivalue.len()
    };

    Ok(LoadReport {
        triples: n,
        multivalue: multivalue.len(),
        dropped_multivalue: dropped,
    })
}

/// Summary of a [`load_triples`] call.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LoadReport {
    /// Total triples consumed.
    pub triples: usize,
    /// Multi-valued literal extras encountered (second+ literal per `(s,p)`).
    pub multivalue: usize,
    /// Of `multivalue`, how many were DROPPED (no quad store supplied). Zero when a
    /// quad store preserved them losslessly.
    pub dropped_multivalue: usize,
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
/// `type` property is emitted as the `rdf:type` edge (so it round-trips once). When
/// `quads` is `Some`, the multi-valued literal extras stored losslessly are unioned
/// back in. The `:NamedGraph` marker node is skipped (it is engine bookkeeping).
pub fn export_triples(
    core: &GraphCore,
    graph_name: &str,
    #[cfg(feature = "rdf-redb")] quads: Option<&crate::quads::QuadStore>,
) -> Result<Vec<Triple>, String> {
    let mut out: Vec<Triple> = Vec::new();

    // Object triples from edges.
    for (s, o, props) in core.get_edges() {
        let v: serde_json::Value = rmp_serde::from_slice(&props).unwrap_or(serde_json::json!({}));
        let pred = v
            .get("type")
            .and_then(|x| x.as_str())
            .ok_or("edge missing type")?;
        out.push(make_triple(&s, pred, &o)?);
    }

    // Literal triples + folded rdf:type from node property blobs.
    for (id, props) in core.get_nodes() {
        if id.starts_with("__named_graph__:") {
            continue; // engine bookkeeping, not RDF.
        }
        let v: serde_json::Value = rmp_serde::from_slice(&props).unwrap_or(serde_json::json!({}));
        let Some(obj) = v.as_object() else { continue };
        // Skip the marker node by its type, too (defensive).
        if obj.get("type").and_then(|t| t.as_str()) == Some(NAMED_GRAPH_MARKER) {
            continue;
        }
        for (k, cell) in obj {
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

    // Union the lossless multi-valued literal extras.
    #[cfg(feature = "rdf-redb")]
    if let Some(store) = quads {
        for (s, p, cell) in store
            .scan_literals(graph_name)
            .map_err(|e| format!("quad store scan: {e}"))?
        {
            if let Some(lit) = cell_to_literal(&cell) {
                let subj = parse_subject(&s)?;
                let pred = NamedNode::new(&p).map_err(|e| format!("bad pred iri {p}: {e}"))?;
                out.push(Triple::new(subj, pred, lit));
            }
        }
    }
    #[cfg(not(feature = "rdf-redb"))]
    {
        let _ = graph_name;
    }

    Ok(out)
}

fn parse_subject(id: &str) -> Result<Subject, String> {
    if let Some(iri) = id.strip_prefix('<').and_then(|s| s.strip_suffix('>')) {
        Ok(Subject::NamedNode(
            NamedNode::new(iri).map_err(|e| format!("bad iri {iri}: {e}"))?,
        ))
    } else if let Some(b) = id.strip_prefix("_:") {
        Ok(Subject::BlankNode(
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

/// Serialize triples to a Turtle string (CONCEPT:EG-050 — the `text/turtle` content-
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
        #[allow(unreachable_patterns)]
        _ => "?".to_string(),
    }
}

fn canonical_triple_str(t: &Triple) -> String {
    let s = match &t.subject {
        Subject::NamedNode(n) => format!("<{}>", n.as_str()),
        Subject::BlankNode(_) => "_:b".to_string(),
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
        let report = load_triples(
            &core,
            &mut iris,
            "g",
            parsed.clone(),
            #[cfg(feature = "rdf-redb")]
            None,
        )
        .expect("load");
        assert_eq!(report.triples, 8);
        assert_eq!(report.multivalue, 0);
        assert!(iris.len() >= 4, "interned {} iris", iris.len());

        let exported = export_triples(
            &core,
            "g",
            #[cfg(feature = "rdf-redb")]
            None,
        )
        .expect("export");
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
        load_triples(
            &core,
            &mut iris,
            "g",
            parsed,
            #[cfg(feature = "rdf-redb")]
            None,
        )
        .unwrap();
        let exported = export_triples(
            &core,
            "g",
            #[cfg(feature = "rdf-redb")]
            None,
        )
        .unwrap();
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

    /// Without a quad store, a multi-valued literal predicate keeps the FIRST value
    /// in the property blob and reports the dropped extras (never a silent loss).
    #[test]
    fn multivalue_without_store_is_reported_not_silent() {
        let ttl = r#"
@prefix ex: <http://example.org/> .
ex:x ex:tag "a" , "b" , "c" .
"#;
        let parsed = parse_turtle(ttl).unwrap();
        assert_eq!(parsed.len(), 3);
        let core = GraphCore::new();
        let mut iris = IriStore::default();
        let report = load_triples(
            &core,
            &mut iris,
            "g",
            parsed,
            #[cfg(feature = "rdf-redb")]
            None,
        )
        .unwrap();
        assert_eq!(report.multivalue, 2, "two extras beyond the first tag");
        assert_eq!(
            report.dropped_multivalue, 2,
            "no store ⇒ extras dropped + reported"
        );
        // The first value is still present.
        let exported = export_triples(
            &core,
            "g",
            #[cfg(feature = "rdf-redb")]
            None,
        )
        .unwrap();
        let tags: Vec<_> = exported
            .iter()
            .filter(|t| t.predicate.as_str().ends_with("tag"))
            .collect();
        assert_eq!(tags.len(), 1, "only the first value lands in the blob");
    }

    /// With a quad store, ALL values of a multi-valued literal predicate round-trip
    /// losslessly (the property blob holds the first; the quad table holds the rest).
    #[cfg(feature = "rdf-redb")]
    #[test]
    fn multivalue_with_quad_store_is_lossless() {
        let ttl = r#"
@prefix ex: <http://example.org/> .
ex:x ex:tag "a" , "b" , "c" .
"#;
        let dir = tempfile::tempdir().unwrap();
        let store = crate::quads::QuadStore::open(dir.path().join("rdf_quads.redb")).unwrap();
        let core = GraphCore::new();
        let mut iris = IriStore::default();
        let report = load_triples(
            &core,
            &mut iris,
            "g",
            parse_turtle(ttl).unwrap(),
            Some(&store),
        )
        .unwrap();
        assert_eq!(report.multivalue, 2);
        assert_eq!(report.dropped_multivalue, 0, "quad store ⇒ nothing dropped");

        let exported = export_triples(&core, "g", Some(&store)).unwrap();
        let mut tags: Vec<String> = exported
            .iter()
            .filter(|t| t.predicate.as_str().ends_with("tag"))
            .filter_map(|t| match &t.object {
                Term::Literal(l) => Some(l.value().to_string()),
                _ => None,
            })
            .collect();
        tags.sort();
        assert_eq!(
            tags,
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            "all three tag values must survive losslessly"
        );
    }

    #[test]
    fn named_graph_marker_is_registered_and_not_exported() {
        let core = GraphCore::new();
        register_named_graph(&core, "my:graph");
        assert!(core.has_node("__named_graph__:my:graph"));
        // The marker must not leak into exported RDF.
        let exported = export_triples(
            &core,
            "my:graph",
            #[cfg(feature = "rdf-redb")]
            None,
        )
        .unwrap();
        assert!(exported.is_empty(), "marker node must not export as RDF");
    }
}
