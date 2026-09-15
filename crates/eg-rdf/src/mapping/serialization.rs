use super::*;

/// Serialize the `GraphCore` back OUT to RDF triples — the inverse mapping. Edges →
/// object triples; node literal-cell properties → literal triples; the folded
/// `type` property is emitted as the `rdf:type` edge (so it round-trips once).
/// Multi-valued literals come from the same node image. The `:NamedGraph` marker
/// node is skipped because it is engine bookkeeping.
pub fn export_triples(core: &GraphCore, graph_name: &str) -> Result<Vec<Triple>, String> {
    let mut out: Vec<Triple> = Vec::new();
    let mut graph_registered = false;

    export_edge_triples(core, &mut out)?;

    for (id, props) in core.get_nodes() {
        if export_node_triples(&id, &props, &mut out)? {
            graph_registered = true;
        }
    }

    let _ = (graph_name, graph_registered);

    Ok(out)
}

/// Object triples from edges. Half of [`export_triples`]'s two sources.
fn export_edge_triples(core: &GraphCore, out: &mut Vec<Triple>) -> Result<(), String> {
    for (s, o, props) in core.get_edges() {
        let relationship = eg_types::msgpack::decode_edge_relationship(&props)
            .ok_or("edge missing relationship")?;
        out.push(make_triple(&s, &relationship, &o)?);
    }
    Ok(())
}

/// Literal triples + folded rdf:type from one node's property blob. Returns whether
/// this node was engine bookkeeping (a `__named_graph__:` marker) rather than RDF. The
/// other half of [`export_triples`]'s two sources.
fn export_node_triples(id: &str, props: &[u8], out: &mut Vec<Triple>) -> Result<bool, String> {
    if id.starts_with("__named_graph__:") {
        return Ok(true); // engine bookkeeping, not RDF.
    }
    let v = eg_types::msgpack::decode_property_value(props).unwrap_or(serde_json::json!({}));
    let Some(obj) = v.as_object() else {
        return Ok(false);
    };
    // Skip the marker node by its type, too (defensive).
    if obj.get("type").and_then(|t| t.as_str()) == Some(NAMED_GRAPH_MARKER) {
        return Ok(true);
    }
    for (k, cell) in obj {
        export_node_cell(id, k, cell, out)?;
    }
    Ok(false)
}

/// One property cell of a node's blob → 0+ triples: the reserved multivalue cell
/// expands to one triple per collided value; `type` (emitted as an explicit rdf:type
/// edge already) and `graph_name` (marker bookkeeping) are skipped; anything else is a
/// plain literal cell. Extracted from [`export_node_triples`]'s per-key loop.
fn export_node_cell(
    id: &str,
    k: &str,
    cell: &serde_json::Value,
    out: &mut Vec<Triple>,
) -> Result<(), String> {
    if k == RDF_MULTI_VALUE_KEY {
        return export_multivalue_cell(id, cell, out);
    }
    if k == "type" || k == "graph_name" {
        return Ok(());
    }
    if let Some(lit) = cell_to_literal(cell) {
        let subj = parse_subject(id)?;
        let pred = NamedNode::new(k).map_err(|e| format!("bad pred iri {k}: {e}"))?;
        out.push(Triple::new(subj, pred, lit));
    }
    Ok(())
}

/// The reserved multivalue cell → one triple per `(predicate, value)` that collided
/// during lowering. Extracted from [`export_node_cell`].
fn export_multivalue_cell(
    id: &str,
    cell: &serde_json::Value,
    out: &mut Vec<Triple>,
) -> Result<(), String> {
    let Some(by_predicate) = cell.as_object() else {
        return Ok(());
    };
    for (predicate, values) in by_predicate {
        let pred =
            NamedNode::new(predicate).map_err(|e| format!("bad pred iri {predicate}: {e}"))?;
        for value in values.as_array().into_iter().flatten() {
            if let Some(lit) = cell_to_literal(value) {
                let subj = parse_subject(id)?;
                out.push(Triple::new(subj, pred.clone(), lit));
            }
        }
    }
    Ok(())
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

/// Serialize triples to RDF/XML (CONCEPT:EG-KG.ontology.feature, feature `rdf-xml`).
#[cfg(feature = "rdf-xml")]
pub fn to_rdfxml(triples: &[Triple]) -> Result<String, String> {
    crate::rdfxml::serialize(triples)
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
