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

/// Project an off-lock [`medulla_core::graph::GraphView`] snapshot to RDF triples — the
/// snapshot twin of [`export_triples`] over the same node/edge encodings (EH-196: the
/// data graph a query-time shape validation reads). Only resources are projected: a node
/// or edge endpoint whose id is not an RDF term (`<iri>` / `_:b`) is a plain property-
/// graph element with no RDF identity, so it contributes no triple rather than failing
/// the whole projection.
pub fn export_view_triples(view: &medulla_core::graph::GraphView) -> Result<Vec<Triple>, String> {
    let mut out: Vec<Triple> = Vec::new();
    for ((s, o), blobs) in &view.edge_properties {
        if !is_rdf_term(s) || !is_rdf_term(o) {
            continue;
        }
        for blob in blobs {
            let relationship = medulla_types::msgpack::decode_edge_relationship(blob)
                .ok_or("edge missing relationship")?;
            out.push(make_triple(s, &relationship, o)?);
        }
    }
    for (id, props) in &view.node_properties {
        if is_rdf_term(id) {
            export_node_triples(id, props, &mut out)?;
        }
    }
    Ok(out)
}

/// Whether a graph node id is an RDF resource term (`<iri>` or `_:bnode`).
pub fn is_rdf_term(id: &str) -> bool {
    (id.starts_with('<') && id.ends_with('>')) || id.starts_with("_:")
}

/// Object triples from edges. Half of [`export_triples`]'s two sources.
fn export_edge_triples(core: &GraphCore, out: &mut Vec<Triple>) -> Result<(), String> {
    for (s, o, props) in core.get_edges() {
        let relationship = medulla_types::msgpack::decode_edge_relationship(&props)
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
    let v = medulla_types::msgpack::decode_property_value(props).unwrap_or(serde_json::json!({}));
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

/// Serialize triples to a Turtle string (CONCEPT:MEDULLA-KG.ontology.content-negotiation-serializers — the `text/turtle` content-
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

// ── MEDULLA-131: the RDF serialization matrix ────────────────────────────────────────
//
// Coverage beyond N-Triples/Turtle. The QUAD formats (N-Quads/TriG) place every triple
// in `graph` (a named-graph IRI) or the default graph; they ride the oxttl quad
// serializers already in the `rdf` feature (pure Rust, in pi — not a heavy dep). RDF/XML
// and JSON-LD 1.1 add pure-Rust quick-xml/oxjsonld codecs behind their own
// features (`rdf-xml`/`json-ld`), kept OUT of pi. These are the graph-result forms wired
// into the `/sparql` content-negotiation seam (CONCEPT:MEDULLA-KG.ontology.content-negotiation-serializers) for CONSTRUCT/DESCRIBE.

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

/// Serialize triples to RDF/XML (CONCEPT:MEDULLA-KG.ontology.feature, feature `rdf-xml`).
#[cfg(feature = "rdf-xml")]
pub fn to_rdfxml(triples: &[Triple]) -> Result<String, String> {
    crate::rdfxml::serialize(triples)
}

/// Serialize triples to JSON-LD 1.1 (CONCEPT:MEDULLA-KG.ontology.feature, feature `json-ld`). Expansion form;
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

/// Project property-graph keys under a caller-supplied RDF namespace for validation.
/// RDF terms retain their identity; conflicting projected identities are refused.
#[cfg(feature = "sparql")]
pub fn export_view_projected(
    view: &medulla_core::graph::GraphView,
    projection: &crate::sparql::Projection,
) -> Result<Vec<Triple>, String> {
    use std::collections::HashMap;
    if projection.base_iri.is_none() {
        return export_view_triples(view);
    }
    let mut identities = HashMap::new();
    for id in view.node_properties.keys() {
        let projected = projection.node_iri(id);
        if identities.insert(projected, id).is_some() {
            return Err("RDF projection has colliding node identities".into());
        }
    }
    let mut out = Vec::new();
    for ((s, o), blobs) in &view.edge_properties {
        let subject = projection.node_iri(s);
        let object = projection.node_iri(o);
        for blob in blobs {
            let relationship = medulla_types::msgpack::decode_edge_relationship(blob)
                .ok_or("edge missing relationship")?;
            out.push(make_triple(
                &subject,
                &projection.pred_iri(&relationship),
                &object,
            )?);
        }
    }
    for (id, props) in &view.node_properties {
        if id.starts_with("__named_graph__:") {
            continue;
        }
        let subject = projection.node_iri(id);
        let value = medulla_types::msgpack::decode_property_value(props).unwrap_or_default();
        let Some(fields) = value.as_object() else {
            continue;
        };
        if let Some(class) = fields.get("type").and_then(serde_json::Value::as_str) {
            if !is_rdf_term(id) {
                let class = projection
                    .type_object_iri(class)
                    .ok_or("missing projection")?;
                out.push(make_triple(
                    &subject,
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                    &class,
                )?);
            }
        }
        let datatypes = fields
            .get("source_datatypes")
            .map(|value| value.as_object().ok_or("invalid source datatype metadata"))
            .transpose()?;
        for (key, cell) in fields {
            if key == "type" || key == "graph_name" || key == "source_datatypes" {
                continue;
            }
            if key == RDF_MULTI_VALUE_KEY {
                export_multivalue_cell(&subject, cell, &mut out)?;
            } else if let Some(literal) =
                projected_literal(cell, datatypes.and_then(|values| values.get(key)))?
            {
                let predicate = NamedNode::new(projection.pred_iri(key))
                    .map_err(|e| format!("bad projected predicate: {e}"))?;
                out.push(Triple::new(parse_subject(&subject)?, predicate, literal));
            }
        }
    }
    Ok(out)
}

/// Native JSON scalars have RDF literal types; typed RDF cells retain their datatype.
#[cfg(feature = "sparql")]
fn projected_literal(
    cell: &serde_json::Value,
    datatype: Option<&serde_json::Value>,
) -> Result<Option<Literal>, String> {
    if let Some(datatype) = datatype {
        let datatype = datatype
            .as_str()
            .filter(|value| medulla_types::connector_pack::source_datatype_supported(value))
            .ok_or("invalid source datatype")?;
        let lexical = match cell {
            serde_json::Value::String(value) => value.clone(),
            serde_json::Value::Bool(value) => value.to_string(),
            serde_json::Value::Number(value) if datatype == "xsd:decimal" => {
                decimal_lexical(&value.to_string())?
            }
            serde_json::Value::Number(value) => value.to_string(),
            _ => return Err("source datatype requires a scalar".into()),
        };
        let iri = format!("http://www.w3.org/2001/XMLSchema#{}", &datatype[4..]);
        let node =
            NamedNode::new(iri).map_err(|error| format!("invalid source datatype: {error}"))?;
        return Ok(Some(Literal::new_typed_literal(lexical, node)));
    }
    if let Some(value) = cell.as_str() {
        return Ok(Some(Literal::new_simple_literal(value)));
    }
    let (lexical, datatype) = if let Some(value) = cell.as_bool() {
        (
            value.to_string(),
            "http://www.w3.org/2001/XMLSchema#boolean",
        )
    } else if let Some(value) = cell.as_i64() {
        (
            value.to_string(),
            "http://www.w3.org/2001/XMLSchema#integer",
        )
    } else if let Some(value) = cell.as_u64() {
        (
            value.to_string(),
            "http://www.w3.org/2001/XMLSchema#integer",
        )
    } else if let Some(value) = cell.as_f64() {
        (value.to_string(), "http://www.w3.org/2001/XMLSchema#double")
    } else {
        return Ok(cell_to_literal(cell));
    };
    let node = NamedNode::new(datatype).map_err(|error| format!("invalid datatype: {error}"))?;
    Ok(Some(Literal::new_typed_literal(lexical, node)))
}

/// Expand JSON exponent notation to an XML Schema decimal lexical form.
#[cfg(feature = "sparql")]
fn decimal_lexical(value: &str) -> Result<String, String> {
    let Some((mantissa, exponent)) = value.split_once(['e', 'E']) else {
        return Ok(value.to_owned());
    };
    let exponent: i32 = exponent.parse().map_err(|_| "invalid decimal exponent")?;
    let (sign, mantissa) = mantissa
        .strip_prefix('-')
        .map_or(("", mantissa), |v| ("-", v));
    let point = mantissa.find('.').unwrap_or(mantissa.len()) as i32 + exponent;
    let digits = mantissa.replace('.', "");
    if point <= 0 {
        return Ok(format!("{sign}0.{}{digits}", "0".repeat((-point) as usize)));
    }
    let point = point as usize;
    if point >= digits.len() {
        return Ok(format!(
            "{sign}{digits}{}",
            "0".repeat(point - digits.len())
        ));
    }
    Ok(format!("{sign}{}.{}", &digits[..point], &digits[point..]))
}

#[cfg(all(test, feature = "sparql"))]
mod projected_decimal_tests {
    use super::decimal_lexical;

    #[test]
    fn exponent_form_becomes_valid_decimal_lexical() {
        assert_eq!(decimal_lexical("1.25e-7").unwrap(), "0.000000125");
        assert_eq!(decimal_lexical("-1.25e+4").unwrap(), "-12500");
        assert_eq!(decimal_lexical("1.25").unwrap(), "1.25");
    }
}
