use super::*;

/// Canonical lossless property-graph rows lowered from an RDF triple stream.
/// Every server surface consumes this representation so literal multiplicity,
/// typing edges, identifiers, and serialization cannot drift by transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoweredTripleGraph {
    pub nodes: Vec<(String, Vec<u8>)>,
    pub edges: Vec<(String, String, Vec<u8>)>,
    pub triples: usize,
    pub multivalue: usize,
    /// One entry per schema-defining triple OCCURRENCE that names a node id
    /// as subject or object (BUG A3, 2026-08-12) -- a duplicate id for every
    /// distinct axiom about it, mirroring exactly how many
    /// `GraphCore::mark_schema_ref` calls the equivalent incremental SPARQL
    /// UPDATE inserts (`eg_rdf::update::insert_triple`) would make for the
    /// SAME triple set. The caller (`load_triples`) feeds each entry to
    /// `GraphCore::mark_schema_ref` once, in the SAME transaction as the node
    /// writes, so a bulk load and an equivalent sequence of incremental
    /// inserts leave the SAME live reverse-index refcount behind.
    pub schema_refs: Vec<String>,
}

/// Mutable accumulator threaded through [`lower_triples`]'s per-triple lowering: nodes'
/// property maps (keyed by canonical id), the edge list, values that collided into a
/// multivalue cell, and which node ids became TBox schema (A18).
#[derive(Default)]
struct LoweredAccum {
    node_props: BTreeMap<String, serde_json::Map<String, serde_json::Value>>,
    edges: Vec<(String, String, String)>,
    multivalue: Vec<(String, String, serde_json::Value)>,
    schema_refs: Vec<String>,
}

/// Lower one triple into `accum`: a literal object merges into (or multivalue-collides
/// with) the subject's property blob; a resource object becomes an edge, folding
/// `rdf:type` into the node-label property and flagging TBox schema refs (A18) exactly
/// as `crate::update::insert_resource_triple` does for the incremental path. Extracted
/// from [`lower_triples`]'s per-triple loop.
fn lower_one_triple(triple: &Triple, accum: &mut LoweredAccum) -> Result<(), String> {
    let subject = subject_id(&triple.subject);
    let predicate = triple.predicate.as_str().to_string();
    match &triple.object {
        Term::Literal(literal) => {
            let properties = accum.node_props.entry(subject.clone()).or_default();
            if properties.contains_key(&predicate) {
                accum
                    .multivalue
                    .push((subject, predicate, literal_to_cell(literal)));
            } else {
                properties.insert(predicate, literal_to_cell(literal));
            }
        }
        #[cfg(feature = "sparql-star")]
        Term::Triple(_) => {}
        object => {
            let object_id = term_node_id(object)
                .ok_or_else(|| "RDF resource object has no canonical node id".to_string())?;
            accum.node_props.entry(subject.clone()).or_default();
            accum.node_props.entry(object_id.clone()).or_default();
            if predicate == RDF_TYPE {
                if let Term::NamedNode(node_type) = object {
                    accum
                        .node_props
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
                        accum.schema_refs.push(subject.clone());
                    }
                }
            } else if TBOX_SCHEMA_PREDICATES.contains(&predicate.as_str()) {
                // A18: a recognized RDFS/OWL schema predicate names an axiom
                // ABOUT both endpoints (a class/property reference on each
                // side), so both are schema -- see the module-level A18 note.
                accum.schema_refs.push(subject.clone());
                accum.schema_refs.push(object_id.clone());
            }
            accum.edges.push((subject, predicate, object_id));
        }
    }
    Ok(())
}

/// Resolve every property key that collided across triples into a reserved multivalue
/// cell (`RDF_MULTI_VALUE_KEY`), appending each colliding value in triple order.
/// Extracted from [`lower_triples`]'s post-loop pass.
fn merge_multivalue_cells(accum: &mut LoweredAccum) -> Result<(), String> {
    for (subject, predicate, cell) in &accum.multivalue {
        let properties = accum.node_props.entry(subject.clone()).or_default();
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
    Ok(())
}

/// Encode every node's property map to its msgpack blob. Extracted from
/// [`lower_triples`]'s final encode pass.
fn encode_lowered_nodes(
    node_props: BTreeMap<String, serde_json::Map<String, serde_json::Value>>,
) -> Result<Vec<(String, Vec<u8>)>, String> {
    node_props
        .into_iter()
        .map(|(id, properties)| {
            let blob = rmp_serde::to_vec_named(&serde_json::Value::Object(properties))
                .map_err(|error| format!("encode RDF node {id}: {error}"))?;
            Ok((id, blob))
        })
        .collect()
}

/// Encode every edge's relationship property to its msgpack blob. Extracted from
/// [`lower_triples`]'s final encode pass.
fn encode_lowered_edges(
    edges: Vec<(String, String, String)>,
) -> Result<Vec<(String, String, Vec<u8>)>, String> {
    edges
        .into_iter()
        .map(|(source, predicate, target)| {
            let blob = rmp_serde::to_vec_named(&serde_json::json!({ "relationship": predicate }))
                .map_err(|error| format!("encode RDF edge: {error}"))?;
            Ok((source, target, blob))
        })
        .collect()
}

/// Lower RDF triples to deterministic graph rows without mutating a graph.
pub fn lower_triples(
    triples: impl IntoIterator<Item = Triple>,
) -> Result<LoweredTripleGraph, String> {
    let mut accum = LoweredAccum::default();
    let mut count = 0usize;

    for triple in triples {
        count += 1;
        lower_one_triple(&triple, &mut accum)?;
    }

    merge_multivalue_cells(&mut accum)?;

    let nodes = encode_lowered_nodes(accum.node_props)?;
    let edges = encode_lowered_edges(accum.edges)?;

    Ok(LoweredTripleGraph {
        nodes,
        edges,
        triples: count,
        multivalue: accum.multivalue.len(),
        schema_refs: accum.schema_refs,
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
    // BUG A3 (2026-08-12): mark every schema-defining triple occurrence this
    // bulk load contributed, so a bulk-loaded ontology is exactly as
    // TBox-exempt as the same triples inserted one at a time via SPARQL
    // UPDATE (`eg_rdf::update::insert_triple`) would be -- see
    // `LoweredTripleGraph::schema_refs`'s doc.
    for id in &lowered.schema_refs {
        core.mark_schema_ref(id);
    }
    let _ = graph_name;

    Ok(LoadReport {
        triples: lowered.triples,
        multivalue: lowered.multivalue,
    })
}
