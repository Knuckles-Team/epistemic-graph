use eg_core::graph::GraphCore;

use crate::mapping::{
    cell_lexical, literal_to_cell, RDF_MULTI_VALUE_KEY, TBOX_SCHEMA_PREDICATES, TBOX_TYPE_OBJECTS,
};

use super::terms::{obj_from_term, ObjTerm};

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

// ── reusable engine retract / insert ops (CONCEPT:EG-KG.query.named-graph-support) ───────────────────────
//
// These are the CLEAN, REUSABLE physical-write primitives the rest of the engine (the
// `RemoveTriples` wire method, the ontology UNLOAD path, the WAL replay) calls — the
// inverse of `mapping::load_triples`, NOT logic buried inside the UPDATE executor.

/// Physically RETRACT a set of RDF triples from a graph core (CONCEPT:EG-KG.query.named-graph-support). Surgical:
/// a literal triple drops one property key (matched by lexical value); a resource triple
/// removes the one matching typed edge, preserving any others between the same pair; a
/// folded `rdf:type` also clears the node `type` label. Returns the count removed. This
/// is the reusable retract op behind the `RemoveTriples` wire method + ontology unload.
pub fn remove_triples(core: &GraphCore, triples: &[oxrdf::Triple]) -> usize {
    let mut removed = 0;
    for t in triples {
        let s = subject_id_of(&t.subject);
        let p = t.predicate.as_str();
        if let Some(obj) = obj_from_term(&t.object) {
            if delete_triple(core, &s, p, &obj) {
                removed += 1;
            }
        }
    }
    removed
}

/// Physically INSERT a set of RDF triples into a graph core, merge-aware (the same
/// projection `load_triples` uses, but incremental — it never overwrites a node's other
/// properties). Returns the count of triples that added something new.
pub fn insert_triples(core: &GraphCore, triples: &[oxrdf::Triple]) -> Result<usize, String> {
    let mut inserted = 0;
    for t in triples {
        let s = subject_id_of(&t.subject);
        let p = t.predicate.as_str();
        if let Some(obj) = obj_from_term(&t.object) {
            if insert_triple(core, &s, p, &obj)? {
                inserted += 1;
            }
        }
    }
    Ok(inserted)
}

/// Canonical node id for an oxrdf subject (`<iri>` / `_:b`).
fn subject_id_of(s: &oxrdf::NamedOrBlankNode) -> String {
    match s {
        oxrdf::NamedOrBlankNode::NamedNode(n) => format!("<{}>", n.as_str()),
        oxrdf::NamedOrBlankNode::BlankNode(b) => format!("_:{}", b.as_str()),
        #[allow(unreachable_patterns)]
        _ => String::new(),
    }
}

// ── the native triple-level write ops ───────────────────────────────────────────

/// Insert one triple into `core` (merge-aware). Returns `true` if it actually added
/// something new (an absent edge or a new/changed property), `false` if already present.
pub(super) fn insert_triple(
    core: &GraphCore,
    s: &str,
    p: &str,
    obj: &ObjTerm,
) -> Result<bool, String> {
    match obj {
        ObjTerm::Literal(lit) => {
            ensure_node(core, s);
            Ok(merge_property(core, s, p, literal_to_cell(lit)))
        }
        ObjTerm::Resource(o) => insert_resource_triple(core, s, p, o),
    }
}

/// The `ObjTerm::Resource` arm of [`insert_triple`]: ensure both endpoint nodes exist,
/// classify the triple as TBox schema (A18, CONCEPT:EG-KG.sharding.row-level-security)
/// BEFORE inserting, then add the edge and — only if it was genuinely new (BUG A3,
/// 2026-08-12: re-inserting an already-present axiom must never inflate the live schema
/// refcount `delete_triple` decrements once per genuine removal) — mark whichever
/// endpoint(s) the classification named as schema.
fn insert_resource_triple(core: &GraphCore, s: &str, p: &str, o: &str) -> Result<bool, String> {
    ensure_node(core, s);
    ensure_node(core, o);
    let mut changed = false;
    let (becomes_schema_subject, becomes_schema_object) =
        classify_schema_triple(core, s, p, o, &mut changed);
    let edge_added = add_edge_if_absent(core, s, o, p)?;
    changed |= edge_added;
    if edge_added {
        if becomes_schema_subject {
            core.mark_schema_ref(s);
        }
        if becomes_schema_object {
            core.mark_schema_ref(o);
        }
    }
    Ok(changed)
}

/// Classify one resource triple as TBox schema, before the edge is inserted. Returns
/// `(becomes_schema_subject, becomes_schema_object)`; also folds an `rdf:type` object
/// into the subject's node-label property (matching the loader) via `changed`. Part of
/// [`insert_resource_triple`]'s A18 schema decision.
fn classify_schema_triple(
    core: &GraphCore,
    s: &str,
    p: &str,
    o: &str,
    changed: &mut bool,
) -> (bool, bool) {
    // rdf:type folds into the node label (matches the loader) AND stays an edge.
    if p == RDF_TYPE {
        let mut becomes_schema_subject = false;
        if let Some(iri) = o.strip_prefix('<').and_then(|x| x.strip_suffix('>')) {
            *changed |= set_type_property(core, s, iri);
            // An explicit `rdf:type owl:Class`/`rdfs:Class`/... declaration makes the
            // SUBJECT itself schema -- see `crate::mapping`'s module-level A18 note and
            // `GraphCore::schema_refs`.
            if TBOX_TYPE_OBJECTS.contains(&iri) {
                becomes_schema_subject = true;
            }
        }
        return (becomes_schema_subject, false);
    }
    if TBOX_SCHEMA_PREDICATES.contains(&p) {
        // A18: a recognized RDFS/OWL schema predicate (e.g. `rdfs:subClassOf`) names an
        // axiom ABOUT both endpoints -- a class/property reference on each side, never a
        // fact about an individual -- so both are schema. This is the exact SPARQL
        // UPDATE path `wire_reason_iri_bridges_string_typed_node` exercises: an
        // `INSERT DATA { <..Sensor> rdfs:subClassOf <..Device> }` axiom would otherwise
        // land with two untagged, unowned class nodes that default-deny RLS hides from
        // every non-`System` actor.
        return (true, true);
    }
    (false, false)
}

/// Delete one triple from `core` (surgical). Returns `true` if it removed something.
pub(super) fn delete_triple(core: &GraphCore, s: &str, p: &str, obj: &ObjTerm) -> bool {
    match obj {
        ObjTerm::Literal(lit) => delete_property(core, s, p, lit.value()),
        ObjTerm::Resource(o) => {
            // BUG A3 (2026-08-12): `edge_removed` (not the combined `removed`
            // below, which also folds in the denormalized `type` property)
            // is the authoritative "was THIS triple genuinely removed" signal
            // -- symmetric with `insert_triple`'s `edge_added` gate above, so
            // the live schema refcount this releases exactly balances what
            // that function incremented for the SAME triple.
            let edge_removed = remove_typed_edge(core, s, o, p);
            let mut removed = edge_removed;
            if p == RDF_TYPE {
                if let Some(iri) = o.strip_prefix('<').and_then(|x| x.strip_suffix('>')) {
                    removed |= clear_type_property(core, s, iri);
                    if edge_removed && TBOX_TYPE_OBJECTS.contains(&iri) {
                        core.unmark_schema_ref(s);
                    }
                }
            } else if edge_removed && TBOX_SCHEMA_PREDICATES.contains(&p) {
                core.unmark_schema_ref(s);
                core.unmark_schema_ref(o);
            }
            removed
        }
    }
}

fn read_node_obj(core: &GraphCore, id: &str) -> serde_json::Map<String, serde_json::Value> {
    core.get_node_properties(id)
        .and_then(|b| eg_types::msgpack::decode_property_value(&b).ok())
        .and_then(|v| match v {
            serde_json::Value::Object(m) => Some(m),
            _ => None,
        })
        .unwrap_or_default()
}

fn write_node_obj(core: &GraphCore, id: &str, map: serde_json::Map<String, serde_json::Value>) {
    let blob = rmp_serde::to_vec_named(&serde_json::Value::Object(map)).unwrap_or_default();
    core.add_node(id.to_string(), blob);
}

fn ensure_node(core: &GraphCore, id: &str) {
    if !core.has_node(id) {
        write_node_obj(core, id, serde_json::Map::new());
    }
}

/// Merge a property cell into the node blob. Returns `true` if it added/changed the key.
fn merge_property(core: &GraphCore, id: &str, key: &str, cell: serde_json::Value) -> bool {
    let mut map = read_node_obj(core, id);
    if map.get(key) == Some(&cell) {
        return false;
    }
    if map.contains_key(key) {
        let extras = map
            .entry(RDF_MULTI_VALUE_KEY.to_string())
            .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
        let Some(extras) = extras.as_object_mut() else {
            return false;
        };
        let values = extras
            .entry(key.to_string())
            .or_insert_with(|| serde_json::Value::Array(Vec::new()));
        let Some(values) = values.as_array_mut() else {
            return false;
        };
        if values.contains(&cell) {
            return false;
        }
        values.push(cell);
    } else {
        map.insert(key.to_string(), cell);
    }
    write_node_obj(core, id, map);
    true
}

/// Remove a literal property `key` whose lexical value matches. Returns `true` if dropped.
fn delete_property(core: &GraphCore, id: &str, key: &str, value: &str) -> bool {
    let mut map = read_node_obj(core, id);
    let primary_matches = map
        .get(key)
        .is_some_and(|cell| cell_lexical(cell).as_deref() == Some(value));
    if primary_matches {
        let promoted = map
            .get_mut(RDF_MULTI_VALUE_KEY)
            .and_then(serde_json::Value::as_object_mut)
            .and_then(|by_predicate| by_predicate.get_mut(key))
            .and_then(serde_json::Value::as_array_mut)
            .and_then(|values| (!values.is_empty()).then(|| values.remove(0)));
        if let Some(promoted) = promoted {
            map.insert(key.to_string(), promoted);
        } else {
            map.remove(key);
        }
        prune_empty_multivalue(&mut map, key);
        write_node_obj(core, id, map);
        return true;
    }
    let removed_extra = map
        .get_mut(RDF_MULTI_VALUE_KEY)
        .and_then(serde_json::Value::as_object_mut)
        .and_then(|by_predicate| by_predicate.get_mut(key))
        .and_then(serde_json::Value::as_array_mut)
        .and_then(|values| {
            values
                .iter()
                .position(|cell| cell_lexical(cell).as_deref() == Some(value))
                .map(|position| values.remove(position))
        })
        .is_some();
    if removed_extra {
        prune_empty_multivalue(&mut map, key);
        write_node_obj(core, id, map);
    }
    removed_extra
}

fn prune_empty_multivalue(map: &mut serde_json::Map<String, serde_json::Value>, predicate: &str) {
    let mut remove_container = false;
    if let Some(by_predicate) = map
        .get_mut(RDF_MULTI_VALUE_KEY)
        .and_then(serde_json::Value::as_object_mut)
    {
        if by_predicate
            .get(predicate)
            .and_then(serde_json::Value::as_array)
            .is_some_and(Vec::is_empty)
        {
            by_predicate.remove(predicate);
        }
        remove_container = by_predicate.is_empty();
    }
    if remove_container {
        map.remove(RDF_MULTI_VALUE_KEY);
    }
}

fn set_type_property(core: &GraphCore, id: &str, type_iri: &str) -> bool {
    let mut map = read_node_obj(core, id);
    let val = serde_json::Value::String(type_iri.to_string());
    if map.get("type") == Some(&val) {
        return false;
    }
    map.insert("type".to_string(), val);
    write_node_obj(core, id, map);
    true
}

fn clear_type_property(core: &GraphCore, id: &str, type_iri: &str) -> bool {
    let mut map = read_node_obj(core, id);
    if map.get("type").and_then(|v| v.as_str()) == Some(type_iri) {
        map.remove("type");
        write_node_obj(core, id, map);
        true
    } else {
        false
    }
}

/// Add a typed edge `s --p--> o` unless one already exists. Returns `true` if added.
fn add_edge_if_absent(core: &GraphCore, s: &str, o: &str, p: &str) -> Result<bool, String> {
    let exists = core
        .get_edge_properties(s, o)
        .iter()
        .any(|blob| eg_types::msgpack::decode_edge_relationship(blob).as_deref() == Some(p));
    if exists {
        return Ok(false);
    }
    let blob = rmp_serde::to_vec_named(&serde_json::json!({ "relationship": p }))
        .map_err(|e| format!("encode edge: {e}"))?;
    core.add_edge(s.to_string(), o.to_string(), blob)?;
    Ok(true)
}

/// Remove the one `s --p--> o` edge, preserving any OTHER typed edges between the pair.
fn remove_typed_edge(core: &GraphCore, s: &str, o: &str, p: &str) -> bool {
    let existing = core.get_edge_properties(s, o);
    let mut survivors = Vec::new();
    let mut removed = false;
    for blob in existing {
        if !removed && eg_types::msgpack::decode_edge_relationship(&blob).as_deref() == Some(p) {
            removed = true; // drop exactly one matching edge
        } else {
            survivors.push(blob);
        }
    }
    if !removed {
        return false;
    }
    core.remove_edge(s.to_string(), o.to_string());
    for blob in survivors {
        let _ = core.add_edge(s.to_string(), o.to_string(), blob);
    }
    true
}
