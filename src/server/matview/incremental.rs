//! CDC-feed glue for DBSP-style incremental plan-backed matviews
//! (CONCEPT:EG-KG.storage.incremental-matview).
//!
//! The facade side of the eg-plan `incremental` circuit: it translates a `CdcEvent`
//! into the circuit's [`Delta`] (the "update = retract old + insert new" idiom the CDC
//! hub already captures as before/after images — no new capture code), seeds a fresh
//! circuit from the current graph state, and (de)serializes a circuit's maintained
//! operator state for the `matview_operator_state` redb table. This is the analogue of
//! turso's `incremental/persistence.rs`.

use eg_plan::incremental::{Circuit, Delta, ZRow};
use eg_types::wire::{CdcEvent, CdcKind};
use serde_json::{Map, Value};

/// Decode a msgpack node-property blob into the circuit's prop-map shape. Absent / empty
/// / non-object blobs decode to an empty map (a reserved-marker CDC event — e.g.
/// `__apply_mutation`, no before/after — therefore carries no label and matches no
/// `Scan`, so it is a safe no-op for every supported plan).
fn decode_props(blob: &[u8], present: bool) -> Map<String, Value> {
    if !present || blob.is_empty() {
        return Map::new();
    }
    eg_types::msgpack::decode_property_value(blob)
        .ok()
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_default()
}

/// Translate ONE `CdcEvent` into the circuit's [`Delta`]. Node adds/removes/updates map
/// to signed id-Z-set rows; EDGE events touch only `Traverse` (out of v1), so for the
/// supported node-based circuits they translate to an EMPTY delta (a correct no-op).
pub fn event_to_delta(event: &CdcEvent) -> Delta {
    match event.kind {
        CdcKind::AddNode => Delta::from(vec![ZRow::insert(
            event.node_id.clone(),
            decode_props(&event.after, event.had_after),
        )]),
        CdcKind::RemoveNode => Delta::from(vec![ZRow::retract(
            event.node_id.clone(),
            decode_props(&event.before, event.had_before),
        )]),
        CdcKind::UpdateNode => {
            let mut rows = Vec::with_capacity(2);
            if event.had_before {
                rows.push(ZRow::retract(
                    event.node_id.clone(),
                    decode_props(&event.before, true),
                ));
            }
            if event.had_after {
                rows.push(ZRow::insert(
                    event.node_id.clone(),
                    decode_props(&event.after, true),
                ));
            }
            Delta::from(rows)
        }
        // Edge deltas do not touch any v1-supported operator (only Traverse, which is
        // fallback-only) — a plan reaching here has no Traverse, so this is a no-op.
        CdcKind::AddEdge | CdcKind::RemoveEdge => Delta::new(),
    }
}

/// Build the SEED delta for a fresh circuit from the current authoritative graph state:
/// one `+1` insert per node (with its decoded props). Applying this to a just-compiled
/// circuit reproduces the full materialization through the SAME gating the incremental
/// path uses, so window (bucket) and set (membership) state are both correctly seeded —
/// exactly how `execute` would have produced the initial rows, but leaving the circuit
/// ready to retract on later deltas.
pub fn seed_delta_from_core(core: &eg_core::graph::GraphCore) -> Delta {
    let ids = core.node_ids();
    let mut rows = Vec::with_capacity(ids.len());
    for id in ids {
        let props = core
            .get_node_properties(&id)
            .map(|blob| decode_props(&blob, true))
            .unwrap_or_default();
        rows.push(ZRow::insert(id, props));
    }
    Delta::from(rows)
}

/// Serialize a circuit's maintained operator state for the `matview_operator_state` redb
/// row (MessagePack).
pub fn encode_operator_state(circuit: &Circuit) -> Result<Vec<u8>, String> {
    rmp_serde::to_vec_named(circuit).map_err(|e| format!("serialize matview operator state: {e}"))
}

/// Decode a `matview_operator_state` redb row back into a circuit.
#[allow(dead_code)]
pub fn decode_operator_state(blob: &[u8]) -> Result<Circuit, String> {
    rmp_serde::from_slice(blob).map_err(|e| format!("decode matview operator state: {e}"))
}
