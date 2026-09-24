//! The per-batch write delta ([`ChangeSet`]) and the edge-relationship capture it carries
//! (CONCEPT:EG-KG.storage.write-changeset; EH-393 edge-type attribution).

use std::collections::BTreeSet;

/// The canonical edge-relationship field of an edge blob — the value a typed `Traverse`
/// matches (`eg_plan`'s `rel_matches` reads the same field).
pub const EDGE_RELATIONSHIP_KEY: &str = "relationship";

/// Which edge relationship types a set of edges carries (EH-393). `touched` records that at
/// least one edge was seen (an edge without a `relationship` field still changes the untyped
/// edge set); `unattributed` that some edge's type could not be read, so every type must be
/// treated as touched.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EdgeRels {
    pub types: BTreeSet<String>,
    pub touched: bool,
    pub unattributed: bool,
}

impl EdgeRels {
    /// The relationship types of the given edge blobs. A blob that does not decode marks the
    /// set `unattributed`; a decodable blob without a string `relationship` has no type.
    pub fn from_blobs<'b>(blobs: impl IntoIterator<Item = &'b [u8]>) -> Self {
        let mut rels = Self::default();
        for blob in blobs {
            rels.touched = true;
            match eg_types::msgpack::decode_property_value(blob) {
                Ok(value) => rels.types.extend(relationship_of(&value)),
                Err(_) => rels.unattributed = true,
            }
        }
        rels
    }

    /// Fold `other` into `self`.
    pub fn merge(&mut self, other: EdgeRels) {
        self.types.extend(other.types);
        self.touched |= other.touched;
        self.unattributed |= other.unattributed;
    }

    /// No edge was seen and nothing is unattributed.
    pub fn is_empty(&self) -> bool {
        !self.touched && !self.unattributed
    }
}

fn relationship_of(value: &serde_json::Value) -> Option<String> {
    value
        .get(EDGE_RELATIONSHIP_KEY)
        .and_then(|rel| rel.as_str())
        .map(str::to_string)
}

/// One node touched by a committed write batch (CONCEPT:EG-KG.storage.write-changeset). Carries
/// the node id and — for adds/updates — OPTIONALLY the new property blob, so a
/// content-derived index (text / temporal) can compute its own delta without
/// re-reading the graph (re-reading `core` under the batch's held topology lock would
/// deadlock). The coalescer captures `properties_msgpack` ONLY when a `needs_content`
/// server index is registered on the graph (CONCEPT:EG-KG.storage.incremental-text /
/// .incremental-temporal — the flag is `IndexManager::wants_change_content`); otherwise
/// it stays `None` and the hot path pays no per-op blob clone (the vector store keys off
/// removals alone). For an ADD the blob is the full property map; for a CAS update it is
/// the `updates` map (a field-scoped content index reads only its own field from it).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NodeChange {
    pub id: String,
    pub properties_msgpack: Option<Vec<u8>>,
    /// Exact top-level fields changed by a partial update. `None` means the
    /// complete node image may have changed (add/upsert/remove semantics), so a
    /// cache that cannot apply a delta must invalidate conservatively.
    #[serde(default)]
    pub changed_fields: Option<Vec<String>>,
}

impl NodeChange {
    pub fn new(id: String) -> Self {
        Self {
            id,
            properties_msgpack: None,
            changed_fields: None,
        }
    }
    pub fn with_properties(id: String, properties_msgpack: Vec<u8>) -> Self {
        Self {
            id,
            properties_msgpack: Some(properties_msgpack),
            changed_fields: None,
        }
    }

    pub fn with_properties_and_fields(
        id: String,
        properties_msgpack: Vec<u8>,
        changed_fields: Vec<String>,
    ) -> Self {
        Self {
            id,
            properties_msgpack: Some(properties_msgpack),
            changed_fields: Some(changed_fields),
        }
    }

    pub fn with_fields(id: String, changed_fields: Vec<String>) -> Self {
        Self {
            id,
            properties_msgpack: None,
            changed_fields: Some(changed_fields),
        }
    }
}

/// One edge touched by a committed write batch.
#[derive(Debug, Clone)]
pub struct EdgeChange {
    pub source: String,
    pub target: String,
}

/// The per-batch delta collected inside the coalescer's single topology lock
/// (CONCEPT:EG-KG.storage.write-changeset). It records exactly which nodes/edges were
/// added / updated / removed by a committed [`WriteOp`](crate) batch, so the
/// [`IndexManager`] can maintain the heavy secondary indexes incrementally instead
/// of dropping and rebuilding them. Only successful ops are recorded (a failed
/// add-edge or a lost CAS contributes nothing).
#[derive(Debug, Clone, Default)]
pub struct ChangeSet {
    pub added_nodes: Vec<NodeChange>,
    pub updated_nodes: Vec<NodeChange>,
    pub removed_nodes: Vec<String>,
    pub added_edges: Vec<EdgeChange>,
    pub removed_edges: Vec<EdgeChange>,
    /// Captured property blob of a removed node, keyed by id (CONCEPT:EG-KG.storage.incremental-index-stamp,
    /// W1.6/P7). A removed node is gone from the property store by the time index maintenance
    /// runs, so its labels / property keys cannot be re-read — a write path that can capture the
    /// blob BEFORE the delete records it here, letting incremental index maintenance remove the id
    /// from exactly the postings it belonged to (and the dependency clock bump exactly the label /
    /// key dimensions it touched) instead of dropping the whole index / coarsely flooring. A
    /// removed id ABSENT from this map falls back to the sound coarse path (scan the warm postings
    /// for the id; floor the dependency clock). Populated only by paths that can capture cheaply.
    pub removed_node_props: std::collections::HashMap<String, Vec<u8>>,
    /// The PRIOR property blob of a node an add overwrote (an upsert of an existing id), keyed by
    /// id and captured before the overwrite (EH-393). Without it an upsert that relabels `A → B`
    /// would retire only `B` — a `Scan { A }` result would survive holding a node that left `A`.
    /// The first capture of a batch wins (it is the pre-batch image). Populated by the write
    /// paths that can read the node under their held guard.
    pub replaced_node_props: std::collections::HashMap<String, Vec<u8>>,
    /// Relationship types of the edges this batch removed — directly, or by cascade from a node
    /// removal — captured BEFORE the delete (EH-393). A removal recorded without a capture marks
    /// it `unattributed`, which retires every typed traversal (the sound fallback).
    pub removed_edge_rels: EdgeRels,
}

impl ChangeSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// No mutation was recorded (an empty batch, or a batch of only no-op writes).
    pub fn is_empty(&self) -> bool {
        self.added_nodes.is_empty()
            && self.updated_nodes.is_empty()
            && self.removed_nodes.is_empty()
            && self.added_edges.is_empty()
            && self.removed_edges.is_empty()
    }

    /// Whether this batch can change a node-derived label/property/JSON-path
    /// posting. Pure edge deltas leave those caches byte-for-byte valid.
    pub fn has_node_changes(&self) -> bool {
        !self.added_nodes.is_empty()
            || !self.updated_nodes.is_empty()
            || !self.removed_nodes.is_empty()
    }

    /// Total number of recorded node + edge changes.
    pub fn len(&self) -> usize {
        self.added_nodes.len()
            + self.updated_nodes.len()
            + self.removed_nodes.len()
            + self.added_edges.len()
            + self.removed_edges.len()
    }

    /// Record `prior`, the blob a following add of `id` overwrites. Keeps the FIRST capture of
    /// the batch, which is the pre-batch image.
    pub fn record_replaced_node(&mut self, id: String, prior: Vec<u8>) {
        self.replaced_node_props.entry(id).or_insert(prior);
    }

    pub fn record_add_node(&mut self, id: String) {
        self.added_nodes.push(NodeChange::new(id));
    }
    pub fn record_update_node(&mut self, id: String) {
        self.updated_nodes.push(NodeChange::new(id));
    }
    /// Record a removal whose blob and incident edges were not captured: the dependency clock
    /// floors for it and treats every edge type as touched.
    pub fn record_remove_node(&mut self, id: String) {
        self.removed_edge_rels.unattributed = true;
        self.removed_nodes.push(id);
    }
    /// Record a removed node together with the property blob it carried just before deletion
    /// (CONCEPT:EG-KG.storage.incremental-index-stamp, W1.6/P7), so incremental index maintenance can
    /// remove the id from exactly its postings and the dependency clock can bump exactly the
    /// label / key dimensions it touched. Use from any write path that reads the blob before the
    /// delete; `record_remove_node` remains correct where the blob is unavailable (coarse fallback).
    ///
    /// The node's incident edges are NOT captured here, so every edge type counts as touched; a
    /// path that can also read them uses [`Self::record_remove_node_captured`].
    pub fn record_remove_node_with_properties(&mut self, id: String, properties_msgpack: Vec<u8>) {
        self.removed_edge_rels.unattributed = true;
        self.removed_node_props
            .insert(id.clone(), properties_msgpack);
        self.removed_nodes.push(id);
    }

    /// Record a removed node with its property blob AND the relationship types of the edges the
    /// removal cascades (EH-393), both read before the delete.
    pub fn record_remove_node_captured(
        &mut self,
        id: String,
        properties_msgpack: Vec<u8>,
        incident: EdgeRels,
    ) {
        self.removed_edge_rels.merge(incident);
        self.removed_node_props
            .insert(id.clone(), properties_msgpack);
        self.removed_nodes.push(id);
    }
    pub fn record_add_edge(&mut self, source: String, target: String) {
        self.added_edges.push(EdgeChange { source, target });
    }
    /// Record an edge-pair removal whose relationship types were not captured: every edge type
    /// counts as touched.
    pub fn record_remove_edge(&mut self, source: String, target: String) {
        self.removed_edge_rels.unattributed = true;
        self.removed_edges.push(EdgeChange { source, target });
    }

    /// Record an edge-pair removal with the relationship types of the parallel edges it deletes,
    /// read before the delete (EH-393).
    pub fn record_remove_edge_captured(&mut self, source: String, target: String, rels: EdgeRels) {
        self.removed_edge_rels.merge(rels);
        self.removed_edges.push(EdgeChange { source, target });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blob(value: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&value).unwrap()
    }

    #[test]
    fn edge_rels_reads_relationship_types_and_flags_undecodable_blobs() {
        let typed = blob(serde_json::json!({"relationship": "KNOWS"}));
        let untyped = blob(serde_json::json!({"weight": 1}));
        let rels = EdgeRels::from_blobs([typed.as_slice(), untyped.as_slice()]);
        assert_eq!(rels.types.iter().collect::<Vec<_>>(), vec!["KNOWS"]);
        assert!(rels.touched && !rels.unattributed);
        let broken = EdgeRels::from_blobs([&[0xc1u8][..]]);
        assert!(
            broken.unattributed,
            "an undecodable edge blob cannot be attributed"
        );
        assert!(EdgeRels::from_blobs(std::iter::empty()).is_empty());
    }

    #[test]
    fn uncaptured_removals_are_unattributed_and_captured_ones_carry_types() {
        let mut uncaptured = ChangeSet::new();
        uncaptured.record_remove_edge("a".into(), "b".into());
        assert!(uncaptured.removed_edge_rels.unattributed);

        let mut captured = ChangeSet::new();
        let rels =
            EdgeRels::from_blobs([blob(serde_json::json!({"relationship": "CITES"})).as_slice()]);
        captured.record_remove_edge_captured("a".into(), "b".into(), rels);
        assert!(!captured.removed_edge_rels.unattributed);
        assert!(captured.removed_edge_rels.types.contains("CITES"));
    }

    #[test]
    fn the_first_replaced_image_of_a_batch_wins() {
        let mut change = ChangeSet::new();
        change.record_replaced_node("n".into(), vec![1]);
        change.record_replaced_node("n".into(), vec![2]);
        assert_eq!(change.replaced_node_props.get("n"), Some(&vec![1]));
    }
}
