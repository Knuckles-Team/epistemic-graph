//! Digest-authenticated affected-row image for complex MutationBatch commits.
//!
//! Runtime-derived mutations still execute against an isolated graph so their
//! result is known before durability, but persistence receives only this bounded
//! row delta rather than a serialized copy of every node, edge and embedding.

#[cfg(test)]
use std::collections::BTreeMap;
use std::collections::{BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use crate::compute::semantic::SemanticStore;
use crate::graph::{
    lift_v2_integrity_policy, GraphCore, GraphSchemaSources, GraphSnapshot, GraphTxn,
    IntegrityPolicyV2,
};
use crate::protocol::Method;

pub(crate) const ROW_DELTA_ALGORITHM: &str = "sha256-row-delta-schema-sources";
/// Persisted legacy rows accepted only by the one-time state decoder.
pub(crate) const LEGACY_ROW_DELTA_ALGORITHM: &str = "sha256-row-delta-v2";
const ROW_DELTA_VERSION: u16 = 3;
const MAX_DELTA_OPERATIONS: usize = 1_000_000;

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    <Option<T> as serde::Deserialize>::deserialize(deserializer)
}

#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GraphRowDelta {
    /// The persisted wire version that was decoded. This is deliberately not
    /// serialized: new rows always encode the current `schema_version`, while recovery
    /// uses it to bind a legacy v2 descriptor to actual v2 bytes.
    #[serde(skip)]
    decoded_wire_version: u16,
    schema_version: u16,
    operations: Vec<Method>,
    #[serde(deserialize_with = "deserialize_required_option")]
    ledger: Option<LedgerDelta>,
    /// Authoritative graph-control transition. `None` means unchanged; `Some`
    /// may carry an empty dynamic map, which is how detaching the final source
    /// is represented without ambiguity.
    #[serde(deserialize_with = "deserialize_required_option")]
    schema_sources: Option<std::sync::Arc<GraphSchemaSources>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GraphRowDeltaCurrent {
    schema_version: u16,
    operations: Vec<Method>,
    #[serde(deserialize_with = "deserialize_required_option")]
    ledger: Option<LedgerDelta>,
    #[serde(deserialize_with = "deserialize_required_option")]
    schema_sources: Option<std::sync::Arc<GraphSchemaSources>>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GraphRowDeltaV2 {
    schema_version: u16,
    operations: Vec<Method>,
    #[serde(deserialize_with = "deserialize_required_option")]
    ledger: Option<LedgerDelta>,
    #[serde(deserialize_with = "deserialize_required_option")]
    integrity_policy: Option<IntegrityPolicyV2>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum GraphRowDeltaWire {
    Current(GraphRowDeltaCurrent),
    V2(GraphRowDeltaV2),
}

impl<'de> Deserialize<'de> for GraphRowDelta {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error as _;
        match GraphRowDeltaWire::deserialize(deserializer)? {
            GraphRowDeltaWire::Current(value) if value.schema_version == ROW_DELTA_VERSION => {
                let schema_sources = value
                    .schema_sources
                    .map(|sources| {
                        std::sync::Arc::unwrap_or_clone(sources)
                            .reconciled_current_core()
                            .map(std::sync::Arc::new)
                    })
                    .transpose()
                    .map_err(D::Error::custom)?;
                Ok(Self {
                    decoded_wire_version: value.schema_version,
                    schema_version: value.schema_version,
                    operations: value.operations,
                    ledger: value.ledger,
                    schema_sources,
                })
            }
            GraphRowDeltaWire::V2(value) if value.schema_version == 2 => Ok(Self {
                decoded_wire_version: value.schema_version,
                schema_version: ROW_DELTA_VERSION,
                operations: value.operations,
                ledger: value.ledger,
                schema_sources: value
                    .integrity_policy
                    .map(|policy| lift_v2_integrity_policy(Some(policy)))
                    .transpose()
                    .map_err(D::Error::custom)?,
            }),
            GraphRowDeltaWire::Current(value) => Err(D::Error::custom(format!(
                "unsupported graph row-delta version {}",
                value.schema_version
            ))),
            GraphRowDeltaWire::V2(value) => Err(D::Error::custom(format!(
                "unsupported legacy graph row-delta version {}",
                value.schema_version
            ))),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LedgerDelta {
    source_len: u64,
    retain: u64,
    append: Vec<String>,
}

impl GraphRowDelta {
    pub(crate) fn between(before: &GraphSnapshot, after: &GraphSnapshot) -> Result<Self, String> {
        let nodes = SnapshotPair::new(node_rows(before), node_rows(after));
        let embeddings = SnapshotPair::new(embedding_rows(before), embedding_rows(after));
        let removals = NodeRemovals::between(&nodes, &embeddings);
        let mut operations = Vec::new();
        push_node_operations(&mut operations, &nodes, &removals);
        push_edge_operations(&mut operations, before, after, &removals);
        push_embedding_operations(&mut operations, &embeddings);
        let delta = Self {
            decoded_wire_version: ROW_DELTA_VERSION,
            schema_version: ROW_DELTA_VERSION,
            operations,
            ledger: ledger_delta(before, after),
            schema_sources: schema_sources_delta(before, after),
        };
        delta.validate()?;
        Ok(delta)
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.schema_version != ROW_DELTA_VERSION {
            return Err("unsupported graph row-delta version".to_string());
        }
        let ledger_operations = match self.ledger.as_ref() {
            Some(ledger) if ledger.retain <= ledger.source_len => {
                usize::try_from(ledger.source_len - ledger.retain)
                    .ok()
                    .and_then(|removed| removed.checked_add(ledger.append.len()))
                    .ok_or_else(|| "graph row delta ledger size is invalid".to_string())?
            }
            Some(_) => return Err("graph row delta ledger prefix is invalid".to_string()),
            None => 0,
        };
        if self
            .operations
            .len()
            .checked_add(ledger_operations)
            .is_none_or(|count| count > MAX_DELTA_OPERATIONS)
        {
            return Err("graph row delta exceeds the operation limit".to_string());
        }
        if self.operations.iter().any(|method| {
            !matches!(
                method,
                Method::AddNode { .. }
                    | Method::RemoveNode { .. }
                    | Method::AddEdge { .. }
                    | Method::RemoveEdge { .. }
                    | Method::AddEmbedding { .. }
            )
        }) {
            return Err("graph row delta contains a non-row operation".to_string());
        }
        validate_schema_sources_update(self.schema_sources.as_ref())
    }

    pub(crate) fn operations(&self) -> &[Method] {
        &self.operations
    }

    pub(crate) fn ledger_patch(&self) -> Option<(u64, u64, &[String])> {
        self.ledger
            .as_ref()
            .map(|ledger| (ledger.source_len, ledger.retain, ledger.append.as_slice()))
    }

    pub(crate) fn schema_sources_update(&self) -> Option<&std::sync::Arc<GraphSchemaSources>> {
        self.schema_sources.as_ref()
    }

    pub(crate) fn matches_algorithm(&self, algorithm: &str) -> bool {
        matches!(
            (algorithm, self.decoded_wire_version),
            (ROW_DELTA_ALGORITHM, ROW_DELTA_VERSION) | (LEGACY_ROW_DELTA_ALGORITHM, 2)
        )
    }

    pub(crate) fn preserves_node_derived_indexes(&self) -> bool {
        self.operations.iter().all(|method| {
            matches!(
                method,
                Method::AddEdge { .. } | Method::RemoveEdge { .. } | Method::AddEmbedding { .. }
            )
        })
    }

    pub(crate) fn to_msgpack(&self) -> Result<Vec<u8>, String> {
        rmp_serde::to_vec_named(self).map_err(|error| error.to_string())
    }

    pub(crate) fn from_msgpack(bytes: &[u8]) -> Result<Self, String> {
        let delta: Self = rmp_serde::from_slice(bytes).map_err(|error| error.to_string())?;
        delta.validate()?;
        Ok(delta)
    }

    pub(crate) fn apply_to(&self, core: &GraphCore) -> Result<(), String> {
        if let Some((source_len, _, _)) = self.ledger_patch() {
            if core.ledger_len() as u64 != source_len {
                return Err("graph row delta ledger pre-image does not match".to_string());
            }
        }
        let mut transaction = core.txn();
        ensure_final_edge_endpoints(&self.operations, &transaction)?;
        let mut semantic = core.semantic_store.write();
        for method in &self.operations {
            apply_row_operation(&mut transaction, &mut semantic, method)?;
        }
        drop(semantic);
        drop(transaction);
        if let Some((_, retain, append)) = self.ledger_patch() {
            let retain = usize::try_from(retain)
                .map_err(|_| "graph row delta ledger offset is invalid".to_string())?;
            core.replace_ledger_suffix(retain, append)?;
        }
        if let Some(sources) = self.schema_sources_update() {
            core.install_schema_sources(std::sync::Arc::clone(sources));
        }
        Ok(())
    }
}

fn validate_schema_sources_update(
    sources: Option<&std::sync::Arc<GraphSchemaSources>>,
) -> Result<(), String> {
    if let Some(sources) = sources {
        sources.validate()?;
    }
    Ok(())
}

/// The before/after images of one keyed row family.
struct SnapshotPair<T> {
    before: T,
    after: T,
}

impl<T> SnapshotPair<T> {
    fn new(before: T, after: T) -> Self {
        Self { before, after }
    }
}

type NodeRows<'a> = HashMap<&'a str, &'a [u8]>;
type EmbeddingRows = HashMap<String, Vec<f32>>;

fn node_rows(snapshot: &GraphSnapshot) -> NodeRows<'_> {
    snapshot
        .nodes
        .iter()
        .map(|(id, properties)| (id.as_str(), properties.as_slice()))
        .collect()
}

fn embedding_rows(snapshot: &GraphSnapshot) -> EmbeddingRows {
    snapshot
        .semantic_store
        .embeddings_snapshot()
        .into_iter()
        .collect()
}

/// Nodes the delta removes, and the subset it must reinsert.
///
/// Removing an embedding while retaining its node is represented using the
/// existing atomic row vocabulary: remove/reinsert that node, then restore its
/// incident edges. RemoveNode already owns semantic cleanup.
struct NodeRemovals {
    removed: BTreeSet<String>,
    forced_replacements: BTreeSet<String>,
}

impl NodeRemovals {
    fn between(
        nodes: &SnapshotPair<NodeRows<'_>>,
        embeddings: &SnapshotPair<EmbeddingRows>,
    ) -> Self {
        let semantic_removals: BTreeSet<String> = embeddings
            .before
            .keys()
            .filter(|node_id| !embeddings.after.contains_key(*node_id))
            .cloned()
            .collect();
        let forced_replacements = semantic_removals
            .iter()
            .filter(|node_id| nodes.after.contains_key(node_id.as_str()))
            .cloned()
            .collect();
        let mut removed: BTreeSet<String> = nodes
            .before
            .keys()
            .filter(|node_id| !nodes.after.contains_key(**node_id))
            .map(|node_id| (*node_id).to_string())
            .collect();
        removed.extend(semantic_removals);
        Self {
            removed,
            forced_replacements,
        }
    }

    fn touches_edge(&self, (source, target): &(String, String)) -> bool {
        self.forced_replacements.contains(source) || self.forced_replacements.contains(target)
    }

    fn removes_endpoint_of(&self, source: &str, target: &str) -> bool {
        self.removed.contains(source) || self.removed.contains(target)
    }
}

fn push_node_operations(
    operations: &mut Vec<Method>,
    nodes: &SnapshotPair<NodeRows<'_>>,
    removals: &NodeRemovals,
) {
    operations.extend(removals.removed.iter().map(|node_id| Method::RemoveNode {
        node_id: node_id.clone(),
    }));
    let changed_nodes: BTreeSet<&str> = nodes
        .after
        .iter()
        .filter_map(|(node_id, properties)| {
            let changed = nodes
                .before
                .get(node_id)
                .is_none_or(|previous| *previous != *properties);
            (changed || removals.forced_replacements.contains(*node_id)).then_some(*node_id)
        })
        .collect();
    operations.extend(changed_nodes.into_iter().map(|node_id| Method::AddNode {
        node_id: node_id.to_string(),
        properties_msgpack: nodes.after[node_id].to_vec(),
    }));
}

fn push_edge_operations(
    operations: &mut Vec<Method>,
    before: &GraphSnapshot,
    after: &GraphSnapshot,
    removals: &NodeRemovals,
) {
    let edges = SnapshotPair::new(edge_groups(before), edge_groups(after));
    let edge_keys: BTreeSet<_> = edges
        .before
        .keys()
        .chain(edges.after.keys())
        .filter(|key| edges.before.get(*key) != edges.after.get(*key) || removals.touches_edge(key))
        .cloned()
        .collect();
    for key in edge_keys {
        let (source, target) = &key;
        if edges.before.contains_key(&key) && !removals.removes_endpoint_of(source, target) {
            operations.push(Method::RemoveEdge {
                source_id: source.clone(),
                target_id: target.clone(),
            });
        }
        let added = edges.after.get(&key).into_iter().flatten();
        operations.extend(added.map(|properties| Method::AddEdge {
            source_id: source.clone(),
            target_id: target.clone(),
            properties_msgpack: properties.clone(),
        }));
    }
}

fn push_embedding_operations(
    operations: &mut Vec<Method>,
    embeddings: &SnapshotPair<EmbeddingRows>,
) {
    let changed_embeddings: BTreeSet<&str> = embeddings
        .after
        .iter()
        .filter_map(|(node_id, embedding)| {
            embeddings
                .before
                .get(node_id)
                .is_none_or(|previous| !same_embedding(previous, embedding))
                .then_some(node_id.as_str())
        })
        .collect();
    operations.extend(
        changed_embeddings
            .into_iter()
            .map(|node_id| Method::AddEmbedding {
                node_id: node_id.to_string(),
                embedding: embeddings.after[node_id].clone(),
            }),
    );
}

fn ledger_delta(before: &GraphSnapshot, after: &GraphSnapshot) -> Option<LedgerDelta> {
    let common_ledger_prefix = before
        .ledger
        .iter()
        .zip(&after.ledger)
        .take_while(|(left, right)| left == right)
        .count();
    (before.ledger != after.ledger).then(|| LedgerDelta {
        source_len: before.ledger.len() as u64,
        retain: common_ledger_prefix as u64,
        append: after.ledger[common_ledger_prefix..].to_vec(),
    })
}

fn schema_sources_delta(
    before: &GraphSnapshot,
    after: &GraphSnapshot,
) -> Option<std::sync::Arc<GraphSchemaSources>> {
    if before.schema_sources == after.schema_sources {
        return None;
    }
    Some(std::sync::Arc::clone(&after.schema_sources))
}

/// Every added edge must have both endpoints present once the delta applies:
/// added by this delta, or already present and not removed by it.
fn ensure_final_edge_endpoints(
    operations: &[Method],
    transaction: &GraphTxn<'_>,
) -> Result<(), String> {
    let (added, removed) = node_row_ids(operations);
    let present_after = |node_id: &str| {
        added.contains(node_id) || (transaction.has_node(node_id) && !removed.contains(node_id))
    };
    for method in operations {
        if let Method::AddEdge {
            source_id,
            target_id,
            ..
        } = method
        {
            if !present_after(source_id) || !present_after(target_id) {
                return Err("graph row delta edge has a missing final endpoint".to_string());
            }
        }
    }
    Ok(())
}

/// The node ids this delta adds and removes.
fn node_row_ids(operations: &[Method]) -> (BTreeSet<&str>, BTreeSet<&str>) {
    let mut added = BTreeSet::new();
    let mut removed = BTreeSet::new();
    for method in operations {
        if let Method::AddNode { node_id, .. } = method {
            added.insert(node_id.as_str());
        } else if let Method::RemoveNode { node_id } = method {
            removed.insert(node_id.as_str());
        }
    }
    (added, removed)
}

fn apply_row_operation(
    transaction: &mut GraphTxn<'_>,
    semantic: &mut SemanticStore,
    method: &Method,
) -> Result<(), String> {
    match method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => transaction.add_node(node_id.clone(), properties_msgpack.clone()),
        Method::RemoveNode { node_id } => {
            transaction.remove_node(node_id.clone());
            semantic.remove_embedding(node_id);
        }
        Method::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
        } => transaction.add_edge(
            source_id.clone(),
            target_id.clone(),
            properties_msgpack.clone(),
        )?,
        Method::RemoveEdge {
            source_id,
            target_id,
        } => transaction.remove_edge(source_id.clone(), target_id.clone()),
        Method::AddEmbedding { node_id, embedding } => semantic
            .add_embedding(node_id.clone(), embedding.clone())
            .map_err(|error| error.to_string())?,
        _ => return Err("graph row delta contains a non-projectable operation".to_string()),
    }
    Ok(())
}

fn edge_groups(snapshot: &GraphSnapshot) -> HashMap<(String, String), Vec<Vec<u8>>> {
    let mut groups = HashMap::new();
    for (source, target, properties) in &snapshot.edges {
        groups
            .entry((source.clone(), target.clone()))
            .or_insert_with(Vec::new)
            .push(properties.as_ref().clone());
    }
    groups
}

fn same_embedding(left: &[f32], right: &[f32]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.to_bits() == right.to_bits())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn install_operator(core: &GraphCore, shapes: &str) {
        let mut sources = (*core.schema_sources()).clone();
        sources
            .attach_dynamic(
                crate::graph::OPERATOR_SOURCE_ID.to_string(),
                crate::graph::GraphSchemaSource::new(
                    crate::graph::SchemaSourceOrigin::Operator,
                    Some(std::sync::Arc::from(shapes)),
                    None,
                    0,
                )
                .unwrap(),
            )
            .unwrap();
        core.install_schema_sources(std::sync::Arc::new(sources));
    }

    fn props(value: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&value).unwrap()
    }

    #[test]
    fn delta_replays_nodes_parallel_edges_and_embeddings() {
        let before = GraphCore::new();
        before.add_node("a".into(), props(serde_json::json!({"v": 1})));
        before.add_node("b".into(), props(serde_json::json!({"v": 2})));
        for index in 0..128 {
            before.add_node(
                format!("untouched-{index:03}"),
                props(serde_json::json!({"payload": "unchanged", "index": index})),
            );
        }
        before
            .add_edge("a".into(), "b".into(), props(serde_json::json!({"n": 1})))
            .unwrap();
        before
            .semantic_store
            .write()
            .add_embedding("a".into(), vec![1.0, 0.0])
            .unwrap();
        let before_snapshot = before.snapshot();

        let after = GraphCore::from_snapshot(before_snapshot.clone(), 0).unwrap();
        after.add_node("a".into(), props(serde_json::json!({"v": 3})));
        after
            .add_edge("a".into(), "b".into(), props(serde_json::json!({"n": 2})))
            .unwrap();
        after
            .semantic_store
            .write()
            .add_embedding("a".into(), vec![0.0, 1.0])
            .unwrap();
        let after_snapshot = after.snapshot();

        let delta = GraphRowDelta::between(&before_snapshot, &after_snapshot).unwrap();
        let encoded = delta.to_msgpack().unwrap();
        assert_eq!(
            encoded,
            GraphRowDelta::between(&before_snapshot, &after_snapshot)
                .unwrap()
                .to_msgpack()
                .unwrap(),
            "row-delta digests must not depend on randomized hash iteration",
        );
        let decoded = GraphRowDelta::from_msgpack(&encoded).unwrap();
        let replay = GraphCore::from_snapshot(before_snapshot.clone(), 0).unwrap();
        decoded.apply_to(&replay).unwrap();
        let replayed = replay.snapshot();

        assert_eq!(canonical_nodes(&replayed), canonical_nodes(&after_snapshot));
        assert_eq!(edge_groups(&replayed), edge_groups(&after_snapshot));
        assert_eq!(replayed.ledger, after_snapshot.ledger);
        assert_eq!(replayed.schema_sources, after_snapshot.schema_sources);
        assert_eq!(
            replayed.semantic_store.embeddings_snapshot(),
            after_snapshot.semantic_store.embeddings_snapshot()
        );
        assert!(encoded.len() < after_snapshot.to_msgpack().unwrap().len());
    }

    #[test]
    fn delta_replays_authoritative_schema_sources() {
        let before = GraphCore::new();
        let before_snapshot = before.snapshot();
        let after = GraphCore::from_snapshot(before_snapshot.clone(), 0).unwrap();
        install_operator(&after, "@prefix sh: <http://www.w3.org/ns/shacl#> .");
        let after_snapshot = after.snapshot();
        let delta = GraphRowDelta::between(&before_snapshot, &after_snapshot).unwrap();
        assert!(delta.schema_sources_update().is_some());
        let replay = GraphCore::from_snapshot(before_snapshot, 0).unwrap();
        delta.apply_to(&replay).unwrap();
        assert_eq!(replay.schema_sources(), after.schema_sources());
    }

    fn replay(before: &GraphSnapshot, after: &GraphSnapshot) -> GraphSnapshot {
        let delta = GraphRowDelta::between(before, after).unwrap();
        let replay = GraphCore::from_snapshot(before.clone(), 0).unwrap();
        delta.apply_to(&replay).unwrap();
        replay.snapshot()
    }

    fn assert_replayed(before: &GraphSnapshot, after: &GraphSnapshot) {
        let replayed = replay(before, after);
        assert_eq!(canonical_nodes(&replayed), canonical_nodes(after));
        assert_eq!(edge_groups(&replayed), edge_groups(after));
        assert_eq!(
            replayed.semantic_store.embeddings_snapshot(),
            after.semantic_store.embeddings_snapshot()
        );
    }

    #[test]
    fn delta_replays_embedding_removal_node_removal_and_ledger_suffix() {
        let before = GraphCore::new();
        for id in ["a", "b", "c"] {
            before.add_node(id.into(), props(serde_json::json!({ "id": id })));
        }
        before
            .add_edge("a".into(), "b".into(), props(serde_json::json!({"e": 1})))
            .unwrap();
        before
            .add_edge("b".into(), "c".into(), props(serde_json::json!({"e": 2})))
            .unwrap();
        before
            .semantic_store
            .write()
            .add_embedding("a".into(), vec![1.0, 0.0])
            .unwrap();
        let before_snapshot = before.snapshot();

        // Drop a's embedding while keeping the node (forced replacement that
        // must restore a->b), and remove c outright (its edge goes with it).
        let mut after_snapshot = before_snapshot.clone();
        after_snapshot.semantic_store = crate::compute::semantic::SemanticStore::new();
        after_snapshot.nodes.retain(|(id, _)| id != "c");
        after_snapshot.edges.retain(|(_, target, _)| target != "c");
        after_snapshot.ledger.push("appended".to_string());

        let delta = GraphRowDelta::between(&before_snapshot, &after_snapshot).unwrap();
        assert!(delta
            .operations()
            .iter()
            .any(|method| matches!(method, Method::RemoveNode { node_id } if node_id == "a")));
        assert!(!delta.preserves_node_derived_indexes());
        let (source_len, retain, append) = delta.ledger_patch().unwrap();
        assert_eq!(source_len, before_snapshot.ledger.len() as u64);
        assert_eq!(retain, before_snapshot.ledger.len() as u64);
        assert_eq!(append, ["appended".to_string()]);
        assert_replayed(&before_snapshot, &after_snapshot);
    }

    #[test]
    fn delta_between_identical_snapshots_is_empty() {
        let core = GraphCore::new();
        core.add_node("a".into(), props(serde_json::json!({"v": 1})));
        let snapshot = core.snapshot();
        let delta = GraphRowDelta::between(&snapshot, &snapshot).unwrap();
        assert!(delta.operations().is_empty());
        assert!(delta.ledger_patch().is_none());
        assert!(delta.schema_sources_update().is_none());
        assert!(delta.preserves_node_derived_indexes());
    }

    #[test]
    fn delta_can_detach_the_final_dynamic_source() {
        let before = GraphCore::new();
        install_operator(&before, "@prefix sh: <http://www.w3.org/ns/shacl#> .");
        let before_snapshot = before.snapshot();
        let mut after_snapshot = before_snapshot.clone();
        after_snapshot.schema_sources =
            std::sync::Arc::new(crate::graph::GraphSchemaSources::default());
        let delta = GraphRowDelta::between(&before_snapshot, &after_snapshot).unwrap();
        let replay = GraphCore::from_snapshot(before_snapshot, 0).unwrap();
        delta.apply_to(&replay).unwrap();
        assert!(replay.schema_sources().dynamic.is_empty());
    }

    #[test]
    fn v2_delta_lifts_a_policy_but_preserves_none_as_unchanged() {
        const V2_DELTA_GOLDEN: &str = "84ae736368656d615f76657273696f6e02aa6f7065726174696f6e7390a66c6564676572c0b0696e746567726974795f706f6c69637981aa7368617065735f74746cd92b407072656669782073683a203c687474703a2f2f7777772e77332e6f72672f6e732f736861636c233e202e";
        let with_policy = GraphRowDeltaV2 {
            schema_version: 2,
            operations: Vec::new(),
            ledger: None,
            integrity_policy: Some(IntegrityPolicyV2 {
                shapes_ttl: "@prefix sh: <http://www.w3.org/ns/shacl#> .".to_string(),
            }),
        };
        let golden = hex::decode(V2_DELTA_GOLDEN).unwrap();
        assert_eq!(rmp_serde::to_vec_named(&with_policy).unwrap(), golden);
        let decoded = GraphRowDelta::from_msgpack(&golden).unwrap();
        assert!(decoded.matches_algorithm(LEGACY_ROW_DELTA_ALGORITHM));
        assert!(!decoded.matches_algorithm(ROW_DELTA_ALGORITHM));
        assert!(decoded
            .schema_sources_update()
            .unwrap()
            .dynamic
            .contains_key(crate::graph::OPERATOR_SOURCE_ID));

        let unchanged = GraphRowDeltaV2 {
            schema_version: 2,
            operations: Vec::new(),
            ledger: None,
            integrity_policy: None,
        };
        let decoded =
            GraphRowDelta::from_msgpack(&rmp_serde::to_vec_named(&unchanged).unwrap()).unwrap();
        assert!(decoded.schema_sources_update().is_none());
        assert!(decoded.matches_algorithm(LEGACY_ROW_DELTA_ALGORITHM));
    }

    fn delta_of(operations: Vec<Method>) -> GraphRowDelta {
        GraphRowDelta {
            decoded_wire_version: ROW_DELTA_VERSION,
            schema_version: ROW_DELTA_VERSION,
            operations,
            ledger: None,
            schema_sources: None,
        }
    }

    #[test]
    fn apply_refuses_missing_endpoints_ledger_mismatch_and_non_row_operations() {
        let core = GraphCore::new();
        core.add_node("a".into(), props(serde_json::json!({})));
        core.add_node("gone".into(), props(serde_json::json!({})));
        let add_edge = |target: &str| Method::AddEdge {
            source_id: "a".to_string(),
            target_id: target.to_string(),
            properties_msgpack: props(serde_json::json!({})),
        };
        let missing = "graph row delta edge has a missing final endpoint";
        assert_eq!(
            delta_of(vec![add_edge("absent")])
                .apply_to(&core)
                .unwrap_err(),
            missing
        );
        let removed_endpoint = delta_of(vec![
            Method::RemoveNode {
                node_id: "gone".to_string(),
            },
            add_edge("gone"),
        ]);
        assert_eq!(removed_endpoint.apply_to(&core).unwrap_err(), missing);
        assert_eq!(core.node_count(), 2, "a refused delta applies nothing");
        let readded_endpoint = delta_of(vec![
            Method::AddNode {
                node_id: "new".to_string(),
                properties_msgpack: props(serde_json::json!({})),
            },
            add_edge("new"),
        ]);
        readded_endpoint.apply_to(&core).unwrap();
        assert_eq!(core.node_count(), 3);

        let mut ledger_mismatch = delta_of(Vec::new());
        ledger_mismatch.ledger = Some(LedgerDelta {
            source_len: core.ledger_len() as u64 + 1,
            retain: 0,
            append: Vec::new(),
        });
        assert_eq!(
            ledger_mismatch.apply_to(&core).unwrap_err(),
            "graph row delta ledger pre-image does not match"
        );

        let non_row = delta_of(vec![Method::ClearGraph]);
        assert!(non_row.validate().is_err());
        assert_eq!(
            non_row.apply_to(&core).unwrap_err(),
            "graph row delta contains a non-projectable operation"
        );
    }

    fn canonical_nodes(snapshot: &GraphSnapshot) -> BTreeMap<String, Vec<u8>> {
        snapshot
            .nodes
            .iter()
            .map(|(id, properties)| (id.clone(), properties.as_ref().clone()))
            .collect()
    }
}
