//! Digest-authenticated affected-row image for complex MutationBatch commits.
//!
//! Runtime-derived mutations still execute against an isolated graph so their
//! result is known before durability, but persistence receives only this bounded
//! row delta rather than a serialized copy of every node, edge and embedding.

#[cfg(test)]
use std::collections::BTreeMap;
use std::collections::{BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use crate::graph::{GraphCore, GraphSnapshot};
use crate::protocol::Method;

pub(crate) const ROW_DELTA_ALGORITHM: &str = "sha256-row-delta-v2";
const ROW_DELTA_VERSION: u16 = 2;
const MAX_DELTA_OPERATIONS: usize = 1_000_000;

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    <Option<T> as serde::Deserialize>::deserialize(deserializer)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GraphRowDelta {
    schema_version: u16,
    operations: Vec<Method>,
    #[serde(deserialize_with = "deserialize_required_option")]
    ledger: Option<LedgerDelta>,
    /// Authoritative graph-control transition. `None` means unchanged; policy
    /// removal is not a current operation and is rejected while deriving a delta.
    #[serde(deserialize_with = "deserialize_required_option")]
    integrity_policy: Option<crate::graph::IntegrityPolicy>,
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
        let before_nodes: HashMap<_, _> = before
            .nodes
            .iter()
            .map(|(id, properties)| (id.as_str(), properties.as_slice()))
            .collect();
        let after_nodes: HashMap<_, _> = after
            .nodes
            .iter()
            .map(|(id, properties)| (id.as_str(), properties.as_slice()))
            .collect();
        let before_embeddings: HashMap<_, _> = before
            .semantic_store
            .embeddings_snapshot()
            .into_iter()
            .collect();
        let after_embeddings: HashMap<_, _> = after
            .semantic_store
            .embeddings_snapshot()
            .into_iter()
            .collect();

        // Removing an embedding while retaining its node is represented using
        // the existing atomic row vocabulary: remove/reinsert that node, then
        // restore its incident edges. RemoveNode already owns semantic cleanup.
        let mut forced_node_replacements = BTreeSet::new();
        let mut semantic_removals = BTreeSet::new();
        for node_id in before_embeddings.keys() {
            if !after_embeddings.contains_key(node_id) {
                semantic_removals.insert(node_id.clone());
                if after_nodes.contains_key(node_id.as_str()) {
                    forced_node_replacements.insert(node_id.clone());
                }
            }
        }

        let mut operations = Vec::new();
        let mut removed_nodes: BTreeSet<String> = before_nodes
            .keys()
            .filter(|node_id| !after_nodes.contains_key(**node_id))
            .map(|node_id| (*node_id).to_string())
            .collect();
        removed_nodes.extend(semantic_removals);
        for node_id in &removed_nodes {
            operations.push(Method::RemoveNode {
                node_id: node_id.clone(),
            });
        }

        let changed_nodes: BTreeSet<&str> = after_nodes
            .iter()
            .filter_map(|(node_id, properties)| {
                let changed = before_nodes
                    .get(node_id)
                    .is_none_or(|previous| *previous != *properties);
                (changed || forced_node_replacements.contains(*node_id)).then_some(*node_id)
            })
            .collect();
        for node_id in changed_nodes {
            operations.push(Method::AddNode {
                node_id: node_id.to_string(),
                properties_msgpack: after_nodes[node_id].to_vec(),
            });
        }

        let before_edges = edge_groups(before);
        let after_edges = edge_groups(after);
        let edge_keys: BTreeSet<_> = before_edges
            .keys()
            .chain(after_edges.keys())
            .filter(|key| {
                let key = *key;
                before_edges.get(key) != after_edges.get(key)
                    || forced_node_replacements.contains(&key.0)
                    || forced_node_replacements.contains(&key.1)
            })
            .cloned()
            .collect();
        for (source, target) in edge_keys {
            let before_values = before_edges.get(&(source.clone(), target.clone()));
            let after_values = after_edges.get(&(source.clone(), target.clone()));
            if before_values.is_some()
                && !removed_nodes.contains(&source)
                && !removed_nodes.contains(&target)
            {
                operations.push(Method::RemoveEdge {
                    source_id: source.clone(),
                    target_id: target.clone(),
                });
            }
            if let Some(values) = after_values {
                for properties in values {
                    operations.push(Method::AddEdge {
                        source_id: source.clone(),
                        target_id: target.clone(),
                        properties_msgpack: properties.clone(),
                    });
                }
            }
        }

        let changed_embeddings: BTreeSet<&str> = after_embeddings
            .iter()
            .filter_map(|(node_id, embedding)| {
                before_embeddings
                    .get(node_id)
                    .is_none_or(|previous| !same_embedding(previous, embedding))
                    .then_some(node_id.as_str())
            })
            .collect();
        for node_id in changed_embeddings {
            operations.push(Method::AddEmbedding {
                node_id: node_id.to_string(),
                embedding: after_embeddings[node_id].clone(),
            });
        }

        let common_ledger_prefix = before
            .ledger
            .iter()
            .zip(&after.ledger)
            .take_while(|(left, right)| left == right)
            .count();
        let ledger = (before.ledger != after.ledger).then(|| LedgerDelta {
            source_len: before.ledger.len() as u64,
            retain: common_ledger_prefix as u64,
            append: after.ledger[common_ledger_prefix..].to_vec(),
        });
        let integrity_policy = if before.integrity_policy == after.integrity_policy {
            None
        } else {
            Some(after.integrity_policy.clone().ok_or_else(|| {
                "current graph row delta cannot remove an integrity policy".to_string()
            })?)
        };
        let delta = Self {
            schema_version: ROW_DELTA_VERSION,
            operations,
            ledger,
            integrity_policy,
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
        Ok(())
    }

    pub(crate) fn operations(&self) -> &[Method] {
        &self.operations
    }

    pub(crate) fn ledger_patch(&self) -> Option<(u64, u64, &[String])> {
        self.ledger
            .as_ref()
            .map(|ledger| (ledger.source_len, ledger.retain, ledger.append.as_slice()))
    }

    pub(crate) fn integrity_policy_update(&self) -> Option<&crate::graph::IntegrityPolicy> {
        self.integrity_policy.as_ref()
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
        let added: BTreeSet<&str> = self
            .operations
            .iter()
            .filter_map(|method| match method {
                Method::AddNode { node_id, .. } => Some(node_id.as_str()),
                _ => None,
            })
            .collect();
        let removed: BTreeSet<&str> = self
            .operations
            .iter()
            .filter_map(|method| match method {
                Method::RemoveNode { node_id } => Some(node_id.as_str()),
                _ => None,
            })
            .collect();
        let mut transaction = core.txn();
        for method in &self.operations {
            if let Method::AddEdge {
                source_id,
                target_id,
                ..
            } = method
            {
                let present_after = |node_id: &str| {
                    added.contains(node_id)
                        || (transaction.has_node(node_id) && !removed.contains(node_id))
                };
                if !present_after(source_id) || !present_after(target_id) {
                    return Err("graph row delta edge has a missing final endpoint".to_string());
                }
            }
        }
        let mut semantic = core.semantic_store.write();
        for method in &self.operations {
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
        }
        drop(semantic);
        drop(transaction);
        if let Some((_, retain, append)) = self.ledger_patch() {
            let retain = usize::try_from(retain)
                .map_err(|_| "graph row delta ledger offset is invalid".to_string())?;
            core.replace_ledger_suffix(retain, append)?;
        }
        if let Some(policy) = self.integrity_policy_update() {
            core.set_integrity_policy(policy.clone());
        }
        Ok(())
    }
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
        assert_eq!(replayed.integrity_policy, after_snapshot.integrity_policy);
        assert_eq!(
            replayed.semantic_store.embeddings_snapshot(),
            after_snapshot.semantic_store.embeddings_snapshot()
        );
        assert!(encoded.len() < after_snapshot.to_msgpack().unwrap().len());
    }

    #[test]
    fn delta_replays_authoritative_integrity_policy() {
        let before = GraphCore::new();
        let before_snapshot = before.snapshot();
        let after = GraphCore::from_snapshot(before_snapshot.clone(), 0).unwrap();
        after.set_integrity_policy(crate::graph::IntegrityPolicy {
            shapes_ttl: "@prefix sh: <http://www.w3.org/ns/shacl#> .".to_string(),
        });
        let after_snapshot = after.snapshot();
        let delta = GraphRowDelta::between(&before_snapshot, &after_snapshot).unwrap();
        assert!(delta.integrity_policy_update().is_some());
        let replay = GraphCore::from_snapshot(before_snapshot, 0).unwrap();
        delta.apply_to(&replay).unwrap();
        assert_eq!(replay.integrity_policy(), after.integrity_policy());
    }

    fn canonical_nodes(snapshot: &GraphSnapshot) -> BTreeMap<String, Vec<u8>> {
        snapshot
            .nodes
            .iter()
            .map(|(id, properties)| (id.clone(), properties.as_ref().clone()))
            .collect()
    }
}
