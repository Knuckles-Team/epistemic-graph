//! Sealed record rows are create-only for generic writers (EH-558).
//!
//! A sealed record class ([`eg_types::sealed_record`]) is stored as an ordinary
//! graph node, so every generic node writer can reach its row. This guard runs in
//! the generic row applier, beside the WorkItem row guard, so every generic node
//! write is checked in the same durable transaction it would commit in. That covers
//! single writes, batched writes, compare-and-set, and the row deltas of staged
//! mutations.
//!
//! * Creating a row of a sealed class is allowed.
//! * A write whose result equals the stored row is allowed, so retries and replays
//!   stay idempotent.
//! * Any write that would change a stored sealed row, or remove it, is refused.
//!   Removal is refused because removing and re-creating a row is an overwrite in
//!   two steps.

use std::borrow::Cow;

use eg_types::sealed_record::is_sealed_record_class;

use super::store_prelude::*;
use super::*;

type NodeRows<'a> = ScopedOwnerTableMut<'a, (&'static str, &'static str), &'static [u8]>;
type NodeMap = serde_json::Map<String, serde_json::Value>;

/// One generic write's effect on a single node row.
enum NodeWrite<'a> {
    /// The row becomes exactly these properties.
    Replace(Cow<'a, str>, Cow<'a, [u8]>),
    /// These fields are merged over the stored row (compare-and-set).
    Merge(Cow<'a, str>, Cow<'a, [u8]>),
    /// A batch upsert, merged by the batch merge rule.
    Upsert(String, Vec<u8>),
    /// The row is removed.
    Remove(Cow<'a, str>),
}

impl NodeWrite<'_> {
    fn node_id(&self) -> &str {
        match self {
            Self::Replace(id, _) | Self::Merge(id, _) | Self::Remove(id) => id,
            Self::Upsert(id, _) => id,
        }
    }
}

/// Refuse a generic node write that would change or remove a stored sealed
/// record row.
pub(crate) fn refuse_generic_sealed_row_write(
    graph: &str,
    method: &Method,
    nodes: &NodeRows<'_>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    for write in node_writes(method)? {
        let stored = stored_sealed_row(graph, write.node_id(), nodes, crypto)?;
        check_node_write(stored.as_deref(), &write)?;
    }
    Ok(())
}

/// The node rows `method` writes. A create-if-absent write never replaces a stored
/// row, so it is not listed.
fn node_writes(method: &Method) -> Result<Vec<NodeWrite<'_>>, String> {
    let write = match method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => NodeWrite::Replace(node_id.into(), properties_msgpack.into()),
        Method::CompareAndSetNodeFields {
            node_id,
            updates_msgpack,
            ..
        } => NodeWrite::Merge(node_id.into(), updates_msgpack.into()),
        Method::RemoveNode { node_id } => NodeWrite::Remove(node_id.into()),
        Method::BatchUpdate { operations_msgpack } => return batch_writes(operations_msgpack),
        _ => return Ok(Vec::new()),
    };
    Ok(vec![write])
}

fn batch_writes(operations_msgpack: &[u8]) -> Result<Vec<NodeWrite<'static>>, String> {
    use crate::algorithms::BatchOperation;

    let writes = crate::algorithms::decode_batch_operations(operations_msgpack)?
        .into_iter()
        .filter_map(|operation| match operation {
            BatchOperation::AddNode {
                id,
                properties_msgpack,
                upsert: true,
            } => Some(NodeWrite::Upsert(id, properties_msgpack)),
            BatchOperation::AddNode {
                id,
                properties_msgpack,
                upsert: false,
            } => Some(NodeWrite::Replace(id.into(), properties_msgpack.into())),
            BatchOperation::RemoveNode { id } => Some(NodeWrite::Remove(id.into())),
            BatchOperation::AddEdge { .. }
            | BatchOperation::RemoveEdge { .. }
            | BatchOperation::AddEmbedding { .. } => None,
        })
        .collect();
    Ok(writes)
}

/// The stored row's plaintext when it is a sealed record; `None` for an absent or
/// ordinary row.
fn stored_sealed_row(
    graph: &str,
    node_id: &str,
    nodes: &NodeRows<'_>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<Vec<u8>>, String> {
    let Some(value) = nodes.get((graph, node_id)).map_err(|e| e.to_string())? else {
        return Ok(None);
    };
    let stored = crypto.unseal(value.value())?;
    Ok(is_sealed_row(&stored).then_some(stored))
}

/// Only the class key is decoded: every generic write pays this check.
#[derive(serde::Deserialize)]
struct RowClass {
    #[serde(rename = "type", default)]
    class: Option<serde_json::Value>,
}

fn is_sealed_row(stored: &[u8]) -> bool {
    decode_durable::<RowClass>(stored)
        .ok()
        .and_then(|row| row.class)
        .is_some_and(|class| class.as_str().is_some_and(is_sealed_record_class))
}

/// Allow the write iff it leaves the stored sealed row exactly as it is.
fn check_node_write(stored: Option<&[u8]>, write: &NodeWrite<'_>) -> Result<(), String> {
    let Some(stored) = stored else {
        return Ok(());
    };
    let current = decode_durable::<NodeMap>(stored).ok();
    let after = row_after(stored, current.clone(), write);
    if current.is_some() && after == current {
        return Ok(());
    }
    Err(format!(
        "sealed record row '{}' is create-only: a generic write may not change or remove it",
        write.node_id()
    ))
}

/// The row a write would leave behind; `None` for a removal or an undecodable write.
fn row_after(stored: &[u8], current: Option<NodeMap>, write: &NodeWrite<'_>) -> Option<NodeMap> {
    match write {
        NodeWrite::Replace(_, properties) => decode_durable(properties).ok(),
        NodeWrite::Merge(_, updates) => {
            let mut merged = current?;
            merged.extend(decode_durable::<NodeMap>(updates).ok()?);
            Some(merged)
        }
        NodeWrite::Upsert(_, properties) => {
            let merged = crate::algorithms::merge_batch_node_properties(stored, properties).ok()?;
            decode_durable(&merged).ok()
        }
        NodeWrite::Remove(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msgpack(value: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&value).unwrap()
    }

    fn sealed() -> Vec<u8> {
        msgpack(
            serde_json::json!({"type": "AnalysisSnapshot", "record": "{}", "analysisDigest": "sha256:ab"}),
        )
    }

    fn replace(properties: Vec<u8>) -> NodeWrite<'static> {
        NodeWrite::Replace("snap".into(), properties.into())
    }

    fn merge(updates: serde_json::Value) -> NodeWrite<'static> {
        NodeWrite::Merge("snap".into(), msgpack(updates).into())
    }

    #[test]
    fn only_rows_of_a_sealed_class_are_guarded() {
        assert!(is_sealed_row(&sealed()));
        assert!(!is_sealed_row(&msgpack(serde_json::json!({"type": "Doc"}))));
        assert!(!is_sealed_row(&msgpack(serde_json::json!({"type": 3}))));
        assert!(!is_sealed_row(b"opaque"));
    }

    #[test]
    fn creating_or_rewriting_identical_content_is_allowed() {
        check_node_write(None, &replace(sealed())).unwrap();
        check_node_write(None, &NodeWrite::Remove("snap".into())).unwrap();
        check_node_write(Some(&sealed()), &replace(sealed())).unwrap();
        // Same map, different key order on the wire.
        let reordered = msgpack(
            serde_json::json!({"analysisDigest": "sha256:ab", "record": "{}", "type": "AnalysisSnapshot"}),
        );
        check_node_write(Some(&sealed()), &replace(reordered)).unwrap();
        check_node_write(Some(&sealed()), &merge(serde_json::json!({"record": "{}"}))).unwrap();
    }

    #[test]
    fn changing_or_removing_a_stored_sealed_row_is_refused() {
        let forged = msgpack(
            serde_json::json!({"type": "AnalysisSnapshot", "record": "{\"forged\":1}", "analysisDigest": "sha256:ab"}),
        );
        let refusals = [
            replace(forged.clone()),
            replace(msgpack(serde_json::json!({"type": "Doc"}))),
            replace(b"opaque".to_vec()),
            merge(serde_json::json!({"record": "{\"forged\":1}"})),
            merge(serde_json::json!({"note": "added"})),
            NodeWrite::Upsert("snap".into(), msgpack(serde_json::json!({"record": "x"}))),
            NodeWrite::Remove("snap".into()),
        ];
        for write in &refusals {
            let error = check_node_write(Some(&sealed()), write).unwrap_err();
            assert!(error.contains("create-only"), "{error}");
        }
    }

    #[test]
    fn batch_writes_are_listed_per_node_and_edges_are_ignored() {
        let operations = msgpack(serde_json::json!([
            {"op": "add_node", "id": "a", "properties": {"type": "Doc"}},
            {"op": "remove_node", "id": "b"},
            {"op": "add_edge", "source": "a", "target": "b"},
        ]));
        let method = Method::BatchUpdate {
            operations_msgpack: operations,
        };
        let ids: Vec<String> = node_writes(&method)
            .unwrap()
            .iter()
            .map(|write| write.node_id().to_string())
            .collect();
        assert_eq!(ids, vec!["a".to_string(), "b".to_string()]);
    }
}
