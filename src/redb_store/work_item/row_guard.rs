//! Native row authority that generic graph-row writers may not touch.
//!
//! Two facts are maintained ONLY by the native WorkItem kernel:
//!
//! * a `ControlLease` row (graph-os EG-2) -- its grant, timing and lifecycle.
//!   A generic writer could otherwise re-activate a revoked lease, extend its
//!   expiry, or plant a forged one;
//! * the kernel-owned fields of a WorkItem row -- `row_revision` (the `version`
//!   the typed reads project) and the provenance references a terminal commit
//!   binds (`outcome_ref`, `outcome_digest`, `trace_ref`, `tool_call_refs`,
//!   graph-os EG-3). A lease's own fields are covered by the first rule.
//!
//! This guard runs beside `work_item_capability::validate_generic_method` in
//! the generic row applier, so every generic node write -- single, batched or
//! create-if-absent -- is checked in the same durable transaction it commits in.

use eg_types::control_lease::is_control_lease_row;
use eg_types::work_item_read::NATIVE_WORK_ITEM_ROW_KEYS;

use super::*;

type NodeRows<'a> = ScopedOwnerTableMut<'a, (&'static str, &'static str), &'static [u8]>;
type NodeMap = serde_json::Map<String, serde_json::Value>;

const LEASE_AUTHORITY: &str = "native control-lease authority required for a ControlLease row";
const NATIVE_KEY_AUTHORITY: &str =
    "native WorkItem authority required for a kernel-owned row field";

/// Refuse a generic node write that would create, change or remove native
/// row authority.
pub(crate) fn refuse_generic_native_row_write(
    graph: &str,
    method: &Method,
    nodes: &NodeRows<'_>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let guard = RowGuard {
        graph,
        nodes,
        crypto,
    };
    match method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        }
        | Method::CreateNodeIfAbsent {
            node_id,
            properties_msgpack,
        } => guard.write(node_id, properties_msgpack),
        Method::CompareAndSetNodeFields {
            node_id,
            updates_msgpack,
            ..
        } => guard.write(node_id, updates_msgpack),
        Method::RemoveNode { node_id } => guard.existing(node_id),
        Method::BatchUpdate { operations_msgpack } => guard.batch(operations_msgpack),
        _ => Ok(()),
    }
}

struct RowGuard<'g, 't> {
    graph: &'g str,
    nodes: &'g NodeRows<'t>,
    crypto: DurableCrypto<'g>,
}

impl RowGuard<'_, '_> {
    /// A write of `incoming` properties onto `node_id`.
    fn write(&self, node_id: &str, incoming: &[u8]) -> Result<(), String> {
        let stored = self.stored(node_id)?;
        refuse_stored_lease(stored.as_ref())?;
        // Legacy opaque node payloads are legal and carry no native fields.
        let Ok(incoming) = decode_durable::<NodeMap>(incoming) else {
            return Ok(());
        };
        if is_control_lease_row(&incoming) {
            return Err(LEASE_AUTHORITY.to_string());
        }
        let native_row = stored.as_ref().is_some_and(is_work_item_row);
        let owned = NATIVE_WORK_ITEM_ROW_KEYS
            .iter()
            .any(|key| incoming.contains_key(*key));
        if native_row && owned {
            return Err(NATIVE_KEY_AUTHORITY.to_string());
        }
        Ok(())
    }

    /// Any generic removal of a row that is already a control lease.
    fn existing(&self, node_id: &str) -> Result<(), String> {
        refuse_stored_lease(self.stored(node_id)?.as_ref())
    }

    /// The stored row, when it decodes as a property map.
    fn stored(&self, node_id: &str) -> Result<Option<NodeMap>, String> {
        let Some(value) = self.nodes.get((self.graph, node_id))? else {
            return Ok(None);
        };
        Ok(decode_durable::<NodeMap>(&self.crypto.unseal(value.value())?).ok())
    }

    fn batch(&self, operations_msgpack: &[u8]) -> Result<(), String> {
        use crate::redb_store::work_item_capability::{batch_node_touches, NodeTouch};
        for touch in batch_node_touches(operations_msgpack)? {
            match touch {
                NodeTouch::Write {
                    id,
                    properties_msgpack,
                } => self.write(&id, &properties_msgpack)?,
                NodeTouch::Remove { id } => self.existing(&id)?,
            }
        }
        Ok(())
    }
}

fn refuse_stored_lease(stored: Option<&NodeMap>) -> Result<(), String> {
    if stored.is_some_and(is_control_lease_row) {
        return Err(LEASE_AUTHORITY.to_string());
    }
    Ok(())
}

fn is_work_item_row(row: &NodeMap) -> bool {
    row.get("node_type").and_then(serde_json::Value::as_str) == Some("WorkItem")
}

#[cfg(test)]
mod tests {
    use super::super::test_shard::{open, with_nodes, GRAPH};
    use super::*;

    fn msgpack(value: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&value).unwrap()
    }

    fn check(shard: &Shard, tag: &str, method: Method) -> Result<(), String> {
        with_nodes(shard, tag, |nodes| {
            Ok(refuse_generic_native_row_write(
                GRAPH,
                &method,
                nodes,
                DurableCrypto::none(),
            ))
        })
    }

    #[test]
    fn generic_writers_cannot_forge_change_or_remove_native_row_authority() {
        let temp = open("row-guard");
        let mut lease = serde_json::Map::new();
        lease.insert("node_type".into(), "ControlLease".into());
        lease.insert("status".into(), "revoked".into());
        with_nodes(&temp.shard, "seed", |nodes| {
            write_work_item_props(nodes, GRAPH, "lease-1", &mut lease, DurableCrypto::none())
        });

        let reactivate = Method::CompareAndSetNodeFields {
            node_id: "lease-1".into(),
            conditions_msgpack: msgpack(serde_json::json!({})),
            updates_msgpack: msgpack(serde_json::json!({"status": "active"})),
        };
        assert!(check(&temp.shard, "cas", reactivate).is_err());
        let remove = Method::RemoveNode {
            node_id: "lease-1".into(),
        };
        assert!(check(&temp.shard, "remove", remove).is_err());
        let forge = Method::CreateNodeIfAbsent {
            node_id: "lease-2".into(),
            properties_msgpack: msgpack(serde_json::json!({"node_type": "ControlLease"})),
        };
        assert!(check(&temp.shard, "forge", forge).is_err());
        let mut work_item = serde_json::Map::new();
        work_item.insert("node_type".into(), "WorkItem".into());
        with_nodes(&temp.shard, "seed-item", |nodes| {
            write_work_item_props(nodes, GRAPH, "wi-1", &mut work_item, DurableCrypto::none())
        });
        let rewind = Method::CompareAndSetNodeFields {
            node_id: "wi-1".into(),
            conditions_msgpack: msgpack(serde_json::json!({})),
            updates_msgpack: msgpack(serde_json::json!({"row_revision": 1})),
        };
        assert!(check(&temp.shard, "rewind", rewind).is_err());
        let repoint = Method::CompareAndSetNodeFields {
            node_id: "wi-1".into(),
            conditions_msgpack: msgpack(serde_json::json!({})),
            updates_msgpack: msgpack(serde_json::json!({"outcome_ref": "forged"})),
        };
        assert!(check(&temp.shard, "repoint", repoint).is_err());

        let ordinary = Method::AddNode {
            node_id: "plain".into(),
            properties_msgpack: msgpack(serde_json::json!({"name": "a document"})),
        };
        check(&temp.shard, "ordinary", ordinary).unwrap();
    }
}
