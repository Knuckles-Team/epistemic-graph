//! Generic writers may not change, remove or forge a sealed record row (EH-558).
//!
//! A sealed record class ([`eg_types::sealed_record`]) is stored as an ordinary
//! graph node, so every generic node writer can reach its row. This guard judges one
//! generic write `Method` against the CURRENT stored rows, read through
//! [`StoredNodeRows`], and is called at every place a generic write is admitted:
//!
//! * the server's commit gateway, before any apply or durable commit, against the
//!   serving projection — every routed mutation, persistent or not;
//! * the embedded engine, before its durable commit or in-memory apply;
//! * the durable generic row applier, inside the write transaction — which also
//!   covers staged row deltas, batch and lifecycle coordinators and replication.
//!
//! The rules:
//! * creating a row of a sealed class is allowed, unless the class is native-only
//!   (a tombstone);
//! * a write whose result equals the stored row is allowed, so retries and replays
//!   stay idempotent;
//! * any write that would change a stored sealed row, or remove it, is refused.
//!   Removal is refused because removing and re-creating a row is an overwrite in two
//!   steps. The owning op, `RetireSealedRecord`, is a native method this guard never
//!   sees as a generic write.

use std::borrow::Cow;

use eg_types::sealed_record::{sealed_class, SealedClass};

use crate::graph::GraphCore;
use crate::protocol::Method;

type NodeMap = serde_json::Map<String, serde_json::Value>;

/// Read access to the current stored node rows a write is judged against.
pub(crate) trait StoredNodeRows {
    /// The stored property blob of `node_id` (plaintext), if the row exists.
    fn stored(&self, node_id: &str) -> Result<Option<Vec<u8>>, String>;
}

impl StoredNodeRows for GraphCore {
    fn stored(&self, node_id: &str) -> Result<Option<Vec<u8>>, String> {
        Ok(self.get_node_properties(node_id))
    }
}

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

/// Refuse a generic node write that would change, remove or forge a sealed record
/// row, judged against `rows`.
pub(crate) fn refuse_generic_sealed_write(
    method: &Method,
    rows: &dyn StoredNodeRows,
) -> Result<(), String> {
    for write in node_writes(method)? {
        let stored = rows.stored(write.node_id())?;
        check_node_write(stored.as_deref(), &write)?;
    }
    Ok(())
}

/// The node rows `method` writes. A create-if-absent write never replaces a stored
/// row, so only its creation is judged.
fn node_writes(method: &Method) -> Result<Vec<NodeWrite<'_>>, String> {
    let write = match method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => NodeWrite::Replace(node_id.into(), properties_msgpack.into()),
        Method::CreateNodeIfAbsent {
            node_id,
            properties_msgpack,
        } => return Ok(created_only(node_id, properties_msgpack)),
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

/// A create-if-absent write can only create: it is judged as a creation (so a
/// native-only class is refused) and never as an overwrite.
fn created_only<'a>(node_id: &'a str, properties: &'a [u8]) -> Vec<NodeWrite<'a>> {
    match row_class(properties) {
        Some(class) if !class.generic_create => {
            vec![NodeWrite::Replace(node_id.into(), properties.into())]
        }
        _ => Vec::new(),
    }
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

/// Only the class key is decoded: every generic write pays this check.
#[derive(serde::Deserialize)]
struct RowClass {
    #[serde(rename = "type", default)]
    class: Option<serde_json::Value>,
}

/// The sealed class of a property blob, decoding only its `type`.
fn row_class(blob: &[u8]) -> Option<&'static SealedClass> {
    let row = eg_types::msgpack::decode_bounded::<RowClass>(blob, row_limits()).ok()?;
    row.class?.as_str().and_then(sealed_class)
}

fn row_limits() -> eg_types::msgpack::MsgpackLimits {
    eg_types::msgpack::MsgpackLimits::new(
        eg_types::msgpack::MAX_PROPERTY_BYTES,
        eg_types::msgpack::MAX_PROPERTY_ITEMS,
        eg_types::msgpack::DEFAULT_MAX_DEPTH,
    )
}

fn decode_row(blob: &[u8]) -> Option<NodeMap> {
    eg_types::msgpack::decode_property_object(blob).ok()
}

/// Judge one write against the stored row.
fn check_node_write(stored: Option<&[u8]>, write: &NodeWrite<'_>) -> Result<(), String> {
    match stored.filter(|blob| row_class(blob).is_some()) {
        Some(sealed) => check_overwrite(sealed, write),
        None => check_creation(write),
    }
}

/// A write onto an ordinary or absent row may not create a native-only sealed row.
fn check_creation(write: &NodeWrite<'_>) -> Result<(), String> {
    let incoming: &[u8] = match write {
        NodeWrite::Replace(_, properties) | NodeWrite::Merge(_, properties) => properties,
        NodeWrite::Upsert(_, properties) => properties,
        NodeWrite::Remove(_) => return Ok(()),
    };
    match row_class(incoming) {
        Some(class) if !class.generic_create => Err(format!(
            "sealed record row '{}' of class {} is written only by its owning op",
            write.node_id(),
            class.name
        )),
        _ => Ok(()),
    }
}

/// A write onto a stored sealed row is allowed iff it leaves the row as it is.
fn check_overwrite(stored: &[u8], write: &NodeWrite<'_>) -> Result<(), String> {
    let current = decode_row(stored);
    if current.is_some() && row_after(stored, current.clone(), write) == current {
        return Ok(());
    }
    Err(format!(
        "sealed record row '{}' is create-only: a generic write may not change or remove \
         it; retire it through RetireSealedRecord",
        write.node_id()
    ))
}

/// The row a write would leave behind; `None` for a removal or an undecodable write.
fn row_after(stored: &[u8], current: Option<NodeMap>, write: &NodeWrite<'_>) -> Option<NodeMap> {
    match write {
        NodeWrite::Replace(_, properties) => decode_row(properties),
        NodeWrite::Merge(_, updates) => {
            let mut merged = current?;
            merged.extend(decode_row(updates)?);
            Some(merged)
        }
        NodeWrite::Upsert(_, properties) => {
            let merged = crate::algorithms::merge_batch_node_properties(stored, properties).ok()?;
            decode_row(&merged)
        }
        NodeWrite::Remove(_) => None,
    }
}

#[cfg(test)]
mod tests;
