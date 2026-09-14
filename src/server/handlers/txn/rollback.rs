//! Private transaction rollback implementation.

use super::*;

pub(super) async fn rollback(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    txn_id: &str,
) -> Response {
    let s = state.read().await;
    if s.open_txns.remove(txn_id).is_some() {
        Response::ok(req_id, ResultPayload::Bool(true))
    } else {
        Response::err(req_id, format!("unknown transaction '{}'", txn_id))
    }
}

/// Apply one staged durable mutation through the held `GraphTxn` (the same engine
/// primitives the write coalescer uses, so behavior is identical). Errors from
/// add_edge / a failed CAS are NOT surfaced per-op in M6: a staged add_edge to a
/// missing endpoint is a no-op (its endpoints were validated into the read-set; if
/// absent the edge simply isn't added), matching the inline best-effort contract.
pub(super) fn apply_staged(gtxn: &mut crate::graph::GraphTxn<'_>, method: &Method) {
    match method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => gtxn.add_node(node_id.clone(), properties_msgpack.clone()),
        Method::RemoveNode { node_id } => gtxn.remove_node(node_id.clone()),
        Method::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
        } => {
            let _ = gtxn.add_edge(
                source_id.clone(),
                target_id.clone(),
                properties_msgpack.clone(),
            );
        }
        Method::RemoveEdge {
            source_id,
            target_id,
        } => gtxn.remove_edge(source_id.clone(), target_id.clone()),
        Method::CompareAndSetNodeFields {
            node_id,
            conditions_msgpack,
            updates_msgpack,
        } => {
            // Decode the condition/update maps; a decode failure is a no-op CAS
            // (the inline path returns Bool(false) and touches nothing).
            if let (Ok(conditions), Ok(updates)) = (
                decode_txn_object(conditions_msgpack),
                decode_txn_object(updates_msgpack),
            ) {
                let _ = gtxn.compare_and_set_fields(node_id, &conditions, &updates);
            }
        }
        // Only durable mutations are ever staged (the protocol restricts Txn* to
        // this set); any other variant here is unreachable.
        _ => {}
    }
}
