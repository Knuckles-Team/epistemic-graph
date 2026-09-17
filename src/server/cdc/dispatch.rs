//! [`emit_for_method`]'s per-`Method` CDC dispatch, split out of `cdc.rs` itself so
//! the parent file's KISS `lines_per_file` budget has room for the hub/ring/
//! continuous-query/trigger machinery it still owns.

use super::*;

/// Emit the CDC event for a successfully-applied durable mutation, reading the
/// post-image from `core`. No-op for `CdcPre::Skip`.
pub fn emit_for_method(hub: &CdcHub, core: &GraphCore, graph: &str, method: &Method, pre: CdcPre) {
    match (method, pre) {
        (
            Method::AddNode { .. }
            | Method::CreateNodeIfAbsent { .. }
            | Method::CompareAndSetNodeFields { .. }
            | Method::RemoveNode { .. },
            CdcPre::Node { before, .. },
        ) => emit_node_change(hub, core, graph, method, before),
        (Method::AddEdge { .. } | Method::RemoveEdge { .. }, CdcPre::Edge { before, .. }) => {
            emit_edge_change(hub, graph, method, before)
        }
        // A whole-graph wipe resets the change feed: the per-node changes are moot once
        // the graph is empty, so the feed rewinds to seq 0 and a consumer re-seeds.
        // FromMsgpack/Reconcile both replace the graph's entire node/edge content
        // with an imported or merged authoritative image (`core.from_msgpack`) --
        // the same "whole graph replaced" shape as `ClearGraph`, so any prior
        // incremental deltas are equally moot. W1c: previously fell to the `_`
        // catch-all (no CDC at all) despite being durable + GATEWAY_ROUTED.
        (Method::ClearGraph | Method::FromMsgpack { .. } | Method::Reconcile { .. }, _) => {
            hub.reset_graph(graph)
        }
        (Method::ApplyChangeEnvelope { envelope }, _) => {
            hub.emit(
                graph,
                CdcKind::UpdateNode,
                envelope.content_version.object_id.clone(),
                String::new(),
                None,
                None,
            );
        }
        // The batch coordinator emits one change event per envelope (mirroring the
        // single method) so policy `emits_cdc: true` stays consistent; at runtime the
        // per-envelope durable outbox is the authoritative change feed.
        (Method::ApplyChangeEnvelopes { envelopes }, _) => {
            emit_change_envelopes(hub, graph, envelopes)
        }
        // `mutating_served_modality` returns `Some` for exactly the variants
        // `ServedModalityOp::mutates()` is true for (its own match covers the same
        // set), so there is no separate guard to test here.
        #[cfg(feature = "modality-serving")]
        (Method::ServedModality { op }, _) => emit_mutating_served_modality(hub, graph, op),
        // ── W1c: close the 9-method audit/CDC-visibility gap for the remaining
        // durable admin/ledger methods. None of these map to a single node/edge
        // row, so -- consistent with `emit_served_modality`'s reserved-marker-id
        // shape below -- each emits ONE `UpdateNode` marker event with a
        // reserved `__`-prefixed id (never a real node id) and no before/after
        // payload, giving CDC consumers an observable "this happened" signal
        // without fabricating a fake property diff. W2.5 fleet server registry:
        // `RegisterServer`'s marker is defense-in-depth only -- that variant
        // self-translates into `Method::AddNode` in `dispatch.rs` BEFORE ever
        // reaching `commit_mutation`, so the REAL CDC event for a registration is
        // AddNode's own (`srv:<name>`, AddNode/UpdateNode kind). ──
        (m, _) => emit_marker_if_known(hub, graph, m),
    }
}

/// The `ApplyChangeEnvelopes` half of [`emit_for_method`]: one change event per
/// envelope. Split out so the outer tuple match stays under the complexity cap.
fn emit_change_envelopes(
    hub: &CdcHub,
    graph: &str,
    envelopes: &[crate::change_envelope::ChangeEnvelope],
) {
    for envelope in envelopes {
        hub.emit(
            graph,
            CdcKind::UpdateNode,
            envelope.content_version.object_id.clone(),
            String::new(),
            None,
            None,
        );
    }
}

/// The `ServedModality` half of [`emit_for_method`]: emit only when `op` is one of the
/// mutating variants [`mutating_served_modality`] maps to a target modality. Split out
/// so the outer tuple match stays under the complexity cap.
#[cfg(feature = "modality-serving")]
fn emit_mutating_served_modality(hub: &CdcHub, graph: &str, op: &eg_types::ServedModalityOp) {
    if let Some(modality) = mutating_served_modality(op) {
        emit_served_modality(hub, graph, modality);
    }
}

/// The trailing fallback of [`emit_for_method`]'s dispatch: emit the reserved marker
/// event for `method` if [`marker_event_id`] classifies it as one, else no-op. Split
/// out so the outer tuple match stays under the complexity cap.
fn emit_marker_if_known(hub: &CdcHub, graph: &str, method: &Method) {
    if let Some(marker) = marker_event_id(method) {
        hub.emit(
            graph,
            CdcKind::UpdateNode,
            marker.to_string(),
            String::new(),
            None,
            None,
        );
    }
}

/// Node half of [`emit_for_method`]'s dispatch (`AddNode`/`CreateNodeIfAbsent`/
/// `CompareAndSetNodeFields`/`RemoveNode`, all reached only under `CdcPre::Node`).
/// Split out so the outer tuple match stays under the complexity cap.
fn emit_node_change(
    hub: &CdcHub,
    core: &GraphCore,
    graph: &str,
    method: &Method,
    before: Option<Vec<u8>>,
) {
    match method {
        Method::AddNode { node_id, .. } => {
            let after = core.get_node_properties(node_id);
            let kind = if before.is_some() {
                CdcKind::UpdateNode
            } else {
                CdcKind::AddNode
            };
            hub.emit(graph, kind, node_id.clone(), String::new(), before, after);
        }
        Method::CreateNodeIfAbsent { node_id, .. } => match before {
            // A losing create is a durable false result, not a row update.
            Some(_) => {}
            None => {
                hub.emit(
                    graph,
                    CdcKind::AddNode,
                    node_id.clone(),
                    String::new(),
                    None,
                    core.get_node_properties(node_id),
                );
            }
        },
        Method::CompareAndSetNodeFields { node_id, .. } => {
            // A CAS that didn't match leaves the blob unchanged; emit UpdateNode with
            // the post-image so an unchanged after == before is observable but harmless.
            let after = core.get_node_properties(node_id);
            hub.emit(
                graph,
                CdcKind::UpdateNode,
                node_id.clone(),
                String::new(),
                before,
                after,
            );
        }
        Method::RemoveNode { node_id } => {
            hub.emit(
                graph,
                CdcKind::RemoveNode,
                node_id.clone(),
                String::new(),
                before,
                None,
            );
        }
        _ => {}
    }
}

/// Edge half of [`emit_for_method`]'s dispatch (`AddEdge`/`RemoveEdge`, reached only
/// under `CdcPre::Edge`). Split out so the outer tuple match stays under the
/// complexity cap.
fn emit_edge_change(hub: &CdcHub, graph: &str, method: &Method, before: Option<Vec<u8>>) {
    match method {
        Method::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
        } => {
            hub.emit(
                graph,
                CdcKind::AddEdge,
                source_id.clone(),
                target_id.clone(),
                before,
                Some(properties_msgpack.clone()),
            );
        }
        Method::RemoveEdge {
            source_id,
            target_id,
        } => {
            hub.emit(
                graph,
                CdcKind::RemoveEdge,
                source_id.clone(),
                target_id.clone(),
                before,
                None,
            );
        }
        _ => {}
    }
}

/// The mutating served-modality op's target modality, or `None` for a variant this
/// dispatch does not emit CDC for. Split out of [`emit_for_method`]'s `ServedModality`
/// arm so the outer tuple match stays under the complexity cap.
#[cfg(feature = "modality-serving")]
fn mutating_served_modality(
    op: &eg_types::ServedModalityOp,
) -> Option<eg_types::ServedModalityKind> {
    use eg_types::ServedModalityOp;
    match op {
        ServedModalityOp::Ingest { modality, .. }
        | ServedModalityOp::IngestStream { modality, .. }
        | ServedModalityOp::Delete { modality, .. }
        | ServedModalityOp::MoveToCold { modality, .. }
        | ServedModalityOp::Restore { modality, .. }
        | ServedModalityOp::CollectTombstones { modality, .. } => Some(*modality),
        _ => None,
    }
}

/// Reserved-marker CDC event id for the "durable but no single node/edge row" methods
/// (W1c): `ApplyMutation`, `ApplyMultisigMutation`, `RegisterServer`, `IcvConfigure`
/// (shacl), `RunDatalogReasoning` (reasoning), `ClearLedger`/`ApplyLedger`, and
/// `CompactNodesByType`. `None` for everything else (including the node/edge/reset/
/// envelope/modality methods, which [`emit_for_method`] matches before ever reaching
/// this fallback arm). Split out so this one repeated marker-emit shape doesn't
/// dominate the outer dispatcher's cyclomatic budget.
fn marker_event_id(method: &Method) -> Option<&'static str> {
    match method {
        Method::ApplyMutation { .. } => Some("__apply_mutation"),
        Method::ApplyMultisigMutation { .. } => Some("__apply_multisig_mutation"),
        Method::RegisterServer { .. } => Some("__register_server"),
        #[cfg(feature = "shacl")]
        Method::IcvConfigure { .. } => Some("__icv_configure"),
        #[cfg(feature = "reasoning")]
        Method::RunDatalogReasoning { .. } => Some("__run_datalog_reasoning"),
        Method::ClearLedger | Method::ApplyLedger { .. } => Some("__ledger"),
        Method::CompactNodesByType { .. } => Some("__compact_nodes_by_type"),
        _ => None,
    }
}
