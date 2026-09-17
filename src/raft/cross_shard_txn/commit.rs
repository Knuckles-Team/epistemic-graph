//! Helpers shared by the cross-shard commit strategies, split out of `mod.rs`
//! (CCCC burn-down lanes L-raft-a and L-raft-b, D-CX-cross-shard-txn-split).
//!
//! `cross_shard_txn/mod.rs` was already well over the KISS whole-file
//! `lines_per_file`/`functions_per_file`/`methods_per_class` thresholds before
//! this split (pre-existing debt); the `kiss-changed-rust` gate only fails a
//! commit that WORSENS an already-crossed whole-file count relative to `HEAD`.
//! The helpers here are ONE copy of logic the 2PC, non-blocking, and Calvin
//! commits (and their recovery paths) each used to spell out: the read-only
//! participant check, the phase-1 prepare/vote tally, the replicated
//! decision-graph write and read, the prepare-record scan, and the OLLP seed
//! reconnaissance. Keeping them in this sibling submodule keeps `mod.rs`'s own
//! counts flat-or-improved while this file stays under every threshold.
//! `CrossShardCoordinator`'s methods here are `pub(super)`: implementation detail
//! of the parent module, not part of the crate's public surface.

use std::collections::BTreeMap;

#[cfg(any(feature = "calvin", test, feature = "harness"))]
use super::RecordKey;
use super::{
    decode_prepared_slices, CrossShardCoordinator, GraphSlice, GroupId, Method, RedbBackend,
    TxnOutcome,
};
#[cfg(any(feature = "nonblocking", test, feature = "harness"))]
use super::{GraphType, MultiRaft, RaftRequest, XSHARD_DECISION_GRAPH};
use crate::graph::GraphCore;

impl CrossShardCoordinator {
    /// Validate every READ-ONLY participant's reads; on the first failure, record
    /// the (optional) recoverable decision as ABORT. `Ok(None)` means every
    /// read-only participant validated and the caller should continue into the
    /// writing-participant protocol; `Ok(Some(outcome))` is the terminal outcome
    /// the caller must return without doing anything else.
    pub(super) async fn abort_on_invalid_read_only(
        &self,
        redb: &RedbBackend,
        txn_id: &str,
        read_only: &BTreeMap<GroupId, Vec<GraphSlice>>,
        retain_decision: bool,
    ) -> Result<Option<TxnOutcome>, String> {
        for (gid, slices) in read_only {
            if !self.validate_read_only_participant(*gid, slices).await? {
                if retain_decision {
                    redb.xshard_recoverable_decision_put(txn_id, false).await?;
                }
                return Ok(Some(TxnOutcome::Aborted));
            }
        }
        Ok(None)
    }

    /// PHASE 1: PREPARE every WRITING participant CONCURRENTLY (EG-081) and tally
    /// the votes. Returns `(all_yes, prepared_groups)`.
    ///
    /// Deadlock-freedom under parallel prepare: a prepare holds NO lock ACROSS
    /// groups. Each `prepare_participant` takes its group's shared app_state READ
    /// guard only for the span of its own OCC validation (released before its
    /// durable write), and the redb writer serializes the prepare-log commits
    /// internally. No prepare ever waits on a lock another prepare holds, so there
    /// is no cross-group lock cycle regardless of the order prepares arrive in —
    /// the former strict GroupId sequencing was a defensive lock-order that the
    /// actual (across-groups lock-free) prepare does not require. The per-group
    /// local lock order inside each group is unchanged. We therefore issue the
    /// independent prepare RPCs/durable-writes as joined futures instead of one at
    /// a time; the joined set preserves input (GroupId) order so the collected
    /// votes are deterministic.
    pub(super) async fn run_prepare_phase(
        &self,
        redb: &RedbBackend,
        txn_id: &str,
        writing: &BTreeMap<GroupId, Vec<GraphSlice>>,
    ) -> (bool, Vec<GroupId>) {
        let prepare_futs = writing.iter().map(|(gid, slices)| {
            let gid = *gid;
            async move {
                (
                    gid,
                    self.prepare_participant(redb, txn_id, gid, slices).await,
                )
            }
        });
        let votes = futures::future::join_all(prepare_futs).await;

        let mut prepared_groups: Vec<GroupId> = Vec::new();
        let mut all_yes = true;
        for (gid, vote) in votes {
            match vote {
                // Ok(true) ⟺ a durable prepare record was committed (commit-before-vote):
                // the group is exactly the set that must be cleared on abort/commit.
                Ok(true) => prepared_groups.push(gid),
                Ok(false) => all_yes = false,
                Err(e) => {
                    tracing::warn!(
                        "xshard {}: prepare of group {} errored ({e}) → abort",
                        txn_id,
                        gid
                    );
                    all_yes = false;
                }
            }
        }
        (all_yes, prepared_groups)
    }
}

/// Every `AddEdge` method in `slice` needs both endpoints present at commit —
/// either already live in `core`, or inserted by some slice in the same
/// prepared set (`slices`). If a concurrent writer removed one, prepare must
/// fail (vote NO). See [`super::CrossShardCoordinator::validate_slices`].
pub(super) fn slice_edge_endpoints_are_live(
    core: &GraphCore,
    slice: &GraphSlice,
    slices: &[GraphSlice],
) -> bool {
    for m in &slice.methods {
        let Method::AddEdge {
            source_id,
            target_id,
            ..
        } = m
        else {
            continue;
        };
        if core.get_node_properties(source_id).is_none() && !slice_inserts_node(slices, source_id) {
            return false;
        }
        if core.get_node_properties(target_id).is_none() && !slice_inserts_node(slices, target_id) {
            return false;
        }
    }
    true
}

/// Does ANY slice in the txn insert `node_id`? An AddEdge whose endpoint is added by
/// a sibling slice in the SAME cross-shard txn is valid (the endpoint will exist
/// after the txn applies), so it must not fail prepare.
fn slice_inserts_node(slices: &[GraphSlice], node_id: &str) -> bool {
    slices.iter().any(|s| {
        s.methods
            .iter()
            .any(|m| matches!(m, Method::AddNode { node_id: nid, .. } if nid == node_id))
    })
}

/// Every durable prepare record, grouped by transaction and participant group.
pub(super) fn prepares_by_txn(
    redb: &RedbBackend,
) -> Result<BTreeMap<String, BTreeMap<GroupId, Vec<GraphSlice>>>, String> {
    let mut by_txn: BTreeMap<String, BTreeMap<GroupId, Vec<GraphSlice>>> = BTreeMap::new();
    for (txn_id, gid, blob) in redb.xshard_scan_prepares()? {
        let slices = decode_prepared_slices(&blob)?;
        by_txn.entry(txn_id).or_default().insert(gid, slices);
    }
    Ok(by_txn)
}

/// The `AddNode(txn_id, properties)` record a replicated decision or sequence writes.
#[cfg(any(feature = "nonblocking", test, feature = "harness"))]
pub(super) fn decision_node_method(
    txn_id: &str,
    properties: &serde_json::Value,
) -> Result<Method, String> {
    Ok(Method::AddNode {
        node_id: txn_id.to_string(),
        properties_msgpack: rmp_serde::to_vec_named(properties).map_err(|e| e.to_string())?,
    })
}

/// Commit one engine-owned `method` on [`XSHARD_DECISION_GRAPH`] through the
/// decision `group`, under internal authority keyed by `(namespace,
/// coordinator_id)`. Returns after the entry is quorum-committed AND applied
/// locally; nothing is written to the coordinator-private redb.
#[cfg(any(feature = "nonblocking", test, feature = "harness"))]
pub(super) async fn write_decision_graph(
    multi: &MultiRaft,
    group: &super::super::multi::Group,
    (namespace, coordinator_id): (&str, &str),
    method: Method,
) -> Result<(), String> {
    let server_secret = multi.app_state().read().await.auth_secret.clone();
    let req = RaftRequest {
        graph_fname: crate::persist::sanitize(XSHARD_DECISION_GRAPH),
        graph_name: XSHARD_DECISION_GRAPH.to_string(),
        graph_type: GraphType::Global,
        committed_at_ms: 0,
        mutation: super::super::RaftMutationContext::internal(
            namespace,
            XSHARD_DECISION_GRAPH,
            coordinator_id,
            0,
            0,
        ),
        command: super::super::ReplicatedMutation::graph(method, &server_secret)?,
    };
    group.client_write(req).await?;
    Ok(())
}

/// The properties of `txn_id`'s node in the REPLICATED decision graph, read from
/// the applied state machine (not the coordinator-private redb), so any replica
/// that applied the entry can answer. `None` when nothing was replicated.
#[cfg(any(feature = "nonblocking", test, feature = "harness"))]
pub(super) async fn replicated_decision_properties(
    multi: &MultiRaft,
    txn_id: &str,
    invalid: &str,
) -> Result<Option<serde_json::Value>, String> {
    let state = multi.app_state();
    let s = state.read().await;
    let Some(entry) = s.registry.get(XSHARD_DECISION_GRAPH) else {
        return Ok(None);
    };
    entry
        .core
        .get_node_properties(txn_id)
        .map(|blob| {
            eg_types::msgpack::decode_property_value(&blob).map_err(|_| invalid.to_string())
        })
        .transpose()
}

/// OLLP reconnaissance: the committed value observed at every seed record.
#[cfg(any(feature = "calvin", test, feature = "harness"))]
pub(super) async fn reconnoiter_seeds(
    coordinator: &CrossShardCoordinator,
    seeds: &[RecordKey],
) -> Result<BTreeMap<RecordKey, Option<Vec<u8>>>, String> {
    let mut observed: BTreeMap<RecordKey, Option<Vec<u8>>> = BTreeMap::new();
    for key in seeds {
        observed.insert(key.clone(), coordinator.reconnoiter(key).await?);
    }
    Ok(observed)
}
