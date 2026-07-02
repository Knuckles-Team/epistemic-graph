//! Per-graph write-ahead log (CONCEPT:KG-2.8 / OS-5.9, Phase B2).
//!
//! The snapshot persistence (`persist.rs`) is an RDB-style point-in-time image
//! written every `--checkpoint-interval`. A crash between checkpoints loses every
//! mutation since the last one. The WAL closes that window: each durable mutation
//! is appended to `<persist_dir>/<graph>.wal` as it is applied, and on restart the
//! engine loads the snapshot and then REPLAYS the WAL tail — so warm restart
//! recovers right up to the last flushed op instead of up to the last checkpoint.
//! (pggraph remains the cross-restart system-of-record; this is the fast local
//! crash-consistency layer.) A checkpoint supersedes the logged ops, so it
//! truncates the WAL afterward.
//!
//! Durability model: `append` FLUSHES (a `write`, no `fsync`) so a PROCESS crash
//! (the common case — kill/segfault) keeps the data in the OS page cache and it
//! survives; `fsync` happens at checkpoint, bounding hard-power-loss exposure to
//! the checkpoint interval (same as before, but now with no between-checkpoint
//! loss on process crashes). Replay tolerates a torn trailing record (a partial
//! op from a crash mid-append) by stopping at the first short/garbage read.
//!
//! Only the persistent DATA mutations are logged. Derived/maintenance ops (decay,
//! evict, touch, parse-repository) are recomputable or best-effort and would bloat
//! the log; they are intentionally excluded.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::graph::GraphCore;
use crate::protocol::Method;

/// True for the methods whose effect must survive a crash via the WAL.
pub fn is_durable_mutation(m: &Method) -> bool {
    // `AddTriples` (feature `rdf`) writes nodes + edges, so it is durable: the
    // dispatch shell records the Method and `apply` below re-parses + re-applies it
    // deterministically on replay, exactly like `BatchUpdate`.
    #[cfg(feature = "rdf")]
    if matches!(
        m,
        Method::AddTriples { .. } | Method::RemoveTriples { .. } | Method::DropNamedGraph
    ) {
        return true;
    }
    // Message-broker admin + publish (CONCEPT:EG-275): each mutates control-graph
    // nodes and replays deterministically (routing + monotonic seq derive from graph
    // state; `apply` below re-runs the SAME broker fn over the same pre-image).
    #[cfg(feature = "broker")]
    if matches!(
        m,
        Method::DeclareExchange { .. }
            | Method::DeleteExchange { .. }
            | Method::BindQueue { .. }
            | Method::UnbindQueue { .. }
            | Method::Publish { .. }
            // Broker policy extensions (CONCEPT:EG-276..280): all mutate control-graph
            // nodes deterministically from explicit args (caller `now_ms`), so replay
            // reproduces identical state (routing + monotonic seq derive from graph).
            | Method::DeclareQueue { .. }
            | Method::PublishEx { .. }
            | Method::BrokerConsume { .. }
            | Method::BrokerAck { .. }
            | Method::BrokerReject { .. }
            | Method::SweepExpired { .. }
            // Streams (CONCEPT:EG-283) + publisher-confirm / consumer-ack (CONCEPT:
            // EG-284): the mutating variants (append/trim/commit/confirm/ack-tag/
            // nack-tag) write control-graph nodes deterministically from explicit args
            // (caller `now_ms` + durable counters), so replay reproduces them. Pure
            // reads (`StreamRead`/`StreamCommittedOffset`) are NOT logged.
            | Method::StreamDeclare { .. }
            | Method::StreamPublish { .. }
            | Method::StreamTrim { .. }
            | Method::StreamCommitOffset { .. }
            | Method::PublishConfirmed { .. }
            | Method::BrokerAckTag { .. }
            | Method::BrokerNackTag { .. }
            // Idempotent producer (CONCEPT:EG-314): the dedup check + high-water-mark
            // bump mutate a durable producer node deterministically from explicit args,
            // so replay reproduces the identical mark + duplicate-verdict.
            | Method::PublishIdempotent { .. }
    ) {
        return true;
    }
    matches!(
        m,
        Method::AddNode { .. }
            | Method::RemoveNode { .. }
            | Method::CompareAndSetNodeFields { .. }
            | Method::AddEdge { .. }
            | Method::RemoveEdge { .. }
            | Method::InvalidateEdge { .. }
            | Method::SupersedeEdge { .. }
            | Method::BatchUpdate { .. }
            | Method::ClaimNext { .. }
            | Method::ClearGraph
            // Agent-memory / scene-graph / trajectory mutations (CONCEPT:EG-318):
            // each writes durable nodes/edges via an eg-core primitive whose
            // generated ids derive deterministically from sorted inputs / node-count
            // / step ordinals and whose only clock is the EXPLICIT caller `now_ms`, so
            // `apply` below re-runs the SAME primitive over the same pre-image and
            // reproduces byte-identical state (mirrors the EG-276..284 broker
            // precedent). The paired READ variants are recomputable ⇒ not logged.
            | Method::CreateSummaryNode { .. }
            | Method::Consolidate { .. }
            | Method::Reinforce { .. }
            | Method::DecayNode { .. }
            | Method::DecayMemories { .. }
            | Method::EvictBelow { .. }
            | Method::Maintain { .. }
            | Method::AddSceneObject { .. }
            | Method::SetPose { .. }
            | Method::Reparent { .. }
            | Method::StartTrajectory { .. }
            | Method::AppendStep { .. }
    )
}

/// Append-only writer for one graph's WAL.
///
/// A plain `File` (no `BufWriter`) so each `append` is a direct `write` that the
/// OS persists immediately — a PROCESS crash keeps it. `len` tracks the logical
/// write position so the checkpoint can capture a position and later truncate
/// exactly the prefix the snapshot superseded (position-based truncation), which
/// is what makes checkpoint + WAL loss-free without holding a lock across the slow
/// snapshot encode.
#[derive(Debug)]
pub struct WalWriter {
    file: File,
    len: u64,
}

impl WalWriter {
    /// Open (or create) the WAL file, positioned for append.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            // Never truncate on open: an existing WAL must be preserved for
            // replay. We position at EOF below and track `len` for position-based
            // prefix truncation after a checkpoint.
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        let len = file.metadata()?.len();
        file.seek(SeekFrom::End(0))?;
        Ok(Self { file, len })
    }

    /// Append a length-prefixed (u32 LE) MessagePack-encoded method.
    pub fn append(&mut self, method: &Method) -> std::io::Result<()> {
        let bytes = rmp_serde::to_vec_named(method)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        self.append_bytes(&bytes)
    }

    /// Append a pre-serialized MessagePack record (length-prefixed). Lets the
    /// caller do the (cheap, CPU-only) serialization off the WAL writer thread —
    /// used by [`crate::wal_service`] so file I/O is the only work on that thread.
    pub fn append_bytes(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        let prefix = (bytes.len() as u32).to_le_bytes();
        self.file.write_all(&prefix)?;
        self.file.write_all(bytes)?;
        self.len += prefix.len() as u64 + bytes.len() as u64;
        Ok(())
    }

    /// fsync the WAL file (group-commit durability point).
    pub fn sync(&mut self) -> std::io::Result<()> {
        self.file.sync_all()
    }

    /// Current logical end position — captured at checkpoint snapshot time and
    /// passed back to [`Self::truncate_prefix`] after the snapshot is durable.
    pub fn position(&self) -> u64 {
        self.len
    }

    /// Discard the first `prefix` bytes (the records the snapshot now supersedes),
    /// keeping everything appended after the checkpoint captured `prefix`. This is
    /// loss-free: ops appended DURING the checkpoint live at offsets ≥ `prefix` and
    /// are retained for replay. fsync'd so the truncation is durable.
    pub fn truncate_prefix(&mut self, prefix: u64) -> std::io::Result<()> {
        self.file.flush()?;
        if prefix >= self.len {
            self.file.set_len(0)?;
            self.file.seek(SeekFrom::Start(0))?;
            self.len = 0;
            return self.file.sync_all();
        }
        let mut tail = Vec::new();
        self.file.seek(SeekFrom::Start(prefix))?;
        self.file.read_to_end(&mut tail)?;
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&tail)?;
        self.len = tail.len() as u64;
        self.file.sync_all()
    }
}

/// Apply one durable mutation to a graph core. Mirrors the dispatch mutation
/// handlers for exactly the `is_durable_mutation` set.
///
/// This is the single canonical "durable Method → GraphCore mutation" path: WAL
/// replay (below) and the Raft state machine (CONCEPT:KG-2.188, `src/raft`) both
/// call it, so a committed Raft log entry applies BYTE-IDENTICALLY to how a
/// replayed WAL record does. Deterministic (replaying the same Method over the
/// same pre-image yields the same state), which is the Raft state-machine contract.
pub fn apply(core: &GraphCore, m: &Method) {
    match m {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => core.add_node(node_id.clone(), properties_msgpack.clone()),
        Method::RemoveNode { node_id } => core.remove_node(node_id.clone()),
        Method::CompareAndSetNodeFields {
            node_id,
            conditions_msgpack,
            updates_msgpack,
        } => {
            // Deterministic replay: decode the blobs and re-run the CAS. Replaying
            // over the same pre-image yields the same outcome; the bool is ignored.
            if let (Ok(conditions), Ok(updates)) = (
                rmp_serde::from_slice::<serde_json::Map<String, serde_json::Value>>(
                    conditions_msgpack,
                ),
                rmp_serde::from_slice::<serde_json::Map<String, serde_json::Value>>(
                    updates_msgpack,
                ),
            ) {
                let _ = core.compare_and_set_fields(node_id, &conditions, &updates);
            }
        }
        Method::ClaimNext {
            label,
            updates_msgpack,
        } => {
            // Deterministic replay (CONCEPT:KG-2.303): re-run the same oldest-pending
            // pick + CAS over identical state. `updates` carries no clock, so the
            // claimed node + merged marker are reproduced byte-identically; the
            // returned node id is ignored on replay.
            if let Ok(updates) =
                rmp_serde::from_slice::<serde_json::Map<String, serde_json::Value>>(updates_msgpack)
            {
                let _ = core.claim_next_fields(label, &updates);
            }
        }
        Method::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
        } => {
            let _ = core.add_edge(
                source_id.clone(),
                target_id.clone(),
                properties_msgpack.clone(),
            );
        }
        Method::RemoveEdge {
            source_id,
            target_id,
        } => core.remove_edge(source_id.clone(), target_id.clone()),
        // Deterministic replay (KG-2.251): re-close the same window / re-supersede.
        // Idempotent — replaying over the already-closed pre-image is a no-op.
        Method::InvalidateEdge {
            source_id,
            target_id,
            relationship,
            invalid_at,
            tx_now,
        } => {
            let _ = core.invalidate_edge(source_id, target_id, relationship, *invalid_at, *tx_now);
        }
        Method::SupersedeEdge {
            source_id,
            target_id,
            properties_msgpack,
            prior_source,
            prior_target,
            prior_relationship,
            valid_at,
            tx_now,
        } => {
            let _ = core.supersede_edge(
                source_id.clone(),
                target_id.clone(),
                properties_msgpack.clone(),
                prior_source,
                prior_target,
                prior_relationship,
                *valid_at,
                *tx_now,
            );
        }
        Method::BatchUpdate { operations_msgpack } => {
            let _ = crate::algorithms::batch_update(core, operations_msgpack);
        }
        Method::ClearGraph => core.clear(),
        // `AddTriples` (feature `rdf`): deterministic replay = re-parse the SAME
        // source text and re-apply the property-graph projection. The multi-valued
        // literal extras live in their OWN durable `rdf_quads.redb` (independently
        // durable), so they need no WAL replay here — only the in-graph nodes/edges
        // are rebuilt. Re-running over the same pre-image yields the same state.
        #[cfg(feature = "rdf")]
        Method::AddTriples { turtle, ntriples } => {
            let parsed = if !turtle.trim().is_empty() {
                eg_rdf::mapping::parse_turtle(turtle)
            } else if !ntriples.trim().is_empty() {
                eg_rdf::mapping::parse_ntriples(ntriples)
            } else {
                Ok(Vec::new())
            };
            if let Ok(triples) = parsed {
                let mut iris = eg_rdf::mapping::IriStore::default();
                // Replay rebuilds the property-graph projection; the lossless quad
                // store is durable on its own file, so pass no store here.
                let _ = eg_rdf::mapping::load_triples(
                    core,
                    &mut iris,
                    "",
                    triples,
                    #[cfg(feature = "rdf-redb")]
                    None,
                );
            }
        }
        // `RemoveTriples` (feature `rdf`, CONCEPT:EG-017): deterministic replay =
        // re-parse the SAME source and re-retract. Idempotent — re-removing an absent
        // triple is a no-op. The lossless quad store is durable on its own file.
        #[cfg(feature = "rdf")]
        Method::RemoveTriples { turtle, ntriples } => {
            let parsed = if !turtle.trim().is_empty() {
                eg_rdf::mapping::parse_turtle(turtle)
            } else if !ntriples.trim().is_empty() {
                eg_rdf::mapping::parse_ntriples(ntriples)
            } else {
                Ok(Vec::new())
            };
            if let Ok(triples) = parsed {
                let _ = eg_rdf::update::remove_triples(core, &triples);
            }
        }
        // `DropNamedGraph` (feature `rdf`, CONCEPT:EG-017): the named graph IS this
        // registry graph, so dropping it clears the whole core. The quad-store rows are
        // dropped durably on their own redb file at original execution, so replay only
        // needs to rebuild the empty in-graph state.
        #[cfg(feature = "rdf")]
        Method::DropNamedGraph => core.clear(),
        // Message-broker replay (CONCEPT:EG-275): re-run the SAME broker fn. Routing
        // reads the (already-replayed) bindings and the seq comes from the durable
        // counter node, so message nodes are reproduced byte-identically; results are
        // ignored on replay.
        #[cfg(feature = "broker")]
        Method::DeclareExchange { exchange, kind } => {
            if let Some(k) = crate::broker::ExchangeKind::parse(kind) {
                let _ = crate::broker::declare_exchange(core, exchange, k);
            }
        }
        #[cfg(feature = "broker")]
        Method::DeleteExchange { exchange } => {
            let _ = crate::broker::delete_exchange(core, exchange);
        }
        #[cfg(feature = "broker")]
        Method::BindQueue {
            exchange,
            queue,
            routing_key,
        } => crate::broker::bind_queue(core, exchange, queue, routing_key),
        #[cfg(feature = "broker")]
        Method::UnbindQueue {
            exchange,
            queue,
            routing_key,
        } => {
            let _ = crate::broker::unbind_queue(core, exchange, queue, routing_key);
        }
        #[cfg(feature = "broker")]
        Method::Publish {
            exchange,
            routing_key,
            payload,
        } => {
            let _ = crate::broker::publish(core, exchange, routing_key, payload);
        }
        // Broker policy extensions (CONCEPT:EG-276..280): re-run the SAME broker fn
        // with the SAME explicit args. Routing reads (already-replayed) bindings, seq
        // comes from the durable counter, and `now_ms` is logged — so the message /
        // dead-letter / claim state is reproduced byte-identically. Results ignored.
        #[cfg(feature = "broker")]
        Method::DeclareQueue {
            queue,
            dl_exchange,
            dl_routing_key,
            max_delivery_count,
            message_ttl_ms,
            queue_expiry_ms,
            max_priority,
        } => {
            let policy = crate::broker::QueuePolicy {
                dl_exchange: dl_exchange.clone(),
                dl_routing_key: dl_routing_key.clone(),
                max_delivery_count: *max_delivery_count,
                message_ttl_ms: *message_ttl_ms,
                queue_expiry_ms: *queue_expiry_ms,
                max_priority: *max_priority,
            };
            crate::broker::declare_queue(core, queue, &policy);
        }
        #[cfg(feature = "broker")]
        Method::PublishEx {
            exchange,
            routing_key,
            payload,
            priority,
            delay_ms,
            ttl_ms,
            now_ms,
        } => {
            let _ = crate::broker::publish_ex(
                core,
                exchange,
                routing_key,
                payload,
                *priority,
                *delay_ms,
                *ttl_ms,
                *now_ms,
            );
        }
        #[cfg(feature = "broker")]
        Method::BrokerConsume {
            queue,
            group,
            consumer,
            now_ms,
            lease_ms,
            prefetch,
        } => {
            let _ = crate::broker::broker_consume(
                core, queue, group, consumer, *now_ms, *lease_ms, *prefetch,
            );
        }
        #[cfg(feature = "broker")]
        Method::BrokerAck { queue, node_id } => {
            let _ = crate::broker::broker_ack(core, queue, node_id);
        }
        #[cfg(feature = "broker")]
        Method::BrokerReject {
            queue,
            node_id,
            requeue,
            now_ms,
        } => {
            let _ = crate::broker::broker_reject(core, queue, node_id, *requeue, *now_ms);
        }
        #[cfg(feature = "broker")]
        Method::SweepExpired { now_ms } => {
            let _ = crate::broker::sweep_expired(core, *now_ms);
        }
        // Streams (CONCEPT:EG-283) + confirms/acks (CONCEPT:EG-284): re-run the SAME
        // broker fn with the SAME explicit args. Offsets/tags come from durable counter
        // nodes and `now_ms` is logged, so message / commit / tag state is reproduced
        // byte-identically. Results are ignored on replay.
        #[cfg(feature = "broker")]
        Method::StreamDeclare {
            stream,
            max_messages,
            max_age_ms,
        } => {
            let retention = crate::broker::StreamRetention {
                max_messages: *max_messages,
                max_age_ms: *max_age_ms,
            };
            crate::broker::declare_stream(core, stream, &retention);
        }
        #[cfg(feature = "broker")]
        Method::StreamPublish {
            stream,
            payload,
            now_ms,
        } => {
            let _ = crate::broker::stream_publish(core, stream, payload, *now_ms);
        }
        #[cfg(feature = "broker")]
        Method::StreamTrim { stream, now_ms } => {
            let _ = crate::broker::stream_trim(core, stream, *now_ms);
        }
        #[cfg(feature = "broker")]
        Method::StreamCommitOffset {
            stream,
            group,
            offset,
        } => {
            crate::broker::commit_offset(core, stream, group, *offset);
        }
        #[cfg(feature = "broker")]
        Method::PublishConfirmed {
            exchange,
            routing_key,
            payload,
            priority,
            delay_ms,
            ttl_ms,
            now_ms,
        } => {
            let _ = crate::broker::publish_confirmed(
                core,
                exchange,
                routing_key,
                payload,
                *priority,
                *delay_ms,
                *ttl_ms,
                *now_ms,
            );
        }
        // Idempotent producer (CONCEPT:EG-314): re-run the SAME publish with the SAME
        // producer_id/seq over the same pre-image — a first apply records the mark +
        // enqueues; a replay of an already-recorded seq is a no-op duplicate. Result
        // ignored.
        #[cfg(feature = "broker")]
        Method::PublishIdempotent {
            exchange,
            routing_key,
            payload,
            producer_id,
            seq,
            priority,
            delay_ms,
            ttl_ms,
            now_ms,
        } => {
            let _ = crate::broker::publish_idempotent(
                core,
                exchange,
                routing_key,
                payload,
                producer_id.as_deref(),
                *seq,
                *priority,
                *delay_ms,
                *ttl_ms,
                *now_ms,
            );
        }
        #[cfg(feature = "broker")]
        Method::BrokerAckTag { delivery_tag } => {
            let _ = crate::broker::broker_ack_tag(core, *delivery_tag);
        }
        #[cfg(feature = "broker")]
        Method::BrokerNackTag {
            delivery_tag,
            requeue,
            now_ms,
        } => {
            let _ = crate::broker::broker_nack_tag(core, *delivery_tag, *requeue, *now_ms);
        }
        // Agent-memory / scene-graph / trajectory replay (CONCEPT:EG-318): re-run the
        // SAME eg-core primitive with the SAME explicit args over the same pre-image.
        // Every generated id derives deterministically (sorted inputs / monotonic
        // node-count / step ordinal) and the only clock is the logged `now_ms`, so the
        // node/edge state is reproduced byte-identically. Results are ignored on replay.
        Method::CreateSummaryNode {
            level,
            child_ids,
            props_msgpack,
        } => {
            let _ = core.create_summary_node(*level, child_ids, wal_json_object(props_msgpack));
        }
        Method::Consolidate {
            episodic_ids,
            semantic_props_msgpack,
        } => {
            let _ = core.consolidate(episodic_ids, wal_json_object(semantic_props_msgpack));
        }
        Method::Reinforce {
            node_id,
            now_ms,
            weight,
        } => {
            let _ = core.reinforce(node_id, *now_ms, *weight);
        }
        Method::DecayNode {
            node_id,
            now_ms,
            half_life_ms,
        } => {
            let _ = core.decay_node(node_id, *now_ms, *half_life_ms);
        }
        Method::DecayMemories {
            now_ms,
            half_life_ms,
            ids,
        } => {
            let _ = core.decay_memories(*now_ms, *half_life_ms, ids);
        }
        Method::EvictBelow {
            ids,
            threshold,
            delete,
        } => {
            let _ = core.evict_below(ids, *threshold, *delete);
        }
        Method::Maintain {
            ids,
            now_ms,
            half_life_ms,
            evict_threshold,
            delete,
        } => {
            let _ = core.maintain(ids, *now_ms, *half_life_ms, *evict_threshold, *delete);
        }
        Method::AddSceneObject {
            pose_msgpack,
            parent,
        } => {
            if let Some(pose) = wal_pose(pose_msgpack) {
                let _ = core.add_scene_object(&pose, parent.as_deref());
            }
        }
        Method::SetPose {
            node_id,
            pose_msgpack,
        } => {
            if let Some(pose) = wal_pose(pose_msgpack) {
                let _ = core.set_pose(node_id, &pose);
            }
        }
        Method::Reparent {
            node_id,
            new_parent,
        } => {
            let _ = core.reparent(node_id, new_parent.as_deref());
        }
        Method::StartTrajectory { props_msgpack } => {
            let _ = core.start_trajectory(wal_json_object(props_msgpack));
        }
        Method::AppendStep {
            traj_id,
            action_msgpack,
            reward,
            state_ref,
            next_state_ref,
            t,
        } => {
            let action = rmp_serde::from_slice::<serde_json::Value>(action_msgpack)
                .unwrap_or(serde_json::Value::Null);
            let _ = core.append_step(
                traj_id,
                action,
                *reward,
                state_ref.as_deref(),
                next_state_ref.as_deref(),
                *t,
            );
        }
        _ => {}
    }
}

/// Decode a MessagePack-encoded JSON object blob for WAL replay (CONCEPT:EG-318). A
/// missing/undecodable/non-object blob yields an empty map — the same discipline the
/// dispatch handler uses, so replay applies the identical props.
fn wal_json_object(blob: &[u8]) -> serde_json::Map<String, serde_json::Value> {
    match rmp_serde::from_slice::<serde_json::Value>(blob) {
        Ok(serde_json::Value::Object(o)) => o,
        _ => serde_json::Map::new(),
    }
}

/// Decode a MessagePack-encoded pose blob for WAL replay (CONCEPT:EG-318/EG-087).
/// `None` only if the blob is not a decodable JSON object — matching the dispatch
/// handler's `decode_pose` so replay reconstructs the identical scene node.
fn wal_pose(blob: &[u8]) -> Option<eg_core::scene::Pose> {
    let val = rmp_serde::from_slice::<serde_json::Value>(blob).ok()?;
    eg_core::scene::Pose::from_json(&val)
}

/// Replay a WAL file into `core` (after the snapshot is loaded). Returns the
/// number of ops applied. A torn trailing record (partial op from a crash mid
/// append) ends replay cleanly rather than erroring.
pub fn replay(core: &GraphCore, path: &Path) -> usize {
    let mut buf = Vec::new();
    if File::open(path)
        .and_then(|mut f| f.read_to_end(&mut buf))
        .is_err()
    {
        return 0;
    }
    let mut off = 0usize;
    let mut applied = 0usize;
    while off + 4 <= buf.len() {
        let len = u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]]) as usize;
        off += 4;
        if off + len > buf.len() {
            break; // torn tail — stop cleanly
        }
        match rmp_serde::from_slice::<Method>(&buf[off..off + len]) {
            Ok(m) => {
                apply(core, &m);
                applied += 1;
            }
            Err(_) => break,
        }
        off += len;
    }
    applied
}

/// The on-disk WAL path for a sanitized graph file-name within a persist dir.
pub fn wal_path(dir: &str, fname: &str) -> PathBuf {
    Path::new(dir).join(format!("{fname}.wal"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    #[test]
    fn wal_roundtrip_recovers_mutations() {
        let dir = std::env::temp_dir().join(format!("eg-wal-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = wal_path(dir.to_str().unwrap(), "g");
        let pa = props(serde_json::json!({"type": "Code", "n": 1}));
        let pe = props(serde_json::json!({"type": "CALLS"}));
        {
            let mut w = WalWriter::open(&path).unwrap();
            w.append(&Method::AddNode {
                node_id: "a".into(),
                properties_msgpack: pa.clone(),
            })
            .unwrap();
            w.append(&Method::AddNode {
                node_id: "b".into(),
                properties_msgpack: props(serde_json::json!({"type": "Code"})),
            })
            .unwrap();
            w.append(&Method::AddEdge {
                source_id: "a".into(),
                target_id: "b".into(),
                properties_msgpack: pe,
            })
            .unwrap();
            w.append(&Method::RemoveNode {
                node_id: "b".into(),
            })
            .unwrap();
        }
        // Fresh graph + replay == the mutation sequence applied in order.
        let g = GraphCore::new();
        let n = replay(&g, &path);
        assert_eq!(n, 4);
        assert_eq!(g.node_count(), 1); // b was removed
        assert_eq!(g.get_node_properties("a"), Some(pa));
        assert!(g.get_node_properties("b").is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn replay_tolerates_torn_tail() {
        let dir = std::env::temp_dir().join(format!("eg-wal-torn-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = wal_path(dir.to_str().unwrap(), "g");
        {
            let mut w = WalWriter::open(&path).unwrap();
            w.append(&Method::AddNode {
                node_id: "a".into(),
                properties_msgpack: props(serde_json::json!({"t": 1})),
            })
            .unwrap();
        }
        // Simulate a crash mid-append: a length header claiming more bytes than exist.
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&(9999u32).to_le_bytes()).unwrap();
            f.write_all(b"partial").unwrap();
        }
        let g = GraphCore::new();
        let n = replay(&g, &path); // must not panic; applies the one good record
        assert_eq!(n, 1);
        assert_eq!(g.node_count(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn truncate_full_prefix_empties_the_log() {
        let dir = std::env::temp_dir().join(format!("eg-wal-trunc-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = wal_path(dir.to_str().unwrap(), "g");
        let mut w = WalWriter::open(&path).unwrap();
        w.append(&Method::AddNode {
            node_id: "a".into(),
            properties_msgpack: props(serde_json::json!({"t": 1})),
        })
        .unwrap();
        w.truncate_prefix(w.position()).unwrap();
        let g = GraphCore::new();
        assert_eq!(replay(&g, &path), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn truncate_prefix_keeps_ops_appended_after_checkpoint() {
        // The checkpoint/WAL race: an op appended AFTER the checkpoint captured the
        // position must survive truncation (loss-free). Snapshot covers op#1; op#2
        // arrives during the checkpoint; truncate_prefix(pos_after_1) must keep op#2.
        let dir = std::env::temp_dir().join(format!("eg-wal-race-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = wal_path(dir.to_str().unwrap(), "g");
        let mut w = WalWriter::open(&path).unwrap();
        w.append(&Method::AddNode {
            node_id: "in_snapshot".into(),
            properties_msgpack: props(serde_json::json!({"t": 1})),
        })
        .unwrap();
        let pos = w.position(); // checkpoint captures here
        w.append(&Method::AddNode {
            node_id: "after_checkpoint".into(),
            properties_msgpack: props(serde_json::json!({"t": 2})),
        })
        .unwrap();
        w.truncate_prefix(pos).unwrap(); // op#1 superseded by snapshot, op#2 kept
        drop(w);

        let g = GraphCore::new();
        let n = replay(&g, &path);
        assert_eq!(n, 1, "only the post-checkpoint op should remain");
        assert!(g.get_node_properties("after_checkpoint").is_some());
        assert!(g.get_node_properties("in_snapshot").is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
