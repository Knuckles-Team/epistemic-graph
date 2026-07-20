//! Tamper-evident hash-chained audit log (CONCEPT:EG-KG.sharding.row-level-security, Lane O).
//!
//! Every durable mutation appends ONE entry to a per-graph hash CHAIN stored in the
//! redb `AUDIT` table, keyed `(graph, seq)`. Each entry binds the previous entry's
//! hash, so altering, reordering, deleting, or inserting any entry breaks the chain
//! at that position — `Method::AuditVerify{graph}` walks the chain and reports the
//! first break (or OK). This turns the ledger into an append-only, tamper-EVIDENT
//! record (PURE-RUST `sha2`, the RustCrypto SHA-256 already in the dep tree).
//!
//! Stored entry layout (value bytes for `(graph, seq)`):
//! ```text
//!   [ prev_hash (32) | entry_hash (32) | line (UTF-8, variable) ]
//! ```
//! where `entry_hash = SHA256( prev_hash || graph || seq_le_u64 || line )` and the
//! genesis entry (`seq == 0`) uses an all-zero `prev_hash`. `line` is a canonical,
//! deterministic description of the mutation (see [`audit_line`]).

#![cfg(feature = "security")]

use sha2::{Digest, Sha256};

use crate::protocol::{AuditReport, Method};

/// A 32-byte chain hash.
pub type Hash = [u8; 32];

/// The genesis previous-hash (all zeros).
pub const GENESIS: Hash = [0u8; 32];

/// Compute one chain link: `entry_hash = SHA256(prev || graph || seq || line)`.
pub fn link_hash(prev: &Hash, graph: &str, seq: u64, line: &[u8]) -> Hash {
    let mut h = Sha256::new();
    h.update(prev);
    h.update(graph.as_bytes());
    h.update(seq.to_le_bytes());
    h.update(line);
    h.finalize().into()
}

/// Serialize one chain entry to its stored value bytes: `prev | hash | line`.
pub fn encode_entry(prev: &Hash, hash: &Hash, line: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(64 + line.len());
    v.extend_from_slice(prev);
    v.extend_from_slice(hash);
    v.extend_from_slice(line);
    v
}

/// Decode a stored chain entry into `(prev_hash, entry_hash, line)`. `None` when the
/// blob is too short to hold the two hashes (a corrupt/truncated row).
pub fn decode_entry(blob: &[u8]) -> Option<(Hash, Hash, &[u8])> {
    if blob.len() < 64 {
        return None;
    }
    let mut prev = [0u8; 32];
    let mut hash = [0u8; 32];
    prev.copy_from_slice(&blob[0..32]);
    hash.copy_from_slice(&blob[32..64]);
    Some((prev, hash, &blob[64..]))
}

/// A canonical, deterministic one-line description of a durable mutation, used as the
/// chained audit `line`. Mirrors the GraphCore ledger vocabulary (`ADD_NODE|…`) but
/// is derived purely from the `Method`, so the audit chain is self-contained (it does
/// not depend on the in-RAM ledger). Multi-row methods (BatchUpdate/ClearGraph) get a
/// single summarizing line — the chain proves the SEQUENCE of operations was not
/// tampered with, which is the audit property.
///
/// ## Exhaustiveness (CONCEPT:EG-KG.sharding.row-level-security, L3/EG-P0-6)
///
/// `redb_store::append_audit_entry` calls this for EVERY `(graph, method)` pair that
/// reaches `commit_ops`/`commit_crossmodal`: every method for which the capability
/// policy declares authoritative durability and a persistence backend is
/// configured. This match is exhaustive
/// over the FULL durable-mutation surface (every `GraphRedb`- and `Outbox`-domain
/// method per `eg_capabilities::policy`, see `crates/eg-capabilities`), so every
/// acknowledged durable mutation chains into the audit log. A method with no
/// durable effect (`DurabilityDomain::None` — e.g. a caller-supplied
/// `ApplyMutation`, `EvictLRU`, or `IcvConfigure`) never reaches this function via
/// the redb write path at all, so it is intentionally absent. The one reserved
/// `ApplyMutation` event below is different: the MutationBatch compiler creates it
/// internally as a digest-only receipt for an authoritative staged-state commit.
pub fn audit_line(method: &Method) -> Option<String> {
    let line = match method {
        // ── Core node/edge CRUD (audited since EG-P0-2) ──────────────────────
        Method::AddNode { node_id, .. } => format!("ADD_NODE|{node_id}"),
        Method::CreateNodeIfAbsent { node_id, .. } => {
            format!("CREATE_NODE_IF_ABSENT|{node_id}")
        }
        Method::RemoveNode { node_id } => format!("REMOVE_NODE|{node_id}"),
        Method::CompareAndSetNodeFields { node_id, .. } => format!("CAS_NODE|{node_id}"),
        Method::AddEdge {
            source_id,
            target_id,
            ..
        } => format!("ADD_EDGE|{source_id}|{target_id}"),
        Method::RemoveEdge {
            source_id,
            target_id,
        } => format!("REMOVE_EDGE|{source_id}|{target_id}"),
        Method::BatchUpdate { .. } => "BATCH_UPDATE".to_string(),
        Method::ClearGraph => "CLEAR_GRAPH".to_string(),
        Method::ApplyChangeEnvelope { envelope } => format!(
            "APPLY_CHANGE_ENVELOPE|{}|{}|{}",
            envelope.envelope_id, envelope.mutation.batch_id, envelope.content_version.digest
        ),
        #[cfg(feature = "modality-serving")]
        Method::ServedModality { op } if op.mutates() => {
            use eg_types::{ServedModalityKind, ServedModalityOp};
            let (operation, modality) = match op {
                ServedModalityOp::Ingest { modality, .. } => ("INGEST", modality),
                ServedModalityOp::IngestStream { modality, .. } => ("INGEST_STREAM", modality),
                ServedModalityOp::Delete { modality, .. } => ("DELETE", modality),
                ServedModalityOp::MoveToCold { modality, .. } => ("MOVE_TO_COLD", modality),
                ServedModalityOp::Restore { modality, .. } => ("RESTORE", modality),
                ServedModalityOp::CollectTombstones { modality, .. } => {
                    ("COLLECT_TOMBSTONES", modality)
                }
                _ => return None,
            };
            let modality = match modality {
                ServedModalityKind::Document => "DOCUMENT",
                ServedModalityKind::Image => "IMAGE",
                ServedModalityKind::Audio => "AUDIO",
                ServedModalityKind::Video => "VIDEO",
            };
            format!("SERVED_MODALITY|{modality}|{operation}")
        }
        Method::ApplyMutation { event_type, query }
            if event_type == "authoritative_state_operation"
                && query.len() == 71
                && query.starts_with("sha256:")
                && query[7..].bytes().all(|byte| byte.is_ascii_hexdigit()) =>
        {
            // State-backed mutations persist a complete, digest-verified graph
            // image in the same transaction. Their canonical operation is opaque
            // by design, so the audit line binds only its SHA-256 receipt.
            format!("AUTHORITATIVE_STATE_MUTATION|{query}")
        }

        // ── Remaining GraphRedb-durable node/edge/RDF primitives (EG-P0-6) ──
        Method::InvalidateEdge {
            source_id,
            target_id,
            ..
        } => format!("INVALIDATE_EDGE|{source_id}|{target_id}"),
        Method::SupersedeEdge {
            source_id,
            target_id,
            ..
        } => format!("SUPERSEDE_EDGE|{source_id}|{target_id}"),
        Method::ClaimNext { label, .. } => format!("CLAIM_NEXT|{label}"),
        Method::ClaimWorkItem { request } => {
            format!("CLAIM_WORK_ITEM|{}", request.tenant_ref)
        }
        Method::RenewWorkItemLease {
            tenant,
            work_item_id,
            lease_epoch,
            ..
        } => format!("RENEW_WORK_ITEM|{tenant}|{work_item_id}|{lease_epoch}"),
        Method::CommitWorkItemResult {
            tenant,
            work_item_id,
            lease_epoch,
            outcome,
            ..
        } => format!("COMMIT_WORK_ITEM|{tenant}|{work_item_id}|{lease_epoch}|{outcome}"),
        Method::CancelWorkItem {
            tenant,
            work_item_id,
            ..
        } => format!("CANCEL_WORK_ITEM|{tenant}|{work_item_id}"),
        Method::DeferWorkItem {
            tenant,
            work_item_id,
            lease_epoch,
            next_retry_at_ms,
            ..
        } => format!("DEFER_WORK_ITEM|{tenant}|{work_item_id}|{lease_epoch}|{next_retry_at_ms}"),
        Method::Sql { query, .. } => format!(
            "SQL_MUTATION|sha256:{}",
            hex::encode(Sha256::digest(query.as_bytes()))
        ),
        Method::CypherQuery { query, .. } => format!(
            "CYPHER_MUTATION|sha256:{}",
            hex::encode(Sha256::digest(query.as_bytes()))
        ),
        Method::GraphQl { query, .. } => format!(
            "GRAPHQL_MUTATION|sha256:{}",
            hex::encode(Sha256::digest(query.as_bytes()))
        ),
        Method::AddEmbedding { node_id, .. } => format!("ADD_EMBEDDING|{node_id}"),
        #[cfg(feature = "rdf")]
        Method::AddTriples { .. } => "ADD_TRIPLES".to_string(),
        #[cfg(feature = "rdf")]
        Method::RemoveTriples { .. } => "REMOVE_TRIPLES".to_string(),
        #[cfg(feature = "rdf")]
        Method::DropNamedGraph => "DROP_NAMED_GRAPH".to_string(),

        // ── Agent-memory / scene-graph / trajectory mutations (CONCEPT:EG-KG.memory.eg-batch-decay-caller) ──
        Method::CreateSummaryNode { .. } => "CREATE_SUMMARY_NODE".to_string(),
        Method::Consolidate { .. } => "CONSOLIDATE".to_string(),
        Method::Reinforce { node_id, .. } => format!("REINFORCE|{node_id}"),
        Method::DecayNode { node_id, .. } => format!("DECAY_NODE|{node_id}"),
        Method::DecayMemories { .. } => "DECAY_MEMORIES".to_string(),
        Method::EvictBelow { .. } => "EVICT_BELOW".to_string(),
        Method::Maintain { .. } => "MAINTAIN".to_string(),
        Method::AddSceneObject { .. } => "ADD_SCENE_OBJECT".to_string(),
        Method::SetPose { node_id, .. } => format!("SET_POSE|{node_id}"),
        Method::Reparent { node_id, .. } => format!("REPARENT|{node_id}"),
        Method::StartTrajectory { .. } => "START_TRAJECTORY".to_string(),
        Method::AppendStep { traj_id, .. } => format!("APPEND_STEP|{traj_id}"),

        // ── Data-mining / graph-learning writeback (CONCEPT:EG-KG.mining.*) ──────────────
        // Durability is `writeback`-conditional; `wal.rs::is_durable_mutation` already
        // gates on the exact condition, so this arm only ever fires when the call
        // actually reached the durable-commit path — no extra guard needed here.
        #[cfg(feature = "mining")]
        Method::MineAssociate { .. } => "MINE_ASSOCIATE".to_string(),
        #[cfg(feature = "mining")]
        Method::MineCluster { .. } => "MINE_CLUSTER".to_string(),
        #[cfg(feature = "mining")]
        Method::MineAnomaly { .. } => "MINE_ANOMALY".to_string(),
        #[cfg(feature = "mining")]
        Method::MineClassifyPredict { .. } => "MINE_CLASSIFY_PREDICT".to_string(),
        #[cfg(feature = "mining")]
        Method::MineReduce { .. } => "MINE_REDUCE".to_string(),
        #[cfg(feature = "mining")]
        Method::MineSequence { .. } => "MINE_SEQUENCE".to_string(),
        #[cfg(feature = "mining")]
        Method::MineForecast { .. } => "MINE_FORECAST".to_string(),
        #[cfg(feature = "mining")]
        Method::MineText { .. } => "MINE_TEXT".to_string(),
        #[cfg(feature = "mining")]
        Method::MineSubgraph { .. } => "MINE_SUBGRAPH".to_string(),
        #[cfg(feature = "mining")]
        Method::MineEntityResolve { .. } => "MINE_ENTITY_RESOLVE".to_string(),
        #[cfg(feature = "mining")]
        Method::MineCausalImpact { .. } => "MINE_CAUSAL_IMPACT".to_string(),
        #[cfg(feature = "mining")]
        Method::MineProcess { .. } => "MINE_PROCESS".to_string(),
        #[cfg(feature = "mining")]
        Method::MineRootCause { .. } => "MINE_ROOT_CAUSE".to_string(),
        #[cfg(feature = "mining")]
        Method::MineRiskPropagation { .. } => "MINE_RISK_PROPAGATION".to_string(),
        #[cfg(feature = "mining")]
        Method::MineOntologyGap { .. } => "MINE_ONTOLOGY_GAP".to_string(),
        #[cfg(feature = "mining")]
        Method::MineRetrievalQuality { .. } => "MINE_RETRIEVAL_QUALITY".to_string(),
        #[cfg(feature = "mining")]
        Method::MineCommunity { .. } => "MINE_COMMUNITY".to_string(),
        #[cfg(feature = "graphlearn")]
        Method::GraphLearnFit { .. } => "GRAPH_LEARN_FIT".to_string(),
        #[cfg(feature = "graphlearn")]
        Method::GraphLearnPredict { .. } => "GRAPH_LEARN_PREDICT".to_string(),

        // ── Message-broker / stream mutations, Outbox domain (CONCEPT:EG-KG.compute.message-broker-exchanges /
        // replayable-append-log / publisher-confirms-consumer-qos) ──────────────────────
        // NOT NODES/EDGES rows (`redb_store::apply_method_rows` is a no-op for
        // them — the control-graph state lives on the in-memory `GraphCore`,
        // replayed via `wal.rs::apply` on restart) but they DO flow through the
        // SAME `record`/`record_durable` → `commit_ops`/`commit_crossmodal` →
        // `append_audit_entry` call as every other durable mutation, so they
        // chain into the SAME per-graph tamper-evident audit log.
        #[cfg(feature = "broker")]
        Method::DeclareExchange { exchange, .. } => format!("DECLARE_EXCHANGE|{exchange}"),
        #[cfg(feature = "broker")]
        Method::DeleteExchange { exchange } => format!("DELETE_EXCHANGE|{exchange}"),
        #[cfg(feature = "broker")]
        Method::BindQueue {
            exchange, queue, ..
        } => format!("BIND_QUEUE|{exchange}|{queue}"),
        #[cfg(feature = "broker")]
        Method::UnbindQueue {
            exchange, queue, ..
        } => format!("UNBIND_QUEUE|{exchange}|{queue}"),
        #[cfg(feature = "broker")]
        Method::Publish {
            exchange,
            routing_key,
            ..
        } => format!("PUBLISH|{exchange}|{routing_key}"),
        #[cfg(feature = "broker")]
        Method::DeclareQueue { queue, .. } => format!("DECLARE_QUEUE|{queue}"),
        #[cfg(feature = "broker")]
        Method::PublishEx {
            exchange,
            routing_key,
            ..
        } => format!("PUBLISH_EX|{exchange}|{routing_key}"),
        #[cfg(feature = "broker")]
        Method::BrokerConsume { queue, .. } => format!("BROKER_CONSUME|{queue}"),
        #[cfg(feature = "broker")]
        Method::BrokerAck { queue, node_id } => format!("BROKER_ACK|{queue}|{node_id}"),
        #[cfg(feature = "broker")]
        Method::BrokerReject { queue, node_id, .. } => format!("BROKER_REJECT|{queue}|{node_id}"),
        #[cfg(feature = "broker")]
        Method::SweepExpired { .. } => "SWEEP_EXPIRED".to_string(),
        #[cfg(feature = "broker")]
        Method::StreamDeclare { stream, .. } => format!("STREAM_DECLARE|{stream}"),
        #[cfg(feature = "broker")]
        Method::StreamPublish { stream, .. } => format!("STREAM_PUBLISH|{stream}"),
        #[cfg(feature = "broker")]
        Method::StreamTrim { stream, .. } => format!("STREAM_TRIM|{stream}"),
        #[cfg(feature = "broker")]
        Method::StreamCommitOffset { stream, group, .. } => {
            format!("STREAM_COMMIT_OFFSET|{stream}|{group}")
        }
        #[cfg(feature = "broker")]
        Method::PublishConfirmed {
            exchange,
            routing_key,
            ..
        } => format!("PUBLISH_CONFIRMED|{exchange}|{routing_key}"),
        #[cfg(feature = "broker")]
        Method::PublishIdempotent {
            exchange,
            routing_key,
            ..
        } => format!("PUBLISH_IDEMPOTENT|{exchange}|{routing_key}"),
        #[cfg(feature = "broker")]
        Method::BrokerAckTag { delivery_tag, .. } => format!("BROKER_ACK_TAG|{delivery_tag}"),
        #[cfg(feature = "broker")]
        Method::BrokerNackTag { delivery_tag, .. } => format!("BROKER_NACK_TAG|{delivery_tag}"),
        #[cfg(feature = "broker")]
        Method::BrokerRenewTag { delivery_tag, .. } => {
            format!("BROKER_RENEW_TAG|{delivery_tag}")
        }

        // Transfer paths are logical operator-provisioned names. Keep them out
        // of the chain so audit records never persist filesystem details.
        #[cfg(feature = "sqlite-file")]
        Method::ImportSqliteFile { .. } => "IMPORT_SQLITE_FILE".to_string(),
        #[cfg(feature = "sqlite-file")]
        Method::ExportSqliteFile { .. } => "EXPORT_SQLITE_FILE".to_string(),

        // Every non-durable method (`DurabilityDomain::None`) never reaches this
        // function via the redb write path in the first place; still falls through
        // here harmlessly for any caller that invokes `audit_line` directly.
        _ => return None,
    };
    Some(line)
}

/// Walk an ordered iterator of `(seq, stored_blob)` entries and verify the chain.
/// The caller supplies the entries in seq order (the redb range scan does this).
pub fn verify_chain<'a, I>(graph: &str, entries: I) -> AuditReport
where
    I: IntoIterator<Item = (u64, &'a [u8])>,
{
    let mut prev = GENESIS;
    // `idx` (0-based position) IS the seq expected at this step — entries arrive in
    // seq order, so a mismatch is a gap (deleted/reordered entry). `walked` counts the
    // entries verified OK before any break (== idx at the break point).
    let mut walked = 0u64;
    for (idx, (seq, blob)) in entries.into_iter().enumerate() {
        let expected_seq = idx as u64;
        if seq != expected_seq {
            return AuditReport {
                graph: graph.to_string(),
                ok: false,
                entries: walked,
                first_broken_seq: Some(seq),
                detail: format!("sequence gap: expected seq {expected_seq}, found {seq}"),
            };
        }
        let (stored_prev, stored_hash, line) = match decode_entry(blob) {
            Some(t) => t,
            None => {
                return AuditReport {
                    graph: graph.to_string(),
                    ok: false,
                    entries: walked,
                    first_broken_seq: Some(seq),
                    detail: "corrupt entry (too short to hold chain hashes)".to_string(),
                };
            }
        };
        // The stored prev must match what we carried forward (catches a deleted /
        // reordered predecessor), and the stored hash must equal the recomputed link
        // (catches an edited line or hash).
        let recomputed = link_hash(&prev, graph, seq, line);
        if stored_prev != prev || stored_hash != recomputed {
            return AuditReport {
                graph: graph.to_string(),
                ok: false,
                entries: walked,
                first_broken_seq: Some(seq),
                detail: format!("hash-chain break at seq {seq} (entry mutated or chain altered)"),
            };
        }
        prev = stored_hash;
        walked += 1;
    }
    AuditReport {
        graph: graph.to_string(),
        ok: true,
        entries: walked,
        first_broken_seq: None,
        detail: "chain verified".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a clean N-entry chain returning the stored blobs in seq order.
    fn build_chain(graph: &str, lines: &[&str]) -> Vec<Vec<u8>> {
        let mut prev = GENESIS;
        let mut out = Vec::new();
        for (seq, line) in lines.iter().enumerate() {
            let hash = link_hash(&prev, graph, seq as u64, line.as_bytes());
            out.push(encode_entry(&prev, &hash, line.as_bytes()));
            prev = hash;
        }
        out
    }

    #[test]
    fn clean_chain_verifies() {
        let g = "agent:a";
        let blobs = build_chain(g, &["ADD_NODE|n1", "ADD_NODE|n2", "ADD_EDGE|n1|n2"]);
        let report = verify_chain(
            g,
            blobs
                .iter()
                .enumerate()
                .map(|(i, b)| (i as u64, b.as_slice())),
        );
        assert!(report.ok, "{report:?}");
        assert_eq!(report.entries, 3);
        assert!(report.first_broken_seq.is_none());
    }

    #[test]
    fn mutated_entry_detected_at_position() {
        let g = "agent:a";
        let mut blobs = build_chain(g, &["ADD_NODE|n1", "ADD_NODE|n2", "ADD_EDGE|n1|n2"]);
        // Tamper the LINE of entry seq=1 (flip its content) WITHOUT recomputing hashes.
        let last = blobs[1].len() - 1;
        blobs[1][last] ^= 0xFF;
        let report = verify_chain(
            g,
            blobs
                .iter()
                .enumerate()
                .map(|(i, b)| (i as u64, b.as_slice())),
        );
        assert!(!report.ok);
        assert_eq!(report.first_broken_seq, Some(1));
    }

    #[test]
    fn deleted_entry_breaks_chain() {
        let g = "agent:a";
        let blobs = build_chain(g, &["ADD_NODE|n1", "ADD_NODE|n2", "ADD_EDGE|n1|n2"]);
        // Drop seq=1: the surviving entries are now seqs 0 and 2 → a gap is detected.
        let kept: Vec<(u64, &[u8])> = vec![(0, blobs[0].as_slice()), (2, blobs[2].as_slice())];
        let report = verify_chain(g, kept);
        assert!(!report.ok);
        assert_eq!(report.first_broken_seq, Some(2));
    }

    #[test]
    fn authoritative_state_receipt_audits_only_a_valid_digest() {
        let digest = "a".repeat(64);
        let receipt = Method::ApplyMutation {
            event_type: "authoritative_state_operation".to_string(),
            query: format!("sha256:{digest}"),
        };
        assert_eq!(
            audit_line(&receipt).as_deref(),
            Some(format!("AUTHORITATIVE_STATE_MUTATION|sha256:{digest}").as_str())
        );
        assert!(audit_line(&Method::ApplyMutation {
            event_type: "authoritative_state_operation".to_string(),
            query: "not-a-digest".to_string(),
        })
        .is_none());
    }
}
