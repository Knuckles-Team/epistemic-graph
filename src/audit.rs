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
//!
//! ## Provenance anchoring (inclusion proofs over this same chain)
//!
//! The chain above proves the *sequence* of mutations is unbroken; it does not, by
//! itself, prove that a node's CURRENT durable bytes still match what a past
//! mutation wrote (a raw byte-flip of a stored row — or an ordinary later
//! overwrite — is invisible to [`verify_chain`] alone). A periodic engine job
//! (`server::persistence::provenance_anchor`) closes that gap for the
//! `:ToolCall`/`:RunTrace` provenance-node window: it Merkle-hashes the window
//! (RFC 6962 §2.1 Merkle Tree Hash — [`mth_from_hashes`]) and folds the root into
//! THIS SAME chain as one more entry ([`provenance_anchor_line`]). A later
//! Merkle inclusion proof ([`audit_path_from_hashes`] / [`recompute_root`])
//! re-hashes a node's current content and walks it up an anchor-time sibling path
//! to that chain-protected root — a mismatch proves the node changed after
//! anchoring. See `redb_store::{provenance_anchor_commit, prove_inclusion}` for
//! the durable read/write side and `Method::AuditProveInclusion` for the served
//! surface.

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
/// durable effect (`DurabilityDomain::None`) never reaches this function via the
/// redb write path at all, so it is intentionally absent (still falls through to
/// the final `_ => return None` for a direct caller). `ApplyMutation`/`EvictLRU`/
/// `IcvConfigure` are NOT such methods (all three are `GraphRedb`-durable and
/// GATEWAY_ROUTED) -- W1c gave each an explicit arm below, closing what used to be
/// a real audit-visibility gap for `ApplyMutation`/`IcvConfigure` (`EvictLRU`
/// already had one). The one reserved digest-guarded `ApplyMutation` arm is
/// different from its general fallback further down: the MutationBatch compiler
/// creates that specific `event_type`/`query` shape internally as a digest-only
/// receipt for an authoritative staged-state commit.
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
        // The batch coordinator's per-envelope rows are audited individually inside the
        // shared transaction (one `audit_line` per envelope operation); this method-level
        // line keeps policy `audited: true` consistent for the coordinator itself.
        Method::ApplyChangeEnvelopes { envelopes } => {
            format!("APPLY_CHANGE_ENVELOPES|{}", envelopes.len())
        }
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
        // Fallback for a caller-supplied `ApplyMutation` that is NOT the opaque
        // digest receipt above (e.g. a direct SPARQL UPDATE `event_type`/`query`
        // pair) -- W1c: this durable admin/ledger method previously fell through
        // to `_ => return None`; it is now audited like every other durable
        // mutation. The query text itself is digested (not persisted verbatim)
        // for the same reason `Sql`/`CypherQuery`/`GraphQl` hash their query.
        Method::ApplyMutation { event_type, query } => format!(
            "APPLY_MUTATION|{event_type}|sha256:{}",
            hex::encode(Sha256::digest(query.as_bytes()))
        ),

        // ── W1c: close the 9-method audit/CDC-visibility gap. These durable
        // admin/ledger methods previously fell through to `_ => return None`
        // (never chained into the tamper-evident audit log) despite being
        // GraphRedb-durable and GATEWAY_ROUTED. Each line below is a canonical,
        // deterministic "who/what" summary (the chain's `graph`+`seq` already
        // bind the "who" via the durable-commit call site; `redb_store`'s
        // `(graph, seq)` key plus the chain hash supply the "when"/ordering). ──
        Method::FromMsgpack { msgpack } => format!(
            "FROM_MSGPACK|sha256:{}",
            hex::encode(Sha256::digest(msgpack))
        ),
        Method::Reconcile {
            graph_name,
            msgpack,
        } => format!(
            "RECONCILE|{graph_name}|sha256:{}",
            hex::encode(Sha256::digest(msgpack))
        ),
        Method::ApplyMultisigMutation {
            signatures,
            threshold,
            mutation_type,
            query,
        } => format!(
            "APPLY_MULTISIG_MUTATION|{mutation_type}|threshold={threshold}|signers={}|sha256:{}",
            signatures.len(),
            hex::encode(Sha256::digest(query.as_bytes()))
        ),
        // W2.5 fleet server registry: this variant self-translates into `Method::AddNode`
        // in `dispatch.rs` BEFORE ever reaching a durable commit (mirroring
        // `ApplyMultisigMutation` above, which translates into `ApplyMutation`), so the
        // REAL audit line durable-committed for a registration is `ADD_NODE|srv:<name>`
        // (AddNode's own arm above). This arm is defense-in-depth only, matching
        // `ApplyMultisigMutation`'s precedent.
        Method::RegisterServer { name, .. } => format!("REGISTER_SERVER|srv:{name}"),
        #[cfg(feature = "shacl")]
        Method::IcvConfigure { graph, mode, .. } => format!(
            "ICV_CONFIGURE|{}|{mode}",
            graph.as_deref().unwrap_or("<default>")
        ),
        #[cfg(feature = "reasoning")]
        Method::RunDatalogReasoning { .. } => "RUN_DATALOG_REASONING".to_string(),
        Method::ClearLedger => "CLEAR_LEDGER".to_string(),
        Method::ApplyLedger { transactions } => {
            format!("APPLY_LEDGER|count={}", transactions.len())
        }
        Method::CompactNodesByType {
            node_type,
            threshold,
        } => format!("COMPACT_NODES_BY_TYPE|{node_type}|threshold={threshold}"),

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
        // BUG-111: never logs the checkpoint/metadata/prio_bucket VALUE itself
        // (privacy) -- only the identity + which single field class changed.
        Method::CasWorkItemMetadata { request } => {
            let field = if request.set_checkpoint_id.is_some() {
                "checkpoint_id"
            } else if request.set_metadata_msgpack.is_some() {
                "metadata"
            } else {
                "prio_bucket"
            };
            format!(
                "CAS_WORK_ITEM_METADATA|{}|{}|{field}",
                request.tenant_ref, request.work_item_id
            )
        }
        Method::ReserveWorkItemResources { request } => format!(
            "RESERVE_WORK_ITEM_RESOURCES|{}|{}|{}",
            request.tenant_ref, request.work_item_id, request.attempt
        ),
        Method::ReleaseWorkItemResources { request } => format!(
            "RELEASE_WORK_ITEM_RESOURCES|{}|{}|{}",
            request.tenant_ref, request.work_item_id, request.attempt
        ),
        Method::ReclaimWorkItemResources { request } => format!(
            "RECLAIM_WORK_ITEM_RESOURCES|{}|{}|{}",
            request.tenant_ref, request.work_item_id, request.attempt
        ),
        Method::UpdateResourceHost { request } => format!(
            "UPDATE_RESOURCE_HOST|{}|{}|{}",
            request.tenant_ref, request.host_ref, request.revision
        ),
        Method::QueryWorkItemReservation { .. } | Method::ResourceReservationStatus { .. } => {
            return None
        }
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
        // ML pipeline (CONCEPT:EG-KG.mining.ml-pipeline): same durable-writeback shape as
        // the Mine*/GraphLearn* family above (`access.rs::requires_write` only reaches the
        // durable-commit path when it actually mutates), so it gets the same audit
        // coverage every sibling in this family already has (GOC-40, eg-capabilities'
        // `audited_matches_audit_rs_exactly` cross-check caught the omission).
        #[cfg(feature = "ml-pipeline")]
        Method::MiningPipelineTrain { .. } => "MINING_PIPELINE_TRAIN".to_string(),
        #[cfg(feature = "ml-pipeline")]
        Method::MiningPipelineServe { .. } => "MINING_PIPELINE_SERVE".to_string(),
        #[cfg(feature = "ml-pipeline")]
        Method::MiningPipelinePredict { .. } => "MINING_PIPELINE_PREDICT".to_string(),

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

// ── Provenance anchoring: RFC 6962 Merkle Tree Hash + audit path ───────────────
//
// A pure-Rust implementation of Certificate Transparency's Merkle Tree Hash
// (RFC 6962 §2.1) and Merkle audit path (§2.1.1). Leaf/internal domain
// separation (the `0x00`/`0x01` prefixes below) defeats the classic
// second-preimage tree-forgery attack; the "largest power of two below n" split
// needs no rebalancing or duplicate-last-node trick for an odd leaf count
// (unlike the weaker Bitcoin-style scheme). These functions operate on
// ALREADY-HASHED leaves ([`merkle_leaf_hash`] applies RFC 6962's own leaf-hash
// step once, up front) so a Merkle audit path for one leaf never requires
// re-reading any OTHER leaf's raw content — only its recorded sibling hashes —
// which is what lets [`crate::redb_store::prove_inclusion`] verify one node
// independent of what happened to its neighbors afterward.

/// Domain-separation tag for a LEAF hash (RFC 6962 `MTH({d(0)})`).
const LEAF_TAG: u8 = 0x00;
/// Domain-separation tag for an INTERNAL node hash (RFC 6962 `MTH(D[n])`, n>1).
const NODE_TAG: u8 = 0x01;

/// Fixed content hashed in place of a provenance node that no longer has a
/// durable row (removed since it was anchored). It can never equal a real
/// anchor-time leaf hash (a real one is content-addressed on that node's actual
/// bytes), so an inclusion proof for a since-removed node fails closed exactly
/// like tampering would, rather than panicking or fabricating a pass.
pub const MISSING_NODE_SENTINEL: &[u8] = b"EG-PROVENANCE-ANCHOR-MISSING-NODE";

/// Which side of its parent a Merkle audit-path sibling hash sits on — the
/// verifier needs this to fold `(running_hash, sibling)` in the right order.
/// Also the wire `MerkleSide` the served `Method::AuditProveInclusion` surface
/// returns (re-exported via `crate::protocol`).
pub use crate::protocol::MerkleSide;

/// One step of a Merkle audit path: a sibling subtree hash plus which side it
/// sits on relative to the path being verified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProofStep {
    pub sibling: Hash,
    pub side: MerkleSide,
}

/// Length-prefix `node_id` before the raw content bytes so the two can never be
/// confused with each other (mirrors `build_envelope_v2_bytes`'s length-prefix
/// convention) and so the node's IDENTITY, not just its content, is bound into
/// the leaf — two different nodes that happen to carry byte-identical properties
/// still hash to distinct leaves.
fn encode_leaf_input(node_id: &str, content: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(4 + node_id.len() + content.len());
    v.extend_from_slice(&(node_id.len() as u32).to_be_bytes());
    v.extend_from_slice(node_id.as_bytes());
    v.extend_from_slice(content);
    v
}

/// A provenance-node leaf hash: `SHA-256(0x00 || len(node_id) || node_id ||
/// content)`, content-addressed on the node's CURRENT durable bytes at the time
/// of the call. Re-computing this later from a since-mutated node yields a
/// different hash — the tamper signal an inclusion proof checks for.
pub fn merkle_leaf_hash(node_id: &str, content: &[u8]) -> Hash {
    let mut h = Sha256::new();
    h.update([LEAF_TAG]);
    h.update(encode_leaf_input(node_id, content));
    h.finalize().into()
}

fn parent_hash(left: &Hash, right: &Hash) -> Hash {
    let mut h = Sha256::new();
    h.update([NODE_TAG]);
    h.update(left);
    h.update(right);
    h.finalize().into()
}

/// Largest power of two strictly less than `n` (RFC 6962's tree-split rule,
/// defined for `n > 1`).
fn largest_power_of_two_below(n: usize) -> usize {
    debug_assert!(
        n > 1,
        "largest_power_of_two_below is only defined for n > 1"
    );
    let mut k = 1usize;
    while k * 2 < n {
        k *= 2;
    }
    k
}

/// RFC 6962 §2.1 Merkle Tree Hash over an ORDERED list of ALREADY-COMPUTED leaf
/// hashes (a provenance anchor persists leaf hashes, not raw node content — see
/// `crate::redb_store::PROVENANCE_ANCHOR_MEMBERS`'s doc). `n == 0` is RFC 6962's
/// defined empty-tree case (the anchoring job never actually calls this on an
/// empty window, but the function stays total). `n == 1` returns the lone leaf
/// hash unchanged — it already carries the RFC's `0x00` leaf tag from
/// [`merkle_leaf_hash`], so it is NOT re-hashed here.
pub fn mth_from_hashes(leaves: &[Hash]) -> Hash {
    match leaves.len() {
        0 => Sha256::digest(b"").into(),
        1 => leaves[0],
        n => {
            let k = largest_power_of_two_below(n);
            let left = mth_from_hashes(&leaves[..k]);
            let right = mth_from_hashes(&leaves[k..]);
            parent_hash(&left, &right)
        }
    }
}

/// RFC 6962 §2.1.1 Merkle audit path for leaf index `m` (0-based) among
/// already-hashed `leaves`: the ordered sibling-hash path from the leaf up to
/// (not including) the root, each step tagged with which side it sits on so
/// [`recompute_root`] does not need to re-derive the recursive split. Empty when
/// `m` is out of bounds or the tree is a single leaf (nothing to prove against).
pub fn audit_path_from_hashes(leaves: &[Hash], m: usize) -> Vec<ProofStep> {
    fn go(leaves: &[Hash], m: usize, out: &mut Vec<ProofStep>) {
        let n = leaves.len();
        if n <= 1 {
            return; // PATH(0, {d(0)}) = {}
        }
        let k = largest_power_of_two_below(n);
        if m < k {
            go(&leaves[..k], m, out);
            out.push(ProofStep {
                sibling: mth_from_hashes(&leaves[k..]),
                side: MerkleSide::Right,
            });
        } else {
            go(&leaves[k..], m - k, out);
            out.push(ProofStep {
                sibling: mth_from_hashes(&leaves[..k]),
                side: MerkleSide::Left,
            });
        }
    }
    let mut out = Vec::new();
    if m < leaves.len() {
        go(leaves, m, &mut out);
    }
    out
}

/// Recompute a Merkle root by folding `leaf_hash` up through `path`. The caller
/// (`crate::redb_store::prove_inclusion`) passes the TARGET node's CURRENT
/// content re-hashed via [`merkle_leaf_hash`] and the ANCHOR-TIME sibling `path`;
/// comparing the result to the chain-protected anchored root is the inclusion
/// proof's verification step. Always returns a value (never fails) so a caller
/// can report both the anchored and the recomputed root even on a mismatch.
pub fn recompute_root(leaf_hash: &Hash, path: &[ProofStep]) -> Hash {
    let mut acc = *leaf_hash;
    for step in path {
        acc = match step.side {
            MerkleSide::Right => parent_hash(&acc, &step.sibling),
            MerkleSide::Left => parent_hash(&step.sibling, &acc),
        };
    }
    acc
}

/// Canonical, deterministic audit-chain LINE for a provenance anchor:
/// `PROVENANCE_ANCHOR|count=<n>|sha256:<root-hex>`. Mirrors the compact
/// `key=value` / `sha256:` idiom [`audit_line`] already uses for
/// `ApplyLedger`/`ApplyMutation`'s digest receipt — the full member list lives in
/// the sibling `PROVENANCE_ANCHOR_MEMBERS` side table (`redb_store.rs`), keyed by
/// this entry's own chain `seq`, so the chained line itself stays small
/// regardless of window size. Unlike every line [`audit_line`] produces, this one
/// has no corresponding `Method` — it is synthesized by the periodic
/// provenance-anchor sweep, not a client request, hence a free function instead
/// of another `audit_line` match arm.
pub fn provenance_anchor_line(count: usize, root: &Hash) -> String {
    format!(
        "PROVENANCE_ANCHOR|count={count}|sha256:{}",
        hex::encode(root)
    )
}

/// Inverse of [`provenance_anchor_line`]: `None` for any line that is not
/// exactly that shape (including an ordinary mutation line) — callers use this
/// to recognize an anchor entry while walking the chain.
pub fn parse_provenance_anchor_line(line: &[u8]) -> Option<(usize, Hash)> {
    let line = std::str::from_utf8(line).ok()?;
    let rest = line.strip_prefix("PROVENANCE_ANCHOR|count=")?;
    let (count_str, rest) = rest.split_once('|')?;
    let count: usize = count_str.parse().ok()?;
    let hex_root = rest.strip_prefix("sha256:")?;
    if hex_root.len() != 64 || !hex_root.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let bytes = hex::decode(hex_root).ok()?;
    let root: Hash = bytes.try_into().ok()?;
    Some((count, root))
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
    fn authoritative_state_receipt_gets_a_short_line_others_fall_back_to_general_audit() {
        let digest = "a".repeat(64);
        let receipt = Method::ApplyMutation {
            event_type: "authoritative_state_operation".to_string(),
            query: format!("sha256:{digest}"),
        };
        assert_eq!(
            audit_line(&receipt).as_deref(),
            Some(format!("AUTHORITATIVE_STATE_MUTATION|sha256:{digest}").as_str())
        );
        // Pre-existing bug found while verifying this task's own "existing audit
        // tests stay green" acceptance bar (unrelated to provenance anchoring --
        // logged to reports/issue-register.md): this assertion used to expect
        // `None` for a malformed digest, but `b1ac4ac` ("W1c", 2026-07-21) added
        // the general `ApplyMutation` fallback arm below the digest-guarded one
        // specifically to CLOSE an audit-visibility gap (every `ApplyMutation`
        // is now audited, never silently dropped) -- it never updated this
        // pre-existing test to match. `None` here would silently RE-OPEN that
        // exact gap, so a malformed "authoritative_state_operation" query must
        // fall through to the general digested `APPLY_MUTATION|...` line, not
        // `None`.
        let malformed = audit_line(&Method::ApplyMutation {
            event_type: "authoritative_state_operation".to_string(),
            query: "not-a-digest".to_string(),
        });
        assert_eq!(
            malformed.as_deref(),
            Some(
                format!(
                    "APPLY_MUTATION|authoritative_state_operation|sha256:{}",
                    hex::encode(Sha256::digest(b"not-a-digest"))
                )
                .as_str()
            )
        );
    }

    // ── Provenance anchoring: Merkle primitives ─────────────────────────────

    #[test]
    fn mth_single_leaf_is_the_leaf_itself() {
        // RFC 6962 `MTH({d0}) = SHA-256(0x00 || d0)` -- already what
        // `merkle_leaf_hash` computed, so a one-leaf tree's root IS that hash,
        // unchanged.
        let leaf = merkle_leaf_hash("n1", b"props");
        assert_eq!(mth_from_hashes(&[leaf]), leaf);
    }

    #[test]
    fn mth_two_leaves_matches_manual_rfc6962_combination() {
        let a = merkle_leaf_hash("n1", b"a");
        let b = merkle_leaf_hash("n2", b"b");
        let root = mth_from_hashes(&[a, b]);
        let mut h = Sha256::new();
        h.update([NODE_TAG]);
        h.update(a);
        h.update(b);
        let expected: Hash = h.finalize().into();
        assert_eq!(root, expected);
    }

    #[test]
    fn audit_path_round_trips_for_every_leaf_in_an_odd_sized_tree() {
        // n=5 exercises an UNBALANCED split (k=4) at the top level -- the case
        // the RFC 6962 "largest power of two below n" rule (not a
        // duplicate-last-node scheme) has to get right.
        let leaves: Vec<Hash> = (0..5)
            .map(|i| merkle_leaf_hash(&format!("n{i}"), format!("props{i}").as_bytes()))
            .collect();
        let root = mth_from_hashes(&leaves);
        for (m, leaf) in leaves.iter().enumerate() {
            let path = audit_path_from_hashes(&leaves, m);
            let recomputed = recompute_root(leaf, &path);
            assert_eq!(recomputed, root, "leaf {m} must verify against the root");
        }
    }

    #[test]
    fn audit_path_detects_a_tampered_leaf() {
        let leaves: Vec<Hash> = (0..5)
            .map(|i| merkle_leaf_hash(&format!("n{i}"), format!("props{i}").as_bytes()))
            .collect();
        let root = mth_from_hashes(&leaves);
        let path = audit_path_from_hashes(&leaves, 2);
        // The verifier re-hashes leaf 2 from CURRENT (here: different/tampered)
        // content and walks the SAME anchor-time sibling path.
        let tampered_leaf = merkle_leaf_hash("n2", b"props2-TAMPERED");
        let recomputed = recompute_root(&tampered_leaf, &path);
        assert_ne!(
            recomputed, root,
            "a tampered leaf must not reproduce the anchored root"
        );
    }

    #[test]
    fn audit_path_is_empty_for_a_single_leaf_tree() {
        let leaf = merkle_leaf_hash("only", b"content");
        assert!(audit_path_from_hashes(&[leaf], 0).is_empty());
        assert_eq!(recompute_root(&leaf, &[]), leaf);
    }

    #[test]
    fn audit_path_out_of_bounds_index_is_empty() {
        let leaves: Vec<Hash> = (0..3)
            .map(|i| merkle_leaf_hash(&format!("n{i}"), b"x"))
            .collect();
        assert!(audit_path_from_hashes(&leaves, 3).is_empty());
    }

    #[test]
    fn provenance_anchor_line_round_trips() {
        let root = merkle_leaf_hash("x", b"y");
        let line = provenance_anchor_line(3, &root);
        assert_eq!(
            line,
            format!("PROVENANCE_ANCHOR|count=3|sha256:{}", hex::encode(root))
        );
        assert_eq!(
            parse_provenance_anchor_line(line.as_bytes()),
            Some((3, root))
        );
    }

    #[test]
    fn parse_provenance_anchor_line_rejects_non_anchor_lines() {
        assert_eq!(parse_provenance_anchor_line(b"ADD_NODE|n1"), None);
        assert_eq!(
            parse_provenance_anchor_line(b"PROVENANCE_ANCHOR|count=abc|sha256:zz"),
            None
        );
        assert_eq!(
            parse_provenance_anchor_line(b"PROVENANCE_ANCHOR|count=1|sha256:tooshort"),
            None
        );
    }

    #[test]
    fn merkle_leaf_hash_binds_the_node_id_not_just_content() {
        // Two different node ids with byte-identical properties must NOT collide.
        let a = merkle_leaf_hash("node-a", b"same-content");
        let b = merkle_leaf_hash("node-b", b"same-content");
        assert_ne!(a, b);
    }
}
