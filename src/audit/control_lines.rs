//! Audit lines for the durable admin, ledger and graph-schema methods.
//!
//! Free functions in their own module rather than closures inside
//! `audit::audit_line`: that function already holds a dozen of them, and each
//! one is a statement, a local and a call it has no room for.

use sha2::{Digest, Sha256};

use crate::protocol::Method;

pub(super) fn admin_audit_line(method: &Method) -> Option<String> {
    match method {
        // ── W1c: close the 9-method audit/CDC-visibility gap. These durable
        // admin/ledger methods previously fell through to `_ => return None`
        // (never chained into the tamper-evident audit log) despite being
        // GraphRedb-durable and GATEWAY_ROUTED. Each line below is a canonical,
        // deterministic "who/what" summary (the chain's `graph`+`seq` already
        // bind the "who" via the durable-commit call site; `redb_store`'s
        // `(graph, seq)` key plus the chain hash supply the "when"/ordering). ──
        Method::FromMsgpack { msgpack } => Some(format!(
            "FROM_MSGPACK|sha256:{}",
            hex::encode(Sha256::digest(msgpack))
        )),
        Method::Reconcile {
            graph_name,
            msgpack,
        } => Some(format!(
            "RECONCILE|{graph_name}|sha256:{}",
            hex::encode(Sha256::digest(msgpack))
        )),
        Method::ApplyMultisigMutation {
            signatures,
            threshold,
            mutation_type,
            query,
        } => Some(format!(
            "APPLY_MULTISIG_MUTATION|{mutation_type}|threshold={threshold}|signers={}|sha256:{}",
            signatures.len(),
            hex::encode(Sha256::digest(query.as_bytes()))
        )),
        // W2.5 fleet server registry: this variant self-translates into `Method::AddNode`
        // in `dispatch.rs` BEFORE ever reaching a durable commit (mirroring
        // `ApplyMultisigMutation` above, which translates into `ApplyMutation`), so the
        // REAL audit line durable-committed for a registration is `ADD_NODE|srv:<name>`
        // (AddNode's own arm above). This arm is defense-in-depth only, matching
        // `ApplyMultisigMutation`'s precedent.
        Method::RegisterServer { name, .. } => Some(format!("REGISTER_SERVER|srv:{name}")),
        #[cfg(feature = "shacl")]
        Method::IcvConfigure { graph, mode, .. } => Some(format!(
            "ICV_CONFIGURE|{}|{mode}",
            graph.as_deref().unwrap_or("<default>")
        )),
        _ => None,
    }
}

// X9. The line names WHAT was done and to WHICH keyed source, and nothing
// else: the documents themselves are graph content, and an audit line is
// not a place to copy them.
pub(super) fn graph_schema_audit_line(method: &Method) -> Option<String> {
    match method {
        #[cfg(feature = "shacl")]
        Method::GraphSchema { op } => Some(format!(
            "GRAPH_SCHEMA|{}|{}",
            op.action_token(),
            op.source_token()
        )),
        _ => None,
    }
}

pub(super) fn graph_crud_audit_line(method: &Method) -> Option<String> {
    match method {
        // ── Core node/edge CRUD (audited since EG-P0-2) ──────────────────────
        Method::AddNode { node_id, .. } => Some(format!("ADD_NODE|{node_id}")),
        Method::CreateNodeIfAbsent { node_id, .. } => {
            Some(format!("CREATE_NODE_IF_ABSENT|{node_id}"))
        }
        Method::RemoveNode { node_id } => Some(format!("REMOVE_NODE|{node_id}")),
        Method::CompareAndSetNodeFields { node_id, .. } => Some(format!("CAS_NODE|{node_id}")),
        Method::AddEdge {
            source_id,
            target_id,
            ..
        } => Some(format!("ADD_EDGE|{source_id}|{target_id}")),
        Method::RemoveEdge {
            source_id,
            target_id,
        } => Some(format!("REMOVE_EDGE|{source_id}|{target_id}")),
        Method::BatchUpdate { .. } => Some("BATCH_UPDATE".to_string()),
        Method::ClearGraph => Some("CLEAR_GRAPH".to_string()),
        _ => None,
    }
}

pub(super) fn change_envelope_audit_line(method: &Method) -> Option<String> {
    match method {
        Method::ApplyChangeEnvelope { envelope } => Some(format!(
            "APPLY_CHANGE_ENVELOPE|{}|{}|{}",
            envelope.envelope_id, envelope.mutation.batch_id, envelope.content_version.digest
        )),
        // The batch coordinator's per-envelope rows are audited individually inside the
        // shared transaction (one `audit_line` per envelope operation); this method-level
        // line keeps policy `audited: true` consistent for the coordinator itself.
        Method::ApplyChangeEnvelopes { envelopes } => {
            Some(format!("APPLY_CHANGE_ENVELOPES|{}", envelopes.len()))
        }
        _ => None,
    }
}

#[cfg(feature = "modality-serving")]
fn modality_operation_name(
    op: &eg_types::ServedModalityOp,
) -> Option<(&'static str, eg_types::ServedModalityKind)> {
    match op {
        eg_types::ServedModalityOp::Ingest { modality, .. } => Some(("INGEST", *modality)),
        eg_types::ServedModalityOp::IngestStream { modality, .. } => {
            Some(("INGEST_STREAM", *modality))
        }
        eg_types::ServedModalityOp::Delete { modality, .. } => Some(("DELETE", *modality)),
        eg_types::ServedModalityOp::MoveToCold { modality, .. } => {
            Some(("MOVE_TO_COLD", *modality))
        }
        eg_types::ServedModalityOp::Restore { modality, .. } => Some(("RESTORE", *modality)),
        eg_types::ServedModalityOp::CollectTombstones { modality, .. } => {
            Some(("COLLECT_TOMBSTONES", *modality))
        }
        _ => None,
    }
}

#[cfg(feature = "modality-serving")]
fn modality_kind_name(modality: eg_types::ServedModalityKind) -> &'static str {
    match modality {
        eg_types::ServedModalityKind::Document => "DOCUMENT",
        eg_types::ServedModalityKind::Image => "IMAGE",
        eg_types::ServedModalityKind::Audio => "AUDIO",
        eg_types::ServedModalityKind::Video => "VIDEO",
    }
}

#[cfg(feature = "modality-serving")]
pub(super) fn modality_audit_line(method: &Method) -> Option<String> {
    let eg_types::protocol::Method::ServedModality { op } = method else {
        return None;
    };
    let (operation, modality) = modality_operation_name(op)?;
    Some(format!(
        "SERVED_MODALITY|{}|{operation}",
        modality_kind_name(modality)
    ))
}

#[cfg(not(feature = "modality-serving"))]
pub(super) fn modality_audit_line(_method: &Method) -> Option<String> {
    None
}

fn is_authoritative_state_receipt(event_type: &str, query: &str) -> bool {
    event_type == "authoritative_state_operation"
        && query.len() == 71
        && query.starts_with("sha256:")
        && query[7..].bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(super) fn mutation_audit_line(method: &Method) -> Option<String> {
    match method {
        Method::ApplyMutation { event_type, query }
            if is_authoritative_state_receipt(event_type, query) =>
        {
            // State-backed mutations persist a complete, digest-verified graph
            // image in the same transaction. Their canonical operation is opaque
            // by design, so the audit line binds only its SHA-256 receipt.
            Some(format!("AUTHORITATIVE_STATE_MUTATION|{query}"))
        }
        // Fallback for a caller-supplied `ApplyMutation` that is NOT the opaque
        // digest receipt above (e.g. a direct SPARQL UPDATE `event_type`/`query`
        // pair) -- W1c: this durable admin/ledger method previously fell through
        // to `_ => return None`; it is now audited like every other durable
        // mutation. The query text itself is digested (not persisted verbatim)
        // for the same reason `Sql`/`CypherQuery`/`GraphQl` hash their query.
        Method::ApplyMutation { event_type, query } => Some(format!(
            "APPLY_MUTATION|{event_type}|sha256:{}",
            hex::encode(Sha256::digest(query.as_bytes()))
        )),
        _ => None,
    }
}

pub(super) fn ledger_audit_line(method: &Method) -> Option<String> {
    match method {
        #[cfg(feature = "reasoning")]
        Method::RunDatalogReasoning { .. } => Some("RUN_DATALOG_REASONING".to_string()),
        Method::ClearLedger => Some("CLEAR_LEDGER".to_string()),
        Method::ApplyLedger { transactions } => {
            Some(format!("APPLY_LEDGER|count={}", transactions.len()))
        }
        Method::CompactNodesByType {
            node_type,
            threshold,
        } => Some(format!(
            "COMPACT_NODES_BY_TYPE|{node_type}|threshold={threshold}"
        )),
        _ => None,
    }
}

pub(super) fn edge_audit_line(method: &Method) -> Option<String> {
    match method {
        // ── Remaining GraphRedb-durable node/edge/RDF primitives (EG-P0-6) ──
        Method::InvalidateEdge {
            source_id,
            target_id,
            ..
        } => Some(format!("INVALIDATE_EDGE|{source_id}|{target_id}")),
        Method::SupersedeEdge {
            source_id,
            target_id,
            ..
        } => Some(format!("SUPERSEDE_EDGE|{source_id}|{target_id}")),
        Method::ClaimNext { label, .. } => Some(format!("CLAIM_NEXT|{label}")),
        _ => None,
    }
}

pub(super) fn capacity_audit_line(method: &Method) -> Option<String> {
    match method {
        Method::ClaimWorkItem { request } => {
            Some(format!("CLAIM_WORK_ITEM|{}", request.tenant_ref))
        }
        Method::KgDelegate { request } => Some(format!(
            "KG_DELEGATE|{}|{}|{}",
            request.context.tenant_id, request.delegation_id, request.idempotency_key
        )),
        Method::SubmitWorkItem { request } => Some(format!(
            "SUBMIT_WORK_ITEM|{}|{}",
            request.context.tenant_id, request.idempotency_key
        )),
        Method::SubmitWorkItems { request } => Some(format!(
            "SUBMIT_WORK_ITEMS|{}|{}|{}",
            request.context.tenant_id,
            request.idempotency_key,
            request.requests.len()
        )),
        Method::AcquireCapacity { request } => Some(format!(
            "ACQUIRE_CAPACITY|{}|{}|{}",
            request.tenant_ref,
            request.idempotency_key,
            request.demands.len()
        )),
        Method::RenewCapacity { request } => Some(format!(
            "RENEW_CAPACITY|{}|{}",
            request.tenant_ref,
            request.leases.len()
        )),
        Method::ReleaseCapacity { request } => Some(format!(
            "RELEASE_CAPACITY|{}|{}",
            request.tenant_ref,
            request.leases.len()
        )),
        Method::ReclaimExpiredCapacity { request } => Some(format!(
            "RECLAIM_EXPIRED_CAPACITY|{}|{}",
            request.tenant_ref, request.max_count
        )),
        Method::UpdateCapacityCell { request } => Some(format!(
            "UPDATE_CAPACITY_CELL|{}|{}",
            request.cell.cell_id, request.cell.epoch
        )),
        _ => None,
    }
}

pub(super) fn work_item_audit_line(method: &Method) -> Option<String> {
    match method {
        Method::RenewWorkItemLease {
            tenant,
            work_item_id,
            lease_epoch,
            ..
        } => Some(format!(
            "RENEW_WORK_ITEM|{tenant}|{work_item_id}|{lease_epoch}"
        )),
        Method::CommitWorkItemResult {
            tenant,
            work_item_id,
            lease_epoch,
            outcome,
            ..
        } => Some(format!(
            "COMMIT_WORK_ITEM|{tenant}|{work_item_id}|{lease_epoch}|{outcome}"
        )),
        Method::CancelWorkItem {
            tenant,
            work_item_id,
            ..
        } => Some(format!("CANCEL_WORK_ITEM|{tenant}|{work_item_id}")),
        Method::DeferWorkItem {
            tenant,
            work_item_id,
            lease_epoch,
            next_retry_at_ms,
            ..
        } => Some(format!(
            "DEFER_WORK_ITEM|{tenant}|{work_item_id}|{lease_epoch}|{next_retry_at_ms}"
        )),
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
            Some(format!(
                "CAS_WORK_ITEM_METADATA|{}|{}|{field}",
                request.tenant_ref, request.work_item_id
            ))
        }
        _ => None,
    }
}

pub(super) fn resource_audit_line(method: &Method) -> Option<String> {
    match method {
        Method::ReserveWorkItemResources { request } => Some(format!(
            "RESERVE_WORK_ITEM_RESOURCES|{}|{}|{}",
            request.tenant_ref, request.work_item_id, request.attempt
        )),
        Method::ReleaseWorkItemResources { request } => Some(format!(
            "RELEASE_WORK_ITEM_RESOURCES|{}|{}|{}",
            request.tenant_ref, request.work_item_id, request.attempt
        )),
        Method::ReclaimWorkItemResources { request } => Some(format!(
            "RECLAIM_WORK_ITEM_RESOURCES|{}|{}|{}",
            request.tenant_ref, request.work_item_id, request.attempt
        )),
        Method::UpdateResourceHost { request } => Some(format!(
            "UPDATE_RESOURCE_HOST|{}|{}|{}",
            request.tenant_ref, request.host_ref, request.revision
        )),
        _ => None,
    }
}

pub(super) fn query_audit_line(method: &Method) -> Option<String> {
    match method {
        Method::Sql { query, .. } => Some(format!(
            "SQL_MUTATION|sha256:{}",
            hex::encode(Sha256::digest(query.as_bytes()))
        )),
        Method::CypherQuery { query, .. } => Some(format!(
            "CYPHER_MUTATION|sha256:{}",
            hex::encode(Sha256::digest(query.as_bytes()))
        )),
        #[cfg(feature = "graphql")]
        Method::GraphQl { query, .. } => Some(format!(
            "GRAPHQL_MUTATION|sha256:{}",
            hex::encode(Sha256::digest(query.as_bytes()))
        )),
        Method::AddEmbedding { node_id, .. } => Some(format!("ADD_EMBEDDING|{node_id}")),
        #[cfg(feature = "rdf")]
        Method::AddTriples { .. } => Some("ADD_TRIPLES".to_string()),
        #[cfg(feature = "rdf")]
        Method::RemoveTriples { .. } => Some("REMOVE_TRIPLES".to_string()),
        #[cfg(feature = "rdf")]
        Method::DropNamedGraph => Some("DROP_NAMED_GRAPH".to_string()),
        _ => None,
    }
}

pub(super) fn memory_audit_line(method: &Method) -> Option<String> {
    match method {
        // ── Agent-memory / scene-graph / trajectory mutations (CONCEPT:EG-KG.memory.eg-batch-decay-caller) ──
        Method::CreateSummaryNode { .. } => Some("CREATE_SUMMARY_NODE".to_string()),
        Method::Consolidate { .. } => Some("CONSOLIDATE".to_string()),
        Method::Reinforce { node_id, .. } => Some(format!("REINFORCE|{node_id}")),
        Method::DecayNode { node_id, .. } => Some(format!("DECAY_NODE|{node_id}")),
        Method::DecayMemories { .. } => Some("DECAY_MEMORIES".to_string()),
        Method::EvictBelow { .. } => Some("EVICT_BELOW".to_string()),
        Method::Maintain { .. } => Some("MAINTAIN".to_string()),
        _ => None,
    }
}

pub(super) fn scene_audit_line(method: &Method) -> Option<String> {
    match method {
        Method::AddSceneObject { .. } => Some("ADD_SCENE_OBJECT".to_string()),
        Method::SetPose { node_id, .. } => Some(format!("SET_POSE|{node_id}")),
        Method::Reparent { node_id, .. } => Some(format!("REPARENT|{node_id}")),
        Method::StartTrajectory { .. } => Some("START_TRAJECTORY".to_string()),
        Method::AppendStep { traj_id, .. } => Some(format!("APPEND_STEP|{traj_id}")),
        _ => None,
    }
}

pub(super) fn mining_audit_line(method: &Method) -> Option<String> {
    match method {
        // ── Data-mining / graph-learning writeback (CONCEPT:EG-KG.mining.*) ──────────────
        // Durability is `writeback`-conditional; `wal.rs::is_durable_mutation` already
        // gates on the exact condition, so this arm only ever fires when the call
        // actually reached the durable-commit path — no extra guard needed here.
        #[cfg(feature = "mining")]
        Method::MineAssociate { .. } => Some("MINE_ASSOCIATE".to_string()),
        #[cfg(feature = "mining")]
        Method::MineCluster { .. } => Some("MINE_CLUSTER".to_string()),
        #[cfg(feature = "mining")]
        Method::MineAnomaly { .. } => Some("MINE_ANOMALY".to_string()),
        #[cfg(feature = "mining")]
        Method::MineClassifyPredict { .. } => Some("MINE_CLASSIFY_PREDICT".to_string()),
        #[cfg(feature = "mining")]
        Method::MineReduce { .. } => Some("MINE_REDUCE".to_string()),
        #[cfg(feature = "mining")]
        Method::MineSequence { .. } => Some("MINE_SEQUENCE".to_string()),
        #[cfg(feature = "mining")]
        Method::MineForecast { .. } => Some("MINE_FORECAST".to_string()),
        #[cfg(feature = "mining")]
        Method::MineText { .. } => Some("MINE_TEXT".to_string()),
        _ => None,
    }
}

pub(super) fn mining_extended_audit_line(method: &Method) -> Option<String> {
    match method {
        #[cfg(feature = "mining")]
        Method::MineSubgraph { .. } => Some("MINE_SUBGRAPH".to_string()),
        #[cfg(feature = "mining")]
        Method::MineEntityResolve { .. } => Some("MINE_ENTITY_RESOLVE".to_string()),
        #[cfg(feature = "mining")]
        Method::MineCausalImpact { .. } => Some("MINE_CAUSAL_IMPACT".to_string()),
        #[cfg(feature = "mining")]
        Method::MineProcess { .. } => Some("MINE_PROCESS".to_string()),
        #[cfg(feature = "mining")]
        Method::MineRootCause { .. } => Some("MINE_ROOT_CAUSE".to_string()),
        #[cfg(feature = "mining")]
        Method::MineRiskPropagation { .. } => Some("MINE_RISK_PROPAGATION".to_string()),
        #[cfg(feature = "mining")]
        Method::MineOntologyGap { .. } => Some("MINE_ONTOLOGY_GAP".to_string()),
        #[cfg(feature = "mining")]
        Method::MineRetrievalQuality { .. } => Some("MINE_RETRIEVAL_QUALITY".to_string()),
        _ => None,
    }
}

pub(super) fn mining_tail_audit_line(method: &Method) -> Option<String> {
    match method {
        #[cfg(feature = "mining")]
        Method::MineCommunity { .. } => Some("MINE_COMMUNITY".to_string()),
        _ => None,
    }
}

pub(super) fn graph_learning_audit_line(method: &Method) -> Option<String> {
    match method {
        #[cfg(feature = "graphlearn")]
        Method::GraphLearnFit { .. } => Some("GRAPH_LEARN_FIT".to_string()),
        #[cfg(feature = "graphlearn")]
        Method::GraphLearnPredict { .. } => Some("GRAPH_LEARN_PREDICT".to_string()),
        _ => None,
    }
}

pub(super) fn ml_pipeline_audit_line(method: &Method) -> Option<String> {
    match method {
        // ML pipeline (CONCEPT:EG-KG.mining.ml-pipeline): same durable-writeback shape as
        // the Mine*/GraphLearn* family above (`access.rs::requires_write` only reaches the
        // durable-commit path when it actually mutates), so it gets the same audit
        // coverage every sibling in this family already has (GOC-40, eg-capabilities'
        // `audited_matches_audit_rs_exactly` cross-check caught the omission).
        #[cfg(feature = "ml-pipeline")]
        Method::MiningPipelineTrain { .. } => Some("MINING_PIPELINE_TRAIN".to_string()),
        #[cfg(feature = "ml-pipeline")]
        Method::MiningPipelineServe { .. } => Some("MINING_PIPELINE_SERVE".to_string()),
        #[cfg(feature = "ml-pipeline")]
        Method::MiningPipelinePredict { .. } => Some("MINING_PIPELINE_PREDICT".to_string()),
        _ => None,
    }
}

pub(super) fn broker_setup_audit_line(method: &Method) -> Option<String> {
    match method {
        // ── Message-broker / stream mutations, Outbox domain (CONCEPT:EG-KG.compute.message-broker-exchanges /
        // replayable-append-log / publisher-confirms-consumer-qos) ──────────────────────
        // NOT NODES/EDGES rows (`redb_store::apply_method_rows` is a no-op for
        // them — the control-graph state lives on the in-memory `GraphCore`,
        // replayed via `wal.rs::apply` on restart) but they DO flow through the
        // SAME `record`/`record_durable` → `commit_ops`/`commit_crossmodal` →
        // `append_audit_entry` call as every other durable mutation, so they
        // chain into the SAME per-graph tamper-evident audit log.
        #[cfg(feature = "broker")]
        Method::DeclareExchange { exchange, .. } => Some(format!("DECLARE_EXCHANGE|{exchange}")),
        #[cfg(feature = "broker")]
        Method::DeleteExchange { exchange } => Some(format!("DELETE_EXCHANGE|{exchange}")),
        #[cfg(feature = "broker")]
        Method::BindQueue {
            exchange, queue, ..
        } => Some(format!("BIND_QUEUE|{exchange}|{queue}")),
        #[cfg(feature = "broker")]
        Method::UnbindQueue {
            exchange, queue, ..
        } => Some(format!("UNBIND_QUEUE|{exchange}|{queue}")),
        #[cfg(feature = "broker")]
        Method::Publish {
            exchange,
            routing_key,
            ..
        } => Some(format!("PUBLISH|{exchange}|{routing_key}")),
        #[cfg(feature = "broker")]
        Method::DeclareQueue { queue, .. } => Some(format!("DECLARE_QUEUE|{queue}")),
        #[cfg(feature = "broker")]
        Method::PublishEx {
            exchange,
            routing_key,
            ..
        } => Some(format!("PUBLISH_EX|{exchange}|{routing_key}")),
        _ => None,
    }
}

pub(super) fn broker_consume_audit_line(method: &Method) -> Option<String> {
    match method {
        #[cfg(feature = "broker")]
        Method::BrokerConsume { queue, .. } => Some(format!("BROKER_CONSUME|{queue}")),
        #[cfg(feature = "broker")]
        Method::BrokerAck { queue, node_id } => Some(format!("BROKER_ACK|{queue}|{node_id}")),
        #[cfg(feature = "broker")]
        Method::BrokerReject { queue, node_id, .. } => {
            Some(format!("BROKER_REJECT|{queue}|{node_id}"))
        }
        #[cfg(feature = "broker")]
        Method::SweepExpired { .. } => Some("SWEEP_EXPIRED".to_string()),
        #[cfg(feature = "broker")]
        Method::StreamDeclare { stream, .. } => Some(format!("STREAM_DECLARE|{stream}")),
        #[cfg(feature = "broker")]
        Method::StreamPublish { stream, .. } => Some(format!("STREAM_PUBLISH|{stream}")),
        #[cfg(feature = "broker")]
        Method::StreamTrim { stream, .. } => Some(format!("STREAM_TRIM|{stream}")),
        _ => None,
    }
}

pub(super) fn broker_misc_audit_line(method: &Method) -> Option<String> {
    match method {
        #[cfg(feature = "broker")]
        Method::StreamCommitOffset { stream, group, .. } => {
            Some(format!("STREAM_COMMIT_OFFSET|{stream}|{group}"))
        }
        #[cfg(feature = "broker")]
        Method::PublishConfirmed {
            exchange,
            routing_key,
            ..
        } => Some(format!("PUBLISH_CONFIRMED|{exchange}|{routing_key}")),
        #[cfg(feature = "broker")]
        Method::PublishIdempotent {
            exchange,
            routing_key,
            ..
        } => Some(format!("PUBLISH_IDEMPOTENT|{exchange}|{routing_key}")),
        #[cfg(feature = "broker")]
        Method::BrokerAckTag { delivery_tag, .. } => Some(format!("BROKER_ACK_TAG|{delivery_tag}")),
        #[cfg(feature = "broker")]
        Method::BrokerNackTag { delivery_tag, .. } => {
            Some(format!("BROKER_NACK_TAG|{delivery_tag}"))
        }
        #[cfg(feature = "broker")]
        Method::BrokerRenewTag { delivery_tag, .. } => {
            Some(format!("BROKER_RENEW_TAG|{delivery_tag}"))
        }
        _ => None,
    }
}
