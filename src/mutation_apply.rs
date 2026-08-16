//! Canonical durable mutation classification and application.
//!
//! Socket, embedded, and Raft execution share this deterministic implementation,
//! so a committed Method produces the same graph state on every path.

use crate::{graph::GraphCore, protocol::Method};

const MAX_DURABLE_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
const MAX_DURABLE_PAYLOAD_ITEMS: usize = 1_000_000;

fn decode_durable_payload<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Option<T> {
    eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_DURABLE_PAYLOAD_BYTES,
            MAX_DURABLE_PAYLOAD_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .ok()
}

/// True for the methods whose effect must survive a crash in the authoritative store.
pub fn is_durable_mutation(m: &Method) -> bool {
    // Served modality mutations use the stronger state-backed MutationBatch path;
    // raw source bytes are replaced by the state-backed receipt before this
    // classifier is consulted. It still reports the durable effect for policy and
    // placement accounting.
    #[cfg(feature = "modality-serving")]
    if let Method::ServedModality { op } = m {
        return op.mutates();
    }
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
    // Message-broker admin + publish (CONCEPT:EG-KG.compute.message-broker-exchanges): each mutates control-graph
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
            // Broker policy extensions (CONCEPT:EG-KG.compute.dead-letter-queues..280): all mutate control-graph
            // nodes deterministically from explicit args (caller `now_ms`), so replay
            // reproduces identical state (routing + monotonic seq derive from graph).
            | Method::DeclareQueue { .. }
            | Method::PublishEx { .. }
            | Method::BrokerConsume { .. }
            | Method::BrokerAck { .. }
            | Method::BrokerReject { .. }
            | Method::SweepExpired { .. }
            // Streams (CONCEPT:EG-KG.compute.replayable-append-log) + publisher-confirm / consumer-ack (CONCEPT:
            // EG-284): the mutating variants (append/trim/commit/confirm/ack-tag/
            // nack-tag/renew-tag) write control-graph nodes deterministically from explicit args
            // (caller `now_ms` + durable counters), so replay reproduces them. Pure
            // reads (`StreamRead`/`StreamCommittedOffset`) are NOT logged.
            | Method::StreamDeclare { .. }
            | Method::StreamPublish { .. }
            | Method::StreamTrim { .. }
            | Method::StreamCommitOffset { .. }
            | Method::PublishConfirmed { .. }
            | Method::BrokerAckTag { .. }
            | Method::BrokerNackTag { .. }
            | Method::BrokerRenewTag { .. }
            // Idempotent producer (CONCEPT:EG-KG.ingest.broker-reject-publish): the dedup check + high-water-mark
            // bump mutate a durable producer node deterministically from explicit args,
            // so replay reproduces the identical mark + duplicate-verdict.
            | Method::PublishIdempotent { .. }
    ) {
        return true;
    }
    // Association-rule mining write-back (CONCEPT:EG-KG.mining.frequent-itemset-mining):
    // durable only when `writeback` materializes `:AssociationRule` nodes. `apply`
    // below re-mines + re-writes deterministically (explicit transactions reproduce
    // byte-identically; a graph-derived source re-derives from the graph, like the
    // broker/memory replay ops). A pure query (writeback=false) is not logged.
    //
    // `MineSequence`/`MineForecast` follow the SAME contract (`:SequentialPattern`/
    // `:Forecast` nodes) and MUST mirror `access::requires_write`'s classification —
    // they were previously missing here, so an acknowledged write was silently
    // dropped on crash and their `mining::replay` arms were dead code (EG-P0-3).
    #[cfg(feature = "mining")]
    if matches!(
        m,
        Method::MineAssociate {
            writeback: true,
            ..
        } | Method::MineCluster {
            writeback: true,
            ..
        } | Method::MineAnomaly {
            writeback: true,
            ..
        } | Method::MineClassifyPredict {
            writeback: true,
            ..
        } | Method::MineReduce {
            writeback: true,
            ..
        } | Method::MineSequence {
            writeback: true,
            ..
        } | Method::MineForecast {
            writeback: true,
            ..
        }
    ) {
        return true;
    }
    // `MineText`: durable only for `lda`/`nmf` writeback (their `:Topic` nodes) —
    // `tfidf` never mutates regardless of the flag, matching
    // `access::requires_write`'s exact condition byte-for-byte (EG-P0-3).
    #[cfg(feature = "mining")]
    if let Method::MineText {
        writeback,
        algorithm,
        ..
    } = m
    {
        return *writeback && !matches!(algorithm, crate::protocol::TextAlgorithm::Tfidf);
    }
    // `MineSubgraph`: durable only for `gspan` writeback (its `:FrequentSubgraph`
    // nodes) — `motif` never mutates regardless of the flag, matching
    // `access::requires_write`'s exact condition byte-for-byte (EG-P0-3).
    #[cfg(feature = "mining")]
    if let Method::MineSubgraph {
        writeback,
        algorithm,
        ..
    } = m
    {
        return *writeback && !matches!(algorithm, crate::protocol::SubgraphAlgorithm::Motif);
    }
    // Residual insight/mining families (CONCEPT:EG-KG.mining.entity-resolution /
    // causal-impact / process-mining / root-cause / risk-propagation /
    // ontology-gap / retrieval-quality / community-writeback): durable only when
    // `writeback` materializes their typed nodes; `apply` re-mines + re-writes
    // deterministically, same contract as the mining family above.
    #[cfg(feature = "mining")]
    if matches!(
        m,
        Method::MineEntityResolve {
            writeback: true,
            ..
        } | Method::MineCausalImpact {
            writeback: true,
            ..
        } | Method::MineProcess {
            writeback: true,
            ..
        } | Method::MineRootCause {
            writeback: true,
            ..
        } | Method::MineRiskPropagation {
            writeback: true,
            ..
        } | Method::MineOntologyGap {
            writeback: true,
            ..
        } | Method::MineRetrievalQuality {
            writeback: true,
            ..
        } | Method::MineCommunity {
            writeback: true,
            ..
        }
    ) {
        return true;
    }
    // Graph-learning write-back (CONCEPT:EG-KG.graphlearn.link-predictor): durable only
    // when `writeback` materializes `:EdgeFunction` / `:PredictedEdge` nodes. `apply`
    // re-derives + re-writes deterministically (seeded). A pure fit/predict is not logged.
    #[cfg(feature = "graphlearn")]
    if matches!(
        m,
        Method::GraphLearnFit {
            writeback: true,
            ..
        } | Method::GraphLearnPredict {
            writeback: true,
            ..
        }
    ) {
        return true;
    }
    matches!(
        m,
        Method::AddNode { .. }
            | Method::CreateNodeIfAbsent { .. }
            | Method::RemoveNode { .. }
            | Method::CompareAndSetNodeFields { .. }
            | Method::AddEdge { .. }
            | Method::RemoveEdge { .. }
            | Method::InvalidateEdge { .. }
            | Method::SupersedeEdge { .. }
            | Method::BatchUpdate { .. }
            | Method::ClaimNext { .. }
            | Method::ClaimWorkItem { .. }
            | Method::RenewWorkItemLease { .. }
            | Method::CommitWorkItemResult { .. }
            | Method::CancelWorkItem { .. }
            | Method::DeferWorkItem { .. }
            | Method::CasWorkItemMetadata { .. }
            | Method::ClearGraph
            // Embedding write (CONCEPT:EG-KG.compute.semantic-search): mutates the
            // per-graph `semantic_store` (HNSW index) — classified a write by
            // `access::requires_write` but previously missing here (EG-P0-3), so an
            // acknowledged embedding write was lost on crash. `apply` below re-runs
            // the SAME deterministic upsert over the same pre-image.
            | Method::AddEmbedding { .. }
            // Agent-memory / scene-graph / trajectory mutations (CONCEPT:EG-KG.memory.eg-batch-decay-caller):
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
/// Apply one durable mutation to a graph core. Mirrors the dispatch mutation
/// handlers for exactly the `is_durable_mutation` set.
///
/// This is the single canonical "durable Method → GraphCore mutation" path: durable log
/// replay (below) and the Raft state machine (CONCEPT:AU-KG.ingest.source-sync-canonical, `src/raft`) both
/// call it, so a committed Raft log entry applies BYTE-IDENTICALLY to how a
/// replayed durable mutation does. Deterministic (replaying the same Method over the
/// same pre-image yields the same state), which is the Raft state-machine contract.
pub fn apply(core: &GraphCore, m: &Method) {
    match m {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => core.add_node(node_id.clone(), properties_msgpack.clone()),
        Method::CreateNodeIfAbsent {
            node_id,
            properties_msgpack,
        } => {
            let _ = core.create_node_if_absent(node_id.clone(), properties_msgpack.clone());
        }
        Method::RemoveNode { node_id } => core.remove_node(node_id.clone()),
        Method::CompareAndSetNodeFields {
            node_id,
            conditions_msgpack,
            updates_msgpack,
        } => {
            // Deterministic replay: decode the blobs and re-run the CAS. Replaying
            // over the same pre-image yields the same outcome; the bool is ignored.
            if let (Some(conditions), Some(updates)) = (
                decode_durable_payload::<serde_json::Map<String, serde_json::Value>>(
                    conditions_msgpack,
                ),
                decode_durable_payload::<serde_json::Map<String, serde_json::Value>>(
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
            // Deterministic replay (CONCEPT:EG-KG.compute.atomically-claim-oldest-pending): re-run the same oldest-pending
            // pick + CAS over identical state. `updates` carries no clock, so the
            // claimed node + merged marker are reproduced byte-identically; the
            // returned node id is ignored on replay.
            if let Some(updates) = decode_durable_payload::<
                serde_json::Map<String, serde_json::Value>,
            >(updates_msgpack)
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
        // Deterministic replay: the embedding upsert has no clock/randomness, so
        // re-running it over the same pre-image reproduces the identical vector.
        // `apply` is void-returning (a best-effort replay applier, matching the
        // `BatchUpdate` arm above) so a rejection can't propagate — but unlike that
        // pre-existing `let _ =`, this is logged: a mismatch here means a
        // pre-validated durable write is being replayed with the WRONG dimension,
        // which should never happen and is worth surfacing (CONCEPT:EG-KG.compute.rank-dim-mismatch-guard,
        // BUG-007) rather than silently discarding it.
        Method::AddEmbedding { node_id, embedding } => {
            if let Err(error) = core
                .semantic_store
                .write()
                .add_embedding(node_id.clone(), embedding.clone())
            {
                tracing::warn!(
                    node_id = node_id.as_str(),
                    %error,
                    "AddEmbedding replay rejected — the durable pre-image had an unexpected dimension"
                );
            }
        }
        // `AddTriples` (feature `rdf`): deterministic replay = re-parse the SAME
        // source text and re-apply the complete property-graph projection. The
        // multi-valued literal extras are embedded in the node blob, so this one
        // replay reconstructs the lossless dataset without a second authority.
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
                // Rebuild the canonical property-graph RDF projection.
                let _ = eg_rdf::mapping::load_triples(core, &mut iris, "", triples);
            }
        }
        // `RemoveTriples` (feature `rdf`, CONCEPT:EG-KG.query.named-graph-support): deterministic replay =
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
        // `DropNamedGraph` (feature `rdf`, CONCEPT:EG-KG.query.named-graph-support): the named graph IS this
        // registry graph, so dropping it clears the whole core. The quad-store rows are
        // dropped durably on their own redb file at original execution, so replay only
        // needs to rebuild the empty in-graph state.
        #[cfg(feature = "rdf")]
        Method::DropNamedGraph => core.clear(),
        // Message-broker replay (CONCEPT:EG-KG.compute.message-broker-exchanges): re-run the SAME broker fn. Routing
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
        // Broker policy extensions (CONCEPT:EG-KG.compute.dead-letter-queues..280): re-run the SAME broker fn
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
        // Streams (CONCEPT:EG-KG.compute.replayable-append-log) + confirms/acks (CONCEPT:EG-KG.compute.publisher-confirms-consumer-qos): re-run the SAME
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
        // Idempotent producer (CONCEPT:EG-KG.ingest.broker-reject-publish): re-run the SAME publish with the SAME
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
        Method::BrokerAckTag {
            delivery_tag,
            consumer,
        } => {
            let _ = crate::broker::broker_ack_tag(core, *delivery_tag, consumer);
        }
        #[cfg(feature = "broker")]
        Method::BrokerNackTag {
            delivery_tag,
            consumer,
            requeue,
            now_ms,
        } => {
            let _ =
                crate::broker::broker_nack_tag(core, *delivery_tag, consumer, *requeue, *now_ms);
        }
        #[cfg(feature = "broker")]
        Method::BrokerRenewTag {
            delivery_tag,
            consumer,
            now_ms,
            lease_ms,
        } => {
            let _ =
                crate::broker::broker_renew_tag(core, *delivery_tag, consumer, *now_ms, *lease_ms);
        }
        // Agent-memory / scene-graph / trajectory replay (CONCEPT:EG-KG.memory.eg-batch-decay-caller): re-run the
        // SAME eg-core primitive with the SAME explicit args over the same pre-image.
        // Every generated id derives deterministically (sorted inputs / monotonic
        // node-count / step ordinal) and the only clock is the logged `now_ms`, so the
        // node/edge state is reproduced byte-identically. Results are ignored on replay.
        Method::CreateSummaryNode {
            level,
            child_ids,
            props_msgpack,
        } => {
            if let Some(props) = durable_json_object(props_msgpack) {
                let _ = core.create_summary_node(*level, child_ids, props);
            }
        }
        Method::Consolidate {
            episodic_ids,
            semantic_props_msgpack,
        } => {
            if let Some(props) = durable_json_object(semantic_props_msgpack) {
                let _ = core.consolidate(episodic_ids, props);
            }
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
            if let Some(pose) = durable_pose(pose_msgpack) {
                let _ = core.add_scene_object(&pose, parent.as_deref());
            }
        }
        Method::SetPose {
            node_id,
            pose_msgpack,
        } => {
            if let Some(pose) = durable_pose(pose_msgpack) {
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
            if let Some(props) = durable_json_object(props_msgpack) {
                let _ = core.start_trajectory(props);
            }
        }
        Method::AppendStep {
            traj_id,
            action_msgpack,
            reward,
            state_ref,
            next_state_ref,
            t,
        } => {
            if let Some(action) = decode_durable_payload::<serde_json::Value>(action_msgpack) {
                let _ = core.append_step(
                    traj_id,
                    action,
                    *reward,
                    state_ref.as_deref(),
                    next_state_ref.as_deref(),
                    *t,
                );
            }
        }
        // Data-mining write-back (CONCEPT:EG-KG.mining.frequent-itemset-mining /
        // dbscan-density / isolation-forest): re-run the mining op + re-materialize
        // the `:AssociationRule` / `:Cluster` / `:Anomaly` nodes. The node ids are a
        // deterministic digest of the mined content, so replay is idempotent.
        #[cfg(all(feature = "mining", feature = "server"))]
        Method::MineAssociate { .. }
        | Method::MineCluster { .. }
        | Method::MineAnomaly { .. }
        | Method::MineClassifyPredict { .. }
        | Method::MineReduce { .. }
        | Method::MineSequence { .. }
        | Method::MineForecast { .. }
        | Method::MineText { .. }
        | Method::MineSubgraph { .. }
        | Method::MineEntityResolve { .. }
        | Method::MineCausalImpact { .. }
        | Method::MineProcess { .. }
        | Method::MineRootCause { .. }
        | Method::MineRiskPropagation { .. }
        | Method::MineOntologyGap { .. }
        | Method::MineRetrievalQuality { .. }
        | Method::MineCommunity { .. } => crate::server::handlers::mining::replay(core, m),
        // Graph-learning write-back (CONCEPT:EG-KG.graphlearn.link-predictor): re-run the
        // fit/predict + re-materialize the `:EdgeFunction` / `:PredictedEdge` nodes.
        // The node ids are a deterministic digest, so replay is idempotent.
        #[cfg(all(feature = "graphlearn", feature = "server"))]
        Method::GraphLearnFit { .. } | Method::GraphLearnPredict { .. } => {
            crate::server::handlers::graphlearn::replay(core, m)
        }
        _ => {}
    }
}

/// Decode a MessagePack-encoded JSON object blob for canonical replay (CONCEPT:EG-KG.memory.eg-batch-decay-caller). A
/// malformed or non-object bytes fail closed so corrupted durable state cannot be
/// replayed as a different mutation with silently-empty properties.
fn durable_json_object(blob: &[u8]) -> Option<serde_json::Map<String, serde_json::Value>> {
    match decode_durable_payload::<serde_json::Value>(blob) {
        Some(serde_json::Value::Object(object)) => Some(object),
        _ => None,
    }
}

/// Decode a MessagePack-encoded pose blob for canonical replay (CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-087).
/// `None` only if the blob is not a decodable JSON object — matching the dispatch
/// handler's `decode_pose` so replay reconstructs the identical scene node.
fn durable_pose(blob: &[u8]) -> Option<eg_core::scene::Pose> {
    let val = decode_durable_payload::<serde_json::Value>(blob)?;
    eg_core::scene::Pose::from_json(&val)
}
