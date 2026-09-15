macro_rules! writer_command_arms {
    (
        $cmd:expr,
        pending = $pending:ident,
        flush_threshold = $flush_threshold:ident,
        flush = $flush:ident,
        shard = $shard:ident,
        crypto = $crypto:ident $(,)?
    ) => {{
        // These aliases intentionally bridge macro hygiene: every value that the
        // generated match arms use is supplied explicitly by `handle_cmd`, while
        // the arm body keeps the concise names it had before it was split out of
        // `writer_thread.rs`.
        let pending = $pending;
        let flush_threshold = $flush_threshold;
        let flush = $flush;
        let shard = $shard;
        let crypto = $crypto;
        match $cmd {
        Cmd::Mutation {
            graph,
            method,
            done,
        } => {
            pending.ops.push((graph, *method));
            pending.waiters.push(done);
            // Bound memory: if a burst outpaces the tick, flush early. The group
            // still amortizes thousands of row writes per commit, and fires every
            // commit-before-ack waiter for the ops in this flush. The threshold is
            // hardware-auto-sized (CONCEPT:AU-KG.backend.b-auto-sizeb) — small on a Pi, large on a big box.
            if pending.ops.len() >= flush_threshold {
                flush(pending);
            }
            false
        }
        Cmd::RegisterGraph {
            graph,
            name,
            graph_type,
            done,
        } => {
            // Flush pending mutations first so a graph's rows and its meta land in a
            // consistent order, then durably write the graph_meta row.
            flush(pending);
            let res = write_graph_meta(shard, &graph, &name, graph_type);
            let _ = done.send(res);
            false
        }
        Cmd::PurgeGraph { graph, done } => {
            // Flush pending mutations first so we never purge a graph and then
            // re-apply a buffered op for it out of order, then drop ALL of its rows
            // (incl. graph_meta) in one durable transaction.
            flush(pending);
            let _ = done.send(purge_graph_rows(shard, &graph));
            false
        }
        Cmd::ReadGraphDump { graph, reply } => {
            // Flush pending so the rehydrated dump reflects the latest durable state,
            // then range-scan ONE graph's rows (CONCEPT:EG-KG.storage.100m-tenant).
            flush(pending);
            let _ = reply.send(read_graph_dump(shard, &graph, crypto));
            false
        }
        Cmd::ReadGraphDumpPage {
            graph,
            query,
            reply,
        } => {
            // Flush pending first (same consistency contract as ReadGraphDump), then
            // fetch ONE bounded page straight off the durable store (CONCEPT:EG-KG.sharding.paged-lazy-open, L38).
            flush(pending);
            let _ = reply.send(crate::redb_store::read_graph_dump_page(
                shard,
                &graph,
                crypto,
                crate::redb_store::PageCursorRef {
                    node_offset: query.node_offset,
                    edge_offset: query.edge_offset,
                    node_after: query.node_after.as_deref(),
                    edge_after: query.edge_after.as_ref().map(|(source, target, ordinal)| {
                        (source.as_str(), target.as_str(), *ordinal)
                    }),
                    page_size: query.page_size,
                },
            ));
            false
        }
        Cmd::ExportGraphRaw { graph, reply } => {
            // CONCEPT:EG-KG.backend.catalog-shard-resolve — flush pending so every committed mutation is captured, then
            // scan this graph's rows VERBATIM (raw blobs — encryption + audit chain kept).
            flush(pending);
            let _ = reply.send(super::super::online_reshard::export_graph_raw(shard, &graph));
            false
        }
        Cmd::ImportGraphRaw { graph, rows, reply } => {
            // CONCEPT:EG-KG.backend.catalog-shard-resolve — flush pending first (consistency), then land the migrated
            // rows verbatim in ONE durable commit (the move's commit-before-ack point).
            flush(pending);
            let _ = reply.send(super::super::online_reshard::import_graph_raw(
                shard, &graph, &rows,
            ));
            false
        }
        Cmd::ImportGraphDelta {
            graph,
            delta,
            reply,
        } => {
            // CONCEPT:EG-KG.backend.flush-pending-first — flush pending first (consistency), then land ONLY the delta
            // rows (upserts + removals) in ONE durable commit (the under-quiesce write).
            flush(pending);
            let _ = reply.send(super::super::online_reshard::import_graph_delta(
                shard, &graph, &delta,
            ));
            false
        }
        #[cfg(feature = "security")]
        Cmd::AuditVerify { graph, reply } => {
            // Flush pending so the chain walk includes the latest durable audit
            // entries, then verify the hash chain (CONCEPT:EG-KG.sharding.row-level-security).
            flush(pending);
            let _ = reply.send(crate::redb_store::verify_audit(shard, &graph));
            false
        }
        #[cfg(all(test, feature = "security"))]
        Cmd::TestTamperAudit { graph, seq, reply } => {
            flush(pending);
            let res = in_graph_write(shard, &graph, "test_tamper_audit", |write| {
                let mut audit = write
                    .graph(&graph)?
                    .open_scoped_table(crate::redb_store::AUDIT)?;
                let mut mutated = audit
                    .get((graph.as_str(), seq))?
                    .ok_or_else(|| "no such audit entry".to_string())?
                    .value()
                    .to_vec();
                let last = mutated
                    .len()
                    .checked_sub(1)
                    .ok_or_else(|| "audit entry is empty".to_string())?;
                mutated[last] ^= 0xFF;
                audit.insert((graph.as_str(), seq), mutated.as_slice())
            });
            let _ = reply.send(res);
            false
        }
        #[cfg(feature = "security")]
        Cmd::ProvenanceAnchorCommit {
            graph,
            root,
            members,
            reply,
        } => {
            // Flush pending first so the anchor's cache-seed (on first touch) and
            // its audit-chain append see the latest durable state, mirroring
            // AuditVerify/TestTamperAudit above.
            flush(pending);
            let res = crate::redb_store::provenance_anchor_commit(
                shard,
                &mut pending.provenance_anchor_cache,
                &mut pending.audit_tail,
                &graph,
                root,
                &members,
            );
            let _ = reply.send(res);
            false
        }
        #[cfg(feature = "security")]
        Cmd::AuditProveInclusion {
            graph,
            node_id,
            anchor_seq,
            reply,
        } => {
            flush(pending);
            let res =
                crate::redb_store::prove_inclusion(shard, &graph, &node_id, anchor_seq, crypto);
            let _ = reply.send(res);
            false
        }
        Cmd::CrossModalCommit { payload, done } => {
            let CrossModalPayload {
                graph,
                methods,
                vectors,
                blob_refs,
                measurements,
            } = *payload;
            // Flush pending first so this cross-modal txn observes the latest durable
            // state (its vector read-modify-write of the SEMANTIC blob must start from
            // the committed store), then land ALL modalities in ONE WriteTransaction.
            flush(pending);
            let op_id = shard_write_attempt_id("commit_crossmodal");
            let res = commit_crossmodal(
                shard,
                &graph,
                crate::redb_store::CrossModalStaged {
                    methods: &methods,
                    vectors: &vectors,
                    blob_refs: &blob_refs,
                    measurements: &measurements,
                },
                &op_id,
                crate::server::txn::now_ms(),
                crypto,
                // Shares the writer's persistent tail cache (CONCEPT:EG-KG.storage.embedded-store).
                #[cfg(feature = "security")]
                &mut pending.audit_tail,
            );
            let _ = done.send(res);
            false
        }
        Cmd::CrossModalBatchCommit { payload, done } => {
            let CrossModalBatchPayload {
                graph,
                batch,
                methods,
                vectors,
                blob_refs,
                measurements,
                result_msgpack,
                committed_at_ms,
            } = *payload;
            // Preserve queue order, then let one immediate transaction own every
            // modality and every universal coordinator record.
            flush(pending);
            let res = commit_mutation_batch_crossmodal(
                shard,
                crate::redb_store::CrossModalCommitInput {
                    graph_fname: &graph,
                    batch: &batch,
                    rows: crate::redb_store::CrossModalBatchRows {
                        methods: &methods,
                        vectors: &vectors,
                        blob_refs: &blob_refs,
                        measurements: &measurements,
                    },
                    result_msgpack: result_msgpack.as_deref(),
                    committed_at_ms,
                },
                crypto,
                #[cfg(feature = "security")]
                &mut pending.audit_tail,
            );
            let _ = done.send(res);
            false
        }
        Cmd::MutationBatchCommit { payload, done } => {
            let MutationBatchPayload {
                graph,
                batch,
                authoritative_state_msgpack,
                result_msgpack,
                committed_at_ms,
                audited,
            } = *payload;
            // Preserve command ordering and make the batch its own indivisible
            // commit point.  Pending best-effort/grouped writes land first; none
            // can be folded into or acknowledged as part of half this batch.
            flush(pending);
            let res = if let Some(state) = authoritative_state_msgpack.as_deref() {
                commit_mutation_batch_state(
                    shard,
                    crate::redb_store::StateCommitInput {
                        graph_fname: &graph,
                        batch: &batch,
                        authoritative_state_msgpack: state,
                        result_msgpack: result_msgpack.as_deref(),
                        committed_at_ms,
                        audited,
                    },
                    crypto,
                    #[cfg(feature = "security")]
                    &mut pending.audit_tail,
                )
            } else {
                commit_mutation_batch(
                    shard,
                    &graph,
                    &batch,
                    result_msgpack.as_deref(),
                    committed_at_ms,
                    crypto,
                    #[cfg(feature = "security")]
                    &mut pending.audit_tail,
                )
            };
            let _ = done.send(res);
            false
        }
        Cmd::MintWorkItemClaimCapability {
            graph,
            request,
            authority,
            done,
        } => {
            flush(pending);
            let res = in_graph_write(shard, &graph, "work_item_capability_mint", |write| {
                crate::redb_store::work_item_capability::mint_claim_capability(
                    write, &graph, &request, &authority, crypto,
                )
            });
            let _ = done.send(res);
            false
        }
        Cmd::VerifyWorkItemClaimCapability {
            graph,
            request,
            authority,
            done,
        } => {
            flush(pending);
            let res = in_graph_write(shard, &graph, "work_item_capability_verify", |write| {
                crate::redb_store::work_item_capability::verify_claim_capability(
                    write, &graph, &request, &authority, crypto,
                )
            });
            let _ = done.send(res);
            false
        }
        Cmd::CommitDevelopmentLane {
            graph,
            method,
            now_ms,
            done,
        } => {
            flush(pending);
            let res = crate::redb_store::development_lane::commit_development_lane(
                shard, &graph, &method, now_ms, crypto,
            );
            let _ = done.send(res);
            false
        }
        Cmd::CommitCapacityLease {
            graph,
            method,
            done,
        } => {
            flush(pending);
            let res = crate::redb_store::capacity_lease::commit(
                shard,
                &graph,
                &method,
                crypto,
                #[cfg(feature = "security")]
                &mut pending.audit_tail,
            );
            let _ = done.send(res);
            false
        }
        Cmd::ChangeEnvelopeCommit { payload, done } => {
            let ChangeEnvelopePayload {
                graph,
                envelope,
                committed_at_ms,
            } = *payload;
            // Ordering and atomicity mirror MutationBatch: pending grouped writes
            // commit first, then this envelope owns one indivisible fsync point.
            flush(pending);
            let res = commit_change_envelope(
                shard,
                &graph,
                &envelope,
                committed_at_ms,
                crypto,
                #[cfg(feature = "security")]
                &mut pending.audit_tail,
            );
            let _ = done.send(res);
            false
        }
        Cmd::ChangeEnvelopesCommit { payload, done } => {
            let ChangeEnvelopesPayload {
                graph,
                envelopes,
                committed_at_ms,
            } = *payload;
            // Same ordering/atomicity as the single envelope: flush any pending
            // grouped writes first, then this whole page owns one indivisible fsync.
            flush(pending);
            let res = commit_change_envelopes(
                shard,
                &graph,
                &envelopes,
                committed_at_ms,
                crypto,
                #[cfg(feature = "security")]
                &mut pending.audit_tail,
            )
            .map_err(|e| (e.index, e.error));
            let _ = done.send(res);
            false
        }
        Cmd::MutationOutboxSubscribe {
            graph,
            consumer,
            topic,
            done,
        } => {
            // The subscription is an immediate ledger write. Flush pending
            // graph mutations first so a subsequent claim sees one committed
            // ordering, then let the shard/kernel enforce same-topic
            // idempotence and cross-topic refusal atomically.
            flush(pending);
            let result = shard.outbox_subscribe(&graph, &consumer, &topic);
            let _ = done.send(result);
            false
        }
        Cmd::MutationOutboxClaim {
            graph,
            consumer,
            budget,
            done,
        } => {
            // A claim observes every prior batch/outbox write and installs all
            // returned leases atomically before any worker is notified. The budget is
            // the value a sweep carries between scopes. The command owns a clone
            // while the writer executes, then returns the updated state with the
            // complete claim outcome so the caller can continue the same sweep
            // without losing an explicit deferral reason.
            flush(pending);
            let mut budget = *budget;
            let result = shard
                .outbox_claim(&graph, &consumer, &mut budget)
                .map(|outcome| (outcome, budget));
            let _ = done.send(result);
            false
        }
        Cmd::MutationOutboxAck {
            graph,
            lease,
            now_ms,
            done,
        } => {
            // One call, not two: the kernel marks the lease delivered and advances
            // this consumer's projection cursor in the SAME transaction, so the
            // crash window between "delivered" and "watermark moved" that a separate
            // cursor write left open is not representable any more.
            flush(pending);
            let result = shard.outbox_ack(&graph, &lease, now_ms);
            let _ = done.send(result);
            false
        }
        Cmd::Shutdown { reply } => {
            let _ = reply.send(());
            true
        }
        Cmd::RaftLogAppend {
            group_id,
            entries,
            done,
        } => {
            // Buffer into the SAME pending batch as M2 mutations; the awaited `done`
            // makes this a commit-before-ack barrier, so the batch commits durably at
            // the next boundary (or immediately, since has_barrier() is now true) and
            // a concurrently-pending graph mutation rides the SAME fsync.
            for (idx, blob) in entries {
                pending.raft_log_ops.push((group_id, idx, blob));
            }
            pending.waiters.push(done);
            false
        }
        Cmd::RaftLogRead {
            group_id,
            lo,
            hi,
            reply,
        } => {
            flush(pending);
            let _ = reply.send(read_raft_log_range(shard, group_id, lo, hi, crypto));
            false
        }
        Cmd::RaftLogDeleteFrom {
            group_id,
            from,
            done,
        } => {
            flush(pending);
            let _ = done.send(delete_raft_log_from(shard, group_id, from));
            false
        }
        Cmd::RaftLogPurgeUpto {
            group_id,
            upto,
            done,
        } => {
            flush(pending);
            let _ = done.send(purge_raft_log_upto(shard, group_id, upto));
            false
        }
        Cmd::RaftLogBounds { group_id, reply } => {
            flush(pending);
            let _ = reply.send(raft_log_bounds(shard, group_id));
            false
        }
        Cmd::RaftMetaPut {
            group_id,
            key,
            val,
            done,
        } => {
            // Flush pending first so meta ordering is consistent with the log, then
            // durably write the meta row in its own transaction.
            flush(pending);
            let _ = done.send(put_raft_meta(shard, group_id, &key, &val));
            false
        }
        Cmd::RaftMetaGet {
            group_id,
            key,
            reply,
        } => {
            flush(pending);
            let _ = reply.send(get_raft_meta(shard, group_id, &key));
            false
        }
        Cmd::XshardPreparePut {
            txn_id,
            group_id,
            slice,
            done,
        } => {
            flush(pending);
            let _ = done.send(put_xshard_prepare(shard, &txn_id, group_id, &slice, crypto));
            false
        }
        Cmd::XshardPrepareGet {
            txn_id,
            group_id,
            reply,
        } => {
            flush(pending);
            let _ = reply.send(get_xshard_prepare(shard, &txn_id, group_id, crypto));
            false
        }
        Cmd::XshardDecisionPut {
            txn_id,
            commit,
            retain_for_parent,
            done,
        } => {
            flush(pending);
            let _ = done.send(put_xshard_decision(
                shard,
                &txn_id,
                commit,
                retain_for_parent,
            ));
            false
        }
        Cmd::XshardRecoverablePendingPut { txn_id, done } => {
            flush(pending);
            let _ = done.send(put_xshard_recoverable_pending(shard, &txn_id));
            false
        }
        Cmd::XshardPrepareClear {
            txn_id,
            group_id,
            done,
        } => {
            flush(pending);
            let _ = done.send(clear_xshard_prepare(shard, &txn_id, group_id));
            false
        }
        Cmd::XshardDecisionClear { txn_id, done } => {
            flush(pending);
            let _ = done.send(clear_xshard_decision(shard, &txn_id));
            false
        }
        Cmd::XshardScanPrepares { reply } => {
            flush(pending);
            let _ = reply.send(scan_xshard_prepares(shard, crypto));
            false
        }
        Cmd::XshardScanDecisions { reply } => {
            flush(pending);
            let _ = reply.send(scan_xshard_decisions(shard));
            false
        }
        Cmd::XshardDecisionGet { txn_id, reply } => {
            flush(pending);
            let _ = reply.send(get_xshard_decision(shard, &txn_id));
            false
        }
        Cmd::XshardDecisionRetainGet { txn_id, reply } => {
            flush(pending);
            let _ = reply.send(get_xshard_decision_retain(shard, &txn_id));
            false
        }
        #[cfg(feature = "compute-dist")]
        Cmd::MatViewPut { name, blob, done } => {
            flush(pending);
            let _ = done.send(crate::redb_store::put_matview(shard, &name, &blob));
            false
        }
        #[cfg(feature = "compute-dist")]
        Cmd::MatViewScan { reply } => {
            flush(pending);
            let _ = reply.send(crate::redb_store::scan_matviews(shard));
            false
        }
        #[cfg(feature = "matview")]
        Cmd::PlanMatViewPut { name, blob, done } => {
            flush(pending);
            let _ = done.send(crate::redb_store::put_plan_matview(shard, &name, &blob));
            false
        }
        #[cfg(feature = "matview")]
        Cmd::PlanMatViewDelete { name, done } => {
            flush(pending);
            let _ = done.send(crate::redb_store::delete_plan_matview(shard, &name));
            false
        }
        #[cfg(feature = "matview")]
        Cmd::PlanMatViewScan { reply } => {
            flush(pending);
            let _ = reply.send(crate::redb_store::scan_plan_matviews(shard));
            false
        }
        #[cfg(feature = "matview")]
        Cmd::MatViewOperatorStatePut { name, blob, done } => {
            flush(pending);
            let _ = done.send(crate::redb_store::put_matview_operator_state(
                shard, &name, &blob,
            ));
            false
        }
        #[cfg(feature = "matview")]
        Cmd::MatViewOperatorStateDelete { name, done } => {
            flush(pending);
            let _ = done.send(crate::redb_store::delete_matview_operator_state(
                shard, &name,
            ));
            false
        }
        #[cfg(feature = "matview")]
        Cmd::MatViewOperatorStateScan { reply } => {
            flush(pending);
            let _ = reply.send(crate::redb_store::scan_matview_operator_state(shard));
            false
        }
        }
    }};
}

pub(crate) use writer_command_arms;
