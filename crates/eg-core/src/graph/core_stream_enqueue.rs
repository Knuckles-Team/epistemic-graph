use super::*;

impl GraphCore {
    // ── Distribution-valued properties (CONCEPT:EG-KG.compute.uncertainty-values) ──────────────────
    //
    // A `Distribution` is just a tagged JSON object, so it round-trips through
    // the existing arbitrary-JSON property map with NO schema change — these
    // are typed convenience accessors over that convention, generalizing the
    // scalar `confidence` field into a full uncertainty distribution.

    /// Read a distribution-valued property `key` off `node_id` (CONCEPT:EG-KG.compute.uncertainty-values).
    /// Returns `None` if the node is absent, its blob is undecodable, the key is
    /// missing, or the stored JSON is not a valid `Distribution`.
    pub fn get_distribution(&self, node_id: &str, key: &str) -> Option<eg_types::Distribution> {
        let bytes = self.get_node_properties(node_id)?;
        let val = decode_property_value(&bytes).ok()?;
        let field = val.as_object()?.get(key)?.clone();
        serde_json::from_value(field).ok()
    }

    /// Store `dist` as the property `key` on `node_id` (CONCEPT:EG-KG.compute.uncertainty-values), merging
    /// into the node's existing properties (other keys preserved). Creates the
    /// node with a single-key object if it does not yet exist. Returns whether
    /// the property was written (only fails if the value cannot be serialized).
    pub fn set_distribution(
        &self,
        node_id: &str,
        key: &str,
        dist: &eg_types::Distribution,
    ) -> bool {
        let dist_val = match serde_json::to_value(dist) {
            Ok(v) => v,
            Err(_) => return false,
        };
        // Merge into the current blob (or start a fresh object).
        let mut obj = match self.get_node_properties(node_id) {
            Some(bytes) => match decode_property_value(&bytes) {
                Ok(serde_json::Value::Object(o)) => o,
                _ => serde_json::Map::new(),
            },
            None => serde_json::Map::new(),
        };
        obj.insert(key.to_string(), dist_val);
        let reenc = match rmp_serde::to_vec_named(&serde_json::Value::Object(obj)) {
            Ok(b) => b,
            Err(_) => return false,
        };
        self.add_node(node_id.to_string(), reenc);
        true
    }

    /// One-shot serializable gated remove (CONCEPT:EG-KG.txn.serializable-mutation-gate). See
    /// [`GraphTxn::remove_node_if`]; the decode → predicate re-check → remove runs
    /// under ONE topology write guard.
    pub fn remove_node_if(&self, node_id: &str, predicate: &eg_types::RowPredicate) -> bool {
        let removed = self.txn().remove_node_if(node_id, predicate);
        if removed {
            self.semantic_store.write().remove_embedding(node_id);
        }
        removed
    }

    /// One-shot atomic compare-and-set over a single `txn` (CONCEPT:EG-KG.compute.backend backend-
    /// agnostic atomic claim). See [`GraphTxn::compare_and_set_fields`] for the
    /// semantics; the whole read-modify-write runs under one topology write guard.
    pub fn compare_and_set_fields(
        &self,
        node_id: &str,
        conditions: &serde_json::Map<String, serde_json::Value>,
        updates: &serde_json::Map<String, serde_json::Value>,
    ) -> bool {
        self.txn()
            .compare_and_set_fields(node_id, conditions, updates)
    }

    /// One-shot serializable gated compare-and-set (CONCEPT:EG-KG.txn.serializable-mutation-gate). See
    /// [`GraphTxn::compare_and_set_fields_if`]; the decode → predicate re-check →
    /// conditional merge runs under ONE topology write guard.
    pub fn compare_and_set_fields_if(
        &self,
        node_id: &str,
        predicate: &eg_types::RowPredicate,
        conditions: &serde_json::Map<String, serde_json::Value>,
        updates: &serde_json::Map<String, serde_json::Value>,
    ) -> bool {
        self.txn()
            .compare_and_set_fields_if(node_id, predicate, conditions, updates)
    }

    /// Atomically claim the oldest pending node of `label` (CONCEPT:EG-KG.compute.atomically-claim-oldest-pending —
    /// native task queue). Scans `label`'s nodes (via the O(1) label index) for
    /// the smallest `seq` whose `status == "pending"`, then CAS-merges `updates`
    /// (condition `status == "pending"`) under one topology write guard. Returns
    /// `(node_id, updated_properties)` or `None` if nothing was claimable.
    ///
    /// Deterministic: the pick is a total order on the unique `seq` (ties broken
    /// by `node_id`) — a pure function of graph state — and `updates` carries no
    /// clock, so WAL replay and the Raft state machine reproduce the identical
    /// claim. This is the single-round-trip form of the client scan+CAS.
    pub fn claim_next_fields(
        &self,
        label: &str,
        updates: &serde_json::Map<String, serde_json::Value>,
    ) -> Option<(String, serde_json::Value)> {
        let rows = self.get_nodes_by_label(label, 0);
        let mut best: Option<(String, i64)> = None;
        for (id, blob) in &rows {
            let Ok(v) = decode_property_value(blob) else {
                continue;
            };
            let Some(obj) = v.as_object() else { continue };
            if obj.get("status").and_then(|s| s.as_str()) != Some("pending") {
                continue;
            }
            let seq = obj.get("seq").and_then(|s| s.as_i64()).unwrap_or(i64::MAX);
            let better = match &best {
                None => true,
                Some((bid, bseq)) => seq < *bseq || (seq == *bseq && id < bid),
            };
            if better {
                best = Some((id.clone(), seq));
            }
        }
        let (id, _) = best?;
        let mut conditions = serde_json::Map::new();
        conditions.insert(
            "status".to_string(),
            serde_json::Value::String("pending".to_string()),
        );
        if self.compare_and_set_fields(&id, &conditions, updates) {
            let blob = self.node_properties.get(&id)?;
            let val = decode_property_value(&blob).ok()?;
            Some((id, val))
        } else {
            None
        }
    }

    /// Atomically append one published message to EACH of `queues` under ONE
    /// topology write guard (CONCEPT:EG-KG.compute.message-broker-exchanges — message-broker enqueue on top of the
    /// KG-2.303 work-queue). For every queue: read+bump its durable monotonic
    /// `next_seq` counter node (`broker:seq:<queue>`) and append a pending message
    /// node labeled `qmsg:<queue>` (the label
    /// [`claim_next_fields`](Self::claim_next_fields) scans) carrying the hex-encoded
    /// payload, so consume/ack reuse the existing claim/CAS path unchanged. Returns
    /// how many queues were enqueued.
    ///
    /// Deterministic: the seq comes purely from the counter node's current state and
    /// no server clock is written, so replaying the same `Method::Publish` over the
    /// same pre-image (WAL / Raft state machine) reproduces byte-identical message
    /// nodes — the same discipline `claim_next_fields` follows. The whole fan-out
    /// runs under ONE `txn()` guard, so a publisher never observes a partial delivery.
    #[cfg(feature = "broker")]
    pub fn broker_enqueue(
        &self,
        queues: &[String],
        exchange: &str,
        routing_key: &str,
        payload_hex: &str,
    ) -> usize {
        self.broker_enqueue_ex(
            queues,
            exchange,
            routing_key,
            payload_hex,
            &serde_json::Map::new(),
        )
    }

    /// Policy-aware sibling of [`broker_enqueue`](Self::broker_enqueue) (CONCEPT:
    /// EG-277/278/279). Identical atomic multi-queue append, but merges `extra` (e.g.
    /// `priority` / `deliver_at` / `expires_at` / `x-death`) into each message node so
    /// the claim predicate can honor per-message priority, delay, and TTL. An EMPTY
    /// `extra` reproduces the exact EG-275 message shape — the plain `broker_enqueue`
    /// simply calls this with no extras, so the two paths never diverge. Deterministic:
    /// `extra` carries only caller-resolved absolute values (no server clock), so WAL
    /// replay of the originating `Publish`/`PublishEx` reproduces identical nodes.
    #[cfg(feature = "broker")]
    pub fn broker_enqueue_ex(
        &self,
        queues: &[String],
        exchange: &str,
        routing_key: &str,
        payload_hex: &str,
        extra: &serde_json::Map<String, serde_json::Value>,
    ) -> usize {
        let mut txn = self.txn();
        let mut delivered = 0usize;
        for q in queues {
            let seq_id = crate::broker::queue_seq_node_id(q);
            // Current counter (missing ⇒ start at 0), then persist the bump.
            let next = txn
                .node_row_map(&seq_id)
                .and_then(|m| m.get("next_seq").and_then(|s| s.as_i64()))
                .unwrap_or(0);
            let seq_props = serde_json::json!({
                "type": "BrokerQueueSeq",
                "queue": q,
                "next_seq": next + 1,
            });
            if let Ok(blob) = rmp_serde::to_vec_named(&seq_props) {
                txn.add_node(seq_id, blob);
            }
            // The pending message node — labeled so `claim_next_fields` delivers it.
            let mut msg_props = serde_json::Map::new();
            msg_props.insert(
                "type".into(),
                serde_json::Value::String(crate::broker::queue_msg_label(q)),
            );
            msg_props.insert("status".into(), serde_json::Value::String("pending".into()));
            msg_props.insert("seq".into(), serde_json::Value::from(next));
            msg_props.insert(
                "exchange".into(),
                serde_json::Value::String(exchange.into()),
            );
            msg_props.insert(
                "routing_key".into(),
                serde_json::Value::String(routing_key.into()),
            );
            msg_props.insert(
                "payload".into(),
                serde_json::Value::String(payload_hex.into()),
            );
            // Merge caller-resolved policy fields (priority/deliver_at/expires_at/…).
            for (k, v) in extra {
                msg_props.insert(k.clone(), v.clone());
            }
            let msg_props = serde_json::Value::Object(msg_props);
            if let Ok(blob) = rmp_serde::to_vec_named(&msg_props) {
                txn.add_node(crate::broker::message_node_id(q, next), blob);
                delivered += 1;
            }
        }
        // Release the write guard, then invalidate the lazy label index (CONCEPT:
        // KG-2.176) so the new `qmsg:<queue>` message nodes are visible to the very
        // next `claim_next_fields` — a raw `txn().add_node` does NOT bump it, unlike
        // the dispatch shell's post-write `mark_dirty`.
        drop(txn);
        if delivered > 0 {
            self.mark_dirty();
        }
        delivered
    }

    /// Append one RETAINED message to `stream` and return its monotonic offset
    /// (CONCEPT:EG-KG.compute.replayable-append-log — replayable append-log streams). Under ONE topology write
    /// guard: read+bump the stream's durable `next_offset` counter node
    /// (`broker:soff:<stream>`) and append a message node labeled `smsg:<stream>`
    /// carrying the hex payload + `ts = now_ms`. Unlike [`broker_enqueue`](Self::
    /// broker_enqueue) the message is NEVER consumed/deleted by a read — it is served
    /// by offset until an explicit retention trim removes it.
    ///
    /// Deterministic: the offset comes purely from the counter node's current state and
    /// `now_ms` is explicit, so replaying `Method::StreamPublish` over the same
    /// pre-image reproduces a byte-identical node — the same discipline the queue
    /// enqueue follows. Bumps the lazy label index so the appended message is visible
    /// to the very next `stream_read`.
    #[cfg(feature = "broker")]
    pub fn stream_append(&self, stream: &str, payload_hex: &str, now_ms: u64) -> i64 {
        let mut txn = self.txn();
        let off_id = crate::broker::stream_offset_node_id(stream);
        let offset = txn
            .node_row_map(&off_id)
            .and_then(|m| m.get("next_offset").and_then(|s| s.as_i64()))
            .unwrap_or(0);
        let off_props = serde_json::json!({
            "type": crate::broker::STREAM_OFFSET_TYPE,
            "stream": stream,
            "next_offset": offset + 1,
        });
        if let Ok(blob) = rmp_serde::to_vec_named(&off_props) {
            txn.add_node(off_id, blob);
        }
        let msg_props = serde_json::json!({
            "type": crate::broker::stream_msg_label(stream),
            "offset": offset,
            "payload": payload_hex,
            "ts": now_ms,
        });
        if let Ok(blob) = rmp_serde::to_vec_named(&msg_props) {
            txn.add_node(crate::broker::stream_msg_node_id(stream, offset), blob);
        }
        drop(txn);
        self.mark_dirty();
        offset
    }

    /// Remove a set of stream message nodes under ONE topology write guard
    /// (CONCEPT:EG-KG.compute.replayable-append-log — retention trim). Returns how many of `ids` were present and
    /// removed. Backs [`broker::stream_trim`](crate::broker::stream_trim): the caller
    /// resolves the offset-ordered drop set from the retention policy, so this is a
    /// deterministic bulk delete that replays byte-identically.
    #[cfg(feature = "broker")]
    pub fn stream_trim_nodes(&self, ids: &[String]) -> usize {
        let mut txn = self.txn();
        let mut removed = 0usize;
        for id in ids {
            if txn.node_properties.contains_key(id) {
                txn.remove_node(id.clone());
                removed += 1;
            }
        }
        drop(txn);
        if removed > 0 {
            self.mark_dirty();
        }
        removed
    }

    /// Issue the next value of a broker-wide monotonic counter node and persist the
    /// bump under ONE topology write guard (CONCEPT:EG-KG.compute.publisher-confirms-consumer-qos — publisher-confirm +
    /// consumer delivery tags). The counter's `last_tag` starts at 0, so the FIRST
    /// issued tag is `1` and every subsequent call returns a strictly greater value.
    /// Deterministic: the value derives purely from the counter node, so replaying the
    /// originating Method reproduces the identical tag.
    #[cfg(feature = "broker")]
    pub fn broker_next_counter(&self, node_id: &str, type_str: &str) -> i64 {
        let mut txn = self.txn();
        let last = txn
            .node_row_map(node_id)
            .and_then(|m| m.get("last_tag").and_then(|s| s.as_i64()))
            .unwrap_or(0);
        let issued = last + 1;
        let props = serde_json::json!({
            "type": type_str,
            "last_tag": issued,
        });
        if let Ok(blob) = rmp_serde::to_vec_named(&props) {
            txn.add_node(node_id.to_string(), blob);
        }
        drop(txn);
        self.mark_dirty();
        issued
    }
}
