use super::*;

impl GraphCore {
    #[cfg(feature = "broker")]
    pub(super) fn broker_resolve_tag_owner(
        txn: &mut GraphTxn<'_>,
        delivery_tag: i64,
        consumer: &str,
    ) -> BrokerTagOwner {
        let lookup_id = crate::broker::dtag_lookup_node_id(delivery_tag);
        let Some(lookup) = txn.node_row_map(&lookup_id) else {
            return BrokerTagOwner::Rejected;
        };
        if lookup.get("type").and_then(serde_json::Value::as_str)
            != Some(crate::broker::DTAG_LOOKUP_TYPE)
        {
            txn.remove_node(lookup_id);
            return BrokerTagOwner::Rejected;
        }
        if lookup
            .get("owner_consumer")
            .and_then(serde_json::Value::as_str)
            != Some(consumer)
        {
            return BrokerTagOwner::Rejected;
        }
        BrokerTagOwner::Owned {
            node_id: Self::broker_lookup_str(&lookup, "node_id"),
            queue: Self::broker_lookup_str(&lookup, "queue"),
            lookup_id,
        }
    }

    /// One string field of a reverse-lookup row, defaulting to empty.
    #[cfg(feature = "broker")]
    pub(super) fn broker_lookup_str(
        lookup: &serde_json::Map<String, serde_json::Value>,
        key: &str,
    ) -> String {
        lookup
            .get(key)
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_string()
    }

    /// Load the message row a resolved tag addresses and confirm it is still the
    /// LIVE claimed generation owned by `consumer`.
    ///
    /// A vanished or superseded row retires the now-stale lookup; an owner
    /// mismatch does NOT (the live lookup stays intact).
    #[cfg(feature = "broker")]
    pub(super) fn broker_take_live_claim(
        txn: &mut GraphTxn<'_>,
        lookup_id: &str,
        node_id: &str,
        delivery_tag: i64,
        consumer: &str,
    ) -> Option<serde_json::Map<String, serde_json::Value>> {
        let Some(properties) = txn.node_row_map(node_id) else {
            txn.remove_node(lookup_id.to_string());
            return None;
        };
        match Self::broker_claim_liveness(&properties, delivery_tag, consumer) {
            BrokerClaimLiveness::Live => Some(properties),
            BrokerClaimLiveness::Stale => {
                txn.remove_node(lookup_id.to_string());
                None
            }
            BrokerClaimLiveness::NotOwner => None,
        }
    }

    /// Classify a message row against the delivery tag being fenced: the live
    /// claimed generation, a superseded/never-claimed one, or one owned by
    /// somebody else.
    #[cfg(feature = "broker")]
    pub(super) fn broker_claim_liveness(
        properties: &serde_json::Map<String, serde_json::Value>,
        delivery_tag: i64,
        consumer: &str,
    ) -> BrokerClaimLiveness {
        let current = properties.get("status").and_then(serde_json::Value::as_str)
            == Some("claimed")
            && properties
                .get("delivery_tag")
                .and_then(serde_json::Value::as_i64)
                == Some(delivery_tag);
        if !current {
            return BrokerClaimLiveness::Stale;
        }
        if properties
            .get("owner_consumer")
            .and_then(serde_json::Value::as_str)
            != Some(consumer)
        {
            return BrokerClaimLiveness::NotOwner;
        }
        BrokerClaimLiveness::Live
    }

    /// Is this delivery still under the queue policy's `max_delivery_count`?
    /// A queue with no policy node, or a policy carrying no cap, is unbounded.
    #[cfg(feature = "broker")]
    pub(super) fn broker_under_max_delivery(
        txn: &GraphTxn<'_>,
        queue: &str,
        delivery_count: i64,
    ) -> bool {
        txn.node_row_map(&crate::broker::queue_policy_node_id(queue))
            .and_then(|policy| {
                policy
                    .get("max_delivery_count")
                    .and_then(serde_json::Value::as_u64)
            })
            .map(|max| (delivery_count as i128) < (max as i128))
            .unwrap_or(true)
    }

    /// Requeue a nacked delivery in the SAME transaction: clear the claim
    /// generation off the message row, retire its lookup, and republish it as
    /// pending.
    #[cfg(feature = "broker")]
    pub(super) fn broker_requeue(
        txn: &mut GraphTxn<'_>,
        lookup_id: String,
        node_id: String,
        mut properties: serde_json::Map<String, serde_json::Value>,
    ) -> BrokerNackTransition {
        properties.insert("status".into(), serde_json::Value::String("pending".into()));
        properties.insert("lease_until".into(), serde_json::Value::Null);
        properties.insert("owner_consumer".into(), serde_json::Value::Null);
        properties.insert("owner_group".into(), serde_json::Value::Null);
        properties.insert("delivery_tag".into(), serde_json::Value::Null);
        let value = serde_json::Value::Object(properties);
        let Ok(blob) = rmp_serde::to_vec_named(&value) else {
            return BrokerNackTransition::Absent;
        };
        txn.remove_node(lookup_id);
        txn.add_node(node_id, blob);
        BrokerNackTransition::Requeued
    }

    /// Does `renewed_until` genuinely EXTEND this row's live lease? A missing
    /// lease, an already-expired one, or a non-extending deadline all reject.
    #[cfg(feature = "broker")]
    pub(super) fn broker_lease_extends(
        properties: &serde_json::Map<String, serde_json::Value>,
        now_ms: u64,
        renewed_until: u64,
    ) -> bool {
        let Some(current_lease_until) = properties
            .get("lease_until")
            .and_then(serde_json::Value::as_u64)
        else {
            return false;
        };
        current_lease_until > now_ms && renewed_until > current_lease_until
    }

    /// Return an expired delivery to pending while retiring its tag generation.
    /// Used by the proactive sweep; the exact observed lease is revalidated so a
    /// concurrent renewal cannot be undone.
    #[cfg(feature = "broker")]
    pub fn broker_release_expired_delivery(
        &self,
        node_id: &str,
        expected_lease_until: Option<u64>,
        now_ms: u64,
    ) -> bool {
        let mut txn = self.txn();
        let Some(mut properties) = txn.node_row_map(node_id) else {
            return false;
        };
        let Some(current_lease) = properties
            .get("lease_until")
            .and_then(serde_json::Value::as_u64)
        else {
            return false;
        };
        if properties.get("status").and_then(serde_json::Value::as_str) != Some("claimed")
            || Some(current_lease) != expected_lease_until
            || current_lease > now_ms
        {
            return false;
        }
        let Some(tag) = properties
            .get("delivery_tag")
            .and_then(serde_json::Value::as_i64)
            .filter(|tag| *tag > 0)
        else {
            return false;
        };
        txn.remove_node(crate::broker::dtag_lookup_node_id(tag));
        properties.insert("status".into(), serde_json::Value::String("pending".into()));
        properties.insert("lease_until".into(), serde_json::Value::Null);
        properties.insert("owner_consumer".into(), serde_json::Value::Null);
        properties.insert("owner_group".into(), serde_json::Value::Null);
        properties.insert("delivery_tag".into(), serde_json::Value::Null);
        let value = serde_json::Value::Object(properties);
        let Ok(blob) = rmp_serde::to_vec_named(&value) else {
            return false;
        };
        txn.add_node(node_id.to_string(), blob);
        true
    }

    /// Idempotent-producer dedup check + high-water-mark bump under ONE topology write
    /// guard (CONCEPT:EG-KG.ingest.effectively-once-publish — effectively-once publish). `node_id` is the producer's
    /// durable dedup node (`broker:producer:<producer_id>`) holding a monotonic
    /// `last_seq`. Returns `true` when `seq` is NEW (strictly above the current mark) —
    /// recording it as the new mark — and `false` when `seq` is a DUPLICATE (at/under
    /// the mark), writing nothing. The mark starts at `-1`, so the first valid `seq`
    /// (0) is accepted. Deterministic: the decision derives purely from the node's
    /// current state, so replaying the originating Method reproduces the identical
    /// mark and duplicate-verdict.
    #[cfg(feature = "broker")]
    pub fn broker_producer_check_and_record(&self, node_id: &str, seq: i64) -> bool {
        let mut txn = self.txn();
        let last = txn
            .node_row_map(node_id)
            .and_then(|m| m.get("last_seq").and_then(|s| s.as_i64()))
            .unwrap_or(-1);
        if seq <= last {
            return false; // duplicate — drop the (unwritten) txn, no state change
        }
        let props = serde_json::json!({
            "type": crate::broker::PRODUCER_SEQ_TYPE,
            "last_seq": seq,
        });
        if let Ok(blob) = rmp_serde::to_vec_named(&props) {
            txn.add_node(node_id.to_string(), blob);
        }
        drop(txn);
        self.mark_dirty();
        true
    }
}
