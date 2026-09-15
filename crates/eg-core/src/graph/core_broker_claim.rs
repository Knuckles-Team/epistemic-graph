use super::*;

impl GraphCore {
    #[cfg(feature = "broker")]
    #[allow(clippy::too_many_arguments)]
    pub fn broker_claim_delivery(
        &self,
        node_id: &str,
        queue: &str,
        group: &str,
        consumer: &str,
        expected_status: &str,
        expected_lease_until: Option<u64>,
        now_ms: u64,
        lease_ms: u64,
    ) -> Option<serde_json::Value> {
        if consumer.trim().is_empty() {
            return None;
        }
        let request = BrokerClaimRequest {
            node_id,
            queue,
            group,
            consumer,
            expected_status,
            expected_lease_until,
            now_ms,
            lease_ms,
        };
        let mut txn = self.txn();
        // Validate + build every blob BEFORE the first mutation: any rejection
        // returns None having written nothing, exactly as the original
        // early-`return None` / `?` chain did under the held write guard.
        let claim = Self::prepare_broker_claim(&txn, &request)?;
        if let Some(tag) = claim.prior_tag {
            txn.remove_node(crate::broker::dtag_lookup_node_id(tag));
        }
        txn.add_node(claim.counter_id, claim.counter_blob);
        txn.add_node(node_id.to_string(), claim.message_blob);
        txn.add_node(claim.lookup_id, claim.lookup_blob);
        Some(claim.message_value)
    }

    /// Revalidate the claim under the held write guard and build every blob it
    /// will install. PURE w.r.t. the graph: it only READS through `txn`, so a
    /// `None` from anywhere below leaves the transaction untouched.
    #[cfg(feature = "broker")]
    pub(super) fn prepare_broker_claim(
        txn: &GraphTxn<'_>,
        request: &BrokerClaimRequest<'_>,
    ) -> Option<BrokerClaim> {
        let mut properties = txn.node_row_map(request.node_id)?;
        if !Self::broker_claim_precondition_ok(&properties, request) {
            return None;
        }
        let counter_id = crate::broker::dtag_seq_node_id();
        let last_tag = Self::broker_last_delivery_tag(txn, &counter_id)?;
        let delivery_tag = last_tag.checked_add(1)?;
        let prior_tag = Self::broker_prior_delivery_tag(&properties)?;
        if !Self::broker_reclaim_tag_ok(request.expected_status, prior_tag, last_tag, delivery_tag)
        {
            return None;
        }
        let lookup_id = crate::broker::dtag_lookup_node_id(delivery_tag);
        if txn.has_node(&lookup_id) {
            return None;
        }
        // Clear the old generation in the owned row before stamping the new one.
        // The prior lookup is retired by the caller before any new lookup is installed.
        properties.insert("delivery_tag".into(), serde_json::Value::Null);
        let next_delivery_count = Self::broker_next_delivery_count(&properties)?;
        let lease_until = Self::broker_lease_until(request.now_ms, request.lease_ms)?;
        Self::broker_stamp_claim(
            &mut properties,
            request,
            next_delivery_count,
            lease_until,
            delivery_tag,
        );
        Self::broker_claim_blobs(
            request,
            properties,
            counter_id,
            lookup_id,
            prior_tag,
            delivery_tag,
        )
    }

    /// Does the message row still satisfy the claim's expected status (and, for a
    /// reclaim, the exact expired lease)? Checked in the original order: status
    /// first, then the lease.
    #[cfg(feature = "broker")]
    pub(super) fn broker_claim_precondition_ok(
        properties: &serde_json::Map<String, serde_json::Value>,
        request: &BrokerClaimRequest<'_>,
    ) -> bool {
        if properties.get("status").and_then(serde_json::Value::as_str)
            != Some(request.expected_status)
        {
            return false;
        }
        if request.expected_status == "claimed" {
            let current_lease = properties
                .get("lease_until")
                .and_then(serde_json::Value::as_u64);
            // A reclaim needs the EXACT lease the scan saw, and that lease must
            // already have expired (a missing lease never expires).
            return current_lease == request.expected_lease_until
                && current_lease.is_some_and(|until| until <= request.now_ms);
        }
        request.expected_status == "pending"
    }

    /// Read the broker-wide delivery-tag counter. An absent counter reads as `0`
    /// (the first issued tag is 1); a wrong-typed, missing, non-integer or
    /// negative `last_tag` rejects the claim (`None`).
    #[cfg(feature = "broker")]
    pub(super) fn broker_last_delivery_tag(txn: &GraphTxn<'_>, counter_id: &str) -> Option<i64> {
        let Some(row) = txn.node_row_map(counter_id) else {
            return Some(0);
        };
        if row.get("type").and_then(serde_json::Value::as_str)
            != Some(crate::broker::BROKER_COUNTER_TYPE)
        {
            return None;
        }
        let value = row.get("last_tag").and_then(serde_json::Value::as_i64)?;
        if value < 0 {
            return None;
        }
        Some(value)
    }

    /// The message's PRIOR positive delivery tag, if any. `Some(None)` = never
    /// delivered (absent/null); the outer `None` = a malformed tag, which rejects
    /// the claim.
    #[cfg(feature = "broker")]
    pub(super) fn broker_prior_delivery_tag(
        properties: &serde_json::Map<String, serde_json::Value>,
    ) -> Option<Option<i64>> {
        match properties.get("delivery_tag") {
            None | Some(serde_json::Value::Null) => Some(None),
            Some(value) => match value.as_i64() {
                Some(tag) if tag > 0 => Some(Some(tag)),
                _ => None,
            },
        }
    }

    /// Is the freshly issued tag admissible? A reclaim must carry a prior tag no
    /// newer than the counter it is fencing; a fresh claim has no such constraint.
    #[cfg(feature = "broker")]
    pub(super) fn broker_reclaim_tag_ok(
        expected_status: &str,
        prior_tag: Option<i64>,
        last_tag: i64,
        delivery_tag: i64,
    ) -> bool {
        if delivery_tag <= 0 {
            return false;
        }
        if expected_status != "claimed" {
            return true;
        }
        prior_tag.is_some_and(|tag| tag <= last_tag)
    }

    /// The message's delivery count after this delivery. An absent count starts at
    /// `0`; a non-integer, negative, or overflowing count rejects the claim.
    #[cfg(feature = "broker")]
    pub(super) fn broker_next_delivery_count(
        properties: &serde_json::Map<String, serde_json::Value>,
    ) -> Option<i64> {
        let count = match properties.get("delivery_count") {
            None => 0,
            Some(value) => {
                let count = value.as_i64()?;
                if count < 0 {
                    return None;
                }
                count
            }
        };
        count.checked_add(1)
    }

    /// The claim's lease deadline: `Some(None)` for a zero-length (unbounded)
    /// lease, the outer `None` when `now_ms + lease_ms` overflows.
    #[cfg(feature = "broker")]
    pub(super) fn broker_lease_until(now_ms: u64, lease_ms: u64) -> Option<Option<u64>> {
        if lease_ms == 0 {
            return Some(None);
        }
        Some(Some(now_ms.checked_add(lease_ms)?))
    }

    /// Stamp the claimed generation onto the owned message row.
    #[cfg(feature = "broker")]
    pub(super) fn broker_stamp_claim(
        properties: &mut serde_json::Map<String, serde_json::Value>,
        request: &BrokerClaimRequest<'_>,
        next_delivery_count: i64,
        lease_until: Option<u64>,
        delivery_tag: i64,
    ) {
        properties.insert("status".into(), serde_json::Value::String("claimed".into()));
        properties.insert(
            "owner_group".into(),
            serde_json::Value::String(request.group.to_string()),
        );
        properties.insert(
            "owner_consumer".into(),
            serde_json::Value::String(request.consumer.to_string()),
        );
        properties.insert(
            "delivery_count".into(),
            serde_json::Value::from(next_delivery_count),
        );
        properties.insert("claimed_at".into(), serde_json::Value::from(request.now_ms));
        properties.insert(
            "lease_until".into(),
            lease_until.map_or(serde_json::Value::Null, serde_json::Value::from),
        );
        properties.insert("delivery_tag".into(), serde_json::Value::from(delivery_tag));
    }

    /// Encode the counter, the stamped message row, and the new O(1) reverse
    /// lookup. Any encode failure rejects the claim before a single write.
    #[cfg(feature = "broker")]
    pub(super) fn broker_claim_blobs(
        request: &BrokerClaimRequest<'_>,
        properties: serde_json::Map<String, serde_json::Value>,
        counter_id: String,
        lookup_id: String,
        prior_tag: Option<i64>,
        delivery_tag: i64,
    ) -> Option<BrokerClaim> {
        let counter = serde_json::json!({
            "type": crate::broker::BROKER_COUNTER_TYPE,
            "last_tag": delivery_tag,
        });
        let lookup = serde_json::json!({
            "type": crate::broker::DTAG_LOOKUP_TYPE,
            "node_id": request.node_id,
            "queue": request.queue,
            "owner_consumer": request.consumer,
        });
        let message_value = serde_json::Value::Object(properties);
        Some(BrokerClaim {
            counter_id,
            lookup_id,
            prior_tag,
            counter_blob: rmp_serde::to_vec_named(&counter).ok()?,
            message_blob: rmp_serde::to_vec_named(&message_value).ok()?,
            lookup_blob: rmp_serde::to_vec_named(&lookup).ok()?,
            message_value,
        })
    }
}
