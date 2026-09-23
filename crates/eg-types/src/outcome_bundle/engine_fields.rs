//! Engine-derived fields of a terminal outcome extension (graph-os EG-4).
//!
//! Two values in a [`TerminalOutcomeExtension`] are not the caller's to know:
//! the native mutation-batch id the terminal commit publishes under (every
//! `outbox_id`, including the one each receipt's own properties must carry),
//! and the SHA-256 of each receipt's `properties_msgpack`. A caller that
//! leaves them EMPTY has the engine fill them in before validation; a caller
//! that supplies them is checked exactly as before, so a wrong value is a
//! refusal, never silently overwritten.

use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

use super::{ReceiptNode, TerminalOutcomeExtension};

impl TerminalOutcomeExtension {
    /// Fill every empty `outbox_id` with `batch_id` and every empty receipt
    /// `payload_digest` with the digest of that receipt's (final) bytes.
    pub fn bind_engine_fields(&mut self, batch_id: &str) {
        fill_empty(&mut self.outcome_bundle.outbox_id, batch_id);
        fill_empty(&mut self.run_event.outbox_id, batch_id);
        for receipt in &mut self.receipt_nodes {
            bind_receipt(receipt, batch_id);
        }
    }
}

fn bind_receipt(receipt: &mut ReceiptNode, batch_id: &str) {
    if receipt.outbox_id.is_empty() {
        receipt.outbox_id = batch_id.to_string();
        receipt.properties_msgpack = with_outbox_property(&receipt.properties_msgpack, batch_id);
    }
    if receipt.payload_digest.is_empty() {
        receipt.payload_digest = hex::encode(Sha256::digest(&receipt.properties_msgpack));
    }
}

/// The receipt properties with an empty (or absent) `outbox_id` set to
/// `batch_id`, re-encoded the way validation decodes them (a MessagePack map
/// of JSON values). Bytes that are not such a map, or that already name an
/// outbox, are returned unchanged for validation to judge.
fn with_outbox_property(bytes: &[u8], batch_id: &str) -> Vec<u8> {
    let Ok(mut properties) = rmp_serde::from_slice::<BTreeMap<String, serde_json::Value>>(bytes)
    else {
        return bytes.to_vec();
    };
    let named = properties
        .get("outbox_id")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|outbox| !outbox.is_empty());
    if named {
        return bytes.to_vec();
    }
    properties.insert("outbox_id".into(), batch_id.into());
    rmp_serde::to_vec_named(&properties).unwrap_or_else(|_| bytes.to_vec())
}

fn fill_empty(field: &mut String, value: &str) {
    if field.is_empty() {
        *field = value.to_string();
    }
}
