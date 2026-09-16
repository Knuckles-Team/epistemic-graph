//! Native typed receipt and effect digest construction.

use super::*;

/// The typed receipt one committed owner mutation records.
///
/// Takes the effect in primitives (`topic`, `key`, `event_bytes`, `headers`)
/// and an already-built `MutationResult`, rather than the entry-typed event and
/// result it used to. Both record families in this owner (RF-ADR-008) mint the
/// same receipt shape over different payloads, and every field below is derived
/// from the operation, the nonce, the batch, or those primitives.
///
/// `slug` names the record family in the receipt's opaque ids so an entry
/// receipt and a graph receipt are distinguishable in the ledger.
pub(crate) struct OwnerReceiptInput<'a> {
    pub(crate) slug: &'a str,
    pub(crate) topic: &'a str,
    pub(crate) key: &'a str,
    pub(crate) event_bytes: &'a [u8],
    pub(crate) headers: &'a BTreeMap<String, String>,
    pub(crate) mutation_result: MutationResult,
    pub(crate) committed_version: u64,
    pub(crate) committed_at_ms: u64,
}

pub(super) fn owner_receipt(
    operation: &eg_types::authority::OperationReplayIdentity,
    nonce: &eg_types::authority::NonceReplayKey,
    batch: &MutationBatch,
    input: OwnerReceiptInput<'_>,
) -> Result<MutationReceipt, String> {
    use eg_types::contract::{MutationDisposition, OpaqueId, UtcUnixNanos};

    let OwnerReceiptInput {
        slug,
        topic,
        key,
        event_bytes,
        headers,
        mutation_result,
        committed_version,
        committed_at_ms,
    } = input;
    let operation_digest = operation.digest()?;
    let nonce_digest = nonce.digest()?;
    let context_digest = batch
        .envelope
        .operation()
        .map(|envelope| envelope.authority.context_digest)
        .unwrap_or(operation_digest);
    let suffix = operation_digest.to_hex();
    let effect_digest = agent_library_effect_digest(topic, key, event_bytes, headers)?;
    let recorded_at = committed_at_ms
        .checked_mul(1_000_000)
        .and_then(|value| i64::try_from(value).ok())
        .map(UtcUnixNanos::new)
        .ok_or_else(|| "agent library receipt time exceeds the supported range".to_string())?;
    let receipt = MutationReceipt {
        receipt_id: OpaqueId::new(format!("{slug}-receipt-{suffix}"))?,
        mutation_id: OpaqueId::new(format!("{slug}-mutation-{suffix}"))?,
        scope: operation.authority_scope.clone(),
        authority_receipt_id: OpaqueId::new(format!("{slug}-authority-{suffix}"))?,
        authority_evidence_digest: context_digest,
        disposition: MutationDisposition::new("committed")?,
        operation_replay_digest: operation_digest,
        nonce_replay_digest: nonce_digest,
        envelope_digest: context_digest,
        effect_id: Some(OpaqueId::new(format!("{slug}-effect-{suffix}"))?),
        effect_digest: Some(effect_digest),
        result_digest: mutation_result.digest()?,
        commit_id: Some(OpaqueId::new(format!(
            "{slug}-commit-{suffix}-{committed_version}"
        ))?),
        result: mutation_result,
        recorded_at,
    };
    receipt.validate()?;
    Ok(receipt)
}

pub(super) fn agent_library_effect_digest(
    topic: &str,
    key: &str,
    payload: &[u8],
    headers: &BTreeMap<String, String>,
) -> Result<eg_types::contract::Digest256, String> {
    let mut header_digest =
        eg_types::contract::Digest256::framed(b"eg/agent-library-effect-headers/v1", &[])?;
    for (header, value) in headers {
        header_digest = eg_types::contract::Digest256::framed(
            b"eg/agent-library-effect-header/v1",
            &[
                header_digest.as_bytes(),
                header.as_bytes(),
                value.as_bytes(),
            ],
        )?;
    }
    eg_types::contract::Digest256::framed(
        b"eg/agent-library-effect/v1",
        &[
            topic.as_bytes(),
            key.as_bytes(),
            payload,
            header_digest.as_bytes(),
        ],
    )
}
