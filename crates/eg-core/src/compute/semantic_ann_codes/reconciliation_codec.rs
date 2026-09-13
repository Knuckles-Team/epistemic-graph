//! The canonical byte codec of the source reconciliation checkpoint.

use super::reconciliation::{
    valid_source_entity_id_for_reconciliation, validate_sql_source_revision,
    SemanticSourceReconciliationCheckpoint, SemanticSourceReconciliationPhase,
    SEMANTIC_RECONCILIATION_CHECKPOINT_MAGIC, SEMANTIC_RECONCILIATION_MAX_CURSOR_BYTES,
    SEMANTIC_RECONCILIATION_MAX_ENTITY_BYTES, SEMANTIC_RECONCILIATION_MAX_REVISION_BYTES,
};
use super::{corrupt, ensure, refused, SemanticCodeError};
use eg_types::semantic_index::SemanticDigest;

/// Largest encoded checkpoint accepted in either direction.
const MAX_ENCODED_BYTES: usize = 16 * 1024;

pub(super) fn validate_reconciliation_checkpoint(
    checkpoint: &SemanticSourceReconciliationCheckpoint,
) -> Result<(), SemanticCodeError> {
    let zero = SemanticDigest::from_bytes([0; 32]);
    ensure(
        checkpoint.source_wakeup_digest != zero,
        "source reconciliation wakeup identity is zero",
    )?;
    validate_sql_source_revision(&checkpoint.source_revision)?;
    ensure(
        checkpoint.source_revision.len() <= SEMANTIC_RECONCILIATION_MAX_REVISION_BYTES,
        "source reconciliation revision exceeds the bounded size",
    )?;
    ensure(
        checkpoint.source_cursor.as_ref().is_none_or(|cursor| {
            !cursor.is_empty() && cursor.len() <= SEMANTIC_RECONCILIATION_MAX_CURSOR_BYTES
        }),
        "source reconciliation cursor exceeds the bounded size",
    )?;
    ensure(
        checkpoint
            .prior_cursor
            .as_deref()
            .is_none_or(valid_source_entity_id_for_reconciliation),
        "source reconciliation prior cursor is not a source identity",
    )?;
    ensure_phase_fields(checkpoint)?;
    ensure(
        checkpoint.complete_snapshot_receipt_digest != Some(zero),
        "source reconciliation complete snapshot proof is zero",
    )
}

/// A scanning checkpoint pages the source and carries no deletion proof; a
/// finalizing one has finished the source and carries the complete snapshot.
fn ensure_phase_fields(
    checkpoint: &SemanticSourceReconciliationCheckpoint,
) -> Result<(), SemanticCodeError> {
    match checkpoint.phase {
        SemanticSourceReconciliationPhase::Scanning => ensure(
            checkpoint.source_cursor.is_some()
                && checkpoint.prior_cursor.is_none()
                && checkpoint.complete_snapshot_receipt_digest.is_none(),
            "scanning reconciliation checkpoint has invalid phase fields",
        ),
        SemanticSourceReconciliationPhase::FinalizingTombstones => ensure(
            checkpoint.source_cursor.is_none()
                && checkpoint.complete_snapshot_receipt_digest.is_some(),
            "tombstone reconciliation checkpoint has invalid phase fields",
        ),
    }
}

pub(super) fn encode_reconciliation_checkpoint(
    checkpoint: &SemanticSourceReconciliationCheckpoint,
) -> Result<Vec<u8>, SemanticCodeError> {
    validate_reconciliation_checkpoint(checkpoint)?;
    let mut encoded = Vec::with_capacity(256);
    encoded.extend_from_slice(SEMANTIC_RECONCILIATION_CHECKPOINT_MAGIC);
    encoded.extend_from_slice(checkpoint.source_wakeup_digest.as_bytes());
    append_checkpoint_bytes(&mut encoded, checkpoint.source_revision.as_bytes())?;
    encoded.push(match &checkpoint.phase {
        SemanticSourceReconciliationPhase::Scanning => 0,
        SemanticSourceReconciliationPhase::FinalizingTombstones => 1,
    });
    append_optional_checkpoint_bytes(&mut encoded, checkpoint.source_cursor.as_deref())?;
    append_optional_checkpoint_bytes(
        &mut encoded,
        checkpoint.prior_cursor.as_deref().map(str::as_bytes),
    )?;
    encoded.extend_from_slice(&checkpoint.rows_seen.to_be_bytes());
    encoded.extend_from_slice(&checkpoint.source_bytes_seen.to_be_bytes());
    encoded.extend_from_slice(&checkpoint.pages_seen.to_be_bytes());
    match checkpoint.complete_snapshot_receipt_digest {
        Some(digest) => {
            encoded.push(1);
            encoded.extend_from_slice(digest.as_bytes());
        }
        None => encoded.push(0),
    }
    ensure(
        encoded.len() <= MAX_ENCODED_BYTES,
        "source reconciliation checkpoint exceeds the bounded size",
    )?;
    Ok(encoded)
}

fn append_checkpoint_bytes(encoded: &mut Vec<u8>, bytes: &[u8]) -> Result<(), SemanticCodeError> {
    let length = u32::try_from(bytes.len())
        .map_err(|_| refused("source reconciliation field is too large"))?;
    encoded.extend_from_slice(&length.to_be_bytes());
    encoded.extend_from_slice(bytes);
    Ok(())
}

fn append_optional_checkpoint_bytes(
    encoded: &mut Vec<u8>,
    bytes: Option<&[u8]>,
) -> Result<(), SemanticCodeError> {
    match bytes {
        Some(bytes) => {
            encoded.push(1);
            append_checkpoint_bytes(encoded, bytes)?;
        }
        None => encoded.push(0),
    }
    Ok(())
}

pub(super) fn decode_reconciliation_checkpoint(
    encoded: &[u8],
) -> Result<SemanticSourceReconciliationCheckpoint, SemanticCodeError> {
    if encoded.len() > MAX_ENCODED_BYTES
        || !encoded.starts_with(SEMANTIC_RECONCILIATION_CHECKPOINT_MAGIC)
    {
        return Err(corrupt(
            "source reconciliation checkpoint has an unknown encoding",
        ));
    }
    let mut offset = SEMANTIC_RECONCILIATION_CHECKPOINT_MAGIC.len();
    let source_wakeup_digest = take_checkpoint_digest(
        encoded,
        &mut offset,
        "source reconciliation wakeup is not 32 bytes",
    )?;
    let revision = take_checkpoint_bytes(
        encoded,
        &mut offset,
        SEMANTIC_RECONCILIATION_MAX_REVISION_BYTES,
    )?;
    let source_revision = checkpoint_utf8(revision, "source reconciliation revision is not UTF-8")?;
    let phase = take_checkpoint_phase(encoded, &mut offset)?;
    let source_cursor = take_optional_checkpoint_bytes(
        encoded,
        &mut offset,
        SEMANTIC_RECONCILIATION_MAX_CURSOR_BYTES,
    )?;
    let prior_cursor = take_optional_checkpoint_bytes(
        encoded,
        &mut offset,
        SEMANTIC_RECONCILIATION_MAX_ENTITY_BYTES,
    )?
    .map(|bytes| checkpoint_utf8(&bytes, "source reconciliation cursor is not UTF-8"))
    .transpose()?;
    let checkpoint = SemanticSourceReconciliationCheckpoint {
        source_wakeup_digest,
        source_revision,
        phase,
        source_cursor,
        prior_cursor,
        rows_seen: take_checkpoint_u64(encoded, &mut offset)?,
        source_bytes_seen: take_checkpoint_u64(encoded, &mut offset)?,
        pages_seen: take_checkpoint_u64(encoded, &mut offset)?,
        complete_snapshot_receipt_digest: take_checkpoint_proof(encoded, &mut offset)?,
    };
    if offset != encoded.len() {
        return Err(corrupt(
            "source reconciliation checkpoint has trailing bytes",
        ));
    }
    validate_reconciliation_checkpoint(&checkpoint)
        .map_err(|error| SemanticCodeError::Corrupt(error.to_string()))?;
    Ok(checkpoint)
}

fn take_checkpoint_phase(
    encoded: &[u8],
    offset: &mut usize,
) -> Result<SemanticSourceReconciliationPhase, SemanticCodeError> {
    match take_checkpoint_byte(encoded, offset)? {
        0 => Ok(SemanticSourceReconciliationPhase::Scanning),
        1 => Ok(SemanticSourceReconciliationPhase::FinalizingTombstones),
        _ => Err(corrupt(
            "source reconciliation checkpoint has an unknown phase",
        )),
    }
}

fn take_checkpoint_proof(
    encoded: &[u8],
    offset: &mut usize,
) -> Result<Option<SemanticDigest>, SemanticCodeError> {
    match take_checkpoint_byte(encoded, offset)? {
        0 => Ok(None),
        1 => take_checkpoint_digest(
            encoded,
            offset,
            "source reconciliation proof is not 32 bytes",
        )
        .map(Some),
        _ => Err(corrupt(
            "source reconciliation checkpoint has an unknown proof marker",
        )),
    }
}

fn take_checkpoint_digest(
    encoded: &[u8],
    offset: &mut usize,
    malformed: &str,
) -> Result<SemanticDigest, SemanticCodeError> {
    let bytes = take_checkpoint_fixed(encoded, offset, 32)?;
    let bytes: [u8; 32] = bytes.try_into().map_err(|_| corrupt(malformed))?;
    Ok(SemanticDigest::from_bytes(bytes))
}

fn checkpoint_utf8(bytes: &[u8], malformed: &str) -> Result<String, SemanticCodeError> {
    String::from_utf8(bytes.to_vec()).map_err(|_| corrupt(malformed))
}

fn take_checkpoint_byte(encoded: &[u8], offset: &mut usize) -> Result<u8, SemanticCodeError> {
    let byte = *encoded
        .get(*offset)
        .ok_or_else(|| corrupt("source reconciliation checkpoint is truncated"))?;
    *offset += 1;
    Ok(byte)
}

fn take_checkpoint_fixed<'a>(
    encoded: &'a [u8],
    offset: &mut usize,
    length: usize,
) -> Result<&'a [u8], SemanticCodeError> {
    let end = (*offset)
        .checked_add(length)
        .ok_or_else(|| corrupt("source reconciliation checkpoint overflows"))?;
    let bytes = encoded
        .get(*offset..end)
        .ok_or_else(|| corrupt("source reconciliation checkpoint is truncated"))?;
    *offset = end;
    Ok(bytes)
}

fn take_checkpoint_u64(encoded: &[u8], offset: &mut usize) -> Result<u64, SemanticCodeError> {
    let bytes = take_checkpoint_fixed(encoded, offset, 8)?;
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| corrupt("source reconciliation counter is not 64 bits"))?;
    Ok(u64::from_be_bytes(bytes))
}

fn take_checkpoint_bytes<'a>(
    encoded: &'a [u8],
    offset: &mut usize,
    max_length: usize,
) -> Result<&'a [u8], SemanticCodeError> {
    let length_bytes: [u8; 4] = take_checkpoint_fixed(encoded, offset, 4)?
        .try_into()
        .map_err(|_| corrupt("source reconciliation field length is not 32 bits"))?;
    let length = u32::from_be_bytes(length_bytes) as usize;
    if length > max_length {
        return Err(corrupt(
            "source reconciliation field exceeds the bounded size",
        ));
    }
    take_checkpoint_fixed(encoded, offset, length)
}

fn take_optional_checkpoint_bytes(
    encoded: &[u8],
    offset: &mut usize,
    max_length: usize,
) -> Result<Option<Vec<u8>>, SemanticCodeError> {
    match take_checkpoint_byte(encoded, offset)? {
        0 => Ok(None),
        1 => Ok(Some(
            take_checkpoint_bytes(encoded, offset, max_length)?.to_vec(),
        )),
        _ => Err(corrupt(
            "source reconciliation optional field has an unknown marker",
        )),
    }
}
