//! The canonical byte codec of the source reconciliation checkpoint.

use super::reconciliation::{
    valid_source_entity_id_for_reconciliation, validate_sql_source_revision,
    SemanticSourceReconciliationCheckpoint, SemanticSourceReconciliationPhase,
    SEMANTIC_RECONCILIATION_CHECKPOINT_MAGIC, SEMANTIC_RECONCILIATION_MAX_CURSOR_BYTES,
    SEMANTIC_RECONCILIATION_MAX_ENTITY_BYTES, SEMANTIC_RECONCILIATION_MAX_REVISION_BYTES,
};
use super::SemanticCodeError;
use eg_types::semantic_index::SemanticDigest;

pub(super) fn validate_reconciliation_checkpoint(
    checkpoint: &SemanticSourceReconciliationCheckpoint,
) -> Result<(), SemanticCodeError> {
    if checkpoint.source_wakeup_digest == SemanticDigest::from_bytes([0; 32]) {
        return Err(SemanticCodeError::Refused(
            "source reconciliation wakeup identity is zero".to_string(),
        ));
    }
    validate_sql_source_revision(&checkpoint.source_revision)?;
    if checkpoint.source_revision.len() > SEMANTIC_RECONCILIATION_MAX_REVISION_BYTES {
        return Err(SemanticCodeError::Refused(
            "source reconciliation revision exceeds the bounded size".to_string(),
        ));
    }
    if let Some(cursor) = checkpoint.source_cursor.as_ref() {
        if cursor.is_empty() || cursor.len() > SEMANTIC_RECONCILIATION_MAX_CURSOR_BYTES {
            return Err(SemanticCodeError::Refused(
                "source reconciliation cursor exceeds the bounded size".to_string(),
            ));
        }
    }
    if let Some(cursor) = checkpoint.prior_cursor.as_deref() {
        if !valid_source_entity_id_for_reconciliation(cursor) {
            return Err(SemanticCodeError::Refused(
                "source reconciliation prior cursor is not a source identity".to_string(),
            ));
        }
    }
    match &checkpoint.phase {
        SemanticSourceReconciliationPhase::Scanning => {
            if checkpoint.source_cursor.is_none()
                || checkpoint.prior_cursor.is_some()
                || checkpoint.complete_snapshot_receipt_digest.is_some()
            {
                return Err(SemanticCodeError::Refused(
                    "scanning reconciliation checkpoint has invalid phase fields".to_string(),
                ));
            }
        }
        SemanticSourceReconciliationPhase::FinalizingTombstones => {
            if checkpoint.source_cursor.is_some()
                || checkpoint.complete_snapshot_receipt_digest.is_none()
            {
                return Err(SemanticCodeError::Refused(
                    "tombstone reconciliation checkpoint has invalid phase fields".to_string(),
                ));
            }
        }
    }
    if checkpoint
        .complete_snapshot_receipt_digest
        .is_some_and(|digest| digest == SemanticDigest::from_bytes([0; 32]))
    {
        return Err(SemanticCodeError::Refused(
            "source reconciliation complete snapshot proof is zero".to_string(),
        ));
    }
    Ok(())
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
    if encoded.len() > 16 * 1024 {
        return Err(SemanticCodeError::Refused(
            "source reconciliation checkpoint exceeds the bounded size".to_string(),
        ));
    }
    Ok(encoded)
}

fn append_checkpoint_bytes(encoded: &mut Vec<u8>, bytes: &[u8]) -> Result<(), SemanticCodeError> {
    let length = u32::try_from(bytes.len()).map_err(|_| {
        SemanticCodeError::Refused("source reconciliation field is too large".to_string())
    })?;
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
    if encoded.len() > 16 * 1024 || !encoded.starts_with(SEMANTIC_RECONCILIATION_CHECKPOINT_MAGIC) {
        return Err(SemanticCodeError::Corrupt(
            "source reconciliation checkpoint has an unknown encoding".to_string(),
        ));
    }
    let mut offset = SEMANTIC_RECONCILIATION_CHECKPOINT_MAGIC.len();
    let wakeup_bytes = take_checkpoint_fixed(encoded, &mut offset, 32)?;
    let source_wakeup_digest =
        SemanticDigest::from_bytes(wakeup_bytes.try_into().map_err(|_| {
            SemanticCodeError::Corrupt("source reconciliation wakeup is not 32 bytes".to_string())
        })?);
    let source_revision = String::from_utf8(
        take_checkpoint_bytes(
            encoded,
            &mut offset,
            SEMANTIC_RECONCILIATION_MAX_REVISION_BYTES,
        )?
        .to_vec(),
    )
    .map_err(|_| {
        SemanticCodeError::Corrupt("source reconciliation revision is not UTF-8".to_string())
    })?;
    let phase = match take_checkpoint_byte(encoded, &mut offset)? {
        0 => SemanticSourceReconciliationPhase::Scanning,
        1 => SemanticSourceReconciliationPhase::FinalizingTombstones,
        _ => {
            return Err(SemanticCodeError::Corrupt(
                "source reconciliation checkpoint has an unknown phase".to_string(),
            ));
        }
    };
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
    .map(|bytes| String::from_utf8(bytes.to_vec()))
    .transpose()
    .map_err(|_| {
        SemanticCodeError::Corrupt("source reconciliation cursor is not UTF-8".to_string())
    })?;
    let rows_seen = take_checkpoint_u64(encoded, &mut offset)?;
    let source_bytes_seen = take_checkpoint_u64(encoded, &mut offset)?;
    let pages_seen = take_checkpoint_u64(encoded, &mut offset)?;
    let complete_snapshot_receipt_digest = match take_checkpoint_byte(encoded, &mut offset)? {
        0 => None,
        1 => {
            let bytes = take_checkpoint_fixed(encoded, &mut offset, 32)?;
            Some(SemanticDigest::from_bytes(bytes.try_into().map_err(
                |_| {
                    SemanticCodeError::Corrupt(
                        "source reconciliation proof is not 32 bytes".to_string(),
                    )
                },
            )?))
        }
        _ => {
            return Err(SemanticCodeError::Corrupt(
                "source reconciliation checkpoint has an unknown proof marker".to_string(),
            ));
        }
    };
    if offset != encoded.len() {
        return Err(SemanticCodeError::Corrupt(
            "source reconciliation checkpoint has trailing bytes".to_string(),
        ));
    }
    let checkpoint = SemanticSourceReconciliationCheckpoint {
        source_wakeup_digest,
        source_revision,
        phase,
        source_cursor,
        prior_cursor,
        rows_seen,
        source_bytes_seen,
        pages_seen,
        complete_snapshot_receipt_digest,
    };
    validate_reconciliation_checkpoint(&checkpoint)
        .map_err(|error| SemanticCodeError::Corrupt(error.to_string()))?;
    Ok(checkpoint)
}

fn take_checkpoint_byte(encoded: &[u8], offset: &mut usize) -> Result<u8, SemanticCodeError> {
    let byte = *encoded.get(*offset).ok_or_else(|| {
        SemanticCodeError::Corrupt("source reconciliation checkpoint is truncated".to_string())
    })?;
    *offset += 1;
    Ok(byte)
}

fn take_checkpoint_fixed<'a>(
    encoded: &'a [u8],
    offset: &mut usize,
    length: usize,
) -> Result<&'a [u8], SemanticCodeError> {
    let end = (*offset).checked_add(length).ok_or_else(|| {
        SemanticCodeError::Corrupt("source reconciliation checkpoint overflows".to_string())
    })?;
    let bytes = encoded.get(*offset..end).ok_or_else(|| {
        SemanticCodeError::Corrupt("source reconciliation checkpoint is truncated".to_string())
    })?;
    *offset = end;
    Ok(bytes)
}

fn take_checkpoint_u64(encoded: &[u8], offset: &mut usize) -> Result<u64, SemanticCodeError> {
    let bytes = take_checkpoint_fixed(encoded, offset, 8)?;
    Ok(u64::from_be_bytes(bytes.try_into().map_err(|_| {
        SemanticCodeError::Corrupt("source reconciliation counter is not 64 bits".to_string())
    })?))
}

fn take_checkpoint_bytes<'a>(
    encoded: &'a [u8],
    offset: &mut usize,
    max_length: usize,
) -> Result<&'a [u8], SemanticCodeError> {
    let length = u32::from_be_bytes(
        take_checkpoint_fixed(encoded, offset, 4)?
            .try_into()
            .map_err(|_| {
                SemanticCodeError::Corrupt(
                    "source reconciliation field length is not 32 bits".to_string(),
                )
            })?,
    ) as usize;
    if length > max_length {
        return Err(SemanticCodeError::Corrupt(
            "source reconciliation field exceeds the bounded size".to_string(),
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
        _ => Err(SemanticCodeError::Corrupt(
            "source reconciliation optional field has an unknown marker".to_string(),
        )),
    }
}
