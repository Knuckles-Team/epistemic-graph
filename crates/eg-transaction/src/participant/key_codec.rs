//! Storage-key encoding for consensus-transaction records.

use super::model::ConsensusTransactionRecordKey;
use super::validate::validate_coordinator_id;
use super::MAX_ENCODED_RECORD_KEY_BYTES;

/// Storage key order is coordinator, record kind, then participant id when the
/// kind has one. Hex encoding keeps arbitrary coordinator bytes unambiguous and
/// leaves the parent shape free of a sentinel participant id.
pub fn encode_record_key(key: &ConsensusTransactionRecordKey) -> Result<String, String> {
    let (coordinator_id, suffix) = match key {
        ConsensusTransactionRecordKey::Parent { coordinator_id } => {
            (coordinator_id, "p".to_string())
        }
        ConsensusTransactionRecordKey::Participant {
            coordinator_id,
            participant_id,
        } => {
            if *participant_id == 0 {
                return Err("consensus participant id must be positive".to_string());
            }
            (coordinator_id, format!("c/{participant_id:016x}"))
        }
    };
    validate_coordinator_id(coordinator_id)?;
    Ok(format!(
        "{}/{suffix}",
        hex::encode(coordinator_id.as_bytes())
    ))
}

pub fn decode_record_key(value: &str) -> Result<ConsensusTransactionRecordKey, String> {
    if value.is_empty() || value.len() > MAX_ENCODED_RECORD_KEY_BYTES {
        return Err("invalid consensus transaction record key".to_string());
    }
    let mut fields = value.split('/');
    let encoded_coordinator = fields
        .next()
        .ok_or_else(|| "invalid consensus transaction record key".to_string())?;
    let kind = fields
        .next()
        .ok_or_else(|| "invalid consensus transaction record key".to_string())?;
    let coordinator_id = String::from_utf8(
        hex::decode(encoded_coordinator)
            .map_err(|_| "invalid consensus transaction record key".to_string())?,
    )
    .map_err(|_| "invalid consensus transaction record key".to_string())?;
    validate_coordinator_id(&coordinator_id)?;
    match (kind, fields.next(), fields.next()) {
        ("p", None, None) => Ok(ConsensusTransactionRecordKey::Parent { coordinator_id }),
        ("c", Some(participant), None) if participant.len() == 16 => {
            let participant_id = u64::from_str_radix(participant, 16)
                .map_err(|_| "invalid consensus transaction record key".to_string())?;
            if participant_id == 0 {
                return Err("consensus participant id must be positive".to_string());
            }
            Ok(ConsensusTransactionRecordKey::Participant {
                coordinator_id,
                participant_id,
            })
        }
        _ => Err("invalid consensus transaction record key".to_string()),
    }
}
