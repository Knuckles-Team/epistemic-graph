//! Exact-length msgpack encoding and bounded decoding of one record.

use super::model::ConsensusTransactionRecord;
use super::validate::{digest, exact_encoded_record_len};
use super::{MAX_CONSENSUS_RECORD_BYTES, MAX_CONSENSUS_RECORD_ITEMS};

pub fn encode_record(record: &ConsensusTransactionRecord) -> Result<Vec<u8>, String> {
    record.validate()?;
    let exact_len = exact_encoded_record_len(record, MAX_CONSENSUS_RECORD_BYTES)?;
    let bytes = rmp_serde::to_vec_named(record)
        .map_err(|_| "unable to encode consensus transaction record".to_string())?;
    if bytes.len() != exact_len {
        return Err("consensus transaction record serialization length changed".to_string());
    }
    Ok(bytes)
}

pub fn decode_record(bytes: &[u8]) -> Result<ConsensusTransactionRecord, String> {
    let record: ConsensusTransactionRecord = eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_CONSENSUS_RECORD_BYTES,
            MAX_CONSENSUS_RECORD_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "invalid durable consensus transaction record".to_string())?;
    record.validate()?;
    Ok(record)
}

pub fn sealed_blob_digest(sealed: &[u8]) -> [u8; 32] {
    digest(sealed)
}
