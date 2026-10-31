//! Durable graph prepares bind the persisted table key to the exact encoded payload.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    prepared_slices_exceed_limits, GraphSlice, GroupId, MAX_PREPARED_TXN_BYTES,
    MAX_PREPARED_TXN_ITEMS,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PrepareRecord {
    version: u8,
    owner: String,
    transaction: String,
    participant: GroupId,
    #[serde(with = "serde_bytes")]
    payload: Vec<u8>,
    digest: [u8; 32],
}

fn digest(transaction: &str, participant: GroupId, payload: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"medulla.graph-prepare.v1\0");
    hash.update((transaction.len() as u64).to_be_bytes());
    hash.update(transaction.as_bytes());
    hash.update(participant.to_be_bytes());
    hash.update((payload.len() as u64).to_be_bytes());
    hash.update(payload);
    hash.finalize().into()
}

fn decode<T: serde::de::DeserializeOwned>(blob: &[u8]) -> Result<T, String> {
    medulla_types::msgpack::decode_bounded(
        blob,
        medulla_types::msgpack::MsgpackLimits::new(
            MAX_PREPARED_TXN_BYTES,
            MAX_PREPARED_TXN_ITEMS,
            medulla_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "invalid durable cross-shard prepare record".to_string())
}

pub(super) fn encode_prepared_slices(
    transaction: &str,
    participant: GroupId,
    slices: &[GraphSlice],
) -> Result<Vec<u8>, String> {
    if prepared_slices_exceed_limits(slices) {
        return Err("cross-shard prepare exceeds limits".to_string());
    }
    let payload = rmp_serde::to_vec_named(slices).map_err(|e| e.to_string())?;
    let record = PrepareRecord {
        version: 1,
        owner: "graph-shard".to_string(),
        transaction: transaction.to_string(),
        participant,
        digest: digest(transaction, participant, &payload),
        payload,
    };
    let blob = rmp_serde::to_vec_named(&record).map_err(|e| e.to_string())?;
    if blob.len() > MAX_PREPARED_TXN_BYTES {
        return Err("cross-shard prepare exceeds limits".to_string());
    }
    Ok(blob)
}

pub(super) fn decode_prepared_slices(
    blob: &[u8],
    transaction: &str,
    participant: GroupId,
) -> Result<Vec<GraphSlice>, String> {
    // Legacy prepares are MessagePack arrays. A new malformed map must never
    // fall back to that legacy interpretation.
    let slices = if matches!(blob.first(), Some(0x90..=0x9f | 0xdc | 0xdd)) {
        decode::<Vec<GraphSlice>>(blob)?
    } else {
        let record: PrepareRecord = decode(blob)?;
        if record.version != 1
            || record.owner != "graph-shard"
            || record.transaction != transaction
            || record.participant != participant
            || record.digest != digest(transaction, participant, &record.payload)
        {
            return Err("cross-shard prepare identity or digest mismatch".to_string());
        }
        decode::<Vec<GraphSlice>>(&record.payload)?
    };
    if prepared_slices_exceed_limits(&slices) {
        return Err("durable cross-shard prepare record exceeds limits".to_string());
    }
    Ok(slices)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slices() -> Vec<GraphSlice> {
        vec![GraphSlice {
            graph_name: "fixture".into(),
            graph_fname: "fixture".into(),
            graph_type: crate::protocol::GraphType::Global,
            methods: vec![],
            placement_epoch: 0,
            fencing_token: None,
        }]
    }

    // spec: MEDULLA-DECISION-ENGINE-R036.102
    #[test]
    fn bound_prepare_round_trip_and_legacy() {
        let expected = slices();
        let encoded = encode_prepared_slices("txn", 7, &expected).unwrap();
        assert_eq!(
            decode_prepared_slices(&encoded, "txn", 7).unwrap()[0].graph_name,
            "fixture"
        );
        let legacy = rmp_serde::to_vec_named(&expected).unwrap();
        assert_eq!(decode_prepared_slices(&legacy, "txn", 7).unwrap().len(), 1);
    }

    // spec: MEDULLA-DECISION-ENGINE-R036.102
    #[test]
    fn bound_prepare_rejects_swapped_keys() {
        let encoded = encode_prepared_slices("txn", 7, &slices()).unwrap();
        assert!(decode_prepared_slices(&encoded, "other", 7).is_err());
        assert!(decode_prepared_slices(&encoded, "txn", 8).is_err());
    }

    // spec: MEDULLA-DECISION-ENGINE-R036.102
    #[test]
    fn bound_prepare_rejects_corruption_and_unsupported_owner() {
        for variant in 0..4 {
            let encoded = encode_prepared_slices("txn", 7, &slices()).unwrap();
            let mut record: PrepareRecord = decode(&encoded).unwrap();
            match variant {
                0 => record.version = 2,
                1 => record.owner = "rbac".into(),
                2 => record.payload[0] ^= 1,
                _ => record.digest[0] ^= 1,
            }
            let changed = rmp_serde::to_vec_named(&record).unwrap();
            assert!(decode_prepared_slices(&changed, "txn", 7).is_err());
        }
    }

    // spec: MEDULLA-DECISION-ENGINE-R036.102
    #[test]
    fn bound_prepare_rejects_invalid_slice_and_truncated_record() {
        let mut invalid = slices();
        invalid[0].graph_name.clear();
        assert!(encode_prepared_slices("txn", 7, &invalid).is_err());
        let legacy = rmp_serde::to_vec_named(&invalid).unwrap();
        assert!(decode_prepared_slices(&legacy, "txn", 7).is_err());
        assert!(decode_prepared_slices(&[0x81], "txn", 7).is_err());
    }
}
