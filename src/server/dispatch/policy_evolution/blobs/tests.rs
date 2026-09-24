//! Frozen-`log_q` replay over the engine's own Blob CAS (a real
//! `RedbChunkStore`, not a fake): the sampler facts a capture holds come back
//! bit-exact from the digest the record names, and the hold check refuses any
//! reference the caller does not own at exactly the declared length.

use eg_types::contract::Digest256;
use eg_types::policy_evolution::{ArrayEncoding, PolicyCapture, PolicyEvolutionRecord};

use super::*;
use crate::server::blob::store::test_support::{chunked_as, reassemble};
use crate::server::blob::{ChunkStore, RedbChunkStore};

const OWNER: &str = "carrier-owner:policy-test";

fn held(
    store: &dyn ChunkStore,
    bytes: &[u8],
    encoding: ArrayEncoding,
    elements: u32,
) -> HeldBlobRef {
    let committed = chunked_as(store, bytes, 8, OWNER);
    HeldBlobRef {
        digest: Digest256::parse(&committed.digest).unwrap(),
        length: bytes.len() as u64,
        encoding,
        elements,
    }
}

fn le_bytes<const N: usize>(values: impl IntoIterator<Item = [u8; N]>) -> Vec<u8> {
    values.into_iter().flatten().collect()
}

fn capture_with(arrays: [HeldBlobRef; 3]) -> PolicyEvolutionRecord {
    let [token_ids, log_q, action_mask] = arrays;
    let record: PolicyCapture = serde_json::from_value(serde_json::json!({
        "capability_id": format!("polcap:{}", "ab".repeat(32)),
        "sampler_version_id": format!("polver:{}", "cd".repeat(32)),
        "trajectory_id": "trajectory:0001", "trajectory_steps": 1, "completion": "terminal",
        "token_count": 6, "policy_token_count": 4,
        "token_ids": token_ids, "log_q": log_q, "action_mask": action_mask,
        "purpose": "training", "trace_fidelity": "full", "captured_at_ms": 3,
    }))
    .unwrap();
    record.validate().unwrap();
    PolicyEvolutionRecord::Capture { record }
}

#[test]
fn frozen_log_q_replays_bit_exact_from_the_engine_blob_cas() {
    let store = RedbChunkStore::open_temp().unwrap();
    let log_q = [-0.25_f32, -1.5, -3.0e-7, f32::MIN_POSITIVE.ln()];
    let q = held(
        &store,
        &le_bytes(log_q.map(f32::to_le_bytes)),
        ArrayEncoding::F32Le,
        4,
    );
    let ids = held(
        &store,
        &le_bytes([1_u32, 2, 7, 9, 11, 40].map(u32::to_le_bytes)),
        ArrayEncoding::U32Le,
        6,
    );
    let mask = held(&store, &[0, 0, 1, 1, 1, 1], ArrayEncoding::U8Mask, 6);
    check_held(&store, OWNER, &[ids, q, mask]).unwrap();

    // Replay: the digest the record holds resolves to the sampler's exact bits.
    let manifest = store.get_manifest(&q.digest.to_hex()).unwrap().unwrap();
    let replayed: Vec<u32> = reassemble(&store, &manifest)
        .chunks_exact(4)
        .map(|word| u32::from_le_bytes(word.try_into().unwrap()))
        .collect();
    assert_eq!(replayed, log_q.map(f32::to_bits));

    // The q facts are part of the record's identity: a recomputed q (say under
    // a newer checkpoint) is a different blob and therefore a different record,
    // never an overwrite of the frozen one.
    let frozen = capture_with([ids, q, mask]).record_id("tenant-a").unwrap();
    assert_eq!(
        frozen,
        capture_with([ids, q, mask]).record_id("tenant-a").unwrap()
    );
    let recomputed = held(
        &store,
        &le_bytes([-0.5_f32; 4].map(f32::to_le_bytes)),
        ArrayEncoding::F32Le,
        4,
    );
    assert_ne!(
        frozen,
        capture_with([ids, recomputed, mask])
            .record_id("tenant-a")
            .unwrap()
    );
}

#[test]
fn a_reference_the_caller_does_not_hold_exactly_is_refused() {
    let store = RedbChunkStore::open_temp().unwrap();
    let q = held(
        &store,
        &le_bytes([-1.0_f32; 2].map(f32::to_le_bytes)),
        ArrayEncoding::F32Le,
        2,
    );
    let missing = |result: Result<(), PolicyRefusal>| result.unwrap_err().code();
    assert_eq!(
        missing(check_held(&store, "carrier-owner:someone-else", &[q])),
        "POLICY_BLOB_MISSING"
    );
    let longer = HeldBlobRef {
        length: q.length + 4,
        ..q
    };
    assert_eq!(
        missing(check_held(&store, OWNER, &[longer])),
        "POLICY_BLOB_MISSING"
    );
    let unknown = HeldBlobRef {
        digest: Digest256::from_bytes([9; 32]),
        ..q
    };
    assert_eq!(
        missing(check_held(&store, OWNER, &[unknown])),
        "POLICY_BLOB_MISSING"
    );
}
