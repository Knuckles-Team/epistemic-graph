//! Tamper-evident hash-chained audit log (CONCEPT:KG-2.231, Lane O).
//!
//! Every durable mutation appends ONE entry to a per-graph hash CHAIN stored in the
//! redb `AUDIT` table, keyed `(graph, seq)`. Each entry binds the previous entry's
//! hash, so altering, reordering, deleting, or inserting any entry breaks the chain
//! at that position — `Method::AuditVerify{graph}` walks the chain and reports the
//! first break (or OK). This turns the ledger into an append-only, tamper-EVIDENT
//! record (PURE-RUST `sha2`, the RustCrypto SHA-256 already in the dep tree).
//!
//! Stored entry layout (value bytes for `(graph, seq)`):
//! ```text
//!   [ prev_hash (32) | entry_hash (32) | line (UTF-8, variable) ]
//! ```
//! where `entry_hash = SHA256( prev_hash || graph || seq_le_u64 || line )` and the
//! genesis entry (`seq == 0`) uses an all-zero `prev_hash`. `line` is a canonical,
//! deterministic description of the mutation (see [`audit_line`]).

#![cfg(feature = "security")]

use sha2::{Digest, Sha256};

use crate::protocol::{AuditReport, Method};

/// A 32-byte chain hash.
pub type Hash = [u8; 32];

/// The genesis previous-hash (all zeros).
pub const GENESIS: Hash = [0u8; 32];

/// Compute one chain link: `entry_hash = SHA256(prev || graph || seq || line)`.
pub fn link_hash(prev: &Hash, graph: &str, seq: u64, line: &[u8]) -> Hash {
    let mut h = Sha256::new();
    h.update(prev);
    h.update(graph.as_bytes());
    h.update(seq.to_le_bytes());
    h.update(line);
    h.finalize().into()
}

/// Serialize one chain entry to its stored value bytes: `prev | hash | line`.
pub fn encode_entry(prev: &Hash, hash: &Hash, line: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(64 + line.len());
    v.extend_from_slice(prev);
    v.extend_from_slice(hash);
    v.extend_from_slice(line);
    v
}

/// Decode a stored chain entry into `(prev_hash, entry_hash, line)`. `None` when the
/// blob is too short to hold the two hashes (a corrupt/truncated row).
pub fn decode_entry(blob: &[u8]) -> Option<(Hash, Hash, &[u8])> {
    if blob.len() < 64 {
        return None;
    }
    let mut prev = [0u8; 32];
    let mut hash = [0u8; 32];
    prev.copy_from_slice(&blob[0..32]);
    hash.copy_from_slice(&blob[32..64]);
    Some((prev, hash, &blob[64..]))
}

/// A canonical, deterministic one-line description of a durable mutation, used as the
/// chained audit `line`. Mirrors the GraphCore ledger vocabulary (`ADD_NODE|…`) but
/// is derived purely from the `Method`, so the audit chain is self-contained (it does
/// not depend on the in-RAM ledger). Multi-row methods (BatchUpdate/ClearGraph) get a
/// single summarizing line — the chain proves the SEQUENCE of operations was not
/// tampered with, which is the audit property.
pub fn audit_line(method: &Method) -> Option<String> {
    let line = match method {
        Method::AddNode { node_id, .. } => format!("ADD_NODE|{node_id}"),
        Method::RemoveNode { node_id } => format!("REMOVE_NODE|{node_id}"),
        Method::CompareAndSetNodeFields { node_id, .. } => format!("CAS_NODE|{node_id}"),
        Method::AddEdge {
            source_id,
            target_id,
            ..
        } => format!("ADD_EDGE|{source_id}|{target_id}"),
        Method::RemoveEdge {
            source_id,
            target_id,
        } => format!("REMOVE_EDGE|{source_id}|{target_id}"),
        Method::BatchUpdate { .. } => "BATCH_UPDATE".to_string(),
        Method::ClearGraph => "CLEAR_GRAPH".to_string(),
        // Other durable mutations (RDF AddTriples, etc.) still chain — by their op name.
        _ => return None,
    };
    Some(line)
}

/// Walk an ordered iterator of `(seq, stored_blob)` entries and verify the chain.
/// The caller supplies the entries in seq order (the redb range scan does this).
pub fn verify_chain<'a, I>(graph: &str, entries: I) -> AuditReport
where
    I: IntoIterator<Item = (u64, &'a [u8])>,
{
    let mut prev = GENESIS;
    // `idx` (0-based position) IS the seq expected at this step — entries arrive in
    // seq order, so a mismatch is a gap (deleted/reordered entry). `walked` counts the
    // entries verified OK before any break (== idx at the break point).
    let mut walked = 0u64;
    for (idx, (seq, blob)) in entries.into_iter().enumerate() {
        let expected_seq = idx as u64;
        if seq != expected_seq {
            return AuditReport {
                graph: graph.to_string(),
                ok: false,
                entries: walked,
                first_broken_seq: Some(seq),
                detail: format!("sequence gap: expected seq {expected_seq}, found {seq}"),
            };
        }
        let (stored_prev, stored_hash, line) = match decode_entry(blob) {
            Some(t) => t,
            None => {
                return AuditReport {
                    graph: graph.to_string(),
                    ok: false,
                    entries: walked,
                    first_broken_seq: Some(seq),
                    detail: "corrupt entry (too short to hold chain hashes)".to_string(),
                };
            }
        };
        // The stored prev must match what we carried forward (catches a deleted /
        // reordered predecessor), and the stored hash must equal the recomputed link
        // (catches an edited line or hash).
        let recomputed = link_hash(&prev, graph, seq, line);
        if stored_prev != prev || stored_hash != recomputed {
            return AuditReport {
                graph: graph.to_string(),
                ok: false,
                entries: walked,
                first_broken_seq: Some(seq),
                detail: format!("hash-chain break at seq {seq} (entry mutated or chain altered)"),
            };
        }
        prev = stored_hash;
        walked += 1;
    }
    AuditReport {
        graph: graph.to_string(),
        ok: true,
        entries: walked,
        first_broken_seq: None,
        detail: "chain verified".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a clean N-entry chain returning the stored blobs in seq order.
    fn build_chain(graph: &str, lines: &[&str]) -> Vec<Vec<u8>> {
        let mut prev = GENESIS;
        let mut out = Vec::new();
        for (seq, line) in lines.iter().enumerate() {
            let hash = link_hash(&prev, graph, seq as u64, line.as_bytes());
            out.push(encode_entry(&prev, &hash, line.as_bytes()));
            prev = hash;
        }
        out
    }

    #[test]
    fn clean_chain_verifies() {
        let g = "agent:a";
        let blobs = build_chain(g, &["ADD_NODE|n1", "ADD_NODE|n2", "ADD_EDGE|n1|n2"]);
        let report = verify_chain(
            g,
            blobs
                .iter()
                .enumerate()
                .map(|(i, b)| (i as u64, b.as_slice())),
        );
        assert!(report.ok, "{report:?}");
        assert_eq!(report.entries, 3);
        assert!(report.first_broken_seq.is_none());
    }

    #[test]
    fn mutated_entry_detected_at_position() {
        let g = "agent:a";
        let mut blobs = build_chain(g, &["ADD_NODE|n1", "ADD_NODE|n2", "ADD_EDGE|n1|n2"]);
        // Tamper the LINE of entry seq=1 (flip its content) WITHOUT recomputing hashes.
        let last = blobs[1].len() - 1;
        blobs[1][last] ^= 0xFF;
        let report = verify_chain(
            g,
            blobs
                .iter()
                .enumerate()
                .map(|(i, b)| (i as u64, b.as_slice())),
        );
        assert!(!report.ok);
        assert_eq!(report.first_broken_seq, Some(1));
    }

    #[test]
    fn deleted_entry_breaks_chain() {
        let g = "agent:a";
        let blobs = build_chain(g, &["ADD_NODE|n1", "ADD_NODE|n2", "ADD_EDGE|n1|n2"]);
        // Drop seq=1: the surviving entries are now seqs 0 and 2 → a gap is detected.
        let kept: Vec<(u64, &[u8])> = vec![(0, blobs[0].as_slice()), (2, blobs[2].as_slice())];
        let report = verify_chain(g, kept);
        assert!(!report.ok);
        assert_eq!(report.first_broken_seq, Some(2));
    }
}
