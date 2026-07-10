//! `StagedWrite` — the shape a `ModalityContract` value takes on while it is staged
//! inside a transaction, before it is either committed (applied durably) or rolled
//! back (discarded, as if it never happened).
//!
//! Modeled on the two staging patterns already in the engine: `eg-tsdb`'s
//! `StagedSeries` (an in-txn overlay of uncommitted `(ts, field_values)` points, read
//! through for read-your-own-writes, then either merged into the durable store on
//! commit or simply dropped on abort — CONCEPT:EG-KG.query.txn-tsdb-read-your) and the
//! engine's WAL (`src/wal.rs`: a durable mutation is appended, then either replayed on
//! restart or truncated at a checkpoint). `StagedWrite` generalizes the SHAPE of that
//! staging step across modalities — an id, a `Put`/`Delete` kind, and an opaque
//! serialized payload — without prescribing HOW a modality's own txn engine actually
//! holds or applies it (that stays modality-specific, exactly like `StagedSeries` vs.
//! the WAL are today).

use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// Whether a staged write sets a value or removes one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WriteKind {
    Put,
    Delete,
}

/// An inert, serializable description of one staged write. `ModalityContract::
/// txn_stage` only DESCRIBES the write — applying it durably or rolling it back is
/// the caller's (the transaction engine's) responsibility, exactly as `eg-tsdb`'s
/// `StagedSeries` overlay is read-through but committed/discarded by the surrounding
/// `GraphTxnState`, not by the series value itself.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StagedWrite {
    /// The id this write affects (the same id passed to `to_rowset`/`txn_stage`).
    pub id: String,
    pub kind: WriteKind,
    /// The serialized modality value (`encode_staged`), empty for `Delete`.
    #[serde(default)]
    pub payload: Vec<u8>,
}

impl StagedWrite {
    /// Stage a `Put` of `payload` under `id` (typically `encode_staged(self)` from
    /// inside `txn_stage`).
    pub fn put(id: impl Into<String>, payload: Vec<u8>) -> Self {
        Self {
            id: id.into(),
            kind: WriteKind::Put,
            payload,
        }
    }

    /// Stage a `Delete` of `id` (no payload).
    pub fn delete(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            kind: WriteKind::Delete,
            payload: Vec::new(),
        }
    }

    /// Roll the staged write back — i.e. discard it, as an aborted transaction
    /// would. Returns the id that would have been affected (for the caller's own
    /// uncommitted-overlay bookkeeping, mirroring how `StagedSeries` is simply
    /// dropped on abort); a `StagedWrite` carries no other rollback state because it
    /// was never applied in the first place — there is nothing else to undo.
    pub fn rollback(&self) -> Option<String> {
        Some(self.id.clone())
    }
}

/// Serialize a modality value into a `StagedWrite` payload (`serde_json` — an opaque,
/// transport-agnostic encoding; a modality's OWN durable format, e.g. eg-tensor's
/// compact byte-blob codec, is unrelated and untouched by this).
pub fn encode_staged<T: Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_vec(value).expect("a ModalityContract value must be serde_json-serializable")
}

/// The inverse of `encode_staged`: decode a `StagedWrite`'s payload back into `T`.
/// Used by the conformance macro's round-trip test; a modality's real txn engine is
/// free to use its own decode path instead.
pub fn decode_staged<T: DeserializeOwned>(staged: &StagedWrite) -> Result<T, serde_json::Error> {
    serde_json::from_slice(&staged.payload)
}
