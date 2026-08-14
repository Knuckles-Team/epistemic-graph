//! GOC-04 (`plans/graph-os-completion-program/lanes/GOC-04-modality-row-chunk-persistence.md`)
//! W02 — versioned row/event/chunk record schemas.
//!
//! ## Scope of this module, and what it deliberately does NOT do yet
//!
//! GOC-04 replaces the whole-runtime `ServedModalityRuntime` snapshot
//! (`served.rs`) with durable, independently addressable rows/events/chunks. Its
//! own design section requires every mutation to go through "GOC-03 prepare
//! participant -> durable rows/manifests/refcount/event -> commit publish +
//! cursor fence" — i.e. the row/event schema is meant to carry a `commit_id`
//! bound to GOC-03's cross-domain commit descriptor.
//!
//! At the time this module was written, GOC-03 ("cross-domain commit currency
//! and projection fences") had not published an interface-freeze decision: no
//! record exists under `plans/graph-os-completion-program/decisions/` or
//! `ledger-events/lanes/` for GOC-03, its `UNIFIED-LEDGER.md` row is still
//! `PROPOSED` with no claimed owner, and no `goc-03-*` worktree exists alongside
//! the other active GOC branches. `DEPENDENCY-MAP.md` marks `GOC-04 <- GOC-03` as
//! an `interface_freeze` edge ("Design/consumer work may start after a reviewed
//! protocol/schema decision" — but the decision must exist), and this lane's own
//! `AGENTS.md`/lane doc "Luna execution instructions" say explicitly: **"Stop for
//! orchestrator guidance if GOC-03 cursor ... fields are not frozen."**
//!
//! So this module defines the row/event/chunk **wire and validation shapes**
//! only — encode/decode, canonical keys, and digest/sequence invariants that do
//! not depend on GOC-03's commit descriptor shape (a `commit_id: OpaqueRef` is
//! carried as an opaque forward reference, not interpreted). It is **not**
//! wired into `served.rs`'s load/store path, the redb persistence registration,
//! or the mutation dispatch handlers (`src/server/handlers/modality.rs`) — that
//! wiring is GOC-04-W03/W04 and requires the GOC-03 commit/barrier contract this
//! module intentionally does not invent. See the lane's handoff checklist.

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;

use crate::artifact::{Classification, OpaqueRef, ProtocolError, ResourceId};

/// Schema version for every row/event/chunk record in this module. Independent
/// of [`crate::artifact::ARTIFACT_PROTOCOL_VERSION`] (that versions the
/// semantic artifact/evidence protocol; this versions the durable row/event/
/// chunk wire shape layered underneath it).
pub const MODALITY_ROW_SCHEMA_VERSION: u16 = 1;

/// Authoritative table names (lane doc "Storage schema"). Kept as protocol
/// constants here — the single place a persistence adapter (GOC-04-W03) reads
/// them from — rather than re-declared per call site.
pub const MODALITY_ARTIFACT: &str = "MODALITY_ARTIFACT";
pub const MODALITY_OCCURRENCE: &str = "MODALITY_OCCURRENCE";
pub const MODALITY_RENDITION: &str = "MODALITY_RENDITION";
pub const MODALITY_SEGMENT: &str = "MODALITY_SEGMENT";
pub const MODALITY_FEATURE: &str = "MODALITY_FEATURE";
pub const MODALITY_EVIDENCE_LOCUS: &str = "MODALITY_EVIDENCE_LOCUS";
pub const MODALITY_CHUNK_MANIFEST: &str = "MODALITY_CHUNK_MANIFEST";
pub const MODALITY_EVENT: &str = "MODALITY_EVENT";
pub const MODALITY_IDEMPOTENCY: &str = "MODALITY_IDEMPOTENCY";
pub const MODALITY_INDEX_POSTING: &str = "MODALITY_INDEX_POSTING";
pub const MODALITY_PROJECTION_CURSOR: &str = "MODALITY_PROJECTION_CURSOR";

/// Every table name this module defines, for a persistence adapter's own
/// registration/iteration (GOC-04-W03) without hand-duplicating the list.
pub const MODALITY_ROW_TABLES: &[&str] = &[
    MODALITY_ARTIFACT,
    MODALITY_OCCURRENCE,
    MODALITY_RENDITION,
    MODALITY_SEGMENT,
    MODALITY_FEATURE,
    MODALITY_EVIDENCE_LOCUS,
    MODALITY_CHUNK_MANIFEST,
    MODALITY_EVENT,
    MODALITY_IDEMPOTENCY,
    MODALITY_INDEX_POSTING,
    MODALITY_PROJECTION_CURSOR,
];

/// Rejection reasons for a malformed row/event/chunk record. Privacy-safe, like
/// [`ProtocolError`]: no rejected value is echoed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowSchemaError {
    UnsupportedVersion,
    ZeroSequence,
    EmptyChunkManifest,
    ChunkCountMismatch,
    ManifestDigestMismatch,
    NonContiguousSequence,
    Codec,
}

impl fmt::Display for RowSchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::UnsupportedVersion => "unsupported modality row schema version",
            Self::ZeroSequence => "zero is not a valid event sequence",
            Self::EmptyChunkManifest => "chunk manifest has no chunk references",
            Self::ChunkCountMismatch => "chunk manifest chunk count does not match total/size",
            Self::ManifestDigestMismatch => "chunk manifest digest does not match its contents",
            Self::NonContiguousSequence => "event sequence is not contiguous",
            Self::Codec => "row/event/chunk record failed to encode or decode",
        })
    }
}

impl std::error::Error for RowSchemaError {}

impl From<ProtocolError> for RowSchemaError {
    fn from(_: ProtocolError) -> Self {
        Self::Codec
    }
}

/// The canonical `(tenant_ref, typed_id)` composite key (lane doc invariant 2:
/// "Keys include tenant and artifact identity; no cross-tenant lookup is
/// possible by key construction alone"). `encode` is a deterministic,
/// length-prefixed byte encoding suitable for a redb key once GOC-04-W03 wires
/// this into persistence; it is exercised here only for round-trip and
/// tenant-isolation properties.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowKey {
    pub tenant_ref: OpaqueRef,
    pub resource: ResourceId,
}

impl RowKey {
    pub fn new(tenant_ref: OpaqueRef, resource: ResourceId) -> Self {
        Self {
            tenant_ref,
            resource,
        }
    }

    /// Deterministic bytes: `len(tenant) || tenant || len(resource_opaque) ||
    /// resource_opaque`. Length-prefixing (rather than a separator byte) makes
    /// two keys with different tenant/resource boundaries unable to collide,
    /// even though [`OpaqueRef`]'s charset already excludes the low ASCII range
    /// a separator would use.
    pub fn encode(&self) -> Vec<u8> {
        let tenant = self.tenant_ref.as_str().as_bytes();
        let resource = self.resource.opaque().as_str().as_bytes();
        let mut out = Vec::with_capacity(8 + tenant.len() + resource.len());
        out.extend_from_slice(&(tenant.len() as u32).to_be_bytes());
        out.extend_from_slice(tenant);
        out.extend_from_slice(&(resource.len() as u32).to_be_bytes());
        out.extend_from_slice(resource);
        out
    }
}

/// A durable row envelope wrapping one canonical entity from
/// `crate::artifact` (`Artifact`/`Occurrence`/`Rendition`/`Segment`/`Feature`/
/// `EvidenceLocus`) — "consume canonical typed IDs/policy fields; no duplicate
/// IDs" (lane doc, `artifact.rs` files/modules note). The envelope adds exactly
/// the durability metadata the entity itself does not carry: schema version,
/// tenant, commit sequence, and policy/derivation digests.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModalityRow<T> {
    pub schema_version: u16,
    pub tenant_ref: OpaqueRef,
    /// Monotonic per-modality commit sequence this row was last written at.
    /// Opaque with respect to GOC-03's eventual commit descriptor shape — this
    /// module only requires it be non-zero and does not interpret it further.
    pub commit_seq: u64,
    pub policy_digest: OpaqueRef,
    #[serde(default)]
    pub derivation_digest: Option<OpaqueRef>,
    pub payload: T,
}

impl<T> ModalityRow<T>
where
    T: Serialize + DeserializeOwned + Clone + PartialEq + fmt::Debug,
{
    pub fn new(
        tenant_ref: OpaqueRef,
        commit_seq: u64,
        policy_digest: OpaqueRef,
        derivation_digest: Option<OpaqueRef>,
        payload: T,
    ) -> Result<Self, RowSchemaError> {
        if commit_seq == 0 {
            return Err(RowSchemaError::ZeroSequence);
        }
        Ok(Self {
            schema_version: MODALITY_ROW_SCHEMA_VERSION,
            tenant_ref,
            commit_seq,
            policy_digest,
            derivation_digest,
            payload,
        })
    }

    pub fn encode(&self) -> Result<Vec<u8>, RowSchemaError> {
        serde_json::to_vec(self).map_err(|_| RowSchemaError::Codec)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RowSchemaError> {
        let row: Self = serde_json::from_slice(bytes).map_err(|_| RowSchemaError::Codec)?;
        if row.schema_version != MODALITY_ROW_SCHEMA_VERSION {
            return Err(RowSchemaError::UnsupportedVersion);
        }
        if row.commit_seq == 0 {
            return Err(RowSchemaError::ZeroSequence);
        }
        Ok(row)
    }
}

/// The operation an event records (superset-compatible with `served::
/// ServedEventKind`, plus `Tombstoned` split out from `Deleted` per the lane
/// doc invariant 6: "Deletion creates a tombstone and retention/legal-hold
/// record; physical GC waits for zero authoritative references").
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModalityRowOperation {
    Inserted,
    Updated,
    Deleted,
    Tombstoned,
    Restored,
    Reindexed,
}

/// `ModalityEventV1` (lane doc "Storage schema"): operation, object id, prior
/// digest, resulting digest, sequence, commit reference, actor, policy digest,
/// and idempotency key. `commit_id` is carried opaquely — see module docs on
/// why this does not yet bind to a concrete GOC-03 descriptor type.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModalityEventV1 {
    pub schema_version: u16,
    /// Contiguous per-modality sequence (invariant 3: starts at 1).
    pub sequence: u64,
    pub tenant_ref: OpaqueRef,
    pub object: ResourceId,
    pub operation: ModalityRowOperation,
    #[serde(default)]
    pub prior_digest: Option<[u8; 32]>,
    pub new_digest: [u8; 32],
    pub commit_id: OpaqueRef,
    pub actor: OpaqueRef,
    pub policy_digest: OpaqueRef,
    pub idempotency_key: OpaqueRef,
}

impl ModalityEventV1 {
    pub fn validate(&self) -> Result<(), RowSchemaError> {
        if self.schema_version != MODALITY_ROW_SCHEMA_VERSION {
            return Err(RowSchemaError::UnsupportedVersion);
        }
        if self.sequence == 0 {
            return Err(RowSchemaError::ZeroSequence);
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, RowSchemaError> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|_| RowSchemaError::Codec)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RowSchemaError> {
        let event: Self = serde_json::from_slice(bytes).map_err(|_| RowSchemaError::Codec)?;
        event.validate()?;
        Ok(event)
    }
}

/// Whether a sequence of events (already ordered as read from storage) is
/// contiguous starting at 1 — the acceptance-gate check "restart reconstruction
/// ... validates event contiguity" reduces to this pure predicate so the real
/// recovery path (GOC-04-W04) can reuse it without re-deriving the rule.
pub fn events_are_contiguous(events: &[ModalityEventV1]) -> bool {
    events
        .iter()
        .enumerate()
        .all(|(index, event)| event.sequence == index as u64 + 1)
}

/// `ChunkManifestV1` (lane doc "Storage schema"): total length, chunk size,
/// ordered chunk refs, codec/MIME, classification, and a manifest digest
/// binding all of it together. Never carries raw bytes — `chunk_refs` are CAS
/// references only (lane doc "Non-goals": "Storing source bytes in rows ...
/// large content is CAS/chunk referenced").
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkManifestV1 {
    pub schema_version: u16,
    pub tenant_ref: OpaqueRef,
    pub total_len: u64,
    pub chunk_size: u32,
    pub chunk_refs: Vec<OpaqueRef>,
    pub codec_ref: OpaqueRef,
    pub classification: Classification,
    pub manifest_digest: [u8; 32],
}

impl ChunkManifestV1 {
    /// Build and self-certify a manifest: rejects a chunk count that does not
    /// match `ceil(total_len / chunk_size)` (a malformed/truncated/duplicated
    /// chunk list) and computes the binding digest itself, so a caller cannot
    /// construct a manifest whose digest does not match its own contents.
    pub fn new(
        tenant_ref: OpaqueRef,
        total_len: u64,
        chunk_size: u32,
        chunk_refs: Vec<OpaqueRef>,
        codec_ref: OpaqueRef,
        classification: Classification,
    ) -> Result<Self, RowSchemaError> {
        let expected_chunks = if total_len == 0 {
            0
        } else {
            if chunk_size == 0 {
                return Err(RowSchemaError::EmptyChunkManifest);
            }
            total_len.div_ceil(u64::from(chunk_size))
        };
        if chunk_refs.len() as u64 != expected_chunks {
            return Err(RowSchemaError::ChunkCountMismatch);
        }
        if total_len > 0 && chunk_refs.is_empty() {
            return Err(RowSchemaError::EmptyChunkManifest);
        }
        let manifest_digest = Self::compute_digest(total_len, chunk_size, &chunk_refs);
        Ok(Self {
            schema_version: MODALITY_ROW_SCHEMA_VERSION,
            tenant_ref,
            total_len,
            chunk_size,
            chunk_refs,
            codec_ref,
            classification,
            manifest_digest,
        })
    }

    fn compute_digest(total_len: u64, chunk_size: u32, chunk_refs: &[OpaqueRef]) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(total_len.to_be_bytes());
        hasher.update(chunk_size.to_be_bytes());
        for chunk_ref in chunk_refs {
            let bytes = chunk_ref.as_str().as_bytes();
            hasher.update((bytes.len() as u32).to_be_bytes());
            hasher.update(bytes);
        }
        hasher.finalize().into()
    }

    /// Recompute the digest over the manifest's own contents and compare —
    /// the acceptance-gate check "A malformed or missing chunk is a typed
    /// unavailable result" starts from this: a manifest that fails `verify`
    /// must never be treated as available.
    pub fn verify(&self) -> Result<(), RowSchemaError> {
        if self.schema_version != MODALITY_ROW_SCHEMA_VERSION {
            return Err(RowSchemaError::UnsupportedVersion);
        }
        let expected = Self::compute_digest(self.total_len, self.chunk_size, &self.chunk_refs);
        if expected != self.manifest_digest {
            return Err(RowSchemaError::ManifestDigestMismatch);
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, RowSchemaError> {
        self.verify()?;
        serde_json::to_vec(self).map_err(|_| RowSchemaError::Codec)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RowSchemaError> {
        let manifest: Self = serde_json::from_slice(bytes).map_err(|_| RowSchemaError::Codec)?;
        manifest.verify()?;
        Ok(manifest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::{ArtifactId, EvidenceLocusId};

    fn r(namespace: &str, suffix: u8) -> OpaqueRef {
        OpaqueRef::scoped(namespace, &format!("00000000000000{suffix:02x}")).unwrap()
    }

    fn tenant(suffix: u8) -> OpaqueRef {
        r("tenant", suffix)
    }

    // ── RowKey ──────────────────────────────────────────────────────────────

    #[test]
    fn row_key_encoding_is_deterministic() {
        let key = RowKey::new(
            tenant(1),
            ResourceId::Artifact(ArtifactId::from_token("0000000000000002").unwrap()),
        );
        assert_eq!(key.encode(), key.encode());
    }

    #[test]
    fn row_key_encoding_distinguishes_tenants_for_the_same_resource() {
        let resource = ResourceId::Artifact(ArtifactId::from_token("0000000000000002").unwrap());
        let a = RowKey::new(tenant(1), resource.clone());
        let b = RowKey::new(tenant(2), resource);
        assert_ne!(
            a.encode(),
            b.encode(),
            "two tenants over the same resource id must not collide in the row key"
        );
    }

    #[test]
    fn row_key_encoding_distinguishes_resources_for_the_same_tenant() {
        let a = RowKey::new(
            tenant(1),
            ResourceId::Artifact(ArtifactId::from_token("0000000000000002").unwrap()),
        );
        let b = RowKey::new(
            tenant(1),
            ResourceId::EvidenceLocus(EvidenceLocusId::from_token("0000000000000002").unwrap()),
        );
        assert_ne!(
            a.encode(),
            b.encode(),
            "an Artifact and an EvidenceLocus sharing a token must not collide"
        );
    }

    // ── ModalityRow ─────────────────────────────────────────────────────────

    #[test]
    fn modality_row_round_trips() {
        let row = ModalityRow::new(tenant(1), 1, r("policy", 2), None, "payload".to_string())
            .expect("valid row");
        let encoded = row.encode().unwrap();
        let decoded = ModalityRow::<String>::decode(&encoded).unwrap();
        assert_eq!(row, decoded);
    }

    #[test]
    fn modality_row_rejects_zero_commit_sequence() {
        let result = ModalityRow::new(tenant(1), 0, r("policy", 2), None, "payload".to_string());
        assert_eq!(result, Err(RowSchemaError::ZeroSequence));
    }

    #[test]
    fn modality_row_decode_rejects_unsupported_schema_version() {
        let row = ModalityRow::new(tenant(1), 1, r("policy", 2), None, "payload".to_string())
            .expect("valid row");
        let mut value: serde_json::Value = serde_json::from_slice(&row.encode().unwrap()).unwrap();
        value["schema_version"] = serde_json::json!(MODALITY_ROW_SCHEMA_VERSION + 1);
        let tampered = serde_json::to_vec(&value).unwrap();
        assert_eq!(
            ModalityRow::<String>::decode(&tampered),
            Err(RowSchemaError::UnsupportedVersion)
        );
    }

    // ── ModalityEventV1 ─────────────────────────────────────────────────────

    fn sample_event(sequence: u64) -> ModalityEventV1 {
        ModalityEventV1 {
            schema_version: MODALITY_ROW_SCHEMA_VERSION,
            sequence,
            tenant_ref: tenant(1),
            object: ResourceId::Artifact(ArtifactId::from_token("0000000000000002").unwrap()),
            operation: ModalityRowOperation::Inserted,
            prior_digest: None,
            new_digest: [7u8; 32],
            commit_id: r("commit", 3),
            actor: r("actor", 4),
            policy_digest: r("policy", 5),
            idempotency_key: r("idempotency", 6),
        }
    }

    #[test]
    fn modality_event_round_trips() {
        let event = sample_event(1);
        let encoded = event.encode().unwrap();
        assert_eq!(ModalityEventV1::decode(&encoded).unwrap(), event);
    }

    #[test]
    fn modality_event_rejects_zero_sequence() {
        assert_eq!(sample_event(0).encode(), Err(RowSchemaError::ZeroSequence));
    }

    #[test]
    fn events_are_contiguous_detects_a_gap() {
        let contiguous = vec![sample_event(1), sample_event(2), sample_event(3)];
        assert!(events_are_contiguous(&contiguous));

        let gapped = vec![sample_event(1), sample_event(3)];
        assert!(!events_are_contiguous(&gapped));

        let out_of_order = vec![sample_event(2), sample_event(1)];
        assert!(!events_are_contiguous(&out_of_order));
    }

    // ── ChunkManifestV1 ─────────────────────────────────────────────────────

    #[test]
    fn chunk_manifest_round_trips_and_self_certifies() {
        let manifest = ChunkManifestV1::new(
            tenant(1),
            10,
            4,
            vec![r("chunk", 1), r("chunk", 2), r("chunk", 3)],
            r("codec", 9),
            Classification::Internal,
        )
        .expect("10 bytes at chunk size 4 needs ceil(10/4)=3 chunks");
        manifest.verify().expect("freshly built manifest verifies");
        let encoded = manifest.encode().unwrap();
        assert_eq!(ChunkManifestV1::decode(&encoded).unwrap(), manifest);
    }

    #[test]
    fn chunk_manifest_allows_zero_length_with_no_chunks() {
        let manifest = ChunkManifestV1::new(
            tenant(1),
            0,
            0,
            Vec::new(),
            r("codec", 9),
            Classification::Public,
        )
        .expect("a zero-length artifact needs no chunks");
        assert!(manifest.chunk_refs.is_empty());
    }

    #[test]
    fn chunk_manifest_rejects_a_chunk_count_that_does_not_match_total_len() {
        let result = ChunkManifestV1::new(
            tenant(1),
            10,
            4,
            vec![r("chunk", 1)], // needs 3, only 1 given
            r("codec", 9),
            Classification::Internal,
        );
        assert_eq!(result, Err(RowSchemaError::ChunkCountMismatch));
    }

    #[test]
    fn chunk_manifest_rejects_nonzero_length_with_zero_chunk_size() {
        let result = ChunkManifestV1::new(
            tenant(1),
            10,
            0,
            vec![r("chunk", 1)],
            r("codec", 9),
            Classification::Internal,
        );
        assert_eq!(result, Err(RowSchemaError::EmptyChunkManifest));
    }

    #[test]
    fn chunk_manifest_decode_rejects_a_tampered_digest() {
        let manifest = ChunkManifestV1::new(
            tenant(1),
            10,
            4,
            vec![r("chunk", 1), r("chunk", 2), r("chunk", 3)],
            r("codec", 9),
            Classification::Internal,
        )
        .unwrap();
        let mut value: serde_json::Value =
            serde_json::from_slice(&serde_json::to_vec(&manifest).unwrap()).unwrap();
        // Flip one byte of the stored digest without recomputing it — simulates
        // bit rot / a corrupted manifest record.
        value["manifest_digest"][0] =
            serde_json::json!(value["manifest_digest"][0].as_u64().unwrap() ^ 0xFF);
        let tampered = serde_json::to_vec(&value).unwrap();
        assert_eq!(
            ChunkManifestV1::decode(&tampered),
            Err(RowSchemaError::ManifestDigestMismatch)
        );
    }

    #[test]
    fn chunk_manifest_never_carries_raw_bytes() {
        // Structural guarantee, not a runtime check: `ChunkManifestV1` has no
        // byte-payload field at all, only `OpaqueRef` chunk references. This
        // test exists so a future edit that adds one is forced to touch it.
        let manifest = ChunkManifestV1::new(
            tenant(1),
            4,
            4,
            vec![r("chunk", 1)],
            r("codec", 9),
            Classification::Restricted,
        )
        .unwrap();
        let encoded = serde_json::to_string(&manifest).unwrap();
        assert!(!encoded.contains("\"bytes\""));
        assert!(!encoded.contains("\"raw\""));
    }
}
