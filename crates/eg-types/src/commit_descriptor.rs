//! GOC-03 — the cross-domain commit-descriptor and read-barrier currency.
//!
//! An acknowledged mutation must have exactly ONE authority sequence, and every
//! reader must be able to ask for an authority-consistent read barrier over it.
//! `CommitDescriptorV1` is that one currency: graph rows, modality rows/chunks,
//! blob references and refcounts, vectors, time-series measurements, evidence/
//! lineage, SQL/lake metadata, terminal analytics outcomes, and the projection
//! outbox all register a participant digest against the same `commit_id`/
//! `commit_seq` rather than inventing a private commit sequence of their own.
//!
//! This is deliberately a NEW, additive module rather than a field bolted onto
//! [`crate::mutation_batch::MutationBatch`]. `MutationBatch` remains the request
//! envelope a single surface (graph/txn/query/rdf/lifecycle/job/broker) stages;
//! `CommitDescriptorV1` is the higher-altitude cross-domain identity a
//! `MutationBatch` — or a cross-modal group of them — commits under. Wiring the
//! descriptor into the `commit_cross_modal_txn` state machine and the durable
//! commit index is tracked separately (GOC-03-W03/W05); this module defines the
//! wire contract those callers, and the GOC-04/09/10/11/12 participants, share so
//! no lane invents a second commit currency while that wiring lands.
//!
//! Digests cover canonical encoded participant bytes, never mutable pointers or
//! raw content — see the lane doc's "Security and privacy" section. This module
//! does not itself carry document/audio/video/table bytes; callers pass a CAS/
//! artifact reference for any bounded diagnostic material.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Current wire/on-disk schema version for [`CommitDescriptorV1`] and
/// [`ProjectionCursorV1`]. One negotiated version is accepted after merge; an
/// unsupported version fails closed (`validate()` returns a typed error, never a
/// best-effort read of a foreign shape). There is no compatibility shim — see the
/// repo's "No Legacy" edict.
pub const COMMIT_DESCRIPTOR_VERSION: u16 = 1;

/// The domain a commit participant registers durable bytes under. Deliberately a
/// distinct vocabulary from [`crate::mutation_batch::MutationDomain`] (which
/// classifies one `MutationOperation`'s durability target): this enum is the
/// commit-descriptor-level participant registry the lane doc's "Registration of
/// graph, modality, vector, blob/refcount, time-series, evidence, table/lake, and
/// analytics-outcome participants" describes. `Outbox` is listed separately from
/// the domain that produced the change because outbox/CDC durability is verified
/// independently of any one domain's payload landing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitParticipantDomain {
    /// Graph node/edge rows (GOC-03 native; also serves GOC-13's cross-node case).
    Graph,
    /// Modality rows/chunks and their event sequence (GOC-04).
    Modality,
    /// Vector/embedding index entries.
    Vector,
    /// Blob content-addressed storage and its refcount ledger.
    Blob,
    /// Time-series measurement batches (GOC-09).
    TimeSeries,
    /// Evidence loci and lineage records.
    Evidence,
    /// SQL catalog / lakehouse table metadata (GOC-10).
    TableLake,
    /// Terminal analytics-job result artifacts (GOC-11).
    AnalyticsOutcome,
    /// The durable outbox/CDC change-envelope intent for this commit.
    Outbox,
}

/// Terminal and in-flight status of a commit descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitStatus {
    /// Descriptor and participant prepare records are durable; not yet published.
    Prepared,
    /// Published: every requested participant's prepare/blob-refcount/outbox
    /// intent was durable and authenticated before this status was recorded.
    Committed,
    /// Prepare failed OCC/policy validation or a participant vetoed; no
    /// authoritative bytes were applied.
    Aborted,
    /// Recovery could not confirm every requested projection's digest against a
    /// `Committed` descriptor; an operator/reconciler must replay or repair
    /// before readers may barrier above this commit.
    ReconcileRequired,
}

/// Per-projection durability/visibility state (invariant 4: a projection may
/// advertise `applied_seq` only after its digest matches and bytes are durable).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionState {
    /// `applied_seq == committed_seq` and the digest matches; safe to barrier.
    Ready,
    /// Behind `committed_seq`; still replaying — not a failure.
    CatchingUp,
    /// Digest mismatch, missing participant record, or reconciliation failed;
    /// requires operator/reconciler action before it can become `Ready`.
    Degraded,
}

fn validate_hex_digest(field: &str, digest: &str) -> Result<(), String> {
    if digest.len() != 64 || !digest.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(format!(
            "{field} must be a 64-character lowercase hexadecimal sha256 digest"
        ));
    }
    Ok(())
}

fn validate_non_empty(field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{field} must not be empty"));
    }
    Ok(())
}

/// The one durable, authenticated commit descriptor shared across every
/// participant domain (lane doc "Commit descriptor"). See each field's doc for
/// its exact contract; [`CommitDescriptorV1::validate`] enforces the invariants
/// that hold independent of persistence-layer wiring.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitDescriptorV1 {
    pub schema_version: u16,
    /// Opaque stable identity for this commit. Shared, unmodified, by every
    /// participant's durable record and by the outbox/CDC envelope it emits.
    pub commit_id: String,
    /// The staged transaction this descriptor publishes (`MutationBatch::batch_id`
    /// for a single-surface commit, or the cross-modal coordinator id).
    pub txn_id: String,
    /// Opaque tenant identity (never a raw tenant name written to CAS/audit text).
    pub tenant_ref: String,
    /// Opaque authenticated principal identity, `principal:sha256:<hex>` shaped
    /// the same way as `MutationRequestContext::principal`.
    pub principal_ref: String,
    /// Opaque identity of the authority (shard/group/node) that assigned
    /// `commit_seq`. Cross-node participation under a different authority is out
    /// of scope here (GOC-13); see `validate_single_authority`.
    pub authority_ref: String,
    /// Monotonic epoch of `authority_ref`, e.g. a Raft term/leadership epoch. A
    /// reader whose observed epoch is behind this value must treat the commit as
    /// unauthenticated for its purposes (stale-epoch rejection, invariant 7).
    pub authority_epoch: u64,
    /// Graph version this commit read against. `0` denotes an authority whose
    /// state does not live in the graph store, mirroring
    /// `mutation_batch::NON_GRAPH_SOURCE_VERSION`.
    pub source_graph_version: u64,
    /// Graph version this commit publishes. For a graph-backed commit this is
    /// `source_graph_version + 1`; a non-graph-authoritative commit repeats
    /// `source_graph_version` (both `0`).
    pub target_graph_version: u64,
    /// Monotonically increasing sequence number for `authority_ref`. Paired with
    /// `fencing_token`; a projection or reader must never accept a `commit_seq`
    /// below one it already observed for the same authority (invariant 2, 6).
    pub commit_seq: u64,
    /// Random, unguessable fencing token minted with `commit_seq`. Prevents a
    /// stale writer/reader that only knows an old `commit_seq` from acting as
    /// though it holds current authority.
    pub fencing_token: u64,
    /// sha256 digest of the canonical encoded mutation payload (deterministic
    /// content digest, not a pointer) used for idempotency-conflict detection.
    pub mutation_digest: String,
    /// Caller-stable retry key; replaying `(tenant_ref, idempotency_key,
    /// mutation_digest)` returns this same `commit_id` (invariant 5).
    pub idempotency_key: String,
    /// Every participant domain this commit touches, mapped to the sha256 digest
    /// of THAT participant's canonical encoded durable bytes. An absent participant
    /// the caller required is a prepare error (never an implicit no-op, per the
    /// lane doc's "Lifecycle" section) — so this map is authoritative for "what
    /// must be durable" as well as "what digest to verify against".
    pub participant_digests: BTreeMap<CommitParticipantDomain, String>,
    /// sha256 digest of the tenant/classification/deletion/retention/legal-hold/
    /// purpose policy snapshot evaluated before prepare (invariant 7). Re-checked
    /// (not just re-hashed) before read-barrier release.
    pub policy_digest: String,
    pub status: CommitStatus,
    pub prepared_at_ms: u64,
    /// Present once `status` leaves `Prepared`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decided_at_ms: Option<u64>,
    /// Optional reference to a bounded diagnostic artifact (CAS/artifact id) — the
    /// descriptor itself never carries raw document/audio/video/table content or
    /// exception text (lane doc "Security and privacy").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic_ref: Option<String>,
}

impl CommitDescriptorV1 {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != COMMIT_DESCRIPTOR_VERSION {
            return Err(format!(
                "unsupported commit descriptor version {} (expected {})",
                self.schema_version, COMMIT_DESCRIPTOR_VERSION
            ));
        }
        for (field, value) in [
            ("commit_id", &self.commit_id),
            ("txn_id", &self.txn_id),
            ("tenant_ref", &self.tenant_ref),
            ("authority_ref", &self.authority_ref),
            ("idempotency_key", &self.idempotency_key),
        ] {
            validate_non_empty(field, value)?;
        }
        let principal_digest = self
            .principal_ref
            .strip_prefix("principal:sha256:")
            .ok_or_else(|| "principal_ref must be an opaque sha256 id".to_string())?;
        validate_hex_digest("principal_ref digest", principal_digest)?;
        validate_hex_digest("mutation_digest", &self.mutation_digest)?;
        validate_hex_digest("policy_digest", &self.policy_digest)?;
        if self.commit_seq == 0 {
            return Err("commit_seq must be non-zero".to_string());
        }
        if self.fencing_token == 0 {
            return Err("fencing_token must be non-zero".to_string());
        }
        if self.source_graph_version == 0 {
            if self.target_graph_version != 0 {
                return Err(
                    "a non-graph-authoritative commit (source_graph_version 0) must repeat \
                     target_graph_version 0"
                        .to_string(),
                );
            }
        } else {
            let expected_target = self
                .source_graph_version
                .checked_add(1)
                .ok_or_else(|| "source_graph_version overflow".to_string())?;
            if self.target_graph_version != expected_target {
                return Err(
                    "target_graph_version must equal source_graph_version + 1 for a \
                     graph-authoritative commit"
                        .to_string(),
                );
            }
        }
        if self.participant_digests.is_empty() {
            return Err("commit descriptor requires at least one participant digest".to_string());
        }
        for (domain, digest) in &self.participant_digests {
            validate_hex_digest(&format!("participant_digests[{domain:?}]"), digest)?;
        }
        match self.status {
            CommitStatus::Prepared => {
                if self.decided_at_ms.is_some() {
                    return Err("a Prepared descriptor must not carry decided_at_ms".to_string());
                }
            }
            CommitStatus::Committed | CommitStatus::Aborted | CommitStatus::ReconcileRequired => {
                let decided = self
                    .decided_at_ms
                    .ok_or_else(|| "a decided descriptor requires decided_at_ms".to_string())?;
                if decided < self.prepared_at_ms {
                    return Err("decided_at_ms must not precede prepared_at_ms".to_string());
                }
            }
        }
        Ok(())
    }
}

// Acceptance gate 9 ("Cross-node participants are explicitly rejected or
// delegated to GOC-13; no undocumented two-phase behavior is introduced"): this
// shape has exactly ONE `authority_ref`/`authority_epoch` pair per descriptor —
// there is no per-participant authority field anywhere on `CommitDescriptorV1`
// or `ProjectionCursorV1`. A cross-node/cross-authority commit therefore cannot
// be expressed through this type at all; it is not a runtime check but a
// structural one, satisfied by omission. GOC-13 is expected to define its own
// coordinator-level type that references one `CommitDescriptorV1` per
// participating authority rather than extending this one to carry several.

/// Durable per-projection watermark and fence (lane doc "Commit descriptor").
/// Distinct from [`crate::mutation_batch::MutationProjectionCursor`], which is
/// keyed by `(projection, tenant, graph, batch_id, outbox_ordinal)` for the
/// existing per-surface outbox delivery loop; `ProjectionCursorV1` is keyed by
/// `(domain, authority_ref)` and advances by `commit_seq`, the cross-domain
/// currency this module defines. A domain-specific projection (GOC-04/09/10/11)
/// is expected to maintain both where it already participates in outbox delivery
/// AND registers as a commit participant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionCursorV1 {
    pub schema_version: u16,
    pub domain: CommitParticipantDomain,
    pub authority_ref: String,
    /// Highest `commit_seq` this projection has been notified is committed.
    pub committed_seq: u64,
    /// Highest `commit_seq` this projection has durably applied AND digest-
    /// verified. `applied_seq <= committed_seq` always.
    pub applied_seq: u64,
    /// Fencing token of the commit at `applied_seq` (0 when `applied_seq == 0`,
    /// i.e. nothing applied yet). A cursor update carrying a fencing token that
    /// does not match the descriptor at that sequence is rejected — this is the
    /// mechanical form of invariant 6 "no acknowledged commit is silently
    /// discarded" for the projection side.
    pub fence: u64,
    /// sha256 digest of the participant bytes this projection actually applied at
    /// `applied_seq`; must equal `CommitDescriptorV1::participant_digests[domain]`
    /// for `applied_seq` to be considered `Ready` (invariant 4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applied_digest: Option<String>,
    pub state: ProjectionState,
    /// Opaque reference to the last reconciliation error, if `state == Degraded`.
    /// Never raw exception text (lane doc "Security and privacy").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error_ref: Option<String>,
    pub updated_at_ms: u64,
}

impl ProjectionCursorV1 {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != COMMIT_DESCRIPTOR_VERSION {
            return Err(format!(
                "unsupported projection cursor version {} (expected {})",
                self.schema_version, COMMIT_DESCRIPTOR_VERSION
            ));
        }
        validate_non_empty("authority_ref", &self.authority_ref)?;
        if self.applied_seq > self.committed_seq {
            return Err("applied_seq must not exceed committed_seq".to_string());
        }
        if self.applied_seq == 0 {
            if self.fence != 0 {
                return Err("fence must be 0 while applied_seq is 0".to_string());
            }
            if self.applied_digest.is_some() {
                return Err("applied_digest must be unset while applied_seq is 0".to_string());
            }
        } else if self.fence == 0 {
            return Err("fence must be non-zero once applied_seq is non-zero".to_string());
        }
        match self.state {
            ProjectionState::Ready => {
                if self.applied_seq != self.committed_seq {
                    return Err("Ready requires applied_seq == committed_seq".to_string());
                }
                if self.applied_digest.is_none() && self.applied_seq != 0 {
                    return Err(
                        "Ready requires applied_digest once applied_seq is non-zero".to_string()
                    );
                }
                if self.last_error_ref.is_some() {
                    return Err("Ready must not carry last_error_ref".to_string());
                }
            }
            ProjectionState::Degraded => {
                if self.last_error_ref.is_none() {
                    return Err("Degraded requires last_error_ref".to_string());
                }
            }
            ProjectionState::CatchingUp => {}
        }
        Ok(())
    }

    /// Attempt to advance this cursor to a newer commit. Rejects sequence
    /// rollback and a fencing-token mismatch — the two mechanical checks
    /// underneath acceptance gate 5 ("Every projection cursor rejects sequence
    /// rollback and cannot advance without matching the descriptor digest").
    /// Digest equality against the owning [`CommitDescriptorV1`] is the caller's
    /// responsibility (this type has no access to the descriptor store); this
    /// method only enforces what is checkable from the two cursor values.
    pub fn advance(
        &self,
        next_committed_seq: u64,
        next_applied_seq: u64,
        next_fence: u64,
        next_applied_digest: Option<String>,
        now_ms: u64,
    ) -> Result<ProjectionCursorV1, String> {
        if next_committed_seq < self.committed_seq {
            return Err(format!(
                "PROJECTION_SEQUENCE_ROLLBACK: next committed_seq {next_committed_seq} is behind \
                 current {}",
                self.committed_seq
            ));
        }
        if next_applied_seq < self.applied_seq {
            return Err(format!(
                "PROJECTION_SEQUENCE_ROLLBACK: next applied_seq {next_applied_seq} is behind \
                 current {}",
                self.applied_seq
            ));
        }
        if next_applied_seq > self.applied_seq && next_fence <= self.fence {
            return Err(
                "PROJECTION_SEQUENCE_ROLLBACK: applying a newer sequence requires a strictly \
                 greater fencing token"
                    .to_string(),
            );
        }
        let state = if next_applied_seq == next_committed_seq
            && (next_applied_seq == 0 || next_applied_digest.is_some())
        {
            ProjectionState::Ready
        } else {
            ProjectionState::CatchingUp
        };
        let next = ProjectionCursorV1 {
            schema_version: self.schema_version,
            domain: self.domain,
            authority_ref: self.authority_ref.clone(),
            committed_seq: next_committed_seq,
            applied_seq: next_applied_seq,
            fence: next_fence,
            applied_digest: next_applied_digest,
            state,
            last_error_ref: None,
            updated_at_ms: now_ms,
        };
        next.validate()?;
        Ok(next)
    }
}

/// Reader-issued request for a barrier over one or more participant domains.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadBarrierRequest {
    pub schema_version: u16,
    pub tenant_ref: String,
    pub authority_ref: String,
    /// Minimum commit sequence the caller requires to be visible.
    pub min_commit_seq: u64,
    /// Domains the caller requires fenced before release. Empty is rejected —
    /// a barrier with no required domain is not a meaningful request.
    pub required_domains: Vec<CommitParticipantDomain>,
}

impl ReadBarrierRequest {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != COMMIT_DESCRIPTOR_VERSION {
            return Err(format!(
                "unsupported read barrier request version {} (expected {})",
                self.schema_version, COMMIT_DESCRIPTOR_VERSION
            ));
        }
        validate_non_empty("tenant_ref", &self.tenant_ref)?;
        validate_non_empty("authority_ref", &self.authority_ref)?;
        if self.required_domains.is_empty() {
            return Err("read barrier request requires at least one domain".to_string());
        }
        Ok(())
    }
}

/// Typed outcome of a [`ReadBarrierRequest`]. Never a best-effort stale success
/// (invariant 4) — a caller either receives every requested cursor fenced at or
/// above `min_commit_seq`, or a typed `PROJECTION_NOT_READY`/`POLICY_DENIED`
/// result carrying the cursors that blocked release.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ReadBarrierResponse {
    /// Every requested domain is `Ready` at or above `min_commit_seq`.
    Released {
        released_seq: u64,
        cursors: Vec<ProjectionCursorV1>,
    },
    /// At least one requested domain is not yet `Ready` at `min_commit_seq`
    /// (`CatchingUp`) or is `Degraded`. Carries the exact cursors so the caller
    /// can retry/back off or surface the degraded domain, never a generic error.
    NotReady { cursors: Vec<ProjectionCursorV1> },
    /// Tenant, classification, deletion, retention, legal-hold, or purpose
    /// policy denied release (invariant 7), evaluated again at barrier time even
    /// though it was already evaluated before prepare.
    PolicyDenied { reason_ref: String },
}

impl ReadBarrierResponse {
    /// The stable error-code-prefixed message this response would carry over the
    /// existing `Response.error: Option<String>` wire convention (see
    /// `src/server/qos.rs`'s `"BUSY: ..."`/`src/server/auth.rs`'s
    /// `"NODE_MISMATCH: ..."` precedent) when it is not a success. `Released`
    /// has no error string — callers branch on `result` in that case.
    pub fn error_code(&self) -> Option<&'static str> {
        match self {
            ReadBarrierResponse::Released { .. } => None,
            ReadBarrierResponse::NotReady { .. } => Some("PROJECTION_NOT_READY"),
            ReadBarrierResponse::PolicyDenied { .. } => Some("POLICY_DENIED"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex_digest(byte: u8) -> String {
        hex::encode([byte; 32])
    }

    // Minimal local hex encoder so this module's tests don't pull a new dep just
    // for fixture construction; eg-types has no `hex` crate dependency.
    mod hex {
        pub fn encode(bytes: [u8; 32]) -> String {
            bytes.iter().map(|b| format!("{b:02x}")).collect()
        }
    }

    fn descriptor(commit_seq: u64, status: CommitStatus) -> CommitDescriptorV1 {
        let mut participants = BTreeMap::new();
        participants.insert(CommitParticipantDomain::Graph, hex_digest(0xAB));
        CommitDescriptorV1 {
            schema_version: COMMIT_DESCRIPTOR_VERSION,
            commit_id: "commit-1".into(),
            txn_id: "txn-1".into(),
            tenant_ref: "tenant-ref-1".into(),
            principal_ref: format!("principal:sha256:{}", hex_digest(0x11)),
            authority_ref: "shard-0".into(),
            authority_epoch: 1,
            source_graph_version: 10,
            target_graph_version: 11,
            commit_seq,
            fencing_token: 0xDEAD_BEEF,
            mutation_digest: hex_digest(0x22),
            idempotency_key: "idem-1".into(),
            participant_digests: participants,
            policy_digest: hex_digest(0x33),
            status,
            prepared_at_ms: 100,
            decided_at_ms: if matches!(status, CommitStatus::Prepared) {
                None
            } else {
                Some(101)
            },
            diagnostic_ref: None,
        }
    }

    // ---- Golden wire fixture: round-trip through both negotiated encodings ----

    #[test]
    fn commit_descriptor_roundtrips_msgpack_and_json() {
        let d = descriptor(42, CommitStatus::Committed);
        d.validate().unwrap();

        let mp = rmp_serde::to_vec_named(&d).unwrap();
        let back: CommitDescriptorV1 = rmp_serde::from_slice(&mp).unwrap();
        assert_eq!(back, d);

        let json = serde_json::to_string(&d).unwrap();
        let back_json: CommitDescriptorV1 = serde_json::from_str(&json).unwrap();
        assert_eq!(back_json, d);
    }

    // ---- FAIL-on-bad: unsupported version is rejected, not silently accepted ----

    #[test]
    fn commit_descriptor_rejects_unsupported_version() {
        let mut d = descriptor(1, CommitStatus::Committed);
        d.schema_version = COMMIT_DESCRIPTOR_VERSION + 1;
        let err = d.validate().unwrap_err();
        assert!(err.contains("unsupported"), "got: {err}");
    }

    #[test]
    fn commit_descriptor_rejects_unknown_field() {
        let d = descriptor(1, CommitStatus::Committed);
        let mut value = serde_json::to_value(&d).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("bogus_future_field".to_string(), serde_json::json!(true));
        let decoded: Result<CommitDescriptorV1, _> = serde_json::from_value(value);
        assert!(
            decoded.is_err(),
            "an unknown field must be rejected, not ignored"
        );
    }

    // ---- PASS-on-good: zero commit_seq / zero fencing_token are rejected ----

    #[test]
    fn commit_descriptor_rejects_zero_commit_seq_and_fence() {
        let mut d = descriptor(1, CommitStatus::Committed);
        d.commit_seq = 0;
        assert!(d.validate().unwrap_err().contains("commit_seq"));

        let mut d2 = descriptor(1, CommitStatus::Committed);
        d2.fencing_token = 0;
        assert!(d2.validate().unwrap_err().contains("fencing_token"));
    }

    #[test]
    fn commit_descriptor_requires_at_least_one_participant() {
        let mut d = descriptor(1, CommitStatus::Committed);
        d.participant_digests.clear();
        assert!(d
            .validate()
            .unwrap_err()
            .contains("at least one participant"));
    }

    #[test]
    fn commit_descriptor_prepared_must_not_carry_decided_at() {
        let mut d = descriptor(1, CommitStatus::Prepared);
        d.decided_at_ms = Some(1);
        assert!(d.validate().unwrap_err().contains("Prepared"));

        let mut committed_missing_decided = descriptor(1, CommitStatus::Committed);
        committed_missing_decided.decided_at_ms = None;
        assert!(committed_missing_decided
            .validate()
            .unwrap_err()
            .contains("decided_at_ms"));
    }

    #[test]
    fn commit_descriptor_target_version_must_be_source_plus_one() {
        let mut d = descriptor(1, CommitStatus::Committed);
        d.target_graph_version = d.source_graph_version; // should be +1
        assert!(d
            .validate()
            .unwrap_err()
            .contains("source_graph_version + 1"));

        let mut non_graph = descriptor(1, CommitStatus::Committed);
        non_graph.source_graph_version = 0;
        non_graph.target_graph_version = 0;
        non_graph.validate().unwrap();

        let mut bad_non_graph = descriptor(1, CommitStatus::Committed);
        bad_non_graph.source_graph_version = 0;
        bad_non_graph.target_graph_version = 1;
        assert!(bad_non_graph
            .validate()
            .unwrap_err()
            .contains("non-graph-authoritative"));
    }

    fn cursor(
        committed: u64,
        applied: u64,
        fence: u64,
        state: ProjectionState,
    ) -> ProjectionCursorV1 {
        ProjectionCursorV1 {
            schema_version: COMMIT_DESCRIPTOR_VERSION,
            domain: CommitParticipantDomain::Graph,
            authority_ref: "shard-0".into(),
            committed_seq: committed,
            applied_seq: applied,
            fence,
            applied_digest: if applied == 0 {
                None
            } else {
                Some(hex_digest(0xAB))
            },
            state,
            last_error_ref: None,
            updated_at_ms: 100,
        }
    }

    #[test]
    fn projection_cursor_ready_requires_applied_equals_committed() {
        let ready = cursor(5, 5, 9, ProjectionState::Ready);
        ready.validate().unwrap();

        let mut mismatched = cursor(5, 3, 9, ProjectionState::Ready);
        mismatched.state = ProjectionState::Ready;
        assert!(mismatched
            .validate()
            .unwrap_err()
            .contains("Ready requires applied_seq"));
    }

    #[test]
    fn projection_cursor_degraded_requires_error_ref() {
        let mut degraded = cursor(5, 3, 9, ProjectionState::Degraded);
        degraded.last_error_ref = None;
        assert!(degraded
            .validate()
            .unwrap_err()
            .contains("Degraded requires"));

        let mut ok = degraded.clone();
        ok.last_error_ref = Some("err-ref-1".into());
        ok.validate().unwrap();
    }

    // ---- FAIL-on-bad: sequence rollback is rejected ----

    #[test]
    fn projection_cursor_advance_rejects_sequence_rollback() {
        let c = cursor(10, 10, 5, ProjectionState::Ready);
        let err = c.advance(9, 9, 6, Some(hex_digest(0xAB)), 200).unwrap_err();
        assert!(
            err.starts_with("PROJECTION_SEQUENCE_ROLLBACK"),
            "got: {err}"
        );
    }

    #[test]
    fn projection_cursor_advance_rejects_non_increasing_fence_on_new_sequence() {
        let c = cursor(10, 10, 5, ProjectionState::Ready);
        // advancing to a strictly newer applied_seq without a strictly greater
        // fence must be rejected — a stale fence cannot claim newer authority.
        let err = c
            .advance(11, 11, 5, Some(hex_digest(0xAB)), 200)
            .unwrap_err();
        assert!(
            err.starts_with("PROJECTION_SEQUENCE_ROLLBACK"),
            "got: {err}"
        );
    }

    // ---- PASS-on-good: a legitimate forward advance succeeds and becomes Ready ----

    #[test]
    fn projection_cursor_advance_accepts_forward_progress() {
        let c = cursor(10, 10, 5, ProjectionState::Ready);
        let next = c.advance(12, 12, 6, Some(hex_digest(0xCD)), 250).unwrap();
        assert_eq!(next.state, ProjectionState::Ready);
        assert_eq!(next.committed_seq, 12);
        assert_eq!(next.applied_seq, 12);

        let catching_up = c.advance(12, 10, 5, None, 250).unwrap();
        assert_eq!(catching_up.state, ProjectionState::CatchingUp);
    }

    #[test]
    fn projection_cursor_advance_distinguishes_empty_ready_from_missing_digest() {
        let empty = cursor(0, 0, 0, ProjectionState::Ready);
        let still_empty = empty.advance(0, 0, 0, None, 200).unwrap();
        assert_eq!(still_empty.state, ProjectionState::Ready);

        let applied = cursor(10, 10, 5, ProjectionState::Ready);
        let missing_digest = applied.advance(10, 10, 5, None, 200).unwrap();
        assert_eq!(missing_digest.state, ProjectionState::CatchingUp);
    }

    // ---- Barrier: PROJECTION_NOT_READY / POLICY_DENIED are typed, never stale success ----

    #[test]
    fn read_barrier_response_error_codes_match_lane_convention() {
        assert_eq!(
            ReadBarrierResponse::NotReady { cursors: vec![] }.error_code(),
            Some("PROJECTION_NOT_READY")
        );
        assert_eq!(
            ReadBarrierResponse::PolicyDenied {
                reason_ref: "policy-1".into()
            }
            .error_code(),
            Some("POLICY_DENIED")
        );
        assert_eq!(
            ReadBarrierResponse::Released {
                released_seq: 1,
                cursors: vec![]
            }
            .error_code(),
            None
        );
    }

    #[test]
    fn read_barrier_request_rejects_empty_domains() {
        let req = ReadBarrierRequest {
            schema_version: COMMIT_DESCRIPTOR_VERSION,
            tenant_ref: "tenant-1".into(),
            authority_ref: "shard-0".into(),
            min_commit_seq: 1,
            required_domains: vec![],
        };
        assert!(req.validate().unwrap_err().contains("at least one domain"));
    }

    #[test]
    fn read_barrier_request_roundtrips() {
        let req = ReadBarrierRequest {
            schema_version: COMMIT_DESCRIPTOR_VERSION,
            tenant_ref: "tenant-1".into(),
            authority_ref: "shard-0".into(),
            min_commit_seq: 7,
            required_domains: vec![
                CommitParticipantDomain::Graph,
                CommitParticipantDomain::Vector,
            ],
        };
        req.validate().unwrap();
        let mp = rmp_serde::to_vec_named(&req).unwrap();
        let back: ReadBarrierRequest = rmp_serde::from_slice(&mp).unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn participant_digest_keys_serialize_snake_case() {
        let d = descriptor(1, CommitStatus::Committed);
        let json = serde_json::to_value(&d).unwrap();
        let keys: Vec<String> = json["participant_digests"]
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        assert_eq!(keys, vec!["graph".to_string()]);
    }
}
