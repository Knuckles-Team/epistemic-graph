//! Hand-written Epistemic Operations wire DTOs NOT covered by the AU protocol
//! catalog gate (`agent_utilities/protocols/epistemic_operations/schemas/v1/catalog.json`,
//! enforced by `scripts/check_epistemic_operations_protocol.py`).
//!
//! GOC-46 finding (2026-08-16): `WorkItemClaimCapability{MintRequest,VerifyRequest,Result}`
//! (BUG dad235b, native WorkItem claim capabilities) and `CasWorkItemMetadata{Request,Result,
//! LeaseFence}` (BUG-111, native scheduling-metadata CAS) are live, wire-dispatched
//! `protocol::Method` payload types with `#[serde(deny_unknown_fields)]` closed-schema
//! semantics identical to a catalog-bound type -- but neither operation was ever declared in
//! the AU catalog. They were hand-added directly into `epistemic_operations.rs` (a file whose
//! own header claims "Generated ... regenerate with the AU protocol gate"), so running that
//! gate with `--write` would silently delete them, breaking the 7 files that consume them
//! (this crate's `protocol.rs`, plus `redb_store.rs`, `redb_store/work_item_capability.rs`,
//! `server/dispatch.rs`, `server/persistence/mod.rs`, `server/persistence/redb_backend.rs`,
//! `raft/mod.rs` in the top-level `epistemic-graph` crate).
//!
//! Extending the catalog to declare them properly requires the generator to support a binary
//! payload type (`Vec<u8>` / `Option<Vec<u8>>` with `#[serde(with = "serde_bytes")]`), which it
//! does not have today -- a real cross-repo (agent-utilities) generator change, not a
//! mechanical schema edit. Until that lands, relocating these types out of the generated file
//! (this module) is the safe interim fix: it makes `epistemic_operations.rs` exactly match the
//! generator's output again (closing the `--write`-deletes-live-code hazard) without deleting
//! any live functionality or requiring an AU-side change. See
//! `plans/graph-os-completion-program/lanes/GOC-46-eg-protocol-query-client-parity.md`.
//!
//! Field shapes, derives, and serde attributes below are copied byte-for-byte from the prior
//! location in `epistemic_operations.rs`; only the module and this header are new.

use serde::{Deserialize, Deserializer, Serialize};

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

/// Versioned native capability request.  The engine derives every authority
/// field (tenant, owner, lease epoch/fence, attempt, and expiry) from the
/// authenticated request context and the live WorkItem row; callers provide
/// only the opaque WorkItem identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkItemClaimCapabilityRequestSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

/// Stable result schema for the narrow native claim-capability checkpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkItemClaimCapabilityResultSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

/// Privacy-safe decision vocabulary.  No authority tuple, owner, lease, or
/// capability metadata is projected in the result.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkItemClaimCapabilityDecision {
    #[serde(rename = "minted")]
    Minted,
    #[serde(rename = "replayed")]
    Replayed,
    #[serde(rename = "verified")]
    Verified,
    #[serde(rename = "input_conflict")]
    InputConflict,
    #[serde(rename = "not_found")]
    NotFound,
    #[serde(rename = "unauthorized")]
    Unauthorized,
    #[serde(rename = "expired")]
    Expired,
    #[serde(rename = "stale")]
    Stale,
    #[serde(rename = "malformed")]
    Malformed,
    #[serde(rename = "retention_exhausted")]
    RetentionExhausted,
}

/// Native mint input.  The authenticated worker/session and current lease
/// authority are deliberately absent: only the engine can derive them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkItemClaimCapabilityMintRequest {
    pub schema_version: WorkItemClaimCapabilityRequestSchemaVersion,
    pub work_item_id: String,
}

/// Native verify input.  The capability is an opaque bounded byte string; no
/// public DTO can reconstruct authority from owner/epoch/fence/attempt fields.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkItemClaimCapabilityVerifyRequest {
    pub schema_version: WorkItemClaimCapabilityRequestSchemaVersion,
    pub work_item_id: String,
    #[serde(with = "serde_bytes")]
    pub capability: Vec<u8>,
}

/// Capability operation result.  Only mint/replay returns the opaque bytes;
/// verification returns a boolean and a privacy-safe decision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkItemClaimCapabilityResult {
    pub schema_version: WorkItemClaimCapabilityResultSchemaVersion,
    pub decision: WorkItemClaimCapabilityDecision,
    pub valid: bool,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub capability: Option<Vec<u8>>,
}

// ── CasWorkItemMetadata (BUG-111) ───────────────────────────────────────
//
// The native, typed compare-and-set for a WorkItem's non-authority
// SCHEDULING METADATA (`checkpoint_id` / `metadata` / `prio_bucket`).
// `work_item_capability::validate_generic_method` (RMDD-29) unconditionally
// refuses a generic `CompareAndSetNodeFields` against any already-claimed
// WorkItem row, with no carve-out for a harmless field — so four
// agent-utilities `work_item.py` call sites (checkpoint / request-input /
// submit-input / set-priority) had no atomic native path at all. This
// closes that gap the same way `ClaimWorkItem`/`RenewWorkItemLease` do:
// one field, atomically compare-and-set inside the SAME durable WorkItem
// transaction, never a side path.
//
// The struct can never touch `status`/`lease_owner`/`lease_epoch`/
// `fencing_token`/`tenant` — there is no field on it that names them — so it
// can never manufacture or extend native lease authority; the guard's own
// purpose is preserved while giving these four sites a real, typed answer
// instead of an unconditional refusal.

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CasWorkItemMetadataRequestSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CasWorkItemMetadataResultSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

/// Three distinct outcomes — deliberately never collapsed to a bool.  A
/// falsy/empty return here would be indistinguishable at the call site from
/// "no such item" (AU-P0-3 fail-closed doctrine): a worker that cannot tell
/// "I lost a race" from "the item vanished" cannot safely decide whether to
/// retry, re-read, or abandon its lease.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CasWorkItemMetadataOutcome {
    /// The compare-and-set matched and the field was durably written.
    #[serde(rename = "applied")]
    Applied,
    /// The row exists but the observed condition (status / lease fence /
    /// the field's own expected prior value) no longer matches — a genuine
    /// lost race, not an error. The caller re-reads and decides whether to
    /// retry.
    #[serde(rename = "conflict")]
    Conflict,
    /// No WorkItem row `work_item_id` exists in this graph for `tenant_ref`.
    #[serde(rename = "not_found")]
    NotFound,
}

/// The live lease tuple a lease-holding caller (checkpoint / request-input)
/// fences on, exactly like `RenewWorkItemLease`. Omit `expected_lease` on
/// the request entirely for a lease-less external/scheduler caller
/// (submit-input / set-priority), which fences on `expected_status` (and,
/// for submit-input, the tenant + the field's own expected prior value)
/// instead.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CasWorkItemMetadataLeaseFence {
    pub worker_ref: String,
    pub lease_epoch: u64,
    pub fencing_token: u64,
}

/// Atomic single-field compare-and-set on one WorkItem's scheduling
/// metadata. Exactly one field-pair — (`expected_checkpoint_id`,
/// `set_checkpoint_id`) xor (`expected_metadata_msgpack`,
/// `set_metadata_msgpack`) xor (`expected_prio_bucket`, `set_prio_bucket`)
/// — must be present; the engine rejects a request naming zero or more than
/// one. `expected_status` must be non-empty: the caller always names the
/// exact status set the row must currently be in.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CasWorkItemMetadataRequest {
    pub schema_version: CasWorkItemMetadataRequestSchemaVersion,
    pub tenant_ref: String,
    pub work_item_id: String,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expected_lease: Option<CasWorkItemMetadataLeaseFence>,
    pub expected_status: Vec<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expected_checkpoint_id: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub set_checkpoint_id: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expected_metadata_msgpack: Option<Vec<u8>>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub set_metadata_msgpack: Option<Vec<u8>>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expected_prio_bucket: Option<i64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub set_prio_bucket: Option<i64>,
    pub now_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CasWorkItemMetadataResult {
    pub schema_version: CasWorkItemMetadataResultSchemaVersion,
    pub outcome: CasWorkItemMetadataOutcome,
    pub work_item_id: String,
    pub changed_work_item_ids: Vec<String>,
}
