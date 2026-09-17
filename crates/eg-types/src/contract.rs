//! Canonical values shared by the storage and mutation kernels.
//!
//! This is the single composed public contract surface. Its private modules
//! separate cryptographic scalars, canonical identifiers/domain tokens, and
//! bounded collections without creating competing schemas or public aliases.

mod collections;
mod crypto;
mod identifiers;

pub use collections::{BoundedVec, RecordBytes};
pub use crypto::{Digest256, Ed25519Signature, Nonce};
pub use identifiers::{
    ActorId, AdmissionOutcome, AudienceId, DecisionOutcome, EffectState, IdempotencyKey,
    IngressSurface, MethodId, MutationDisposition, MutationDomain, OpaqueId, Operation,
    PolicyRevision, ProtocolId, PurposeKind, ReplayStatus, RequestedMutationResult, ResourceId,
    SchemaId, ScopeKind, TenantId, UtcUnixNanos, VerificationStatus,
};

/// Whether a located-evidence region is well-formed: every coordinate finite
/// and a strictly positive extent. The wire contract and the modality artifact
/// contract both validate image and page regions against this one definition.
pub fn valid_evidence_region(x: f64, y: f64, width: f64, height: f64) -> bool {
    x.is_finite()
        && y.is_finite()
        && width.is_finite()
        && height.is_finite()
        && width > 0.0
        && height > 0.0
}

pub const SHA256_BYTES: usize = 32;
pub const SHA256_HEX_BYTES: usize = SHA256_BYTES * 2;
pub const NONCE_BYTES: usize = 32;
pub const ED25519_SIGNATURE_BYTES: usize = 64;
pub const MAX_TENANT_ID_BYTES: usize = 255;
pub const MAX_RESOURCE_ID_BYTES: usize = 1_024;
pub const MAX_OPAQUE_ID_BYTES: usize = 1_024;
pub const MAX_METHOD_ID_BYTES: usize = 255;
pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = 1_024;
pub const MAX_SCOPE_COMPONENTS: usize = 64;
pub const MAX_MUTATION_EFFECTS: usize = 4_096;
pub const MAX_OUTBOX_INTENTS: usize = 4_096;
pub const MAX_PARTICIPANTS: usize = 64;
pub const MAX_RECORD_BYTES: usize = 1024 * 1024;
pub const MAX_MUTATION_ENVELOPE_BYTES: usize = 16 * 1024 * 1024;
