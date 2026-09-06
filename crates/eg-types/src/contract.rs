//! Canonical values shared by the storage and mutation kernels.
//!
//! This is the single composed public contract surface. Its private modules
//! separate cryptographic scalars, canonical identifiers/domain tokens, and
//! bounded collections without creating competing schemas or public aliases.

mod collections;
mod crypto;
mod identifiers;

pub use collections::{BoundedVecV1, RecordBytesV1};
pub use crypto::{Digest256V1, Ed25519SignatureV1, NonceV1};
pub use identifiers::{
    ActorIdV1, AdmissionStateV1, AudienceIdV1, DecisionOutcomeV1, EffectStateV1, IdempotencyKeyV1,
    IngressSurfaceV1, MethodIdV1, MutationDispositionV1, MutationDomainV1, OpaqueIdV1, OperationV1,
    PolicyRevisionV1, ProtocolIdV1, PurposeKindV1, ReplayStatusV1, RequestedMutationResultV1,
    ResourceIdV1, SchemaIdV1, ScopeKindV1, TenantIdV1, UtcUnixNanosV1, VerificationStatusV1,
};

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
