//! Immutable authority and replay evidence consumed by the mutation kernel.
//!
//! `VerifiedAuthorityV1` is serializable evidence, not an executable capability.
//! The request boundary must validate it and mint a private admitted-session
//! token before `eg-transaction` may execute an effect.

mod context;
mod evidence;
mod replay;

pub use self::context::{
    AuthorityContextV1, AuthorityScopeV1, AUTHORITY_CONTEXT_SCHEMA_V1, AUTHORITY_PROTOCOL_V1,
};
pub use self::evidence::{SignedAuthorityEnvelopeEvidenceV1, VerifiedAuthorityV1};
pub use self::replay::{NonceReplayKeyV1, OperationReplayIdentityV1, ReplayReceiptV1};
pub use crate::contract::{
    AdmissionStateV1, DecisionOutcomeV1, EffectStateV1, IngressSurfaceV1, OperationV1,
    PurposeKindV1, ReplayStatusV1, ScopeKindV1, VerificationStatusV1,
};

#[cfg(test)]
mod tests;
