//! Current-only untrusted mutation request/evidence DTOs produced by domain
//! compilers and inspected by the future `eg-transaction` admission boundary.

mod budget;
mod effect_decode;
mod effects;
mod envelope;
mod envelope_access;
mod envelope_digests;
mod envelope_evidence;
mod envelope_validation;
mod outbox_decode;
mod payload;
mod receipt;
mod targets;

pub use crate::contract::{MutationDispositionV1, MutationDomainV1, RequestedMutationResultV1};
pub use effects::{MutationEffectV1, RecordMutationV1};
pub use envelope::{MutationEnvelopePartsV1, MutationEnvelopeV1, MutationPayloadV1};
pub use outbox_decode::ProvenanceBindingV1;
pub use receipt::{MutationReceiptV1, MutationResultV1};
pub use targets::{MutationPreconditionV1, RecordTargetV1};

pub const MUTATION_ENVELOPE_SCHEMA_V1: &str = "mutation-envelope.v1";
pub const MAX_MUTATION_PRECONDITIONS: usize = 4_096;
pub const MAX_PROVENANCE_REFS: usize = 4_096;

const EFFECT_FIXED_BUDGET: usize = 256;
const OUTBOX_FIXED_BUDGET: usize = 512;
const OUTBOX_HEADER_FIXED_BUDGET: usize = 128;
const PROVENANCE_FIXED_BUDGET: usize = 128;

#[cfg(test)]
mod tests;
