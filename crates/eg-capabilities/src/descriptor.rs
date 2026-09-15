//! `MethodDescriptor`: the canonical engine-contract row for one `Method` variant
//! (RF-RULING-003).
//!
//! [`MethodPolicy`] answers "what does this method DO to engine state". A descriptor
//! answers the rest of the contract question RF-RULING-003 makes EG the sole owner of:
//! which request/result schema the wire carries, which error codes the engine may
//! legitimately return, how the mutation replays, which generated consumers exist, how
//! stable the surface is, and which persisted format identities its effect touches.
//!
//! ## What is authored vs derived
//!
//! A domain row authors only the facts that are genuinely per-method: the
//! [`MethodPolicy`], the consumer profiles, and the stability tag. Everything else is a
//! DERIVATION with one documented rule, so 400+ rows cannot drift into 400+ independent
//! guesses:
//!
//! - the request schema is always the `Method` variant's own subschema, keyed by the
//!   row's id -- a variant's inline fields ARE its request schema.
//! - the RESULT is not authored here at all: it is the `eg_types::result_contract`
//!   marker the handler encodes through, so the compiler, not a row, fixes its type.
//! - `error_set` follows [`error_set_for`] (policy-shaped, not per-method prose).
//! - `replay_class` follows [`replay_class_for`] (RF-RULING-004's two replay identities).
//! - `format_identities` follows [`format_identities_for`] (the durability domain names
//!   the persisted format the effect touches).

use crate::{DurabilityDomain, MethodPolicy};

/// A method's wire identity: the exact serde tag of its `Method` variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MethodId(pub &'static str);

impl MethodId {
    pub const fn as_str(&self) -> &'static str {
        self.0
    }
}

/// RF-RULING-004's replay identities, as they apply to one method.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReplayClass {
    /// A durable mutation admitted through `MutationKernel`: a fresh nonce plus the
    /// exact `OperationReplayIdentity` replays the prior result.
    OperationIdentity,
    /// A state transition that consumes a `NonceReplayKey` but owns no durable
    /// operation identity (process/session-scoped staging and lifecycle).
    NonceOnly,
    /// No mutation to replay.
    NotReplayable,
}

/// A generated client surface. Only `PythonClient` is emitted today; the rest are
/// declared so a later profile is a descriptor edit, not a second registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConsumerProfile {
    PythonClient,
    Cli,
    Mcp,
    Rest,
    Go,
    Js,
}

impl ConsumerProfile {
    pub const fn as_str(&self) -> &'static str {
        match self {
            ConsumerProfile::PythonClient => "python",
            ConsumerProfile::Cli => "cli",
            ConsumerProfile::Mcp => "mcp",
            ConsumerProfile::Rest => "rest",
            ConsumerProfile::Go => "go",
            ConsumerProfile::Js => "js",
        }
    }
}

/// How the surface may be consumed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Stability {
    /// Published to at least one generated consumer profile.
    Stable,
    /// Engine-internal: reachable on the wire, but no consumer is generated and none
    /// may be assumed. Replaces the deleted unbound-method baseline file.
    Internal,
    /// Scheduled for removal; still generated.
    Deprecated,
}

impl Stability {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Stability::Stable => "stable",
            Stability::Internal => "internal",
            Stability::Deprecated => "deprecated",
        }
    }
}

pub(crate) const PYTHON: &[ConsumerProfile] = &[ConsumerProfile::PythonClient];
pub(crate) const NO_CONSUMER: &[ConsumerProfile] = &[];

/// The per-method facts a domain row authors by hand.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MethodSpec {
    pub policy: MethodPolicy,
    pub consumer_profiles: &'static [ConsumerProfile],
    pub stability: Stability,
}

/// The complete contract row for one `Method` variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MethodDescriptor {
    pub id: MethodId,
    pub domain: &'static str,
    pub error_set: &'static [&'static str],
    pub policy: MethodPolicy,
    pub replay_class: ReplayClass,
    pub consumer_profiles: &'static [ConsumerProfile],
    pub stability: Stability,
    pub format_identities: &'static [&'static str],
    /// The row's free-text note, carried through to the Markdown ledger.
    pub note: &'static str,
}

impl MethodDescriptor {
    /// Assemble a descriptor from its authored row plus the four documented derivations.
    pub(crate) const fn assemble(
        id: &'static str,
        domain: &'static str,
        spec: MethodSpec,
        note: &'static str,
    ) -> Self {
        Self {
            id: MethodId(id),
            domain,
            error_set: error_set_for(&spec.policy),
            policy: spec.policy,
            replay_class: replay_class_for(&spec.policy),
            consumer_profiles: spec.consumer_profiles,
            stability: spec.stability,
            format_identities: format_identities_for(spec.policy.durability_domain),
            note,
        }
    }

    /// True when a generated client function exists for `profile`.
    pub fn serves(&self, profile: ConsumerProfile) -> bool {
        let mut i = 0;
        while i < self.consumer_profiles.len() {
            if self.consumer_profiles[i] as u8 == profile as u8 {
                return true;
            }
            i += 1;
        }
        false
    }
}

// ── Derivations ─────────────────────────────────────────────────────────────

/// Codes every method may return: an argument the wire decoder rejects, and the
/// dispatch authorization refusal keyed on `MethodPolicy::authz_action`.
const READ_ERRORS: &[&str] = &["INVALID_ARGUMENT", "ACCESS_DENIED"];
/// A volatile (process/session) transition adds only the staging-conflict code.
const VOLATILE_ERRORS: &[&str] = &["INVALID_ARGUMENT", "ACCESS_DENIED", "CONFLICT"];
/// A durable mutation additionally replays or refuses on identity, is redirected by
/// placement, and is refused outright on a read-only replica.
const DURABLE_ERRORS: &[&str] = &[
    "INVALID_ARGUMENT",
    "ACCESS_DENIED",
    "CONFLICT",
    "IDEMPOTENCY_CONFLICT",
    "REDIRECTED",
    "READ_ONLY",
];

/// The error codes a method may legitimately return, derived from its policy.
///
/// EG has no per-method error catalog today (the wire carries `error: Option<String>`),
/// so a per-method prose list would be 408 invented values. These three policy-shaped
/// sets are the honest catalog: they are exactly the classes the dispatch, authorization,
/// placement, and mutation-admission paths can produce for a method of that shape.
pub const fn error_set_for(policy: &MethodPolicy) -> &'static [&'static str] {
    if !policy.mutates {
        return READ_ERRORS;
    }
    if policy.is_durable() {
        DURABLE_ERRORS
    } else {
        VOLATILE_ERRORS
    }
}

/// RF-RULING-004 replay class, derived from the policy's durability.
///
/// A durable mutation is admitted by `MutationKernel` and therefore carries a stable
/// `OperationReplayIdentity`. An explicitly volatile transition consumes only a
/// `NonceReplayKey`. A read mutates nothing and is never replayed.
pub const fn replay_class_for(policy: &MethodPolicy) -> ReplayClass {
    if !policy.mutates {
        return ReplayClass::NotReplayable;
    }
    if policy.is_durable() {
        ReplayClass::OperationIdentity
    } else {
        ReplayClass::NonceOnly
    }
}

const GRAPH_FORMATS: &[&str] = &[
    "STORAGE_KERNEL_SCHEMA_VERSION",
    "GRAPH_SNAPSHOT_SCHEMA_VERSION",
    "GRAPH_META_SCHEMA_VERSION",
];
const JOBS_FORMATS: &[&str] = &[
    "STORAGE_KERNEL_SCHEMA_VERSION",
    "ANALYTICS_JOB_SCOPE_INCARNATION",
];
const STATECHART_FORMATS: &[&str] = &[
    "STORAGE_KERNEL_SCHEMA_VERSION",
    "INSTANCE_MUTATION_INCARNATION",
];
const CONTROL_FORMATS: &[&str] = &[
    "RBAC_SCOPE_INCARNATION",
    "CONSENSUS_TRANSACTION_SCHEMA_VERSION",
];
/// Every other durable store is opened and identified by `StorageKernel` alone
/// (RF-RULING-004), which owns no second per-domain format constant in this tree.
const STORE_FORMATS: &[&str] = &["STORAGE_KERNEL_SCHEMA_VERSION"];
const NO_FORMATS: &[&str] = &[];

/// The persisted format identities a method's effect touches.
///
/// The durability domain IS the format owner: a method that commits to `GraphRedb`
/// touches the graph snapshot/meta identities and nothing else. `gen_contract` resolves
/// each name against the real constant it collects from the tree, so a renamed or
/// re-versioned constant is a receipt diff rather than silent rot.
pub const fn format_identities_for(domain: DurabilityDomain) -> &'static [&'static str] {
    match domain {
        DurabilityDomain::GraphRedb => GRAPH_FORMATS,
        DurabilityDomain::JobsRedb => JOBS_FORMATS,
        DurabilityDomain::StatechartRedb => STATECHART_FORMATS,
        DurabilityDomain::ControlRedb => CONTROL_FORMATS,
        DurabilityDomain::SeriesRedb
        | DurabilityDomain::KvRedb
        | DurabilityDomain::BlobRedb
        | DurabilityDomain::Outbox
        | DurabilityDomain::SemanticIndexRedb
        | DurabilityDomain::ReasoningProjection => STORE_FORMATS,
        DurabilityDomain::VolatileControl | DurabilityDomain::None => NO_FORMATS,
    }
}
