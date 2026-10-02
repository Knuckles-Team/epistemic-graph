//! Just-in-time RBAC elevation (EH-404): an `rbac.elevation` control lease that
//! the graph-access chokepoint (`IsolationLayer::check_access`) consults.
//!
//! An elevation lets one verified identity use exactly the named graph actions
//! for a bounded time, after a DIFFERENT identity approved it. It shares the
//! control-lease contract (`crate::control_lease`): a typed kind, an immutable
//! grant body, a hard expiry capped at [`MAX_ELEVATION_SPAN_MS`] (the control
//! lease span cap), and a one-way lifecycle whose every edge is a table entry.
//! It adds what a generic control lease cannot enforce by itself:
//!
//! * **two-person rule** -- the approver must share no identity (principal,
//!   effective agent, or delegation hop) with the requester, and must act
//!   directly: a delegated (agent-on-behalf-of) context can never approve;
//! * **exact scope** -- a grant names one graph and one action per scope, the
//!   graph is a literal name (a wildcard is refused, never widened), and the
//!   action is `read` or `write` only: no elevation can reach `admin`, so no
//!   elevation can mint a permanent grant for itself;
//! * **hard expiry at check time** -- `permits` compares the lease's
//!   `hard_expires_at_ms` with the caller's clock on every check; the sweep
//!   that marks a lease `expired` is bookkeeping, not the authority;
//! * **no extension** -- there is no renew or extend operation: a new window
//!   is a new request with a new approval;
//! * **replay-bound approval** -- an approval names the digest of the exact
//!   request it approves, and only a `requested` lease can be approved.
//!
//! The ledger lives inside the RBAC policy image, so an elevation is written
//! in the same durable transaction as every other authorization change and is
//! never reachable through the generic control-lease methods:
//! `IssueControlLease` refuses the reserved kind [`RBAC_ELEVATION_KIND`].
//!
//! Identity is never a field a caller supplies: the server stamps an
//! [`ElevationActor`] from the verified request context.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

mod ledger;

pub use ledger::{
    ElevationAuditEntry, ElevationEvent, ElevationLease, ElevationLedger, ElevationStatus,
    ELEVATION_RETENTION_MS, MAX_ELEVATION_AUDIT_ENTRIES, MAX_ELEVATION_LEASES,
    MAX_LIVE_ELEVATIONS_PER_GRANTEE,
};

/// The reserved control-lease kind of an elevation.
pub use crate::control_lease::RBAC_ELEVATION_KIND;
/// Longest window an approval can open: the control-lease span cap (24 h).
pub const MAX_ELEVATION_SPAN_MS: u64 = crate::control_lease::MAX_CONTROL_LEASE_SPAN_MS;
/// Shortest window worth approving; anything shorter is a caller error.
pub const MIN_ELEVATION_SPAN_MS: u64 = 1_000;
/// How long a request waits for its approval before it expires unapproved.
pub const ELEVATION_REQUEST_TTL_MS: u64 = 60 * 60 * 1000;
/// Most scopes one elevation may name.
pub const MAX_ELEVATION_SCOPES: usize = 16;
/// Longest elevation id.
pub const MAX_ELEVATION_ID_BYTES: usize = 128;
/// Longest graph name a scope may name.
pub const MAX_ELEVATION_GRAPH_BYTES: usize = 256;
/// Longest justification.
pub const MAX_ELEVATION_JUSTIFICATION_BYTES: usize = 1024;
/// The scope a principal must hold, exactly, to approve an elevation.
pub const APPROVE_ELEVATION_SCOPE: &str = "rbac:approve-elevation";

/// Graph names no elevation may name: the admin pseudo-graph that
/// `has_admin_capability` evaluates against.
const RESERVED_GRAPHS: [&str; 1] = ["__admin__"];
/// Characters a glob would read as a wildcard.
const WILDCARD_CHARS: [char; 2] = ['*', '?'];

/// The graph action an elevation may grant. Deliberately NOT
/// `crate::acl::RbacAction`: `Admin` is unrepresentable here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ElevationAction {
    Read,
    Write,
}

/// One granted (graph, action) pair. The graph is a literal graph name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ElevationScope {
    pub graph: String,
    pub action: ElevationAction,
}

/// `request`: ask for `scopes` for `span_ms` once approved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ElevationRequest {
    pub elevation_id: String,
    pub scopes: Vec<ElevationScope>,
    /// Window length. The clock starts at approval, never at request.
    pub span_ms: u64,
    pub justification: String,
}

/// `approve`: approve the request whose digest is `request_digest`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ElevationApproval {
    pub elevation_id: String,
    /// `request_digest` of the lease view the approver decided on.
    pub request_digest: String,
}

/// `revoke`: end a requested or active elevation now.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ElevationRevoke {
    pub elevation_id: String,
}

/// Every elevation operation. The read/write split and the authorization
/// action live on the op.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum RbacElevationOp {
    Request {
        request: ElevationRequest,
    },
    Approve {
        request: ElevationApproval,
    },
    Revoke {
        request: ElevationRevoke,
    },
    /// The elevations visible to the caller: an approver sees every one,
    /// anyone else only those they are a party to.
    List,
}

impl RbacElevationOp {
    pub fn is_mutation(&self) -> bool {
        match self {
            Self::Request { .. } | Self::Approve { .. } | Self::Revoke { .. } => true,
            Self::List => false,
        }
    }

    /// The capability-ledger action. Approval additionally requires the exact
    /// [`APPROVE_ELEVATION_SCOPE`] at the handler: no wildcard or aggregate
    /// scope stands in for it.
    pub fn authz_action(&self) -> &'static str {
        match self {
            Self::Request { .. } | Self::Revoke { .. } => "rbac:elevation",
            Self::Approve { .. } => APPROVE_ELEVATION_SCOPE,
            Self::List => "rbac:elevation-read",
        }
    }

    /// The op's wire tag, for audit lines and diagnostics.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Request { .. } => "request",
            Self::Approve { .. } => "approve",
            Self::Revoke { .. } => "revoke",
            Self::List => "list",
        }
    }

    /// The elevation an op names, if any.
    pub fn elevation_id(&self) -> Option<&str> {
        match self {
            Self::Request { request } => Some(&request.elevation_id),
            Self::Approve { request } => Some(&request.elevation_id),
            Self::Revoke { request } => Some(&request.elevation_id),
            Self::List => None,
        }
    }
}

/// Whether the actor acted directly or through a delegation chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ElevationDelegation {
    Direct,
    Delegated,
}

/// Whether the actor holds the exact approval scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ElevationStanding {
    Requester,
    Approver,
}

/// Who performed an elevation op. SERVER-STAMPED from the verified request
/// context at the request boundary; a caller-supplied value is overwritten
/// before the op reaches consensus or the ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ElevationActor {
    /// The effective identity `check_access` authorizes.
    pub agent_id: String,
    /// One-way [`party_id`]s of the principal, the effective agent and every
    /// delegation hop; sorted and unique.
    pub parties: Vec<String>,
    pub delegation: ElevationDelegation,
    pub standing: ElevationStanding,
}

impl ElevationActor {
    /// Build an actor from raw verified identities. Raw subjects are hashed
    /// here and never stored.
    pub fn from_identities<'a>(
        agent_id: &str,
        identities: impl IntoIterator<Item = &'a str>,
        delegation: ElevationDelegation,
        standing: ElevationStanding,
    ) -> Self {
        let mut parties: Vec<String> = identities.into_iter().map(party_id).collect();
        parties.push(party_id(agent_id));
        parties.sort();
        parties.dedup();
        Self {
            agent_id: agent_id.to_string(),
            parties,
            delegation,
            standing,
        }
    }

    /// Whether this actor shares any identity with `parties`.
    pub fn shares_party(&self, parties: &[String]) -> bool {
        self.parties
            .iter()
            .any(|party| parties.binary_search(party).is_ok())
    }

    /// One digest naming the whole party set, for audit entries.
    pub fn parties_digest(&self) -> String {
        digest_hex(
            b"eg/elevation-actor/v1\0",
            self.parties.join("\n").as_bytes(),
        )
    }
}

/// The one-way identifier of one identity string.
pub fn party_id(identity: &str) -> String {
    digest_hex(b"eg/elevation-party/v1\0", identity.as_bytes())
}

fn digest_hex(domain: &[u8], body: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(body);
    hex::encode(hasher.finalize())
}

/// Why an elevation op was refused. Every refusal is typed; none is a
/// fail-open fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElevationRefusal {
    /// A field is outside its native bounds.
    InvalidRequest,
    /// A scope names a wildcard; elevations name literal graphs only.
    Wildcard,
    /// A scope names a reserved graph.
    ReservedGraph,
    /// The elevation id is already in the ledger.
    Collision,
    /// The ledger or the grantee's live set is full.
    LedgerFull,
    /// No elevation with this id.
    NotFound,
    /// The approver shares an identity with the requester.
    SelfApproval,
    /// The approver acted through a delegation chain.
    DelegatedApprover,
    /// The actor does not hold the exact approval scope.
    NotApprover,
    /// The elevation is not in a state this op may move it from: an approval
    /// of anything but a `requested` lease (a replay), or ending one that has
    /// already ended.
    Conflict,
    /// The approval names a different request than the one on record.
    DigestMismatch,
    /// The actor is neither a party to the elevation nor an approver.
    NotParty,
    /// The actor is not a registered identity.
    UnknownActor,
}

impl ElevationRefusal {
    pub fn code(self) -> &'static str {
        match self {
            Self::InvalidRequest => "ELEVATION_INVALID",
            Self::Wildcard => "ELEVATION_WILDCARD",
            Self::ReservedGraph => "ELEVATION_RESERVED_GRAPH",
            Self::Collision => "ELEVATION_COLLISION",
            Self::LedgerFull => "ELEVATION_LEDGER_FULL",
            Self::NotFound => "ELEVATION_NOT_FOUND",
            Self::SelfApproval => "ELEVATION_SELF_APPROVAL",
            Self::DelegatedApprover => "ELEVATION_DELEGATED_APPROVER",
            Self::NotApprover => "ELEVATION_NOT_APPROVER",
            Self::Conflict => "ELEVATION_CONFLICT",
            Self::DigestMismatch => "ELEVATION_DIGEST_MISMATCH",
            Self::NotParty => "ELEVATION_NOT_PARTY",
            Self::UnknownActor => "ELEVATION_UNKNOWN_ACTOR",
        }
    }
}

impl std::fmt::Display for ElevationRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: elevation refused ({self:?})", self.code())
    }
}

impl std::error::Error for ElevationRefusal {}

fn bounded_text(value: &str, max: usize) -> Result<(), ElevationRefusal> {
    if crate::contract::bounded_control_free_text(value, max) {
        Ok(())
    } else {
        Err(ElevationRefusal::InvalidRequest)
    }
}

/// Validate an elevation id (also used by approve/revoke).
pub fn validate_elevation_id(elevation_id: &str) -> Result<(), ElevationRefusal> {
    bounded_text(elevation_id, MAX_ELEVATION_ID_BYTES)
}

impl ElevationScope {
    /// A scope must name one literal, non-reserved graph.
    pub fn validate(&self) -> Result<(), ElevationRefusal> {
        let graph = self.graph.as_str();
        if graph.contains(WILDCARD_CHARS) {
            return Err(ElevationRefusal::Wildcard);
        }
        if RESERVED_GRAPHS.contains(&graph) {
            return Err(ElevationRefusal::ReservedGraph);
        }
        if graph != graph.trim() {
            return Err(ElevationRefusal::InvalidRequest);
        }
        bounded_text(graph, MAX_ELEVATION_GRAPH_BYTES)
    }
}

impl ElevationRequest {
    pub fn validate(&self) -> Result<(), ElevationRefusal> {
        validate_elevation_id(&self.elevation_id)?;
        bounded_text(&self.justification, MAX_ELEVATION_JUSTIFICATION_BYTES)?;
        let span_ok = (MIN_ELEVATION_SPAN_MS..=MAX_ELEVATION_SPAN_MS).contains(&self.span_ms);
        if !span_ok || self.scopes.is_empty() || self.scopes.len() > MAX_ELEVATION_SCOPES {
            return Err(ElevationRefusal::InvalidRequest);
        }
        for scope in &self.scopes {
            scope.validate()?;
        }
        if self.canonical_scopes().len() != self.scopes.len() {
            return Err(ElevationRefusal::InvalidRequest);
        }
        Ok(())
    }

    /// Scopes sorted and de-duplicated: the stored, digested form.
    pub fn canonical_scopes(&self) -> Vec<ElevationScope> {
        let mut scopes = self.scopes.clone();
        scopes.sort();
        scopes.dedup();
        scopes
    }
}

#[cfg(test)]
mod tests;
