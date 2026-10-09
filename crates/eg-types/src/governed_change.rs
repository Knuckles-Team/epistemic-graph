//! Governed changes (EH-560): a reserved control-lease family whose approval
//! EG proves two-person.
//!
//! A generic `action.approval` control lease can be created and approved by
//! anyone holding lease-write, so EG cannot tell whether a second person
//! approved it. A governed change can: the proposal, the approval and the
//! consumption are EG transitions with EG-enforced rules --
//!
//! * **reserved kinds** -- every kind is `governed.<name>`, registered in
//!   [`GOVERNED_KINDS`] with its proposer and approver scopes; the generic
//!   `IssueControlLease` refuses the `governed.` prefix outright;
//! * **two-person** -- the approver holds the kind's EXACT approver scope,
//!   acts directly (never through a delegation chain) and shares no identity
//!   (principal, effective agent or delegation hop) with the proposer;
//! * **exact digest binding** -- a proposal names a `target` and the digest of
//!   the exact candidate; the approval names that digest, and the consumer
//!   must present the same kind, target and digest;
//! * **single use, bounded time** -- an approval is consumed at most once and
//!   only inside its window; nothing extends a window.
//!
//! Schema repair (`governed.schema-repair`) is the first consumer.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::identity::{AuditRecord, AuditTrail, IdentityEvent};

/// Prefix of every governed kind; reserved against generic control leases.
pub const GOVERNED_KIND_PREFIX: &str = "governed.";
/// The schema-repair kind (the first consumer).
pub const SCHEMA_REPAIR_KIND: &str = "governed.schema-repair";
/// Every governed kind: `(kind, proposer scope, approver scope)`.
pub const GOVERNED_KINDS: [(&str, &str, &str); 1] = [(
    SCHEMA_REPAIR_KIND,
    "governance:propose",
    "governance:approve-schema-repair",
)];
/// Read scope for `get`/`list`.
pub const GOVERNANCE_READ_SCOPE: &str = "governance:read";
/// Longest proposal and approval windows (the control-lease span cap).
pub const MAX_GOVERNED_WINDOW_MS: u64 = crate::control_lease::MAX_CONTROL_LEASE_SPAN_MS;
/// Most changes (live and recently ended) one ledger holds.
pub const MAX_GOVERNED_CHANGES: usize = 1024;
/// How long an ended change stays on record (its id cannot be reused).
pub const GOVERNED_RETENTION_MS: u64 = 24 * 60 * 60 * 1000;
const MAX_TEXT: usize = 1024;

/// The scopes a kind names, or `None` for an unregistered kind.
pub fn kind_scopes(kind: &str) -> Option<(&'static str, &'static str)> {
    GOVERNED_KINDS
        .iter()
        .find(|(name, _, _)| *name == kind)
        .map(|(_, propose, approve)| (*propose, *approve))
}

/// Who acted, stamped from the verified request context at the boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GovernedActor {
    pub agent_id: String,
    /// One-way ids of the principal, the effective agent and every
    /// delegation hop (sorted, unique) -- the two-person comparison set.
    pub parties: Vec<String>,
    pub delegated: bool,
    /// The actor's exact `governance:*` scopes.
    pub scopes: BTreeSet<String>,
}

impl GovernedActor {
    /// Build an actor from raw verified identities (hashed, never stored).
    pub fn from_identities<'a>(
        agent_id: &str,
        identities: impl IntoIterator<Item = &'a str>,
        delegated: bool,
        scopes: BTreeSet<String>,
    ) -> Self {
        let mut parties: Vec<String> = identities
            .into_iter()
            .chain(std::iter::once(agent_id))
            .map(crate::rbac_elevation::party_id)
            .collect();
        parties.sort();
        parties.dedup();
        Self {
            agent_id: agent_id.to_string(),
            parties,
            delegated,
            scopes,
        }
    }

    fn shares_party(&self, parties: &[String]) -> bool {
        self.parties
            .iter()
            .any(|party| parties.binary_search(party).is_ok())
    }
}

/// Where a change is in its one-way lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum GovernedStatus {
    Proposed,
    Approved,
    Consumed,
    Revoked,
    Expired,
}

/// One governed change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GovernedChange {
    pub change_id: String,
    pub kind: String,
    pub target: String,
    pub digest: String,
    pub justification: String,
    pub proposer: String,
    pub proposer_parties: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approver: Option<String>,
    pub status: GovernedStatus,
    pub proposed_at_ms: u64,
    /// A proposal not approved by then expires unapproved.
    pub proposal_expires_at_ms: u64,
    /// The approved window's length, fixed at proposal.
    pub window_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approved_until_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_at_ms: Option<u64>,
}

impl GovernedChange {
    /// The status at `now_ms` (expiry is decided here, on every read).
    pub fn status_at(&self, now_ms: u64) -> GovernedStatus {
        let expired = match self.status {
            GovernedStatus::Proposed => now_ms >= self.proposal_expires_at_ms,
            GovernedStatus::Approved => self
                .approved_until_ms
                .is_some_and(|until| now_ms >= until),
            GovernedStatus::Consumed | GovernedStatus::Revoked | GovernedStatus::Expired => false,
        };
        if expired {
            GovernedStatus::Expired
        } else {
            self.status
        }
    }
}

/// `propose`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GovernedProposal {
    pub change_id: String,
    pub kind: String,
    /// What the change applies to (for schema repair: the approved-source key).
    pub target: String,
    /// The digest of the exact candidate.
    pub digest: String,
    pub justification: String,
    /// How long the proposal waits for an approval.
    pub proposal_ttl_ms: u64,
    /// How long an approval stays consumable.
    pub window_ms: u64,
}

/// `approve`: approve the change whose candidate digest is `digest`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GovernedApproval {
    pub change_id: String,
    pub digest: String,
}

/// What a consumer presents: the exact change it is about to apply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GovernedConsumption {
    pub change_id: String,
    pub kind: String,
    pub target: String,
    pub digest: String,
}

/// Every governed-change operation. Consumption is NOT a wire op: only the
/// engine path that applies the change (e.g. the approved schema attach)
/// consumes, through `IsolationLayer::try_consume_governed_change`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum GovernedChangeOp {
    Propose { request: GovernedProposal },
    Approve { request: GovernedApproval },
    Revoke { change_id: String },
    Get { change_id: String },
    List,
}

impl GovernedChangeOp {
    pub fn is_mutation(&self) -> bool {
        matches!(
            self,
            Self::Propose { .. } | Self::Approve { .. } | Self::Revoke { .. }
        )
    }

    /// The coarse capability gate. The kind's EXACT proposer/approver scope
    /// is checked by the ledger itself (an approval names only a change id,
    /// so its kind -- and its approver scope -- is known only there).
    pub fn authz_action(&self) -> &'static str {
        match self {
            Self::Propose { .. } => "governance:propose",
            Self::Approve { .. } | Self::Revoke { .. } | Self::Get { .. } | Self::List => {
                GOVERNANCE_READ_SCOPE
            }
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Propose { .. } => "propose",
            Self::Approve { .. } => "approve",
            Self::Revoke { .. } => "revoke",
            Self::Get { .. } => "get",
            Self::List => "list",
        }
    }
}

/// Why a governed op was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GovernedRefusal {
    InvalidRequest,
    /// The kind is not registered.
    UnknownKind,
    /// The actor lacks the kind's exact proposer/approver scope.
    NotAuthorized,
    Collision,
    Full,
    NotFound,
    /// The approver shares an identity with the proposer.
    SelfApproval,
    /// The approver acted through a delegation chain.
    DelegatedApprover,
    /// The approval or consumption names a different candidate.
    DigestMismatch,
    /// The change is not in a state this op may move it from (a replayed
    /// approval, a second consumption, an expired window).
    Conflict,
}

impl GovernedRefusal {
    pub fn code(self) -> &'static str {
        match self {
            Self::InvalidRequest => "GOVERNED_INVALID",
            Self::UnknownKind => "GOVERNED_UNKNOWN_KIND",
            Self::NotAuthorized => "GOVERNED_NOT_AUTHORIZED",
            Self::Collision => "GOVERNED_COLLISION",
            Self::Full => "GOVERNED_LEDGER_FULL",
            Self::NotFound => "GOVERNED_NOT_FOUND",
            Self::SelfApproval => "GOVERNED_SELF_APPROVAL",
            Self::DelegatedApprover => "GOVERNED_DELEGATED_APPROVER",
            Self::DigestMismatch => "GOVERNED_DIGEST_MISMATCH",
            Self::Conflict => "GOVERNED_CONFLICT",
        }
    }
}

impl std::fmt::Display for GovernedRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: governed change refused ({self:?})", self.code())
    }
}

impl std::error::Error for GovernedRefusal {}

fn bounded(value: &str) -> Result<(), GovernedRefusal> {
    let clean = !value.trim().is_empty() && !value.chars().any(char::is_control);
    if clean && value.len() <= MAX_TEXT {
        Ok(())
    } else {
        Err(GovernedRefusal::InvalidRequest)
    }
}

impl GovernedProposal {
    fn validate(&self) -> Result<(), GovernedRefusal> {
        for field in [
            &self.change_id,
            &self.kind,
            &self.target,
            &self.digest,
            &self.justification,
        ] {
            bounded(field)?;
        }
        let windows = [self.proposal_ttl_ms, self.window_ms];
        if windows
            .iter()
            .any(|window| !(1_000..=MAX_GOVERNED_WINDOW_MS).contains(window))
        {
            return Err(GovernedRefusal::InvalidRequest);
        }
        Ok(())
    }
}

/// Every governed change of one authorization image, with its audit trail.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GovernedLedger {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    changes: BTreeMap<String, GovernedChange>,
    #[serde(default, skip_serializing_if = "AuditTrail::is_empty")]
    audit: AuditTrail,
}

impl GovernedLedger {
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty() && self.audit.is_empty()
    }

    pub fn audit(&self) -> &AuditTrail {
        &self.audit
    }

    fn record(&mut self, actor: &str, event: IdentityEvent, change_id: &str, now_ms: u64) {
        self.audit.append(AuditRecord {
            at_ms: now_ms,
            actor: actor.to_string(),
            event,
            target: Some(change_id.to_string()),
            ip_prefix: None,
            detail: String::new(),
        });
    }

    /// A change as it stands at `now_ms`.
    pub fn get(&self, change_id: &str, now_ms: u64) -> Option<GovernedChange> {
        self.changes.get(change_id).map(|change| {
            let mut view = change.clone();
            view.status = change.status_at(now_ms);
            view
        })
    }

    /// Every change as it stands at `now_ms`.
    pub fn list(&self, now_ms: u64) -> Vec<GovernedChange> {
        self.changes
            .keys()
            .filter_map(|id| self.get(id, now_ms))
            .collect()
    }

    /// Drop ended changes past retention, then check the bound.
    fn make_room(&mut self, now_ms: u64) -> Result<(), GovernedRefusal> {
        self.changes.retain(|_, change| {
            change
                .ended_at_ms
                .is_none_or(|ended| now_ms < ended.saturating_add(GOVERNED_RETENTION_MS))
        });
        if self.changes.len() >= MAX_GOVERNED_CHANGES {
            return Err(GovernedRefusal::Full);
        }
        Ok(())
    }

    pub fn propose(
        &mut self,
        actor: &GovernedActor,
        request: &GovernedProposal,
        now_ms: u64,
    ) -> Result<GovernedChange, GovernedRefusal> {
        request.validate()?;
        let (propose, _) = kind_scopes(&request.kind).ok_or(GovernedRefusal::UnknownKind)?;
        if !actor.scopes.contains(propose) {
            return Err(GovernedRefusal::NotAuthorized);
        }
        if self.changes.contains_key(&request.change_id) {
            return Err(GovernedRefusal::Collision);
        }
        self.make_room(now_ms)?;
        let change = GovernedChange {
            change_id: request.change_id.clone(),
            kind: request.kind.clone(),
            target: request.target.clone(),
            digest: request.digest.clone(),
            justification: request.justification.clone(),
            proposer: actor.agent_id.clone(),
            proposer_parties: actor.parties.clone(),
            approver: None,
            status: GovernedStatus::Proposed,
            proposed_at_ms: now_ms,
            proposal_expires_at_ms: now_ms.saturating_add(request.proposal_ttl_ms),
            window_ms: request.window_ms,
            approved_until_ms: None,
            ended_at_ms: None,
        };
        self.changes.insert(change.change_id.clone(), change.clone());
        self.record(&actor.agent_id, IdentityEvent::ChangeProposed, &change.change_id, now_ms);
        Ok(change)
    }

    pub fn approve(
        &mut self,
        actor: &GovernedActor,
        request: &GovernedApproval,
        now_ms: u64,
    ) -> Result<GovernedChange, GovernedRefusal> {
        let change = self
            .changes
            .get(&request.change_id)
            .ok_or(GovernedRefusal::NotFound)?;
        let (_, approve) = kind_scopes(&change.kind).ok_or(GovernedRefusal::UnknownKind)?;
        if !actor.scopes.contains(approve) {
            return Err(GovernedRefusal::NotAuthorized);
        }
        if actor.delegated {
            return Err(GovernedRefusal::DelegatedApprover);
        }
        if actor.shares_party(&change.proposer_parties) {
            return Err(GovernedRefusal::SelfApproval);
        }
        if change.status_at(now_ms) != GovernedStatus::Proposed {
            return Err(GovernedRefusal::Conflict);
        }
        if change.digest != request.digest {
            return Err(GovernedRefusal::DigestMismatch);
        }
        let change = self
            .changes
            .get_mut(&request.change_id)
            .ok_or(GovernedRefusal::NotFound)?;
        change.status = GovernedStatus::Approved;
        change.approver = Some(actor.agent_id.clone());
        change.approved_until_ms = Some(now_ms.saturating_add(change.window_ms));
        let approved = change.clone();
        self.record(&actor.agent_id, IdentityEvent::ChangeApproved, &approved.change_id, now_ms);
        Ok(approved)
    }

    /// End a live change now. The proposer or any approver of the kind may.
    pub fn revoke(
        &mut self,
        actor: &GovernedActor,
        change_id: &str,
        now_ms: u64,
    ) -> Result<GovernedChange, GovernedRefusal> {
        let change = self.changes.get(change_id).ok_or(GovernedRefusal::NotFound)?;
        let (_, approve) = kind_scopes(&change.kind).ok_or(GovernedRefusal::UnknownKind)?;
        let party = actor.shares_party(&change.proposer_parties);
        if !party && !actor.scopes.contains(approve) {
            return Err(GovernedRefusal::NotAuthorized);
        }
        let live = matches!(
            change.status_at(now_ms),
            GovernedStatus::Proposed | GovernedStatus::Approved
        );
        if !live {
            return Err(GovernedRefusal::Conflict);
        }
        let change = self
            .changes
            .get_mut(change_id)
            .ok_or(GovernedRefusal::NotFound)?;
        change.status = GovernedStatus::Revoked;
        change.ended_at_ms = Some(now_ms);
        let revoked = change.clone();
        self.record(&actor.agent_id, IdentityEvent::ChangeRevoked, change_id, now_ms);
        Ok(revoked)
    }

    /// Consume an approved change for exactly `consumption`: once, inside
    /// its window. The consumer is the engine path that applies it.
    pub fn consume(
        &mut self,
        consumption: &GovernedConsumption,
        consumer: &str,
        now_ms: u64,
    ) -> Result<GovernedChange, GovernedRefusal> {
        let change = self
            .changes
            .get(&consumption.change_id)
            .ok_or(GovernedRefusal::NotFound)?;
        let exact = change.kind == consumption.kind
            && change.target == consumption.target
            && change.digest == consumption.digest;
        if !exact {
            return Err(GovernedRefusal::DigestMismatch);
        }
        if change.status_at(now_ms) != GovernedStatus::Approved {
            return Err(GovernedRefusal::Conflict);
        }
        let change = self
            .changes
            .get_mut(&consumption.change_id)
            .ok_or(GovernedRefusal::NotFound)?;
        change.status = GovernedStatus::Consumed;
        change.ended_at_ms = Some(now_ms);
        let consumed = change.clone();
        self.record(consumer, IdentityEvent::ChangeConsumed, &consumption.change_id, now_ms);
        Ok(consumed)
    }
}

#[cfg(test)]
mod tests;
