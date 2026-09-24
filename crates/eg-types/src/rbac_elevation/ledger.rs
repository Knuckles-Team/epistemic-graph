//! The elevation ledger: every lease, its one-way lifecycle, the check-time
//! grant decision, and a bounded hash-chained audit trail. Pure data and pure
//! transitions over a caller-supplied clock, so the whole lifecycle is
//! exhaustively testable here and replays identically on every replica.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    validate_elevation_id, ElevationAction, ElevationActor, ElevationApproval, ElevationDelegation,
    ElevationRefusal, ElevationRequest, ElevationRevoke, ElevationScope, ElevationStanding,
    ELEVATION_REQUEST_TTL_MS, RBAC_ELEVATION_KIND,
};

/// Most leases (live and recently ended) one ledger holds.
pub const MAX_ELEVATION_LEASES: usize = 1024;
/// Most requested-or-active leases one grantee may hold at once.
pub const MAX_LIVE_ELEVATIONS_PER_GRANTEE: usize = 8;
/// How long an ended lease stays on record (so its id cannot be reused).
pub const ELEVATION_RETENTION_MS: u64 = 24 * 60 * 60 * 1000;
/// Most audit entries retained; older entries roll off the front, and the
/// chain stays verifiable from the oldest retained entry's `prev`.
pub const MAX_ELEVATION_AUDIT_ENTRIES: usize = 1024;

/// Where an elevation is in its one-way lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ElevationStatus {
    /// Waiting for a second identity's approval; grants nothing.
    Requested,
    /// Approved and inside its window; grants its scopes.
    Active,
    Revoked,
    Expired,
}

/// Every legal lifecycle edge. Anything absent -- re-approving, reviving an
/// ended lease, approving an expired request -- is a conflict.
const LEGAL_TRANSITIONS: [(ElevationStatus, ElevationStatus); 5] = [
    (ElevationStatus::Requested, ElevationStatus::Active),
    (ElevationStatus::Requested, ElevationStatus::Revoked),
    (ElevationStatus::Requested, ElevationStatus::Expired),
    (ElevationStatus::Active, ElevationStatus::Revoked),
    (ElevationStatus::Active, ElevationStatus::Expired),
];

impl ElevationStatus {
    fn may_move_to(self, to: Self) -> bool {
        LEGAL_TRANSITIONS.contains(&(self, to))
    }

    fn is_live(self) -> bool {
        matches!(self, Self::Requested | Self::Active)
    }
}

/// What an audit entry records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ElevationEvent {
    Requested,
    Approved,
    Revoked,
    Expired,
}

/// One elevation: a control lease of kind [`RBAC_ELEVATION_KIND`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ElevationLease {
    pub elevation_id: String,
    /// Always [`RBAC_ELEVATION_KIND`].
    pub kind: String,
    /// The effective identity the scopes are granted to (the requester).
    pub grantee: String,
    /// The requester's one-way party ids; an approver may share none.
    pub requester_parties: Vec<String>,
    /// The approver's one-way party ids, once approved.
    pub approver_parties: Vec<String>,
    /// The immutable grant body, sorted and unique.
    pub scopes: Vec<ElevationScope>,
    pub span_ms: u64,
    pub justification: String,
    pub status: ElevationStatus,
    pub requested_at_ms: u64,
    pub approved_at_ms: Option<u64>,
    /// `approved_at_ms + span_ms`; the authority `permits` checks.
    pub hard_expires_at_ms: Option<u64>,
    pub ended_at_ms: Option<u64>,
    /// Digest of the request as recorded; an approval must name it.
    pub request_digest: String,
    /// 1 at request, bumped by every transition.
    pub revision: u64,
}

impl ElevationLease {
    /// The status an observer at `now_ms` must act on: a lease past its hard
    /// expiry (or a request past its approval window) is expired even before
    /// the ledger's sweep has written that down.
    pub fn effective_status(&self, now_ms: u64) -> ElevationStatus {
        match self.status {
            ElevationStatus::Active if self.hard_expires_at_ms.is_none_or(|end| now_ms >= end) => {
                ElevationStatus::Expired
            }
            ElevationStatus::Requested if now_ms >= self.request_deadline_ms() => {
                ElevationStatus::Expired
            }
            status => status,
        }
    }

    fn request_deadline_ms(&self) -> u64 {
        self.requested_at_ms
            .saturating_add(ELEVATION_REQUEST_TTL_MS)
    }

    /// Whether this lease grants `action` on `graph` to `agent_id` at `now_ms`.
    fn grants(&self, agent_id: &str, scope: &ElevationScope, now_ms: u64) -> bool {
        self.grantee == agent_id
            && self.effective_status(now_ms) == ElevationStatus::Active
            && self.scopes.binary_search(scope).is_ok()
    }

    fn is_party(&self, actor: &ElevationActor) -> bool {
        actor.shares_party(&self.requester_parties)
    }

    fn move_to(&mut self, to: ElevationStatus, now_ms: u64) -> Result<(), ElevationRefusal> {
        if !self.status.may_move_to(to) {
            return Err(ElevationRefusal::Conflict);
        }
        self.status = to;
        self.revision += 1;
        if to == ElevationStatus::Active {
            self.approved_at_ms = Some(now_ms);
            self.hard_expires_at_ms = Some(now_ms.saturating_add(self.span_ms));
        } else {
            self.ended_at_ms = Some(self.end_time(to, now_ms));
        }
        Ok(())
    }

    /// When an ending edge ended the lease: an expiry ended at its deadline,
    /// not whenever the sweep noticed.
    fn end_time(&self, to: ElevationStatus, now_ms: u64) -> u64 {
        if to == ElevationStatus::Expired {
            self.natural_deadline_ms().min(now_ms)
        } else {
            now_ms
        }
    }

    /// The approved window's end, or the unapproved request's deadline.
    fn natural_deadline_ms(&self) -> u64 {
        self.hard_expires_at_ms
            .unwrap_or_else(|| self.request_deadline_ms())
    }
}

/// The digested identity of one request, bound into every approval.
#[derive(Serialize)]
struct RequestDigestBody<'a> {
    elevation_id: &'a str,
    grantee: &'a str,
    requester_parties: &'a [String],
    scopes: &'a [ElevationScope],
    span_ms: u64,
    justification: &'a str,
    requested_at_ms: u64,
}

/// One tamper-evident audit record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ElevationAuditEntry {
    pub seq: u64,
    pub at_ms: u64,
    pub event: ElevationEvent,
    pub elevation_id: String,
    /// Digest of the acting party set (empty for a sweep expiry).
    pub actor: String,
    pub revision: u64,
    /// `chain` of the previous entry (empty for the first ever).
    pub prev: String,
    /// sha256 over `prev` and this entry's other fields.
    pub chain: String,
}

#[derive(Serialize)]
struct AuditChainBody<'a> {
    prev: &'a str,
    seq: u64,
    at_ms: u64,
    event: ElevationEvent,
    elevation_id: &'a str,
    actor: &'a str,
    revision: u64,
}

impl ElevationAuditEntry {
    fn chain_of(&self) -> String {
        let body = AuditChainBody {
            prev: &self.prev,
            seq: self.seq,
            at_ms: self.at_ms,
            event: self.event,
            elevation_id: &self.elevation_id,
            actor: &self.actor,
            revision: self.revision,
        };
        let bytes = serde_json::to_vec(&body).unwrap_or_default();
        let mut hasher = Sha256::new();
        hasher.update(b"eg/elevation-audit/v1\0");
        hasher.update(bytes);
        hex::encode(hasher.finalize())
    }
}

/// Every elevation of one RBAC policy image plus its audit trail.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ElevationLedger {
    leases: BTreeMap<String, ElevationLease>,
    audit: Vec<ElevationAuditEntry>,
    next_seq: u64,
}

impl ElevationLedger {
    pub fn is_empty(&self) -> bool {
        self.leases.is_empty() && self.audit.is_empty()
    }

    pub fn get(&self, elevation_id: &str) -> Option<&ElevationLease> {
        self.leases.get(elevation_id)
    }

    pub fn audit(&self) -> &[ElevationAuditEntry] {
        &self.audit
    }

    /// The check-time decision: the id of a lease that grants `action` on
    /// `graph` to `agent_id` at `now_ms`, if any. Expiry is decided HERE,
    /// against the caller's clock, on every call.
    pub fn permitting(
        &self,
        agent_id: &str,
        graph: &str,
        action: ElevationAction,
        now_ms: u64,
    ) -> Option<&str> {
        let scope = ElevationScope {
            graph: graph.to_string(),
            action,
        };
        self.leases
            .values()
            .find(|lease| lease.grants(agent_id, &scope, now_ms))
            .map(|lease| lease.elevation_id.as_str())
    }

    /// Leases visible to `actor` at `now_ms`, with their effective status.
    pub fn visible_to(&self, actor: &ElevationActor, now_ms: u64) -> Vec<ElevationLease> {
        self.leases
            .values()
            .filter(|lease| actor.standing == ElevationStanding::Approver || lease.is_party(actor))
            .map(|lease| {
                let mut view = lease.clone();
                view.status = lease.effective_status(now_ms);
                view
            })
            .collect()
    }

    /// Record a new request. It grants nothing until a different identity
    /// approves it.
    pub fn request(
        &mut self,
        actor: &ElevationActor,
        request: &ElevationRequest,
        now_ms: u64,
    ) -> Result<ElevationLease, ElevationRefusal> {
        request.validate()?;
        self.settle(now_ms);
        if self.leases.contains_key(&request.elevation_id) {
            return Err(ElevationRefusal::Collision);
        }
        if self.leases.len() >= MAX_ELEVATION_LEASES
            || self.live_count(&actor.agent_id) >= MAX_LIVE_ELEVATIONS_PER_GRANTEE
        {
            return Err(ElevationRefusal::LedgerFull);
        }
        let lease = new_lease(actor, request, now_ms);
        self.leases
            .insert(lease.elevation_id.clone(), lease.clone());
        self.record(
            &lease,
            ElevationEvent::Requested,
            &actor.parties_digest(),
            now_ms,
        );
        Ok(lease)
    }

    /// Approve a request. Two-person, direct, digest-bound, one-shot.
    pub fn approve(
        &mut self,
        actor: &ElevationActor,
        approval: &ElevationApproval,
        now_ms: u64,
    ) -> Result<ElevationLease, ElevationRefusal> {
        validate_elevation_id(&approval.elevation_id)?;
        check_approver(actor)?;
        self.settle(now_ms);
        let lease = self
            .leases
            .get_mut(&approval.elevation_id)
            .ok_or(ElevationRefusal::NotFound)?;
        if lease.is_party(actor) {
            return Err(ElevationRefusal::SelfApproval);
        }
        if lease.status != ElevationStatus::Requested {
            return Err(ElevationRefusal::Conflict);
        }
        if lease.request_digest != approval.request_digest {
            return Err(ElevationRefusal::DigestMismatch);
        }
        lease.move_to(ElevationStatus::Active, now_ms)?;
        lease.approver_parties = actor.parties.clone();
        let lease = lease.clone();
        self.record(
            &lease,
            ElevationEvent::Approved,
            &actor.parties_digest(),
            now_ms,
        );
        Ok(lease)
    }

    /// End a requested or active elevation at once. Revocation only narrows,
    /// so a party to the elevation (self de-escalation) or any approver may
    /// revoke, directly or delegated.
    pub fn revoke(
        &mut self,
        actor: &ElevationActor,
        revoke: &ElevationRevoke,
        now_ms: u64,
    ) -> Result<ElevationLease, ElevationRefusal> {
        validate_elevation_id(&revoke.elevation_id)?;
        self.settle(now_ms);
        let lease = self
            .leases
            .get_mut(&revoke.elevation_id)
            .ok_or(ElevationRefusal::NotFound)?;
        if actor.standing != ElevationStanding::Approver && !lease.is_party(actor) {
            return Err(ElevationRefusal::NotParty);
        }
        lease.move_to(ElevationStatus::Revoked, now_ms)?;
        let lease = lease.clone();
        self.record(
            &lease,
            ElevationEvent::Revoked,
            &actor.parties_digest(),
            now_ms,
        );
        Ok(lease)
    }

    fn live_count(&self, grantee: &str) -> usize {
        self.leases
            .values()
            .filter(|lease| lease.grantee == grantee && lease.status.is_live())
            .count()
    }

    /// Bookkeeping before every mutation: write down expiries (each audited)
    /// and drop ended leases past retention. Never the grant authority --
    /// `permitting` decides expiry on its own.
    fn settle(&mut self, now_ms: u64) {
        let lapsed: Vec<String> = self
            .leases
            .values()
            .filter(|lease| lease.status.is_live() && !lease.effective_status(now_ms).is_live())
            .map(|lease| lease.elevation_id.clone())
            .collect();
        for elevation_id in lapsed {
            self.expire(&elevation_id, now_ms);
        }
        self.leases.retain(|_, lease| {
            lease
                .ended_at_ms
                .is_none_or(|ended| now_ms < ended.saturating_add(ELEVATION_RETENTION_MS))
        });
    }

    fn expire(&mut self, elevation_id: &str, now_ms: u64) {
        let Some(lease) = self.leases.get_mut(elevation_id) else {
            return;
        };
        if lease.move_to(ElevationStatus::Expired, now_ms).is_ok() {
            let lease = lease.clone();
            self.record(&lease, ElevationEvent::Expired, "", now_ms);
        }
    }

    fn record(&mut self, lease: &ElevationLease, event: ElevationEvent, actor: &str, now_ms: u64) {
        let prev = self
            .audit
            .last()
            .map(|entry| entry.chain.clone())
            .unwrap_or_default();
        let mut entry = ElevationAuditEntry {
            seq: self.next_seq,
            at_ms: now_ms,
            event,
            elevation_id: lease.elevation_id.clone(),
            actor: actor.to_string(),
            revision: lease.revision,
            prev,
            chain: String::new(),
        };
        entry.chain = entry.chain_of();
        self.next_seq += 1;
        self.audit.push(entry);
        if self.audit.len() > MAX_ELEVATION_AUDIT_ENTRIES {
            self.audit.remove(0);
        }
    }

    #[cfg(test)]
    pub(super) fn audit_for_test(&mut self) -> &mut Vec<ElevationAuditEntry> {
        &mut self.audit
    }

    /// Recompute every retained entry's chain link.
    pub fn verify_audit_chain(&self) -> bool {
        self.audit
            .windows(2)
            .all(|pair| pair[1].prev == pair[0].chain)
            && self
                .audit
                .iter()
                .all(|entry| entry.chain == entry.chain_of())
    }
}

fn check_approver(actor: &ElevationActor) -> Result<(), ElevationRefusal> {
    if actor.standing != ElevationStanding::Approver {
        return Err(ElevationRefusal::NotApprover);
    }
    if actor.delegation != ElevationDelegation::Direct {
        return Err(ElevationRefusal::DelegatedApprover);
    }
    Ok(())
}

fn new_lease(actor: &ElevationActor, request: &ElevationRequest, now_ms: u64) -> ElevationLease {
    let scopes = request.canonical_scopes();
    let body = RequestDigestBody {
        elevation_id: &request.elevation_id,
        grantee: &actor.agent_id,
        requester_parties: &actor.parties,
        scopes: &scopes,
        span_ms: request.span_ms,
        justification: &request.justification,
        requested_at_ms: now_ms,
    };
    let mut hasher = Sha256::new();
    hasher.update(b"eg/elevation-request/v1\0");
    hasher.update(serde_json::to_vec(&body).unwrap_or_default());
    ElevationLease {
        elevation_id: request.elevation_id.clone(),
        kind: RBAC_ELEVATION_KIND.to_string(),
        grantee: actor.agent_id.clone(),
        requester_parties: actor.parties.clone(),
        approver_parties: Vec::new(),
        scopes,
        span_ms: request.span_ms,
        justification: request.justification.clone(),
        status: ElevationStatus::Requested,
        requested_at_ms: now_ms,
        approved_at_ms: None,
        hard_expires_at_ms: None,
        ended_at_ms: None,
        request_digest: hex::encode(hasher.finalize()),
        revision: 1,
    }
}
