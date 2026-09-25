//! The identity audit trail: a bounded, hash-chained record of every
//! identity event (§5.2), written in the same durable image as the change it
//! records. No entry carries a secret, a hash or a sealed value.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Most entries retained; older ones roll off the front, and the chain stays
/// verifiable from the oldest retained entry's `prev`.
pub const MAX_IDENTITY_AUDIT_ENTRIES: usize = 4_096;

/// What an entry records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum IdentityEvent {
    Initialized,
    ModeTransition,
    PolicyUpdated,
    UserCreated,
    UserUpdated,
    UserStatusChanged,
    UserUnlocked,
    PasswordSet,
    PasswordChanged,
    LoginSucceeded,
    LoginFailed,
    LoginThrottled,
    SessionRevoked,
    TokenIssued,
    TokenRedeemed,
    ApiKeyIssued,
    ApiKeyUsed,
    ApiKeyRevoked,
    MfaEnrolled,
    MfaVerified,
    MfaFailed,
    RecoveryCodesSet,
    RoleChanged,
    GroupChanged,
    IdpChanged,
    IdentityLinked,
    IdentityUnlinked,
    /// A `RegisterIdentity` outside the store (IDM-03).
    RbacIdentityRegistered,
    /// An `RbacAdmin` role or grant change (IDM-03).
    RbacPolicyChanged,
    /// A sampled authorization denial (IDM-03 durable denial sample).
    AccessDenied,
    /// An administrator's SQL dump was merged.
    Imported,
    /// EH-560 governed changes.
    ChangeProposed,
    ChangeApproved,
    ChangeRevoked,
    ChangeConsumed,
    /// Directory provisioning (SCIM / LDAP sync).
    UserProvisioned,
    UserDeprovisioned,
    DirectoryGroupChanged,
}

/// One tamper-evident entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct IdentityAuditEntry {
    pub seq: u64,
    pub at_ms: u64,
    /// The acting principal (or `engine` for a store-internal event).
    pub actor: String,
    pub event: IdentityEvent,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ip_prefix: Option<String>,
    /// A short, secret-free detail (an op name, a reason code).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub detail: String,
    /// `chain` of the previous entry (empty for the first ever).
    pub prev: String,
    /// sha256 over `prev` and every other field of this entry.
    pub chain: String,
}

/// What a caller records; the trail assigns `seq`, `prev` and `chain`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditRecord {
    pub at_ms: u64,
    pub actor: String,
    pub event: IdentityEvent,
    pub target: Option<String>,
    pub ip_prefix: Option<String>,
    pub detail: String,
}

#[derive(Serialize)]
struct ChainBody<'a> {
    prev: &'a str,
    seq: u64,
    at_ms: u64,
    actor: &'a str,
    event: IdentityEvent,
    target: Option<&'a str>,
    ip_prefix: Option<&'a str>,
    detail: &'a str,
}

impl IdentityAuditEntry {
    fn chain_of(&self) -> String {
        let body = ChainBody {
            prev: &self.prev,
            seq: self.seq,
            at_ms: self.at_ms,
            actor: &self.actor,
            event: self.event,
            target: self.target.as_deref(),
            ip_prefix: self.ip_prefix.as_deref(),
            detail: &self.detail,
        };
        let bytes = serde_json::to_vec(&body).unwrap_or_default();
        let mut hasher = Sha256::new();
        hasher.update(b"eg/identity-audit/v1\0");
        hasher.update(bytes);
        hex::encode(hasher.finalize())
    }
}

/// The bounded trail.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditTrail {
    entries: std::collections::VecDeque<IdentityAuditEntry>,
    next_seq: u64,
}

impl AuditTrail {
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn entries(&self) -> impl Iterator<Item = &IdentityAuditEntry> {
        self.entries.iter()
    }

    /// Append one record, rolling the oldest entry off past the bound.
    pub fn append(&mut self, record: AuditRecord) {
        let prev = self
            .entries
            .back()
            .map(|entry| entry.chain.clone())
            .unwrap_or_default();
        let mut entry = IdentityAuditEntry {
            seq: self.next_seq,
            at_ms: record.at_ms,
            actor: record.actor,
            event: record.event,
            target: record.target,
            ip_prefix: record.ip_prefix,
            detail: record.detail,
            prev,
            chain: String::new(),
        };
        entry.chain = entry.chain_of();
        self.next_seq += 1;
        self.entries.push_back(entry);
        while self.entries.len() > MAX_IDENTITY_AUDIT_ENTRIES {
            self.entries.pop_front();
        }
    }

    /// Verify every retained link. `Err(seq)` names the first broken entry.
    pub fn verify(&self) -> Result<(), u64> {
        let mut prev: Option<&str> = None;
        for entry in &self.entries {
            let linked = prev.is_none_or(|chain| chain == entry.prev);
            if !linked || entry.chain != entry.chain_of() {
                return Err(entry.seq);
            }
            prev = Some(&entry.chain);
        }
        Ok(())
    }

    /// Entries with `seq > after`, at most `limit`.
    pub fn page(&self, after: Option<u64>, limit: usize) -> Vec<IdentityAuditEntry> {
        self.entries
            .iter()
            .filter(|entry| after.is_none_or(|after| entry.seq > after))
            .take(limit)
            .cloned()
            .collect()
    }
}
