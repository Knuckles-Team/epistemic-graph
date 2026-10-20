//! Exact profile grant records for RbacOwner. Records do not authenticate an issuer.
//!
//! Arbora owns issuance. These checks cannot replace the current policy ceiling,
//! authenticated human approval, or the reserved owner transaction at admission.
use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

const MAX_TEXT_BYTES: usize = 4096;
pub const MAX_PROFILE_GRANT_BYTES: usize = 64 * 1024;
const INVALID: &str = "PROFILE_GRANT_INVALID";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileGrantKey {
    pub tenant: String,
    pub grant_id: String,
}

impl ProfileGrantKey {
    pub fn validate(&self) -> Result<(), String> {
        exact_text(&self.tenant)?;
        exact_text(&self.grant_id)
    }

    /// JSON tuple encoding prevents delimiter aliases between tenant and grant ID.
    pub fn encode(&self) -> Result<String, String> {
        self.validate()?;
        serde_json::to_string(&(&self.tenant, &self.grant_id)).map_err(|_| INVALID.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinnedProfileComponent {
    pub reference: String,
    pub revision: u64,
    /// Agent Library definition digests retain their `sha256:` prefix.
    pub digest: String,
}

impl PinnedProfileComponent {
    fn validate(&self) -> Result<(), String> {
        exact_text(&self.reference)?;
        digest(&self.digest)?;
        nonzero(self.revision)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileGrantBinding {
    pub beneficiary_principal: String,
    pub graph: String,
    pub profile: PinnedProfileComponent,
    pub capability: PinnedProfileComponent,
    pub scopes: BTreeSet<String>,
}

impl ProfileGrantBinding {
    fn validate(&self) -> Result<(), String> {
        exact_text(&self.beneficiary_principal)?;
        exact_text(&self.graph)?;
        self.profile.validate()?;
        self.capability.validate()?;
        if self.scopes.is_empty() {
            return Err(INVALID.into());
        }
        for scope in &self.scopes {
            exact_text(scope)?;
        }
        Ok(())
    }
}

/// Provenance to retain, not a caller-supplied authorization verdict.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProfileGrantApproval {
    DirectHuman {
        issuer_tenant: String,
        issuer_principal: String,
        approved_request_digest: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileGrantRevocation {
    pub revision: u64,
    pub revoked_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileUseGrant {
    pub schema_version: u16,
    pub key: ProfileGrantKey,
    pub binding: ProfileGrantBinding,
    pub approval: ProfileGrantApproval,
    pub revision: u64,
    pub policy_revision: u64,
    pub issued_at_ms: u64,
    pub expires_at_ms: u64,
    pub revocation: Option<ProfileGrantRevocation>,
}

impl ProfileUseGrant {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != 1 || self.expires_at_ms <= self.issued_at_ms {
            return Err(INVALID.into());
        }
        self.key.validate()?;
        self.binding.validate()?;
        nonzero(self.revision)?;
        nonzero(self.policy_revision)?;
        let ProfileGrantApproval::DirectHuman {
            issuer_tenant,
            issuer_principal,
            approved_request_digest,
        } = &self.approval;
        exact_text(issuer_principal)?;
        if issuer_tenant != &self.key.tenant
            || issuer_principal == &self.binding.beneficiary_principal
        {
            return Err(INVALID.into());
        }
        digest(approved_request_digest)?;
        if let Some(revocation) = &self.revocation {
            if revocation.revision == 0
                || revocation.revision > self.revision
                || revocation.revoked_at_ms < self.issued_at_ms
            {
                return Err(INVALID.into());
            }
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, String> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|_| INVALID.to_string())?;
        if bytes.len() > MAX_PROFILE_GRANT_BYTES {
            return Err(INVALID.into());
        }
        Ok(bytes)
    }

    pub fn decode(key: &ProfileGrantKey, bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() > MAX_PROFILE_GRANT_BYTES {
            return Err(INVALID.into());
        }
        let record: Self = serde_json::from_slice(bytes).map_err(|_| INVALID.to_string())?;
        record.validate()?;
        if &record.key != key {
            return Err("PROFILE_GRANT_KEY_MISMATCH".into());
        }
        Ok(record)
    }

    /// Check a reserved record against exact current facts. This is not admission.
    /// The owner must supply decision time and policy/session state atomically.
    pub fn validate_binding(
        &self,
        key: &ProfileGrantKey,
        binding: &ProfileGrantBinding,
        revision: u64,
        policy_revision: u64,
        now_ms: u64,
    ) -> Result<(), String> {
        self.validate()?;
        if &self.key != key
            || &self.binding != binding
            || self.revision != revision
            || self.policy_revision != policy_revision
        {
            return Err("PROFILE_GRANT_BINDING_MISMATCH".into());
        }
        if self.revocation.is_some() {
            return Err("PROFILE_GRANT_REVOKED".into());
        }
        if now_ms < self.issued_at_ms || now_ms >= self.expires_at_ms {
            return Err("PROFILE_GRANT_EXPIRED".into());
        }
        Ok(())
    }

    /// Permanent local record transition; authorization belongs to the owner caller.
    pub(crate) fn revoke(&self, expected_revision: u64, now_ms: u64) -> Result<Self, String> {
        self.validate()?;
        if self.revision != expected_revision {
            return Err("PROFILE_GRANT_STALE_REVISION".into());
        }
        if self.revocation.is_some() {
            return Err("PROFILE_GRANT_REVOKED".into());
        }
        if now_ms < self.issued_at_ms {
            return Err(INVALID.into());
        }
        let mut next = self.clone();
        next.revision = next.revision.checked_add(1).ok_or(INVALID)?;
        next.revocation = Some(ProfileGrantRevocation {
            revision: next.revision,
            revoked_at_ms: now_ms,
        });
        Ok(next)
    }
}

fn exact_text(value: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > MAX_TEXT_BYTES
        || value.trim() != value
        || value
            .chars()
            .any(|c| c.is_control() || matches!(c, '*' | '?'))
    {
        return Err(INVALID.into());
    }
    Ok(())
}

fn digest(value: &str) -> Result<(), String> {
    let encoded = value.strip_prefix("sha256:").ok_or(INVALID)?;
    if encoded.len() != 64
        || !encoded
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(INVALID.into());
    }
    Ok(())
}

fn nonzero(value: u64) -> Result<(), String> {
    if value == 0 {
        return Err(INVALID.into());
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests;
