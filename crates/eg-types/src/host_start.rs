//! Issuer-bound host-start records. Arbora owns admission and launch policy;
//! this module only describes the assertions its authorized issuer persists.
use serde::{Deserialize, Serialize};

use crate::control_lease::{IssueControlLeaseRequest, MAX_CONTROL_LEASE_SPAN_MS};

pub const HOST_START_KIND: &str = "host.start";
pub const HOST_START_ISSUE_SCOPE: &str = "host:issue-start";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum HostStartInterfaces {
    Terminal,
    Web,
    Both,
}

/// Immutable assertions from the access owner, not evidence of session liveness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HostStartBinding {
    pub principal_ref: String,
    pub session_ref: String,
    pub workspace_ref: String,
    pub configuration_digest: String,
    pub interfaces: HostStartInterfaces,
    pub web_logs: bool,
    pub policy_revision: String,
    pub session_expires_at_ms: u64,
}

/// Storage-owned provenance, never read from the arbitrary grant body.
/// `issuer_ref` is the existing MutationBatch privacy-safe principal fingerprint
/// (`principal:sha256:<digest>`), not a raw principal or a caller assertion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HostStartProvenance {
    pub schema_version: u32,
    pub issuer_ref: String,
}

impl HostStartProvenance {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != 1 {
            return Err("unsupported host-start provenance version".into());
        }
        bounded("issuer_ref", &self.issuer_ref)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct IssueHostStartGrantRequest {
    pub tenant: String,
    pub lease_id: String,
    pub binding: HostStartBinding,
    pub expires_at_ms: u64,
    pub idempotency_key: String,
}

fn bounded(field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
        return Err(format!("host-start {field} is outside native bounds"));
    }
    Ok(())
}

impl IssueHostStartGrantRequest {
    /// Build a lease using engine time. This never validates a live session;
    /// the access owner must resolve that authority before and after awaits.
    pub fn lease_at(&self, now_ms: u64) -> Result<IssueControlLeaseRequest, String> {
        for (field, value) in [
            ("tenant", &self.tenant),
            ("lease_id", &self.lease_id),
            ("idempotency_key", &self.idempotency_key),
            ("principal_ref", &self.binding.principal_ref),
            ("session_ref", &self.binding.session_ref),
            ("workspace_ref", &self.binding.workspace_ref),
            ("policy_revision", &self.binding.policy_revision),
        ] {
            bounded(field, value)?;
        }
        let digest = self.binding.configuration_digest.as_bytes();
        if digest.len() != 64
            || !digest
                .iter()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err("host-start configuration digest must be lowercase SHA-256".into());
        }
        let expires = self
            .expires_at_ms
            .min(self.binding.session_expires_at_ms)
            .min(now_ms.saturating_add(MAX_CONTROL_LEASE_SPAN_MS));
        if now_ms == 0 || expires <= now_ms {
            return Err("host-start grant is already expired".into());
        }
        let value = serde_json::to_value(&self.binding).map_err(|e| e.to_string())?;
        let grant = value
            .as_object()
            .ok_or("invalid host-start binding")?
            .clone();
        let lease = IssueControlLeaseRequest {
            tenant: self.tenant.clone(),
            lease_id: self.lease_id.clone(),
            kind: HOST_START_KIND.into(),
            grant,
            pending_call_registration: None,
            issued_at_ms: now_ms,
            expires_at_ms: expires,
            hard_expires_at_ms: expires,
            idempotency_key: self.idempotency_key.clone(),
        };
        lease.validate_body()?;
        Ok(lease)
    }
}

#[cfg(test)]
mod tests;
