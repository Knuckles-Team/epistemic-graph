//! Public approval-key facts. The broker verifies Ed25519 possession before
//! registration; these records neither authenticate a caller nor grant authority.

use serde::{Deserialize, Serialize};

use super::{IdentityRefusal, Secret};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ApprovalKeyPurpose {
    SignedApproval,
}

/// A trusted broker's assertion, not a native signature-verification result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ApprovalKeyAttestation {
    BrokerVerifiedEd25519,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ApprovalKeyBinding {
    pub key_id: String,
    pub principal_id: String,
    pub tenant: String,
    pub purpose: ApprovalKeyPurpose,
    /// Exactly 32 public-key bytes encoded as 64 lowercase hex characters.
    pub public_key_hex: String,
    /// Zero creates a key; a positive revision replaces an existing live key.
    pub expected_revision: u64,
    pub expires_at_ms: u64,
}

impl ApprovalKeyBinding {
    /// Ed25519 signs these bytes directly, not a digest or a JSON rendering.
    /// After the NUL-terminated ASCII domain, each string has a u32 big-endian
    /// UTF-8 byte length; the two integers are u64 big-endian. No normalization.
    pub fn registration_message(&self) -> Result<Vec<u8>, IdentityRefusal> {
        self.validate()?;
        let mut bytes = b"eg/identity/approval-key-registration/v1\0".to_vec();
        for value in [
            self.principal_id.as_str(),
            self.tenant.as_str(),
            self.key_id.as_str(),
            "signed_approval",
            self.public_key_hex.as_str(),
        ] {
            let length = u32::try_from(value.len()).map_err(|_| IdentityRefusal::InvalidRequest)?;
            bytes.extend_from_slice(&length.to_be_bytes());
            bytes.extend_from_slice(value.as_bytes());
        }
        bytes.extend_from_slice(&self.expected_revision.to_be_bytes());
        bytes.extend_from_slice(&self.expires_at_ms.to_be_bytes());
        Ok(bytes)
    }

    pub(crate) fn validate(&self) -> Result<(), IdentityRefusal> {
        super::text::identifier(&self.key_id)?;
        super::validate_principal_id(&self.principal_id)?;
        super::text::bounded(&self.tenant, super::text::MAX_ID_BYTES)?;
        let valid_public_key = self.public_key_hex.len() == 64
            && self
                .public_key_hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
        valid_public_key
            .then_some(())
            .ok_or(IdentityRefusal::InvalidRequest)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ApprovalKeyRegistration {
    pub session_token: Secret,
    pub binding: ApprovalKeyBinding,
    pub attestation: ApprovalKeyAttestation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ApprovalKeyLookup {
    pub key_id: String,
    pub principal_id: String,
    pub purpose: ApprovalKeyPurpose,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ApprovalKeyRevocation {
    pub session_token: Secret,
    pub key: ApprovalKeyLookup,
    pub expected_revision: u64,
}

/// Only public verification facts. No session, credential hash or secret.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ApprovalKeyRecord {
    pub key_id: String,
    pub principal_id: String,
    pub tenant: String,
    pub purpose: ApprovalKeyPurpose,
    pub public_key_hex: String,
    pub revision: u64,
    pub expires_at_ms: u64,
    pub revoked_at_ms: Option<u64>,
}
