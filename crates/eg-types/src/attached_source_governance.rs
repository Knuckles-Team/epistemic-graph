//! Typed model for governed write-back to attached sources (EG-UNIFIED-DATA-PLANE-R012).
//!
//! EG writes back to an attached source only through an approved, idempotent path
//! with an audit reservation, and prefers the application's own API over a direct
//! write when the application declares business logic of its own. This module
//! defines the typed request and its fail-closed validation; it does not perform
//! the write itself.

use serde::{Deserialize, Serialize};

use crate::contract::Digest256;

/// Which path a write-back takes to reach the attached source.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum AttachedSourceWritePath {
    /// Writes through the application's own API, which may enforce business logic.
    ApplicationApi,
    /// Writes directly to the attached source's storage, bypassing any app-side logic.
    DirectWrite,
}

/// A request to write back to an attached source, prior to validation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct AttachedSourceWriteRequest {
    pub source_instance_id: String,
    /// Reference to the approval authorizing this write, if any.
    pub approval_ref: Option<String>,
    /// Digest of the audit reservation recorded before the write is attempted, if any.
    pub audit_reservation_digest: Option<Digest256>,
    /// Caller-supplied idempotency key, required so a retried write has no further effect.
    pub idempotency_key: Option<String>,
    pub path: AttachedSourceWritePath,
    /// Whether the attached application declares business logic of its own
    /// (validation, derived fields, side effects) that a direct write would bypass.
    pub source_declares_business_logic: bool,
}

/// Reasons a write-back request is refused before it reaches the attached source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttachedSourceWriteRefusal {
    MissingApproval,
    MissingAuditReservation,
    MissingIdempotencyKey,
    DirectWriteBypassesBusinessLogic,
}

impl AttachedSourceWriteRequest {
    /// Validates the request against the R012 write-back policy: approval, an
    /// audit reservation and an idempotency key are each required, and a direct
    /// write is refused when the source declares business logic of its own.
    pub fn validate(&self) -> Result<(), AttachedSourceWriteRefusal> {
        if self.approval_ref.as_deref().unwrap_or("").is_empty() {
            return Err(AttachedSourceWriteRefusal::MissingApproval);
        }
        if self.audit_reservation_digest.is_none() {
            return Err(AttachedSourceWriteRefusal::MissingAuditReservation);
        }
        if self.idempotency_key.as_deref().unwrap_or("").is_empty() {
            return Err(AttachedSourceWriteRefusal::MissingIdempotencyKey);
        }
        let is_direct_write = self.path == AttachedSourceWritePath::DirectWrite;
        if self.source_declares_business_logic && is_direct_write {
            return Err(AttachedSourceWriteRefusal::DirectWriteBypassesBusinessLogic);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(byte: u8) -> Digest256 {
        Digest256::from_bytes([byte; 32])
    }

    fn approved_request() -> AttachedSourceWriteRequest {
        AttachedSourceWriteRequest {
            source_instance_id: "source-1".to_string(),
            approval_ref: Some("approval:42".to_string()),
            audit_reservation_digest: Some(digest(1)),
            idempotency_key: Some("idempotency-1".to_string()),
            path: AttachedSourceWritePath::ApplicationApi,
            source_declares_business_logic: true,
        }
    }

    // spec: EG-UNIFIED-DATA-PLANE-R012.1
    #[test]
    fn fully_approved_idempotent_app_api_write_is_accepted() {
        assert!(approved_request().validate().is_ok());
    }

    // spec: EG-UNIFIED-DATA-PLANE-R012.1
    #[test]
    fn write_without_approval_is_refused() {
        let mut request = approved_request();
        request.approval_ref = None;
        assert_eq!(
            request.validate(),
            Err(AttachedSourceWriteRefusal::MissingApproval)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R012.1
    #[test]
    fn write_without_audit_reservation_is_refused() {
        let mut request = approved_request();
        request.audit_reservation_digest = None;
        assert_eq!(
            request.validate(),
            Err(AttachedSourceWriteRefusal::MissingAuditReservation)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R012.1
    #[test]
    fn write_without_idempotency_key_is_refused() {
        let mut request = approved_request();
        request.idempotency_key = None;
        assert_eq!(
            request.validate(),
            Err(AttachedSourceWriteRefusal::MissingIdempotencyKey)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R012.1
    #[test]
    fn direct_write_to_a_source_with_its_own_business_logic_is_refused() {
        let mut request = approved_request();
        request.path = AttachedSourceWritePath::DirectWrite;
        assert_eq!(
            request.validate(),
            Err(AttachedSourceWriteRefusal::DirectWriteBypassesBusinessLogic)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R012.1
    #[test]
    fn direct_write_to_a_source_without_business_logic_is_accepted() {
        let mut request = approved_request();
        request.path = AttachedSourceWritePath::DirectWrite;
        request.source_declares_business_logic = false;
        assert!(request.validate().is_ok());
    }
}
