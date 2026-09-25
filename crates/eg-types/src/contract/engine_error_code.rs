//! Shared wire refusal codes. Domain-specific vocabularies remain beside their
//! result types and are included in the generated error catalog.

use super::closed_error_codes;

closed_error_codes! {
    /// Refusals emitted before a request reaches its domain handler.
    pub enum EngineErrorCode {
        InvalidArgument => "INVALID_ARGUMENT",
        AccessDenied => "ACCESS_DENIED",
        Conflict => "CONFLICT",
        IdempotencyConflict => "IDEMPOTENCY_CONFLICT",
        Redirected => "REDIRECTED",
        ReadOnly => "READ_ONLY",
        AuthTenantMismatch => "AUTH_TENANT_MISMATCH",
        AuthAudienceMismatch => "AUTH_AUDIENCE_MISMATCH",
        AuthPolicyVersionMismatch => "AUTH_POLICY_VERSION_MISMATCH",
        NodeMismatch => "NODE_MISMATCH",
        PolicyNativeAuthorityRequired => "POLICY_NATIVE_AUTHORITY_REQUIRED",
        CapacityDenied => "CAPACITY_DENIED",
        UqlBudgetExceeded => "UQL_BUDGET_EXCEEDED",
        UqlUnsupported => "UQL_UNSUPPORTED",
        UqlCredentialBearingSpec => "UQL_CREDENTIAL_BEARING_SPEC",
        EngineUnavailable => "ENGINE_UNAVAILABLE",
        EngineResourceExhausted => "ENGINE_RESOURCE_EXHAUSTED",
        EngineDeadlineExceeded => "ENGINE_DEADLINE_EXCEEDED",
    }
}
