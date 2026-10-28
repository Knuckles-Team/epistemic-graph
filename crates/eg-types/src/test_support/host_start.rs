//! Valid host-start request shared by DTO, carrier and native authority tests.
use crate::host_start::{HostStartBinding, HostStartInterfaces, IssueHostStartGrantRequest};

pub fn host_start_request(tenant: &str) -> IssueHostStartGrantRequest {
    IssueHostStartGrantRequest {
        tenant: tenant.into(),
        lease_id: "host-grant".into(),
        binding: HostStartBinding {
            principal_ref: "subject".into(),
            session_ref: "session".into(),
            workspace_ref: "workspace".into(),
            configuration_digest: "a".repeat(64),
            interfaces: HostStartInterfaces::Both,
            web_logs: false,
            policy_revision: "policy-1".into(),
            session_expires_at_ms: 5_000,
        },
        expires_at_ms: 10_000,
        idempotency_key: "issue-host".into(),
    }
}
