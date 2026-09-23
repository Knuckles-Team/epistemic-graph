//! Fleet catalog writes: one record revision in the writing tenant's own scope
//! of the Agent Library owner (`persistence::fleet_records`).
//!
//! Tenant and writer come only from the verified request context. The store
//! decides the compare-and-set inside its admitted write, so a writer that
//! lost a race is refused (`FLEET_REVISION_CONFLICT`) rather than silently
//! overwriting, and a byte-identical repeat answers `replayed`.

use eg_types::fleet_catalog::{
    FleetDiscoveryRecordRequest, FleetOverrideClearRequest, FleetOverrideSetRequest,
    FleetWriteDisposition, FleetWriteReceipt,
};
use eg_types::result_contract::cluster::{
    FleetCatalogClearOverride, FleetCatalogRecordDiscovery, FleetCatalogSetOverride,
};
use eg_types::result_contract::MethodResult;
use serde::Serialize;

use super::records::{
    content_digest, discovery_record_id, encode_body, override_record_id, DiscoveryBody,
    OverrideBody,
};
use super::*;
use crate::server::persistence::agent_library::AgentLibraryStore;
use crate::server::persistence::fleet_records::{
    FleetRecordFamily, FleetRecordOutcome, FleetRecordWrite,
};

/// Bind one typed body to its row: tenant, writer and time from `verified`.
fn record_write<B: Serialize>(
    verified: &VerifiedRequestContext,
    family: FleetRecordFamily,
    record_id: String,
    body: &B,
    expected_revision: Option<u64>,
) -> Result<FleetRecordWrite, String> {
    let tenant_id = verified.tenant().to_string();
    Ok(FleetRecordWrite {
        content_digest: content_digest(&tenant_id, body)?,
        body: encode_body(body)?,
        tenant_id,
        family,
        record_id,
        writer: verified.principal_persistence_id(),
        written_at_ms: authoritative_now_ms(),
        expected_revision,
    })
}

fn receipt(write: &FleetRecordWrite, outcome: FleetRecordOutcome) -> FleetWriteReceipt {
    let (revision, disposition) = match outcome {
        FleetRecordOutcome::Written { revision } => (revision, FleetWriteDisposition::Written),
        FleetRecordOutcome::Replayed { revision } => (revision, FleetWriteDisposition::Replayed),
    };
    FleetWriteReceipt {
        record_id: write.record_id.clone(),
        revision,
        disposition,
        observed_at_ms: write.written_at_ms,
    }
}

fn discovery_write(
    verified: &VerifiedRequestContext,
    request: FleetDiscoveryRecordRequest,
) -> Result<FleetRecordWrite, String> {
    let record_id = discovery_record_id(&request.server_name, &request.scope);
    let expected_revision = request.expected_revision;
    let body = DiscoveryBody {
        server_name: request.server_name,
        scope: request.scope,
        connector: request.connector,
        outcome: request.outcome,
        counts: request.counts,
        observer: verified.principal_persistence_id(),
    };
    record_write(
        verified,
        FleetRecordFamily::Discovery,
        record_id,
        &body,
        expected_revision,
    )
}

fn override_write(
    verified: &VerifiedRequestContext,
    body: OverrideBody,
    expected_revision: Option<u64>,
) -> Result<FleetRecordWrite, String> {
    let record_id = override_record_id(body.field, &body.component_id);
    record_write(
        verified,
        FleetRecordFamily::Override,
        record_id,
        &body,
        expected_revision,
    )
}

/// Commit the write and answer it as `M`'s declared result.
fn commit<M: MethodResult<Body = FleetWriteReceipt>>(
    store: &AgentLibraryStore,
    req_id: u64,
    write: Result<FleetRecordWrite, String>,
) -> Response {
    let answered = write.and_then(|write| {
        store
            .put_fleet_record(&write)
            .map(|outcome| receipt(&write, outcome))
    });
    respond::<M>(req_id, answered)
}

pub(super) fn record_discovery(
    store: &AgentLibraryStore,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: FleetDiscoveryRecordRequest,
) -> Response {
    commit::<FleetCatalogRecordDiscovery>(store, req_id, discovery_write(verified, request))
}

pub(super) fn set_override(
    store: &AgentLibraryStore,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: FleetOverrideSetRequest,
) -> Response {
    let body = OverrideBody {
        field: request.value.field(),
        component_id: request.component_id,
        value: Some(request.value),
        set_by: verified.principal_persistence_id(),
    };
    let write = override_write(verified, body, request.expected_revision);
    commit::<FleetCatalogSetOverride>(store, req_id, write)
}

pub(super) fn clear_override(
    store: &AgentLibraryStore,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: FleetOverrideClearRequest,
) -> Response {
    let body = OverrideBody {
        field: request.field,
        component_id: request.component_id,
        value: None,
        set_by: verified.principal_persistence_id(),
    };
    let write = override_write(verified, body, request.expected_revision);
    commit::<FleetCatalogClearOverride>(store, req_id, write)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_receipt_reports_the_store_outcome_and_the_logical_record_id() {
        let write = FleetRecordWrite {
            tenant_id: "tenant-a".to_string(),
            family: FleetRecordFamily::Discovery,
            record_id: "srvobs:github:tenant_local".to_string(),
            content_digest: "d".to_string(),
            body: Vec::new(),
            writer: "principal:sha256:ab".to_string(),
            written_at_ms: 42,
            expected_revision: None,
        };
        let written = receipt(&write, FleetRecordOutcome::Written { revision: 3 });
        assert_eq!(written.record_id, "srvobs:github:tenant_local");
        assert_eq!(
            (
                written.revision,
                written.disposition,
                written.observed_at_ms
            ),
            (3, FleetWriteDisposition::Written, 42)
        );
        assert_eq!(
            receipt(&write, FleetRecordOutcome::Replayed { revision: 3 }).disposition,
            FleetWriteDisposition::Replayed
        );
    }
}
