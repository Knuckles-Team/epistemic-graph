//! Privacy-safe deterministic identities for mutation batches and coordinators.

use crate::protocol::Method;

/// Durable pseudonym used by every native coordinator retry check.
pub(crate) fn principal_fingerprint(principal: &str) -> Result<String, String> {
    use sha2::{Digest, Sha256};
    let principal = principal.trim();
    if principal.is_empty() {
        return Err("durable mutation authority requires a verified principal".to_string());
    }
    if principal
        .strip_prefix("principal:sha256:")
        .is_some_and(|digest| {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        })
    {
        return Ok(principal.to_string());
    }
    Ok(format!(
        "principal:sha256:{}",
        hex::encode(Sha256::digest(principal.as_bytes()))
    ))
}

/// Stable lifecycle batch id used both before the first commit and by a retry
/// reconciling a crash after durability but before registry publication.
pub(crate) fn lifecycle_batch_id(action: &str, graph: &str, request_id: u64) -> String {
    use sha2::{Digest, Sha256};
    let material = format!("{action}\0{graph}\0{request_id}");
    format!(
        "lifecycle:{}",
        hex::encode(Sha256::digest(material.as_bytes()))
    )
}

pub(crate) fn opaque_request_key(
    namespace: &str,
    graph: &str,
    request_id: u64,
    method: &Method,
) -> String {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(namespace.as_bytes());
    digest.update([0]);
    digest.update(graph.as_bytes());
    digest.update([0]);
    digest.update(request_id.to_be_bytes());
    digest.update(rmp_serde::to_vec_named(method).unwrap_or_default());
    format!("{namespace}:{}", hex::encode(digest.finalize()))
}

/// Durable identity for one WorkItem transition.
///
/// Terminal transitions carry an explicit caller-stable idempotency key. Their
/// durable identity must therefore be independent of the transport request id:
/// a retry after an uncertain acknowledgement arrives under a fresh request id,
/// but must reconstruct the exact same batch/context/outbox identity. The digest
/// is scope-bound so equal caller keys in different tenants or graphs cannot
/// collide in the shard-global batch table. Raw scope/key material is never
/// copied into the derived identifiers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkItemBatchIdentity {
    pub(crate) batch_id: String,
    pub(crate) idempotency_key: String,
    pub(crate) durable_request_id: u64,
    pub(crate) uses_native_row_cas: bool,
}

pub(crate) fn work_item_batch_identity(
    graph: &str,
    tenant: &str,
    transport_request_id: u64,
    method: &Method,
) -> Result<WorkItemBatchIdentity, String> {
    let terminal_key = match method {
        Method::SubmitWorkItem { request } => Some(request.idempotency_key.clone()),
        Method::SubmitWorkItems { request } => Some(request.idempotency_key.clone()),
        Method::CommitWorkItemResult {
            idempotency_key, ..
        }
        | Method::CancelWorkItem {
            idempotency_key, ..
        }
        | Method::DeferWorkItem {
            idempotency_key, ..
        } => Some(idempotency_key.clone()),
        Method::ClaimWorkItem { .. }
        | Method::RenewWorkItemLease { .. }
        | Method::CasWorkItemMetadata { .. } => None,
        Method::ReserveWorkItemResources { request }
        | Method::ReleaseWorkItemResources { request }
        | Method::ReclaimWorkItemResources { request } => Some(request.idempotency_key.clone()),
        Method::UpdateResourceHost { request } => Some(format!(
            "resource-host:{}:{}",
            request.host_ref, request.revision
        )),
        _ => {
            return Err("WorkItem identity requires a WorkItem operation".to_string());
        }
    };

    if let Some(terminal_key) = terminal_key {
        if terminal_key.trim().is_empty() {
            return Err("terminal WorkItem mutation requires idempotency_key".to_string());
        }
        use sha2::{Digest, Sha256};
        let mut digest = Sha256::new();
        digest.update(b"epistemic-graph.work-item-terminal.v1");
        for field in [graph.as_bytes(), tenant.as_bytes(), terminal_key.as_bytes()] {
            digest.update((field.len() as u64).to_be_bytes());
            digest.update(field);
        }
        let digest: [u8; 32] = digest.finalize().into();
        let mut request_bytes = [0u8; 8];
        request_bytes.copy_from_slice(&digest[..8]);
        let durable_request_id = u64::from_be_bytes(request_bytes).max(1);
        let digest = hex::encode(digest);
        return Ok(WorkItemBatchIdentity {
            batch_id: format!("work:{digest}"),
            idempotency_key: format!("work-idem:{digest}"),
            durable_request_id,
            // SubmitWorkItem is a native transaction: its command sequence,
            // dependency edges, and graph version advance are serialized by the
            // same redb writer as the row-local fenced transitions. Keeping the
            // stable command-key replay on the native-CAS path means a retry
            // does not manufacture a new graph-version expectation.
            uses_native_row_cas: true,
        });
    }

    let batch_id = opaque_request_key("work", graph, transport_request_id, method);
    use sha2::{Digest, Sha256};
    let idempotency_key = format!(
        "work-idem:{}",
        hex::encode(Sha256::digest(batch_id.as_bytes()))
    );
    Ok(WorkItemBatchIdentity {
        batch_id,
        idempotency_key,
        durable_request_id: transport_request_id,
        uses_native_row_cas: false,
    })
}

/// Stable privacy-safe identity for a multi-request/native coordinator. The raw
/// transaction/session id is never written to durable status or outbox rows.
pub(crate) fn opaque_coordinator_key(namespace: &str, graph: &str, coordinator_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(namespace.as_bytes());
    digest.update([0]);
    digest.update(graph.as_bytes());
    digest.update([0]);
    digest.update(coordinator_id.as_bytes());
    format!("{namespace}:{}", hex::encode(digest.finalize()))
}
