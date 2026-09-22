//! EH-219 typed WorkItem reads: `GetWorkItem` and `ListWorkItems`.
//!
//! Dispatch has already resolved the graph, its ACL and placement, and (under
//! raft) made the read linearizable on the placement leader. What this module
//! adds is the tenant rule and the native store call:
//!
//! * the request's `tenant` must equal the VERIFIED carrier tenant. A mismatch
//!   is `ACCESS_DENIED`, never a cross-tenant read -- and unlike the resource
//!   and capacity reads there is no aggregate-reader or `kg:admin` exception:
//!   a WorkItem's caller view belongs to its tenant alone.
//! * rows are read off the redb authority's MVCC snapshot; any other backend
//!   fails closed, because only the redb authority holds native WorkItem rows.
//!   (A build without `redb` does not compile this module at all: the methods
//!   then fall to the "not available in this server build" arm, and `Health`
//!   never advertises them.)

use std::sync::Arc;

use eg_types::work_item_read::WorkItemListRequest;

use crate::protocol::{Response, ResultPayload};
use crate::server::persistence::PersistenceBackend;

/// One decoded WorkItem read.
pub(crate) enum WorkItemRead {
    Get {
        tenant: String,
        work_item_id: String,
    },
    List(WorkItemListRequest),
}

impl WorkItemRead {
    fn tenant(&self) -> &str {
        match self {
            Self::Get { tenant, .. } => tenant,
            Self::List(request) => &request.tenant,
        }
    }
}

/// Answer one WorkItem read for the verified carrier tenant.
pub(crate) async fn answer(
    req_id: u64,
    graph_name: &str,
    verified_tenant: &str,
    persistence: &Option<Arc<dyn PersistenceBackend>>,
    read: WorkItemRead,
) -> Response {
    if let Err(denied) = require_carrier_tenant(read.tenant(), verified_tenant) {
        crate::metrics::access_denied();
        return Response::err(req_id, denied);
    }
    let graph = crate::persist::sanitize(graph_name);
    match serve_native(&graph, persistence, read).await {
        Ok(payload) => Response::ok(req_id, payload),
        Err(error) => Response::err(req_id, format!("WorkItem read failed: {error}")),
    }
}

/// The request's tenant is a correlation, not an authority claim: it must be
/// the tenant the verified carrier names.
fn require_carrier_tenant(requested: &str, verified: &str) -> Result<(), String> {
    if verified.is_empty() || requested != verified {
        return Err(
            "ACCESS_DENIED: WorkItem read tenant must match verified request tenant".into(),
        );
    }
    Ok(())
}

async fn serve_native(
    graph: &str,
    persistence: &Option<Arc<dyn PersistenceBackend>>,
    read: WorkItemRead,
) -> Result<ResultPayload, String> {
    use eg_types::result_contract::coordination::{GetWorkItem, ListWorkItems};
    let backend = persistence
        .as_ref()
        .and_then(|backend| backend.as_redb())
        .ok_or_else(|| NATIVE_UNAVAILABLE.to_string())?;
    match read {
        WorkItemRead::Get {
            tenant,
            work_item_id,
        } => ResultPayload::of::<GetWorkItem>(
            backend
                .read_work_item(graph, &tenant, &work_item_id)
                .await?,
        ),
        WorkItemRead::List(request) => {
            ResultPayload::of::<ListWorkItems>(backend.list_work_items(graph, request).await?)
        }
    }
}

const NATIVE_UNAVAILABLE: &str = "native WorkItem reads require the redb persistence backend";

#[cfg(test)]
mod tests {
    use super::require_carrier_tenant;

    #[test]
    fn only_the_verified_tenant_may_read_its_work_items() {
        require_carrier_tenant("tenant-a", "tenant-a").unwrap();
        let denied = require_carrier_tenant("tenant-b", "tenant-a").unwrap_err();
        assert!(denied.starts_with("ACCESS_DENIED:"), "{denied}");
        assert!(
            require_carrier_tenant("", "").is_err(),
            "an unverified tenant reads nothing"
        );
    }
}
