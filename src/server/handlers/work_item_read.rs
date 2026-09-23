//! EH-219 typed WorkItem reads (`GetWorkItem`, `ListWorkItems`) and the
//! graph-os EG-2 control-lease read (`GetControlLease`).
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
    /// graph-os EG-3: a terminal WorkItem and its committed provenance.
    Outcome {
        tenant: String,
        work_item_id: String,
    },
    /// graph-os EG-5: one page of control leases.
    ControlLeases(eg_types::control_lease::ListControlLeasesRequest),
    /// graph-os EG-2: one native control lease.
    ControlLease {
        tenant: String,
        lease_id: String,
    },
}

impl WorkItemRead {
    fn tenant(&self) -> &str {
        match self {
            Self::Get { tenant, .. } | Self::Outcome { tenant, .. } => tenant,
            Self::List(request) => &request.tenant,
            Self::ControlLeases(request) => &request.tenant,
            Self::ControlLease { tenant, .. } => tenant,
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
    if let Err(denied) =
        super::native_write_authority::require_carrier_tenant(read.tenant(), verified_tenant)
    {
        crate::metrics::access_denied();
        return Response::err(req_id, denied);
    }
    let graph = crate::persist::sanitize(graph_name);
    match serve_native(&graph, persistence, read).await {
        Ok(payload) => Response::ok(req_id, payload),
        Err(error) => Response::err(req_id, format!("WorkItem read failed: {error}")),
    }
}

async fn serve_native(
    graph: &str,
    persistence: &Option<Arc<dyn PersistenceBackend>>,
    read: WorkItemRead,
) -> Result<ResultPayload, String> {
    use eg_types::result_contract::coordination::{
        GetControlLease, GetWorkItem, GetWorkItemOutcome, ListControlLeases, ListWorkItems,
    };
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
        WorkItemRead::Outcome {
            tenant,
            work_item_id,
        } => ResultPayload::of::<GetWorkItemOutcome>(
            backend
                .read_work_item_outcome(graph, &tenant, &work_item_id)
                .await?,
        ),
        WorkItemRead::ControlLeases(request) => ResultPayload::of::<ListControlLeases>(
            backend.list_control_leases(graph, request).await?,
        ),
        WorkItemRead::ControlLease { tenant, lease_id } => ResultPayload::of::<GetControlLease>(
            backend
                .read_control_lease(graph, &tenant, &lease_id)
                .await?,
        ),
    }
}

const NATIVE_UNAVAILABLE: &str = "native WorkItem reads require the redb persistence backend";
