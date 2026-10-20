//! EH-219 typed WorkItem reads (`GetWorkItem`, `ListWorkItems`), the
//! graph-os EG-2 control-lease read (`GetControlLease`) and the EH-348 Gap
//! reads (`GapGet`, `GapList`).
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
    RunEvents {
        request: eg_types::run_stream::GetRunEventsRequest,
        actor: String,
    },
    Checkpoint {
        request: eg_types::run_checkpoint::GetRunCheckpointRequest,
        actor: String,
    },
    SessionUsage {
        tenant: String,
        context_id: String,
        actor: String,
    },
    CurrentPlan {
        tenant: String,
        run_id: String,
        actor: String,
    },
    Get {
        tenant: String,
        work_item_id: String,
    },
    List(WorkItemListRequest),
    /// graph-os EG-3: a terminal WorkItem and its committed provenance.
    Outcome {
        tenant: String,
        work_item_id: String,
        include_replay_cursor: bool,
        actor: String,
    },
    /// graph-os EG-5: one page of control leases.
    ControlLeases(eg_types::control_lease::ListControlLeasesRequest),
    /// graph-os EG-2: one native control lease.
    ControlLease {
        tenant: String,
        lease_id: String,
    },
    /// EH-348: one canonical Gap.
    Gap {
        tenant: String,
        gap_id: String,
    },
    /// EH-348: one page of Gaps.
    Gaps(eg_types::work_market::GapListRequest),
}

impl WorkItemRead {
    fn tenant(&self) -> &str {
        match self {
            Self::Get { tenant, .. } | Self::Outcome { tenant, .. } => tenant,
            Self::CurrentPlan { tenant, .. } | Self::SessionUsage { tenant, .. } => tenant,
            Self::Checkpoint { request, .. } => &request.tenant,
            Self::RunEvents { request, .. } => &request.tenant,
            Self::List(request) => &request.tenant,
            Self::ControlLeases(request) => &request.tenant,
            Self::ControlLease { tenant, .. } | Self::Gap { tenant, .. } => tenant,
            Self::Gaps(request) => &request.tenant,
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
        GapGet, GapList, GetControlLease, GetWorkItem, GetWorkItemOutcome, ListControlLeases,
        ListWorkItems,
    };
    let backend = persistence
        .as_ref()
        .and_then(|backend| backend.as_redb())
        .ok_or_else(|| NATIVE_UNAVAILABLE.to_string())?;
    match read {
        WorkItemRead::RunEvents { request, actor } => {
            let actor = crate::server::mutation_batch::principal_fingerprint(&actor)?;
            ResultPayload::of::<eg_types::result_contract::coordination::GetRunEvents>(
                backend.read_run_events(graph, request, &actor).await?,
            )
        }
        WorkItemRead::Checkpoint { request, actor } => {
            let actor = crate::server::mutation_batch::principal_fingerprint(&actor)?;
            ResultPayload::of::<eg_types::result_contract::coordination::GetRunCheckpoint>(
                backend.read_run_checkpoint(graph, request, &actor).await?,
            )
        }
        WorkItemRead::SessionUsage {
            tenant,
            context_id,
            actor,
        } => {
            let principal = crate::server::mutation_batch::principal_fingerprint(&actor)?;
            ResultPayload::of::<eg_types::result_contract::coordination::GetSessionUsage>(
                backend
                    .read_session_usage(graph, &tenant, &context_id, &principal)
                    .await?,
            )
        }
        WorkItemRead::CurrentPlan {
            tenant,
            run_id,
            actor,
        } => {
            let actor = crate::server::mutation_batch::principal_fingerprint(&actor)?;
            ResultPayload::of::<eg_types::result_contract::coordination::GetCurrentPlan>(
                backend
                    .read_current_plan(graph, &tenant, &run_id, &actor)
                    .await?,
            )
        }
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
            include_replay_cursor,
            actor,
        } => {
            let mut outcome = backend
                .read_work_item_outcome(graph, &tenant, &work_item_id)
                .await?;
            if let (false, Some(view)) = (include_replay_cursor, outcome.as_mut()) {
                view.terminal_event.without_replay_identity();
                view.committed_run_usage = None;
            }
            if let Some(view) = outcome.as_mut() {
                if let Some(usage) = &view.committed_run_usage {
                    let principal = crate::server::mutation_batch::principal_fingerprint(&actor)?;
                    if usage.tenant != tenant || usage.principal != principal {
                        return Err("ACCESS_DENIED: committed run usage owner mismatch".into());
                    }
                }
            }
            ResultPayload::of::<GetWorkItemOutcome>(outcome)
        }
        WorkItemRead::ControlLeases(request) => ResultPayload::of::<ListControlLeases>(
            backend.list_control_leases(graph, request).await?,
        ),
        WorkItemRead::ControlLease { tenant, lease_id } => ResultPayload::of::<GetControlLease>(
            backend
                .read_control_lease(graph, &tenant, &lease_id)
                .await?,
        ),
        WorkItemRead::Gap { tenant, gap_id } => {
            ResultPayload::of::<GapGet>(backend.read_gap(graph, &tenant, &gap_id).await?)
        }
        WorkItemRead::Gaps(request) => {
            ResultPayload::of::<GapList>(backend.list_gaps(graph, request).await?)
        }
    }
}

const NATIVE_UNAVAILABLE: &str = "native WorkItem reads require the redb persistence backend";
