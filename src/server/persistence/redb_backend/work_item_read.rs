//! EH-219 typed WorkItem reads, served off the writer shard's MVCC snapshot
//! through [`RedbBackend::read_snapshot`] -- never the writer channel.
//!
//! Inherent on the concrete backend rather than on `PersistenceBackend`: only
//! the redb authority holds native WorkItem rows, and the handler reaches it
//! through `PersistenceBackend::as_redb`, failing closed on any other backend.

use eg_types::control_lease::{ControlLeasePage, ControlLeaseView, ListControlLeasesRequest};
use eg_types::work_item_read::{
    WorkItemListRequest, WorkItemOutcomeView, WorkItemPage, WorkItemView,
};
use eg_types::work_market::{GapListRequest, GapPage, GapView};

use super::RedbBackend;

/// A snapshot read keyed by `(graph, tenant, id)` -- the shape of every typed
/// tenant-bound point read below.
type TenantPointRead<T> = for<'a> fn(
    &'a crate::redb_store::Shard,
    &str,
    &str,
    &str,
    crate::redb_store::DurableCrypto<'a>,
) -> Result<T, String>;

/// A snapshot listing keyed by `(graph, request)`.
type TenantListRead<Q, T> = for<'a> fn(
    &'a crate::redb_store::Shard,
    &str,
    &Q,
    crate::redb_store::DurableCrypto<'a>,
) -> Result<T, String>;

impl RedbBackend {
    async fn tenant_point_read<T: Send + 'static>(
        &self,
        graph_fname: &str,
        tenant: &str,
        id: &str,
        read: TenantPointRead<T>,
    ) -> Result<T, String> {
        let graph = graph_fname.to_owned();
        let tenant = tenant.to_owned();
        let id = id.to_owned();
        self.read_snapshot(graph_fname, move |shard, crypto| {
            read(shard, &graph, &tenant, &id, crypto)
        })
        .await
    }

    /// `GetWorkItem`: the caller's view of one row, `None` when no WorkItem
    /// with this id belongs to `tenant`.
    pub(crate) async fn read_work_item(
        &self,
        graph_fname: &str,
        tenant: &str,
        work_item_id: &str,
    ) -> Result<Option<WorkItemView>, String> {
        let read: TenantPointRead<_> = crate::redb_store::work_item::read_work_item;
        self.tenant_point_read(graph_fname, tenant, work_item_id, read)
            .await
    }

    /// `GetWorkItemOutcome`: a WorkItem of `tenant` and its committed provenance.
    pub(crate) async fn read_work_item_outcome(
        &self,
        graph_fname: &str,
        tenant: &str,
        work_item_id: &str,
    ) -> Result<Option<WorkItemOutcomeView>, String> {
        let read: TenantPointRead<_> = crate::redb_store::work_item::read_work_item_outcome;
        self.tenant_point_read(graph_fname, tenant, work_item_id, read)
            .await
    }

    /// `GetControlLease`: one native control lease of `tenant`.
    pub(crate) async fn read_control_lease(
        &self,
        graph_fname: &str,
        tenant: &str,
        lease_id: &str,
    ) -> Result<Option<ControlLeaseView>, String> {
        let read: TenantPointRead<_> = crate::redb_store::work_item::read_control_lease;
        self.tenant_point_read(graph_fname, tenant, lease_id, read)
            .await
    }

    /// `ListControlLeases`: one bounded, tenant-bound page.
    pub(crate) async fn list_control_leases(
        &self,
        graph_fname: &str,
        request: ListControlLeasesRequest,
    ) -> Result<ControlLeasePage, String> {
        let read: TenantListRead<_, _> = crate::redb_store::work_item::list_control_leases;
        self.tenant_list_read(graph_fname, request, read).await
    }

    /// `GapGet`: one canonical Gap of `tenant`.
    pub(crate) async fn read_gap(
        &self,
        graph_fname: &str,
        tenant: &str,
        gap_id: &str,
    ) -> Result<Option<GapView>, String> {
        let read: TenantPointRead<_> = crate::redb_store::work_item::read_gap;
        self.tenant_point_read(graph_fname, tenant, gap_id, read)
            .await
    }

    /// `GapList`: one bounded, tenant-bound page of Gaps.
    pub(crate) async fn list_gaps(
        &self,
        graph_fname: &str,
        request: GapListRequest,
    ) -> Result<GapPage, String> {
        let read: TenantListRead<_, _> = crate::redb_store::work_item::list_gaps;
        self.tenant_list_read(graph_fname, request, read).await
    }

    /// `ListWorkItems`: one bounded, tenant-bound page.
    pub(crate) async fn list_work_items(
        &self,
        graph_fname: &str,
        request: WorkItemListRequest,
    ) -> Result<WorkItemPage, String> {
        let read: TenantListRead<_, _> = crate::redb_store::work_item::list_work_items;
        self.tenant_list_read(graph_fname, request, read).await
    }

    async fn tenant_list_read<Q: Send + 'static, T: Send + 'static>(
        &self,
        graph_fname: &str,
        request: Q,
        read: TenantListRead<Q, T>,
    ) -> Result<T, String> {
        let graph = graph_fname.to_owned();
        self.read_snapshot(graph_fname, move |shard, crypto| {
            read(shard, &graph, &request, crypto)
        })
        .await
    }
}

/// An owner-bound point read over one authoritative graph snapshot.
type OwnerPointRead<T> = for<'a> fn(
    &'a crate::redb_store::Shard,
    &str,
    &str,
    &str,
    &str,
    crate::redb_store::DurableCrypto<'a>,
) -> Result<T, String>;

impl RedbBackend {
    async fn owner_point_read<T: Send + 'static>(
        &self,
        graph_fname: &str,
        tenant: &str,
        run_id: &str,
        actor: &str,
        read: OwnerPointRead<T>,
    ) -> Result<T, String> {
        let (graph, tenant, run_id, actor) = (
            graph_fname.to_owned(),
            tenant.to_owned(),
            run_id.to_owned(),
            actor.to_owned(),
        );
        self.read_snapshot(graph_fname, move |shard, crypto| {
            read(shard, &graph, &tenant, &run_id, &actor, crypto)
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn read_run_work_item(
        &self,
        graph_fname: &str,
        tenant: &str,
        run_id: &str,
        actor: &str,
        selection: Option<Box<eg_types::inline_history::RunSelectionRequest>>,
        completed_text_history: bool,
    ) -> Result<Option<eg_types::work_item_read::RunWorkItemView>, String> {
        if selection.is_some() && !completed_text_history {
            return Err("RUN_ADMISSION_UNAVAILABLE: completed-text history opt-in required".into());
        }
        if let Some(selection) = selection {
            let (graph, tenant, run_id, actor) = (
                graph_fname.to_owned(),
                tenant.to_owned(),
                run_id.to_owned(),
                actor.to_owned(),
            );
            self.read_snapshot(graph_fname, move |shard, crypto| {
                crate::redb_store::work_item::history_selection::read_selection(
                    shard,
                    &graph,
                    &tenant,
                    &run_id,
                    &actor,
                    &selection,
                    crypto,
                    crate::server::dispatch::authoritative_now_ms(),
                )
            })
            .await
        } else {
            let read: OwnerPointRead<Option<eg_types::work_item_read::RunWorkItemView>> =
                if completed_text_history {
                    crate::redb_store::work_item::read_run_admission
                } else {
                    crate::redb_store::work_item::read_run_admission_legacy
                };
            self.owner_point_read(graph_fname, tenant, run_id, actor, read)
                .await
        }
    }

    pub(crate) async fn read_conversation_head(
        &self,
        graph_fname: &str,
        tenant: &str,
        context_id: &str,
        actor: &str,
    ) -> Result<Option<eg_types::inline_history::ConversationHeadView>, String> {
        self.owner_point_read(
            graph_fname,
            tenant,
            context_id,
            actor,
            crate::redb_store::work_item::history_selection::read_head,
        )
        .await
    }

    pub(crate) async fn read_session_usage(
        &self,
        graph_fname: &str,
        tenant: &str,
        context_id: &str,
        actor: &str,
    ) -> Result<eg_types::run_usage::SessionUsage, String> {
        self.owner_point_read(
            graph_fname,
            tenant,
            context_id,
            actor,
            crate::redb_store::work_item::read_session_usage,
        )
        .await
    }

    pub(crate) async fn current_plan_owner(
        &self,
        graph_fname: &str,
        tenant: &str,
        run_id: &str,
        actor: &str,
    ) -> Result<bool, String> {
        self.owner_point_read(
            graph_fname,
            tenant,
            run_id,
            actor,
            crate::redb_store::work_item::current_plan::is_owner,
        )
        .await
    }

    pub(crate) async fn read_current_plan(
        &self,
        graph_fname: &str,
        tenant: &str,
        run_id: &str,
        actor: &str,
    ) -> Result<Option<eg_types::current_plan::CurrentPlanSnapshot>, String> {
        self.owner_point_read(
            graph_fname,
            tenant,
            run_id,
            actor,
            |shard, graph, tenant, run_id, actor, crypto| {
                crate::redb_store::work_item::current_plan::read_current(
                    shard,
                    graph,
                    tenant,
                    run_id,
                    actor,
                    crypto,
                    crate::server::dispatch::authoritative_now_ms(),
                )
            },
        )
        .await
    }
}

impl RedbBackend {
    pub(crate) async fn checkpoint_owner(
        &self,
        graph_fname: &str,
        request: eg_types::run_checkpoint::GetRunCheckpointRequest,
        actor: &str,
    ) -> Result<bool, String> {
        let (graph, actor) = (graph_fname.to_owned(), actor.to_owned());
        self.read_snapshot(graph_fname, move |shard, crypto| {
            crate::redb_store::work_item::checkpoint::is_owner(
                shard, &graph, &request, &actor, crypto,
            )
        })
        .await
    }
    pub(crate) async fn read_run_checkpoint(
        &self,
        graph_fname: &str,
        request: eg_types::run_checkpoint::GetRunCheckpointRequest,
        actor: &str,
    ) -> Result<Option<eg_types::run_checkpoint::RunCheckpointRead>, String> {
        let (graph, actor) = (graph_fname.to_owned(), actor.to_owned());
        self.read_snapshot(graph_fname, move |shard, crypto| {
            crate::redb_store::work_item::checkpoint::read_current(
                shard,
                &graph,
                &request,
                &actor,
                crypto,
                crate::server::dispatch::authoritative_now_ms(),
            )
        })
        .await
    }
}
