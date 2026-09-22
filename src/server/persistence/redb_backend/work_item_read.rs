//! EH-219 typed WorkItem reads, served off the writer shard's MVCC snapshot
//! through [`RedbBackend::read_snapshot`] -- never the writer channel.
//!
//! Inherent on the concrete backend rather than on `PersistenceBackend`: only
//! the redb authority holds native WorkItem rows, and the handler reaches it
//! through `PersistenceBackend::as_redb`, failing closed on any other backend.

use eg_types::control_lease::ControlLeaseView;
use eg_types::work_item_read::{
    WorkItemListRequest, WorkItemOutcomeView, WorkItemPage, WorkItemView,
};

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

    /// `ListWorkItems`: one bounded, tenant-bound page.
    pub(crate) async fn list_work_items(
        &self,
        graph_fname: &str,
        request: WorkItemListRequest,
    ) -> Result<WorkItemPage, String> {
        let graph = graph_fname.to_owned();
        self.read_snapshot(graph_fname, move |shard, crypto| {
            crate::redb_store::work_item::list_work_items(shard, &graph, &request, crypto)
        })
        .await
    }
}
