//! EH-219 typed WorkItem reads, served off the writer shard's MVCC snapshot
//! through [`RedbBackend::read_snapshot`] -- never the writer channel.
//!
//! Inherent on the concrete backend rather than on `PersistenceBackend`: only
//! the redb authority holds native WorkItem rows, and the handler reaches it
//! through `PersistenceBackend::as_redb`, failing closed on any other backend.

use eg_types::control_lease::ControlLeaseView;
use eg_types::work_item_read::{WorkItemListRequest, WorkItemPage, WorkItemView};

use super::RedbBackend;

impl RedbBackend {
    /// `GetWorkItem`: the caller's view of one row, `None` when no WorkItem
    /// with this id belongs to `tenant`.
    pub(crate) async fn read_work_item(
        &self,
        graph_fname: &str,
        tenant: &str,
        work_item_id: &str,
    ) -> Result<Option<WorkItemView>, String> {
        let graph = graph_fname.to_owned();
        let tenant = tenant.to_owned();
        let work_item_id = work_item_id.to_owned();
        self.read_snapshot(graph_fname, move |shard, crypto| {
            crate::redb_store::work_item::read_work_item(
                shard,
                &graph,
                &tenant,
                &work_item_id,
                crypto,
            )
        })
        .await
    }

    /// `GetControlLease`: one native control lease of `tenant`.
    pub(crate) async fn read_control_lease(
        &self,
        graph_fname: &str,
        tenant: &str,
        lease_id: &str,
    ) -> Result<Option<ControlLeaseView>, String> {
        let graph = graph_fname.to_owned();
        let tenant = tenant.to_owned();
        let lease_id = lease_id.to_owned();
        self.read_snapshot(graph_fname, move |shard, crypto| {
            crate::redb_store::work_item::read_control_lease(
                shard, &graph, &tenant, &lease_id, crypto,
            )
        })
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
