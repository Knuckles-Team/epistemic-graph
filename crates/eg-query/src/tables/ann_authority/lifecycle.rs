//! The SQL family of the user-managed index lifecycle (EH-352): typed create
//! and status over the maintained ANN authority. The typed drop is
//! [`TableStore::drop_ann_index`], which the store owns because it is fenced
//! in the same owner write that removes the index's generations.
//!
//! [`TableStore::managed_index_status`] is what the SQL status relation
//! (`information_schema.eg_index_status`) lists. A per-read projection store
//! holds none of the tenant's indexes, so the server ADOPTS into it the rows of
//! the tenant's indexes the reader may see ([`UserAnnAuthority::adopt_statuses`]);
//! the projection then lists exactly those.

use eg_core::index::ManagedIndexStatus;

use super::{read, write, AnnIndexStatus, UserAnnAuthority};
use crate::sql::AnnIndexPlan;
use crate::tables::TableStore;

impl UserAnnAuthority {
    /// Serve `statuses` — rows another authority vouched for, already filtered
    /// to what the reader may see — alongside this store's own indexes. Adopted
    /// rows accumulate: the tenant's table indexes and the request graph's edge
    /// indexes each adopt theirs.
    pub fn adopt_statuses(&self, statuses: Vec<ManagedIndexStatus>) {
        write(&self.adopted).extend(statuses);
    }
}

impl TableStore {
    /// Register `plan` — the typed create of the managed-index lifecycle — and
    /// return its status: `requested` until the maintenance worker builds it.
    pub fn create_ann_index(&self, plan: &AnnIndexPlan) -> Result<AnnIndexStatus, String> {
        self.put_ann_index(plan)?;
        let index = TableStore::ann_index_key(plan);
        self.ann_index_status()?
            .into_iter()
            .find(|status| status.index == index)
            .ok_or_else(|| format!("ANN index `{index}` is not registered after its creation"))
    }

    /// Every managed index this store serves, by name: its own maintained ANN
    /// indexes, then any adopted rows.
    pub fn managed_index_status(&self) -> Result<Vec<ManagedIndexStatus>, String> {
        let mut statuses: Vec<ManagedIndexStatus> = self
            .ann_index_status()?
            .iter()
            .map(AnnIndexStatus::managed)
            .collect();
        statuses.extend(read(&self.ann_authority().adopted).iter().cloned());
        statuses.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(statuses)
    }
}
