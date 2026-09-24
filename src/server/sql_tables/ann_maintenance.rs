//! The maintenance worker of the maintained user-table ANN authority (RF-019).
//!
//! Every SQL catalog this process has opened lives in this module's parent's one
//! registry, and each owns a `UserAnnAuthority`. This worker is the only
//! production caller of `TableStore::refresh_ann_generations`: it runs on the
//! blocking pool, off every request path, one catalog at a time, so generation
//! builds never run concurrently with each other and never on a query or a write.
//! A restart empties every authority; the first pass after a catalog is reopened
//! rebuilds its generations from the durable rows.

use eg_query::{AnnRefreshOutcome, AnnRefreshPolicy, TableStore};

/// One periodic pass: refresh every open catalog's ANN generations on the
/// blocking pool. Logs counts only — index names are tenant data.
pub async fn sweep_ann_generations() {
    match tokio::task::spawn_blocking(refresh_open_catalogs).await {
        Ok(tally) => tally.log(),
        Err(error) => tracing::warn!(%error, "SQL ANN maintenance pass did not complete"),
    }
}

/// What one pass did, summed over every open catalog.
#[derive(Default)]
struct Tally {
    activated: usize,
    failed: usize,
    catalogs_failed: usize,
}

impl Tally {
    fn add(&mut self, outcome: &AnnRefreshOutcome) {
        match outcome {
            AnnRefreshOutcome::Activated { .. } => self.activated += 1,
            AnnRefreshOutcome::Failed { .. } => self.failed += 1,
            AnnRefreshOutcome::Current { .. }
            | AnnRefreshOutcome::Deferred { .. }
            | AnnRefreshOutcome::InFlight { .. }
            | AnnRefreshOutcome::Superseded { .. } => {}
        }
    }

    fn log(&self) {
        if self.activated > 0 {
            tracing::info!(activated = self.activated, "SQL ANN generations activated");
        }
        if self.failed > 0 || self.catalogs_failed > 0 {
            tracing::warn!(
                failed_indexes = self.failed,
                failed_catalogs = self.catalogs_failed,
                "SQL ANN maintenance failures; the index status carries each reason"
            );
        }
    }
}

fn refresh_open_catalogs() -> Tally {
    let mut tally = Tally::default();
    for store in open_catalogs(&mut tally) {
        match store.refresh_ann_generations(AnnRefreshPolicy::Throttled) {
            Ok(outcomes) => outcomes.iter().for_each(|outcome| tally.add(outcome)),
            Err(_) => tally.catalogs_failed += 1,
        }
    }
    tally
}

/// A snapshot of the open catalogs, so no build runs under the registry lock.
fn open_catalogs(tally: &mut Tally) -> Vec<TableStore> {
    match super::registry().lock() {
        Ok(stores) => stores.values().cloned().collect(),
        Err(_) => {
            tally.catalogs_failed += 1;
            Vec::new()
        }
    }
}
