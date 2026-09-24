//! The backend's side of the bounded background node-payload scrub (EH-384,
//! CONCEPT:EG-KG.storage.node-payload-scrub).
//!
//! One step runs one bounded pass per shard: the pass reads its cursor and
//! walks node payloads off the writer thread (an MVCC read, no write lock),
//! then the writer thread durably records the advanced cursor in a
//! control-only maintenance write. Findings are logged by name and counted by
//! cause on the storage-scrub metrics.

use super::*;
use crate::redb_store::scrub::{load_scrub_cursor, scrub_pass, ScrubBudget, ScrubPass};
use tokio::sync::oneshot;

impl RedbBackend {
    /// One bounded scrub pass on every shard, in shard order.
    pub(crate) async fn scrub_step(&self, budget: ScrubBudget) -> Result<Vec<ScrubPass>, String> {
        let mut passes = Vec::with_capacity(self.shards.len());
        for writer in &self.shards {
            passes.push(scrub_shard(writer, budget).await?);
        }
        Ok(passes)
    }
}

async fn scrub_shard(writer: &ShardWriter, budget: ScrubBudget) -> Result<ScrubPass, String> {
    let pass = writer
        .read_off_writer(None, move |shard, crypto| {
            scrub_pass(shard, crypto, &load_scrub_cursor(shard)?, budget)
        })
        .await?;
    let (done, persisted) = oneshot::channel();
    let cursor = pass.next.clone();
    writer
        .send_off_reactor(None, Cmd::ScrubCursorPut { cursor, done }, "storage scrub")
        .await?;
    persisted
        .await
        .map_err(|_| "redb writer dropped the storage scrub cursor completion".to_string())??;
    report(writer, &pass);
    Ok(pass)
}

/// Name every finding in the log and count the pass on the scrub metrics.
fn report(writer: &ShardWriter, pass: &ScrubPass) {
    for finding in &pass.findings {
        tracing::error!(
            shard = %writer.db_path,
            graph = %finding.graph,
            node = %finding.key,
            cause = finding.cause.code(),
            "storage scrub: {finding}"
        );
    }
    let causes: Vec<&str> = pass.findings.iter().map(|f| f.cause.code()).collect();
    crate::metrics::storage_scrub_pass(pass.scanned, &causes, pass.next.graph.is_none());
}
