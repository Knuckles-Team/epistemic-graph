use super::*;
#[cfg(any(feature = "compute-dist", feature = "matview"))]
use crate::redb_store::MatViewScanResult;
use crate::server::persistence::writer_reply::await_writer_reply;
use tokio::sync::oneshot;

// ── Durable materialized-view records ─────────────────────────────────────
#[cfg(any(feature = "raft", feature = "matview"))]
impl RedbBackend {
    /// Durably upsert a named materialized view's serialized blob (CONCEPT:EG-KG.storage.feature).
    /// Awaits the fsync so a `CreateMatView`/`RefreshMatView` ack means it is on disk.
    #[cfg(feature = "compute-dist")]
    pub async fn matview_put(&self, name: &str, blob: Vec<u8>) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let name = name.to_string();
        let tx = self.shard0().tx.clone();
        tokio::task::spawn_blocking(move || {
            tx.send(Cmd::MatViewPut { name, blob, done })
                .map_err(|_| ())
        })
        .await
        .map_err(|e| format!("matview_put join error: {e}"))?
        .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped matview_put completion".to_string())?
    }

    /// Scan every persisted materialized view `(name, blob)` (reload on boot).
    #[cfg(feature = "compute-dist")]
    pub fn matview_scan(&self) -> MatViewScanResult {
        let (reply, rx) = std::sync::mpsc::sync_channel(1);
        self.shard0()
            .tx
            .send(Cmd::MatViewScan { reply })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        await_writer_reply(&rx, "matview_scan")?
    }

    /// Durably upsert a PLAN-BACKED matview definition (CONCEPT:EG-KG.storage.plan-backed-matview).
    /// Awaits the fsync so a `PlanMatViewDefine` ack means the definition is on disk.
    #[cfg(feature = "matview")]
    pub async fn plan_matview_put(&self, name: &str, blob: Vec<u8>) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let name = name.to_string();
        let tx = self.shard0().tx.clone();
        tokio::task::spawn_blocking(move || {
            tx.send(Cmd::PlanMatViewPut { name, blob, done })
                .map_err(|_| ())
        })
        .await
        .map_err(|e| format!("plan_matview_put join error: {e}"))?
        .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped plan_matview_put completion".to_string())?
    }

    /// Durably delete a plan-backed matview definition (awaits the fsync).
    #[cfg(feature = "matview")]
    pub async fn plan_matview_delete(&self, name: &str) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let name = name.to_string();
        let tx = self.shard0().tx.clone();
        tokio::task::spawn_blocking(move || {
            tx.send(Cmd::PlanMatViewDelete { name, done })
                .map_err(|_| ())
        })
        .await
        .map_err(|e| format!("plan_matview_delete join error: {e}"))?
        .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped plan_matview_delete completion".to_string())?
    }

    /// Scan every persisted plan-backed matview `(name, blob)` (reload on boot).
    #[cfg(feature = "matview")]
    pub fn plan_matview_scan(&self) -> MatViewScanResult {
        let (reply, rx) = std::sync::mpsc::sync_channel(1);
        self.shard0()
            .tx
            .send(Cmd::PlanMatViewScan { reply })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        await_writer_reply(&rx, "plan_matview_scan")?
    }

    /// Durably upsert an incremental matview's operator-state snapshot
    /// (CONCEPT:EG-KG.storage.incremental-matview). Awaits the fsync.
    #[cfg(feature = "matview")]
    pub async fn matview_operator_state_put(
        &self,
        name: &str,
        blob: Vec<u8>,
    ) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let name = name.to_string();
        let tx = self.shard0().tx.clone();
        tokio::task::spawn_blocking(move || {
            tx.send(Cmd::MatViewOperatorStatePut { name, blob, done })
                .map_err(|_| ())
        })
        .await
        .map_err(|e| format!("matview_operator_state_put join error: {e}"))?
        .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped matview_operator_state_put completion".to_string())?
    }

    /// Durably delete an incremental matview's operator-state snapshot (awaits the fsync).
    #[cfg(feature = "matview")]
    pub async fn matview_operator_state_delete(&self, name: &str) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let name = name.to_string();
        let tx = self.shard0().tx.clone();
        tokio::task::spawn_blocking(move || {
            tx.send(Cmd::MatViewOperatorStateDelete { name, done })
                .map_err(|_| ())
        })
        .await
        .map_err(|e| format!("matview_operator_state_delete join error: {e}"))?
        .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await.map_err(|_| {
            "redb writer dropped matview_operator_state_delete completion".to_string()
        })?
    }

    /// Scan every persisted incremental-matview operator-state snapshot `(name, blob)`.
    #[cfg(feature = "matview")]
    pub fn matview_operator_state_scan(&self) -> MatViewScanResult {
        let (reply, rx) = std::sync::mpsc::sync_channel(1);
        self.shard0()
            .tx
            .send(Cmd::MatViewOperatorStateScan { reply })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        await_writer_reply(&rx, "matview_operator_state_scan")?
    }
}
