//! EH-384, backend level: the scrub cursor is durable. A step that stops
//! mid-graph resumes there after the backend is closed and reopened, the
//! resumed walk skips nothing, and the next cycle finds the same unreadable
//! row again.

use super::*;
use crate::protocol::Method;
use crate::redb_store::scrub::{ScrubBudget, ScrubCursor};
use crate::redb_store::{NodeUnreadable, UnsealFailure};
use crate::server::persistence::PersistenceBackend;

const GRAPH: &str = "scrub_backend_graph";
const CORRUPT: &str = "n2";

#[cfg(feature = "security")]
struct NoEncryptionKeyEnv(Vec<(&'static str, Option<std::ffi::OsString>)>);

#[cfg(feature = "security")]
impl NoEncryptionKeyEnv {
    // The caller holds the crate-wide environment WRITE lock until this guard
    // drops. Other tests may already have provisioned the shared at-rest key.
    fn set() -> Self {
        let names = [
            crate::crypto::ENCRYPTION_KEY_ENV,
            crate::crypto::ENCRYPTION_KEY_ID_ENV,
            crate::crypto::ENCRYPTION_KEY_VERSION_ENV,
            crate::crypto::ENCRYPTION_REQUIRED_ENV,
        ];
        let previous = names
            .into_iter()
            .map(|name| (name, std::env::var_os(name)))
            .collect();
        for name in names {
            std::env::remove_var(name);
        }
        std::env::set_var(crate::crypto::ENCRYPTION_REQUIRED_ENV, "off");
        Self(previous)
    }
}

#[cfg(feature = "security")]
impl Drop for NoEncryptionKeyEnv {
    fn drop(&mut self) {
        for (name, value) in self.0.drain(..) {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
    }
}

fn node(id: &str) -> Method {
    let properties_msgpack = if id == CORRUPT {
        // Sealed framing with no configured key: unreadable, and nothing on
        // the write path opens it.
        let mut blob = vec![0xE6];
        blob.extend_from_slice(&[0u8; 40]);
        blob
    } else {
        rmp_serde::to_vec_named(&serde_json::json!({ "id": id })).expect("encode node")
    };
    Method::AddNode {
        node_id: id.to_string(),
        properties_msgpack,
    }
}

async fn step(backend: &RedbBackend) -> (u64, Vec<NodeUnreadable>, ScrubCursor) {
    let mut passes = backend
        .scrub_step(ScrubBudget { rows: 3 })
        .await
        .expect("scrub step");
    assert_eq!(passes.len(), 1, "one shard under test");
    let pass = passes.remove(0);
    (pass.scanned, pass.findings, pass.next)
}

#[cfg(feature = "security")]
// spec: EG-DURABLE-KERNEL-R020
// spec: EG-DURABLE-KERNEL-R069
#[tokio::test(flavor = "multi_thread")]
async fn scrub_cursor_survives_reopen_and_the_walk_skips_nothing() {
    let _env_lock = crate::crypto::acquire_test_env_lock().await;
    let _no_key = NoEncryptionKeyEnv::set();
    let dir = std::env::temp_dir().join(format!("eg-scrub-backend-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let dir_s = dir.to_string_lossy().to_string();
    let backend = RedbBackend::open_with_shards(dir_s.clone(), 64, 1).expect("open");
    for id in ["n0", "n1", "n2", "n3", "n4"] {
        backend
            .record_durable(GRAPH, &node(id))
            .await
            .expect("an unreadable payload still commits");
    }
    let finding = NodeUnreadable::new(GRAPH, CORRUPT, UnsealFailure::SealedWithoutKey);

    let (scanned, findings, cursor) = step(&backend).await;
    assert_eq!((scanned, findings), (3, vec![finding.clone()]));
    assert_eq!(cursor.graph.as_deref(), Some(GRAPH));
    assert_eq!(cursor.after.as_deref(), Some(CORRUPT));
    backend.shutdown();
    drop(backend);

    let reopened = RedbBackend::open_with_shards(dir_s, 64, 1).expect("reopen");
    let (scanned, findings, cursor) = step(&reopened).await;
    assert_eq!(
        (scanned, findings.len()),
        (2, 0),
        "resumed after n2: n3, n4"
    );
    assert_eq!((cursor.graph, cursor.cycles), (None, 1));
    let (scanned, findings, _) = step(&reopened).await;
    assert_eq!(
        (scanned, findings),
        (3, vec![finding]),
        "the next cycle finds it again"
    );
    reopened.shutdown();
    drop(reopened);
    let _ = std::fs::remove_dir_all(&dir);
}
