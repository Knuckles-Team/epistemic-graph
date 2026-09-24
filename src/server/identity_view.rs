//! The published snapshot of each engine's identity store (IDM-01/IDM-02).
//!
//! The engine publishes its store, keyed by its persistence directory, after
//! every write that changes it. Two readers consume the snapshot where they
//! cannot reach the server state: the SQL projection (the identity
//! relations) and request verification (a trusted issuer restricted to one
//! principal kind proves the kind against the store). No snapshot -- an
//! in-memory engine -- means no store: a kind-restricted issuer then fails
//! closed.
#![cfg(feature = "security")]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};

use eg_types::identity::{IdentityStore, UserKind};

type Views = RwLock<HashMap<PathBuf, Arc<IdentityStore>>>;

fn views() -> &'static Views {
    static VIEWS: OnceLock<Views> = OnceLock::new();
    VIEWS.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Publish `store` as the identity view of the engine at `persist_dir`.
pub(crate) fn publish(persist_dir: Option<&str>, store: &IdentityStore) {
    let Some(dir) = persist_dir else {
        return;
    };
    if let Ok(mut views) = views().write() {
        views.insert(PathBuf::from(dir), Arc::new(store.clone()));
    }
}

/// The last published store of the engine at `persist_dir`.
pub(crate) fn published(persist_dir: &Path) -> Option<Arc<IdentityStore>> {
    views().read().ok()?.get(persist_dir).cloned()
}

/// The kind of an ACTIVE principal of the engine at `persist_dir`.
pub(crate) fn active_principal_kind(persist_dir: Option<&str>, principal: &str) -> Option<UserKind> {
    let store = published(Path::new(persist_dir?))?;
    let user = store.user(principal)?;
    user.status.is_active().then_some(user.kind)
}
