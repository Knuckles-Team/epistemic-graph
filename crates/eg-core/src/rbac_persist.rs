//! Durable RBAC / identity persistence (CONCEPT:EG-303, feature `security`).
//!
//! The RBAC evaluator (CONCEPT:EG-092, [`crate::rbac::RbacPolicy`]) and the
//! registered [`AgentIdentity`] set both live in-memory in the
//! [`IsolationLayer`](crate::isolation::IsolationLayer). CONCEPT:EG-303 makes that
//! state **durable**: it is written through to a redb table on every
//! `RbacAdmin`/`register_identity` mutation and reloaded at boot, so roles, grants
//! and identities survive a process restart.
//!
//! Design (mirrors the redb-backed cold tier, CONCEPT:KG-2.233):
//!   * ONE redb table `rbac_v1` in `{persist_dir}/rbac.redb` (a separate file, like
//!     the blob CAS / cold tier), two well-known keys:
//!       - `policy`     → serde_json bytes of the whole [`RbacPolicy`];
//!       - `identities` → serde_json bytes of a `BTreeMap<agent_id, AgentIdentity>`.
//!   * The identity map is a `BTreeMap` so the persisted bytes are **deterministic**
//!     (stable key order); a save→reopen always restores the identical logical state.
//!   * An EMPTY/absent store loads the exact in-memory defaults — i.e. today's
//!     pre-EG-303 behavior. The `IsolationLayer` only ever attaches a store when a
//!     persist dir is explicitly configured; with none it stays fully in-memory and
//!     every write-through is a no-op (backward-compatible).

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;
use std::sync::Arc;

use redb::{Database, ReadableDatabase, TableDefinition};

use crate::acl::AgentIdentity;
use crate::rbac::RbacPolicy;

/// `key → serde_json bytes`. One table, two well-known keys (`policy`,
/// `identities`) written in a single durable transaction (CONCEPT:EG-303).
const RBAC_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("rbac_v1");
const POLICY_KEY: &str = "policy";
const IDENTITIES_KEY: &str = "identities";

/// Errors from the durable RBAC store (CONCEPT:EG-303). redb's several fallible
/// surfaces are flattened to a message string (matching the cold-tier convention);
/// io + serde carry their native errors so callers can inspect them.
#[derive(Debug)]
pub enum RbacPersistError {
    /// Creating the persist dir / opening the redb file failed.
    Io(std::io::Error),
    /// (De)serializing the policy or the identity map failed.
    Serde(serde_json::Error),
    /// A redb transaction/table/storage/commit operation failed.
    Redb(String),
}

impl fmt::Display for RbacPersistError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RbacPersistError::Io(e) => write!(f, "rbac persist io error: {e}"),
            RbacPersistError::Serde(e) => write!(f, "rbac persist serde error: {e}"),
            RbacPersistError::Redb(e) => write!(f, "rbac persist redb error: {e}"),
        }
    }
}

impl std::error::Error for RbacPersistError {}

impl From<std::io::Error> for RbacPersistError {
    fn from(e: std::io::Error) -> Self {
        RbacPersistError::Io(e)
    }
}

impl From<serde_json::Error> for RbacPersistError {
    fn from(e: serde_json::Error) -> Self {
        RbacPersistError::Serde(e)
    }
}

/// A durable, redb-backed snapshot of the RBAC policy + registered identities
/// (CONCEPT:EG-303). Cheap to `clone` (shares one `Arc<Database>`), so an
/// `IsolationLayer` that derives `Clone` can hold `Option<Arc<RbacStore>>`.
pub struct RbacStore {
    db: Arc<Database>,
}

impl fmt::Debug for RbacStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RbacStore").finish_non_exhaustive()
    }
}

impl RbacStore {
    /// Open (or create) `{dir}/rbac.redb` and ensure the table exists
    /// (CONCEPT:EG-303). The dir is created if absent. Opening validates that the
    /// store is writable up front, so subsequent write-throughs are best-effort.
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<Self, RbacPersistError> {
        std::fs::create_dir_all(dir.as_ref())?;
        let path = dir.as_ref().join("rbac.redb");
        let db = Database::create(&path).map_err(|e| RbacPersistError::Redb(e.to_string()))?;
        let wtx = db
            .begin_write()
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
        wtx.open_table(RBAC_TABLE)
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
        wtx.commit()
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
        Ok(Self { db: Arc::new(db) })
    }

    /// Load the persisted policy + identities (CONCEPT:EG-303). An EMPTY/absent
    /// store yields the in-memory defaults — an empty [`RbacPolicy`] and no
    /// identities — i.e. exactly today's pre-EG-303 boot state.
    pub fn load(&self) -> Result<(RbacPolicy, BTreeMap<String, AgentIdentity>), RbacPersistError> {
        let rtx = self
            .db
            .begin_read()
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
        let t = rtx
            .open_table(RBAC_TABLE)
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
        let policy = match t
            .get(POLICY_KEY)
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?
        {
            Some(v) => serde_json::from_slice(v.value())?,
            None => RbacPolicy::new(),
        };
        let identities = match t
            .get(IDENTITIES_KEY)
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?
        {
            Some(v) => serde_json::from_slice(v.value())?,
            None => BTreeMap::new(),
        };
        Ok((policy, identities))
    }

    /// Write-through the FULL RBAC state (policy + identities) in ONE durable
    /// (immediate-fsync) transaction (CONCEPT:EG-303). Re-serializing the whole
    /// (small, admin-scale) state on each mutation keeps the two keys mutually
    /// consistent and the write path trivially correct.
    pub fn save(
        &self,
        policy: &RbacPolicy,
        identities: &BTreeMap<String, AgentIdentity>,
    ) -> Result<(), RbacPersistError> {
        let policy_bytes = serde_json::to_vec(policy)?;
        let ident_bytes = serde_json::to_vec(identities)?;
        let mut wtx = self
            .db
            .begin_write()
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
        wtx.set_durability(redb::Durability::Immediate)
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
        {
            let mut t = wtx
                .open_table(RBAC_TABLE)
                .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
            t.insert(POLICY_KEY, policy_bytes.as_slice())
                .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
            t.insert(IDENTITIES_KEY, ident_bytes.as_slice())
                .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
        }
        wtx.commit()
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acl::{
        AgentRole, Grant, GrantEffect, RbacAction, ResourceContext, ResourceSelector, Role,
    };

    /// A unique temp dir per test invocation (no external dev-dep needed).
    fn tmp_dir(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "eg303-{}-{}-{}",
            tag,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ))
    }

    fn identity(id: &str, roles: Vec<String>) -> AgentIdentity {
        AgentIdentity {
            agent_id: id.to_string(),
            role: AgentRole::Agent,
            teams: vec![],
            roles,
        }
    }

    #[test]
    fn eg303_store_saves_and_reloads_policy_and_identities() {
        let dir = tmp_dir("store-rt");
        let mut policy = RbacPolicy::new();
        policy.add_role(Role::with_parents("editor", vec!["reader".into()]));
        policy.add_grant(Grant {
            role: "editor".into(),
            resource: ResourceSelector::Label("Doc".into()),
            action: RbacAction::Write,
            effect: GrantEffect::Allow,
        });
        let mut identities = BTreeMap::new();
        identities.insert("sam".to_string(), identity("sam", vec!["editor".into()]));

        {
            let store = RbacStore::open(&dir).unwrap();
            store.save(&policy, &identities).unwrap();
        }
        // Reopen the SAME dir — the state is durable across "process" lifetimes.
        let store = RbacStore::open(&dir).unwrap();
        let (loaded_policy, loaded_ids) = store.load().unwrap();
        assert_eq!(loaded_policy.grants().len(), 1);
        assert!(loaded_policy.is_allowed(
            &["editor"],
            &ResourceContext {
                graph: "g".into(),
                label: Some("Doc".into())
            },
            RbacAction::Write
        ));
        assert_eq!(loaded_ids.len(), 1);
        assert_eq!(loaded_ids["sam"].roles, vec!["editor".to_string()]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn eg303_absent_store_loads_in_memory_defaults() {
        let dir = tmp_dir("absent");
        let store = RbacStore::open(&dir).unwrap();
        let (policy, ids) = store.load().unwrap();
        assert!(policy.is_empty());
        assert!(ids.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn eg303_identity_bytes_are_deterministic() {
        // The BTreeMap identity container gives a stable byte serialization: two
        // saves of the same logical state produce identical persisted bytes.
        let mut a = BTreeMap::new();
        a.insert("b".to_string(), identity("b", vec!["r".into()]));
        a.insert("a".to_string(), identity("a", vec![]));
        // Insert in the OTHER order — BTreeMap normalizes ordering.
        let mut b = BTreeMap::new();
        b.insert("a".to_string(), identity("a", vec![]));
        b.insert("b".to_string(), identity("b", vec!["r".into()]));
        assert_eq!(
            serde_json::to_vec(&a).unwrap(),
            serde_json::to_vec(&b).unwrap()
        );
    }
}
