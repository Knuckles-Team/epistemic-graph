//! Durable RBAC / identity persistence (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence, feature `security`).
//!
//! The RBAC evaluator (CONCEPT:EG-KG.compute.feature, [`crate::rbac::RbacPolicy`]) and the
//! registered [`AgentIdentity`] set both live in-memory in the
//! [`IsolationLayer`](crate::isolation::IsolationLayer). CONCEPT:EG-KG.compute.durable-rbac-identity-persistence makes that
//! state **durable**: it is written through to a redb table on every
//! `RbacAdmin`/`register_identity` mutation and reloaded at boot, so roles, grants
//! and identities survive a process restart.
//!
//! Design (mirrors the redb-backed cold tier, CONCEPT:EG-KG.coordination.distributed-cache-coherence):
//!   * ONE redb table `rbac_v1` in `{persist_dir}/rbac.redb` (a separate file, like
//!     the blob CAS / cold tier), three well-known keys:
//!       - `policy`     → serde_json bytes of the whole [`RbacPolicy`];
//!       - `identities` → serde_json bytes of a `BTreeMap<agent_id, AgentIdentity>`;
//!       - `bootstrap`  → the one-time identity-bootstrap state.
//!   * The identity map is a `BTreeMap` so the persisted bytes are **deterministic**
//!     (stable key order); a save→reopen always restores the identical logical state.
//!   * A brand-new store is atomically bootstrapped with the current empty,
//!     default-deny policy image plus an explicit empty identity map. A partial or
//!     absent image after bootstrap is corruption and fails closed at boot.

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;
use std::sync::Arc;

use redb::{Database, ReadableDatabase, TableDefinition};
use sha2::{Digest, Sha256};

use crate::acl::AgentIdentity;
use crate::rbac::RbacPolicy;

/// `key → serde_json bytes`. One table, three well-known keys (`policy`,
/// `identities`, `bootstrap`) written in a single durable transaction
/// (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence).
const RBAC_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("rbac_v1");
const POLICY_KEY: &str = "policy";
const IDENTITIES_KEY: &str = "identities";
const BOOTSTRAP_KEY: &str = "bootstrap";

/// Durable lifecycle for the only request admitted before an identity exists.
/// `Consumed` is never inferred from an empty identity map: removing every
/// identity therefore fails closed instead of silently reopening bootstrap.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityBootstrapState {
    #[default]
    Pending,
    Consumed,
}

/// Errors from the durable RBAC store (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence). redb's several fallible
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
    /// One or both mandatory current state records are absent.
    IncompleteState(&'static str),
}

impl fmt::Display for RbacPersistError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RbacPersistError::Io(e) => write!(f, "rbac persist io error: {e}"),
            RbacPersistError::Serde(e) => write!(f, "rbac persist serde error: {e}"),
            RbacPersistError::Redb(e) => write!(f, "rbac persist redb error: {e}"),
            RbacPersistError::IncompleteState(message) => {
                write!(f, "rbac persist current-state error: {message}")
            }
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
/// (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence). Cheap to `clone` (shares one `Arc<Database>`), so an
/// `IsolationLayer` can hold it behind the [`RbacPolicyStore`] adapter.
pub struct RbacStore {
    db: Arc<Database>,
}

/// Adapter seam for durable identity/RBAC policy state.  Production uses
/// [`RbacStore`]; tests or alternate storage backends can implement the same
/// atomic snapshot contract without changing [`IsolationLayer`](crate::isolation::IsolationLayer).
pub trait RbacPolicyStore: Send + Sync {
    fn load(
        &self,
    ) -> Result<
        (
            RbacPolicy,
            BTreeMap<String, AgentIdentity>,
            IdentityBootstrapState,
        ),
        RbacPersistError,
    >;
    fn save(
        &self,
        policy: &RbacPolicy,
        identities: &BTreeMap<String, AgentIdentity>,
        bootstrap: IdentityBootstrapState,
    ) -> Result<(), RbacPersistError>;
}

/// Current in-process policy store used by embedded/test isolation layers that do not
/// have a filesystem persistence directory. It preserves the same atomic full-image
/// contract as redb; there is no `None`/no-op persistence state.
#[derive(Debug, Default)]
pub struct MemoryRbacStore {
    state: parking_lot::RwLock<(
        RbacPolicy,
        BTreeMap<String, AgentIdentity>,
        IdentityBootstrapState,
    )>,
}

impl MemoryRbacStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl RbacPolicyStore for MemoryRbacStore {
    fn load(
        &self,
    ) -> Result<
        (
            RbacPolicy,
            BTreeMap<String, AgentIdentity>,
            IdentityBootstrapState,
        ),
        RbacPersistError,
    > {
        Ok(self.state.read().clone())
    }

    fn save(
        &self,
        policy: &RbacPolicy,
        identities: &BTreeMap<String, AgentIdentity>,
        bootstrap: IdentityBootstrapState,
    ) -> Result<(), RbacPersistError> {
        *self.state.write() = (policy.clone(), identities.clone(), bootstrap);
        Ok(())
    }
}

impl fmt::Debug for RbacStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RbacStore").finish_non_exhaustive()
    }
}

impl RbacStore {
    /// Open (or create) `{dir}/rbac.redb` and ensure the table exists
    /// (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence). The dir is created if absent. Opening validates that the
    /// store is writable up front; subsequent fallible write-throughs still
    /// propagate commit failures so callers can roll state back and fail closed.
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
        eg_mutation_store::initialize(&db).map_err(RbacPersistError::Redb)?;
        let store = Self { db: Arc::new(db) };
        store.bootstrap_current_state()?;
        Ok(store)
    }

    /// Atomically create the explicit current bootstrap image for a brand-new store.
    /// A partial image is never repaired because that could silently discard part
    /// of an authorization state transition.
    fn bootstrap_current_state(&self) -> Result<(), RbacPersistError> {
        let rtx = self
            .db
            .begin_read()
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
        let table = rtx
            .open_table(RBAC_TABLE)
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
        let policy_present = table
            .get(POLICY_KEY)
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?
            .is_some();
        let identities_present = table
            .get(IDENTITIES_KEY)
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?
            .is_some();
        let bootstrap_present = table
            .get(BOOTSTRAP_KEY)
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?
            .is_some();
        drop(table);
        drop(rtx);
        match (policy_present, identities_present, bootstrap_present) {
            (true, true, true) => Ok(()),
            (false, false, false) => self.save(
                &RbacPolicy::new(),
                &BTreeMap::new(),
                IdentityBootstrapState::Pending,
            ),
            _ => Err(RbacPersistError::IncompleteState(
                "policy, identities, and bootstrap state must all be present",
            )),
        }
    }

    /// Load the mandatory persisted policy, identities, and bootstrap lifecycle
    /// (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence). Missing records are
    /// rejected; only [`RbacStore::open`] may create the explicit bootstrap image.
    pub fn load(
        &self,
    ) -> Result<
        (
            RbacPolicy,
            BTreeMap<String, AgentIdentity>,
            IdentityBootstrapState,
        ),
        RbacPersistError,
    > {
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
            None => {
                return Err(RbacPersistError::IncompleteState(
                    "mandatory policy record is absent",
                ))
            }
        };
        let identities = match t
            .get(IDENTITIES_KEY)
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?
        {
            Some(v) => serde_json::from_slice(v.value())?,
            None => {
                return Err(RbacPersistError::IncompleteState(
                    "mandatory identities record is absent",
                ))
            }
        };
        let bootstrap = match t
            .get(BOOTSTRAP_KEY)
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?
        {
            Some(v) => serde_json::from_slice(v.value())?,
            None => {
                return Err(RbacPersistError::IncompleteState(
                    "mandatory identity bootstrap record is absent",
                ))
            }
        };
        Ok((policy, identities, bootstrap))
    }

    /// Write-through the FULL RBAC state in ONE durable
    /// (immediate-fsync) transaction (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence). Re-serializing the whole
    /// (small, admin-scale) state on each mutation keeps the three keys mutually
    /// consistent and the write path trivially correct.
    pub fn save(
        &self,
        policy: &RbacPolicy,
        identities: &BTreeMap<String, AgentIdentity>,
        bootstrap: IdentityBootstrapState,
    ) -> Result<(), RbacPersistError> {
        let policy_bytes = serde_json::to_vec(policy)?;
        let ident_bytes = serde_json::to_vec(identities)?;
        let bootstrap_bytes = serde_json::to_vec(&bootstrap)?;
        let mut state_digest = Sha256::new();
        state_digest.update(&policy_bytes);
        state_digest.update([0]);
        state_digest.update(&ident_bytes);
        state_digest.update([0]);
        state_digest.update(&bootstrap_bytes);
        let digest = hex::encode(state_digest.finalize());

        // A response-lost retry may arrive after the desired image already committed.
        // Compare the authoritative bytes first so that retry is an exact no-op. This
        // also lets a legitimate later transition back to an older logical image
        // commit at a new version; a digest-only batch id would incorrectly replay the
        // first historical occurrence and leave the newer on-disk image unchanged.
        let already_current = {
            let rtx = self
                .db
                .begin_read()
                .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
            let table = rtx
                .open_table(RBAC_TABLE)
                .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
            let policy_matches = table
                .get(POLICY_KEY)
                .map_err(|e| RbacPersistError::Redb(e.to_string()))?
                .map(|value| value.value() == policy_bytes.as_slice())
                .unwrap_or(false);
            let identities_matches = table
                .get(IDENTITIES_KEY)
                .map_err(|e| RbacPersistError::Redb(e.to_string()))?
                .map(|value| value.value() == ident_bytes.as_slice())
                .unwrap_or(false);
            let bootstrap_matches = table
                .get(BOOTSTRAP_KEY)
                .map_err(|e| RbacPersistError::Redb(e.to_string()))?
                .map(|value| value.value() == bootstrap_bytes.as_slice())
                .unwrap_or(false);
            policy_matches && identities_matches && bootstrap_matches
        };
        if already_current {
            return Ok(());
        }

        let expected = eg_mutation_store::version(&self.db, "native", "security-control")
            .map_err(RbacPersistError::Redb)?;
        let target = expected.checked_add(1).ok_or_else(|| {
            RbacPersistError::Redb("identity/RBAC state version overflow".to_string())
        })?;
        let batch_id = format!("rbac:{target}:{digest}");
        let batch = eg_types::MutationBatch {
            schema_version: eg_types::MUTATION_BATCH_VERSION,
            batch_id: batch_id.clone(),
            context: eg_types::MutationRequestContext {
                request_id: 0,
                principal: "principal:sha256:d70d97fc35a6e2dfbef26a2bca76a96c6dd2c4142ae2a14850deaf61b478bba0".to_string(),
                purpose: None,
                policy_fingerprint: None,
                trace_id: None,
            },
            tenant: "native".to_string(),
            graph: "security-control".to_string(),
            placement_epoch: 0,
            idempotency_key: batch_id.clone(),
            expected_graph_version: Some(expected),
            fencing_token: None,
            authoritative_state: None,
            operations: vec![eg_types::MutationOperation {
                ordinal: 0,
                surface: eg_types::MutationSurface::Other,
                domain: eg_types::mutation_batch::MutationDomain::ControlPlane,
                method: eg_types::protocol::Method::ApplyMutation {
                    event_type: "security_state_snapshot".to_string(),
                    query: format!("sha256:{digest}"),
                },
            }],
            outbox: vec![eg_types::MutationOutboxIntent {
                topic: "engine.security.committed".to_string(),
                key: batch_id,
                payload: digest.as_bytes().to_vec(),
                headers: std::collections::BTreeMap::new(),
            }],
            created_at_ms: 0,
        };
        let mut wtx = self
            .db
            .begin_write()
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
        wtx.set_durability(redb::Durability::Immediate)
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
        let source_version =
            match eg_mutation_store::begin(&wtx, &batch).map_err(RbacPersistError::Redb)? {
                eg_mutation_store::Begin::Replay(_) => {
                    wtx.abort()
                        .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
                    return Ok(());
                }
                eg_mutation_store::Begin::Apply { source_version } => source_version,
            };
        {
            let mut t = wtx
                .open_table(RBAC_TABLE)
                .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
            t.insert(POLICY_KEY, policy_bytes.as_slice())
                .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
            t.insert(IDENTITIES_KEY, ident_bytes.as_slice())
                .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
            t.insert(BOOTSTRAP_KEY, bootstrap_bytes.as_slice())
                .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
        }
        eg_mutation_store::finish(
            &wtx,
            &batch,
            Some(
                rmp_serde::to_vec_named(&true)
                    .map_err(|e| RbacPersistError::Redb(e.to_string()))?,
            ),
            0,
            source_version,
        )
        .map_err(RbacPersistError::Redb)?;
        eg_mutation_store::commit(wtx, &batch).map_err(RbacPersistError::Redb)?;
        Ok(())
    }
}

impl RbacPolicyStore for RbacStore {
    fn load(
        &self,
    ) -> Result<
        (
            RbacPolicy,
            BTreeMap<String, AgentIdentity>,
            IdentityBootstrapState,
        ),
        RbacPersistError,
    > {
        RbacStore::load(self)
    }

    fn save(
        &self,
        policy: &RbacPolicy,
        identities: &BTreeMap<String, AgentIdentity>,
        bootstrap: IdentityBootstrapState,
    ) -> Result<(), RbacPersistError> {
        RbacStore::save(self, policy, identities, bootstrap)
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
            store
                .save(&policy, &identities, IdentityBootstrapState::Consumed)
                .unwrap();
        }
        // Reopen the SAME dir — the state is durable across "process" lifetimes.
        let store = RbacStore::open(&dir).unwrap();
        let (loaded_policy, loaded_ids, bootstrap) = store.load().unwrap();
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
        assert_eq!(bootstrap, IdentityBootstrapState::Consumed);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn eg303_new_store_bootstraps_explicit_default_deny_state() {
        let dir = tmp_dir("absent");
        let store = RbacStore::open(&dir).unwrap();
        let (policy, ids, bootstrap) = store.load().unwrap();
        assert!(policy.grants().is_empty());
        assert!(ids.is_empty());
        assert_eq!(bootstrap, IdentityBootstrapState::Pending);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn consumed_bootstrap_never_reopens_when_identities_are_empty() {
        let dir = tmp_dir("bootstrap-consumed");
        {
            let store = RbacStore::open(&dir).unwrap();
            store
                .save(
                    &RbacPolicy::new(),
                    &BTreeMap::new(),
                    IdentityBootstrapState::Consumed,
                )
                .unwrap();
        }
        let store = RbacStore::open(&dir).unwrap();
        let (_, identities, bootstrap) = store.load().unwrap();
        assert!(identities.is_empty());
        assert_eq!(bootstrap, IdentityBootstrapState::Consumed);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn eg303_partial_current_state_fails_closed() {
        let dir = tmp_dir("partial");
        let store = RbacStore::open(&dir).unwrap();
        let wtx = store.db.begin_write().unwrap();
        {
            let mut table = wtx.open_table(RBAC_TABLE).unwrap();
            table.remove(IDENTITIES_KEY).unwrap();
        }
        wtx.commit().unwrap();
        assert!(matches!(
            store.load(),
            Err(RbacPersistError::IncompleteState(_))
        ));
        drop(store);
        assert!(matches!(
            RbacStore::open(&dir),
            Err(RbacPersistError::IncompleteState(_))
        ));
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
