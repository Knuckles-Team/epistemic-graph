//! The COLD tier backing store (CONCEPT:EG-KG.storage.durable-redb-cold-tier).
//!
//! The COLD tier is where the cache offloads bytes it can no longer keep in RAM — the
//! "survive OOM by spilling to disk" rung. [`ColdStore`] is the pluggable seam:
//!
//! * [`MemoryColdStore`] — always available, dependency-free. Holds the spilled blobs
//!   in a separate `HashMap` so the tier machinery (and all its tests) work in a pure
//!   RAM build. It models the tier boundary without a durable device.
//! * [`RedbColdStore`] — behind the `durable` feature. A crash-safe, single-writer
//!   redb blob store, reusing the engine's KG-2.195 durability tier, so demoted KV
//!   bytes SURVIVE an OOM / process restart. This is the true offload target.
//!
//! `ColdStore` is intentionally object-safe (`Box<dyn ColdStore<K>>`) and moves only
//! bytes — the cache owns all per-key metadata (score, compression flag, sizes), so a
//! backend is a dumb blob device.

use std::io;

/// A pluggable COLD-tier blob device the tiered cache spills bytes into (CONCEPT:EG-KG.storage.durable-redb-cold-tier).
///
/// Object-safe by design: keys and values cross as borrows / owned `Vec<u8>`, so it can
/// be held as `Box<dyn ColdStore<K>>`. The cache keeps the per-key metadata; a store
/// only persists / returns opaque blobs.
pub trait ColdStore<K> {
    /// Write (or overwrite) the blob for `key`.
    fn put(&mut self, key: &K, bytes: &[u8]) -> io::Result<()>;
    /// Read the blob for `key`, if present.
    fn get(&self, key: &K) -> io::Result<Option<Vec<u8>>>;
    /// Remove the blob for `key` (no-op if absent).
    fn remove(&mut self, key: &K) -> io::Result<()>;
    /// A short label for diagnostics / stats.
    fn kind(&self) -> &'static str;
}

/// An in-RAM COLD store (CONCEPT:EG-KG.storage.durable-redb-cold-tier) — always available, no dependency.
///
/// It keeps spilled blobs in a *separate* map from the HOT/WARM tiers, so demotion to
/// COLD is a real tier transition (bytes leave the HOT/WARM budgets) even in a pure-RAM
/// build. It is NOT durable — use [`RedbColdStore`] (feature `durable`) for
/// offload-that-survives-a-restart.
#[derive(Debug, Default)]
pub struct MemoryColdStore<K> {
    blobs: std::collections::HashMap<K, Vec<u8>>,
}

impl<K: std::hash::Hash + Eq + Clone> MemoryColdStore<K> {
    /// A fresh, empty in-RAM cold store.
    pub fn new() -> Self {
        MemoryColdStore {
            blobs: std::collections::HashMap::new(),
        }
    }
}

impl<K: std::hash::Hash + Eq + Clone> ColdStore<K> for MemoryColdStore<K> {
    fn put(&mut self, key: &K, bytes: &[u8]) -> io::Result<()> {
        self.blobs.insert(key.clone(), bytes.to_vec());
        Ok(())
    }
    fn get(&self, key: &K) -> io::Result<Option<Vec<u8>>> {
        Ok(self.blobs.get(key).cloned())
    }
    fn remove(&mut self, key: &K) -> io::Result<()> {
        self.blobs.remove(key);
        Ok(())
    }
    fn kind(&self) -> &'static str {
        "memory"
    }
}

/// A key that the durable ([`RedbColdStore`]) tier can render to stable bytes
/// (CONCEPT:EG-KG.storage.durable-redb-cold-tier). Only the durable tier needs this; the RAM tiers are content with
/// `Hash + Eq`.
#[cfg(feature = "durable")]
pub trait ColdKey {
    /// Stable byte encoding of the key (used as the redb primary key).
    fn cold_key(&self) -> Vec<u8>;
}

#[cfg(feature = "durable")]
impl ColdKey for String {
    fn cold_key(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

#[cfg(feature = "durable")]
impl ColdKey for Vec<u8> {
    fn cold_key(&self) -> Vec<u8> {
        self.clone()
    }
}

#[cfg(feature = "durable")]
impl ColdKey for u64 {
    fn cold_key(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

/// A durable, crash-safe COLD store backed by redb (CONCEPT:EG-KG.storage.durable-redb-cold-tier, feature `durable`).
///
/// Demoted KV bytes are written under the key's [`ColdKey::cold_key`] encoding into a
/// single redb table and committed (fsync), so they SURVIVE an OOM / process restart —
/// the true "offload to disk to survive OOM" behavior. Reuses the engine's KG-2.195
/// redb durability tier; a build without `durable` never links redb.
///
/// RF-RULING-004: `eg-storage` is the sole physical owner of the cold-tier file
/// (declared `OwnerLayout::Kv`, whose owner tables are `kv` and
/// `eg_kvcache_cold`) and `eg-transaction` the sole writer. This crate holds only
/// the capabilities they issue and never opens a database.
#[cfg(feature = "durable")]
pub struct RedbColdStore {
    kernel: eg_storage::StorageKernelV1,
    mutations: eg_transaction::MutationKernelV1,
    owner: eg_storage::OwnedStoreHandle<eg_storage::KvOwner>,
}

#[cfg(feature = "durable")]
const COLD: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("eg_kvcache_cold");

/// Operator-facing identity of the ONE physical cold-tier owner file.
#[cfg(feature = "durable")]
const COLD_PHYSICAL_STORE: &str = "eg-kvcache:cold-tier";
#[cfg(feature = "durable")]
const COLD_SCOPE_TENANT: &str = "native";
#[cfg(feature = "durable")]
const COLD_SCOPE_RESOURCE: &str = "kvcache-cold";
#[cfg(feature = "durable")]
const COLD_SCOPE_INCARNATION: &str = "kvcache-cold:v1";

#[cfg(feature = "durable")]
fn cold_scope_identity() -> io::Result<eg_types::MutationScopeIdentity> {
    eg_types::MutationScopeIdentity::fixed_native(
        COLD_SCOPE_TENANT,
        eg_types::mutation_batch::MutationDomain::KvStore,
        COLD_SCOPE_RESOURCE,
        COLD_SCOPE_INCARNATION,
    )
    .map_err(io::Error::other)
}

/// The batch for one cold-tier row change.
///
/// A demoted KV page carries no caller identity, so this is a MAINTENANCE
/// mutation (RF-RULING-005): still ledgered, fenced and version-bumping, because
/// an un-ledgered owner write would be a second physical authority. `batch_id` is
/// `(kind, scope version)` -- unique per attempt and stable across a crash-retry
/// of that attempt, so a retry replays instead of conflicting.
#[cfg(feature = "durable")]
fn cold_batch(
    kind: &str,
    identity: &eg_types::MutationScopeIdentity,
    principal: &str,
    expected_version: u64,
) -> io::Result<eg_types::MutationBatch> {
    let batch_id = format!("kvcache-cold-{kind}:v{expected_version}");
    let batch = eg_types::MutationBatch {
        schema_version: eg_types::MUTATION_BATCH_VERSION,
        batch_id: batch_id.clone(),
        context: eg_types::MutationRequestContext {
            request_id: 0,
            principal: principal.to_string(),
            purpose: None,
            policy_fingerprint: None,
            trace_id: None,
            verified_capabilities: std::collections::BTreeSet::new(),
        },
        identity: identity.clone(),
        placement_epoch: 0,
        idempotency_key: batch_id.clone(),
        version_expectation: eg_types::VersionExpectation::Native(expected_version),
        fencing_token: None,
        authoritative_state: None,
        operations: vec![eg_types::MutationOperation {
            ordinal: 0,
            surface: eg_types::MutationSurface::Other,
            domain: eg_types::mutation_batch::MutationDomain::KvStore,
            method: eg_types::protocol::Method::ApplyMutation {
                event_type: format!("kvcache_cold_{kind}"),
                query: batch_id,
            },
        }],
        outbox: Vec::new(),
        created_at_ms: 0,
    };
    batch.validate().map_err(io::Error::other)?;
    Ok(batch)
}

#[cfg(feature = "durable")]
impl RedbColdStore {
    /// Open (creating if absent) a durable cold store at `path` through the
    /// storage kernel.
    ///
    /// `verifier` is the composition root's proof authority: only it may decide
    /// that `principal` may serve this store's fixed cold-tier scope.
    pub fn open<P: AsRef<std::path::Path>>(
        path: P,
        verifier: &dyn eg_storage::ScopeGrantVerifier,
        principal: &str,
        proof: &[u8],
    ) -> io::Result<Self> {
        let path = path.as_ref();
        let identity = cold_scope_identity()?;
        let physical =
            eg_storage::PhysicalStoreIdentity::new(COLD_PHYSICAL_STORE).map_err(io::Error::other)?;
        // The kernel materializes the whole declared census at create and
        // re-validates it on every open, so a first `get` before any `put`
        // succeeds without this crate opening a table itself.
        let kernel = if path.exists() {
            eg_storage::StorageKernelV1::open_owner::<eg_storage::KvOwner>(path, physical, None)
        } else {
            eg_storage::StorageKernelV1::create_owner::<eg_storage::KvOwner>(path, physical, None)
        }
        .map_err(io::Error::other)?;
        let (kernel, authority) = kernel
            .into_read_and_mutation_authority()
            .map_err(io::Error::other)?;
        let mutations = eg_transaction::MutationKernelV1::new(authority);
        let grant = kernel
            .authenticate_scope::<eg_storage::KvOwner>(
                verifier,
                identity,
                principal.to_string(),
                proof,
            )
            .map_err(io::Error::other)?;
        let owner = kernel.bind_serving_scope(grant, 0).map_err(io::Error::other)?;
        mutations.bootstrap_ledger(&owner).map_err(io::Error::other)?;
        Ok(RedbColdStore {
            kernel,
            mutations,
            owner,
        })
    }

    /// Apply one cold-tier row change as an admitted maintenance mutation.
    fn maintain<F>(&self, kind: &str, apply: F) -> io::Result<()>
    where
        F: FnOnce(
            &eg_transaction::AdmittedOwnerWrite<'_, eg_storage::KvOwner>,
        ) -> io::Result<()>,
    {
        let read = self
            .kernel
            .read_scope(&self.owner)
            .map_err(io::Error::other)?;
        let expected_version = eg_transaction::version(&read).map_err(io::Error::other)?;
        drop(read);
        let batch = cold_batch(
            kind,
            self.owner.identity(),
            self.owner.principal(),
            expected_version,
        )?;
        let (write, begun) = self
            .mutations
            .admit_maintenance(&self.owner, &batch)
            .map_err(io::Error::other)?;
        let source_version = match begun {
            eg_transaction::Begin::Replay(_) => {
                return write.abort().map_err(io::Error::other);
            }
            eg_transaction::Begin::Apply { source_version } => source_version,
        };
        let owner_write = write
            .owner_rows(&self.owner, &batch)
            .map_err(io::Error::other)?;
        let staged = apply(&owner_write);
        // Always close the owner capability: dropping it unfinished poisons the
        // write and would mask the staging error.
        owner_write.finish_owner().map_err(io::Error::other)?;
        if let Err(error) = staged {
            write.abort().map_err(io::Error::other)?;
            return Err(error);
        }
        self.mutations
            .finish(&write, &batch, None, 0, source_version)
            .map_err(io::Error::other)?;
        self.mutations
            .commit(write, &batch)
            .map_err(io::Error::other)
    }
}

#[cfg(feature = "durable")]
impl<K: ColdKey> ColdStore<K> for RedbColdStore {
    fn put(&mut self, key: &K, bytes: &[u8]) -> io::Result<()> {
        self.maintain("put", |wtx| {
            wtx.open_table(COLD)
                .map_err(io::Error::other)?
                .insert(key.cold_key().as_slice(), bytes)
                .map_err(|e| io::Error::other(e.to_string()))?;
            Ok(())
        })
    }

    fn get(&self, key: &K) -> io::Result<Option<Vec<u8>>> {
        let read = self
            .kernel
            .read_scope(&self.owner)
            .map_err(io::Error::other)?;
        let t = read.open_table(COLD).map_err(io::Error::other)?;
        let v = t
            .get(key.cold_key().as_slice())
            .map_err(|e| io::Error::other(e.to_string()))?
            .map(|g| g.value().to_vec());
        Ok(v)
    }

    fn remove(&mut self, key: &K) -> io::Result<()> {
        self.maintain("remove", |wtx| {
            wtx.open_table(COLD)
                .map_err(io::Error::other)?
                .remove(key.cold_key().as_slice())
                .map_err(|e| io::Error::other(e.to_string()))?;
            Ok(())
        })
    }

    fn kind(&self) -> &'static str {
        "redb"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test-only composition root. Production supplies the real scope-grant proof
    /// authority; this one still checks the layout, the principal and the proof,
    /// so a store opened for the wrong layout fails closed in tests too.
    #[cfg(feature = "durable")]
    struct TestScopeVerifier;

    #[cfg(feature = "durable")]
    const TEST_PRINCIPAL: &str =
        "principal:sha256:1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90a";
    #[cfg(feature = "durable")]
    const TEST_PROOF: &[u8] = b"eg-kvcache-test-scope-grant";

    #[cfg(feature = "durable")]
    impl eg_storage::ScopeGrantVerifier for TestScopeVerifier {
        fn verify(
            &self,
            _physical: &eg_storage::PhysicalStoreIdentity,
            layout: eg_storage::OwnerLayout,
            _identity: &eg_types::MutationScopeIdentity,
            principal: &str,
            proof: &[u8],
        ) -> Result<(), String> {
            if layout != eg_storage::OwnerLayout::Kv
                || principal != TEST_PRINCIPAL
                || proof != TEST_PROOF
            {
                return Err("test scope authority rejected".to_string());
            }
            Ok(())
        }
    }

    /// Open the durable cold store the way the composition root would.
    #[cfg(feature = "durable")]
    fn open_test_cold_store(path: &std::path::Path) -> io::Result<RedbColdStore> {
        RedbColdStore::open(path, &TestScopeVerifier, TEST_PRINCIPAL, TEST_PROOF)
    }

    /// CONCEPT:EG-KG.storage.durable-redb-cold-tier — the RAM cold store round-trips blobs (models the tier boundary).
    #[test]
    fn eg185_memory_cold_store_roundtrips() {
        let mut cs: MemoryColdStore<String> = MemoryColdStore::new();
        cs.put(&"k".to_string(), b"hello").unwrap();
        assert_eq!(
            cs.get(&"k".to_string()).unwrap().as_deref(),
            Some(&b"hello"[..])
        );
        cs.remove(&"k".to_string()).unwrap();
        assert_eq!(cs.get(&"k".to_string()).unwrap(), None);
    }

    /// CONCEPT:EG-KG.storage.durable-redb-cold-tier — the durable redb cold tier persists across a store reopen
    /// (offload survives a restart).
    #[cfg(feature = "durable")]
    #[test]
    fn eg185_durable_redb_cold_store_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cold.redb");
        {
            let mut cs = open_test_cold_store(&path).unwrap();
            ColdStore::<String>::put(&mut cs, &"tok".to_string(), b"kvbytes").unwrap();
        }
        // Reopen a fresh handle — the bytes must still be there.
        let cs = open_test_cold_store(&path).unwrap();
        assert_eq!(
            ColdStore::<String>::get(&cs, &"tok".to_string())
                .unwrap()
                .as_deref(),
            Some(&b"kvbytes"[..]),
            "durable cold tier must survive a reopen (OOM/restart offload)"
        );
    }
}
