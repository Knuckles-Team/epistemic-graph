//! Generic namespaced Key→Value surface (CONCEPT:EG-KG.storage.namespaced-kv-surface).
//!
//! A drop-in KV store over the SAME durable substrate the rest of the engine uses. It is
//! NOT graph-scoped — a pair is keyed by `(namespace, key)` and lives off the node/edge
//! graph — so, like the BLOB substrate and the TSDB series store, the `Kv*` methods
//! self-route at the top of dispatch BEFORE the per-graph chain and read their store off
//! [`ServerState`](crate::server::ServerState).
//!
//! ## Durability
//!
//! With a persist dir the store owns `{persist_dir}/kv.redb` as ONE physical owner file
//! under `eg_storage::OwnerLayout::Kv`, and EVERY mutation is an admitted transaction of
//! the mutation kernel — `*_batch` carrying the caller's identity, plain
//! `put`/`delete`/`cas` as owner MAINTENANCE mutations (RF-RULING-005), all ledgered,
//! fenced and version-bumping. Durability is the kernel's and is commit-before-ack, so a
//! `KvPut` that returned `Ok` survives a `kill -9`. With NO persist dir the store is an
//! in-memory ordered map (no durable place ⇒ ephemeral), like the blob substrate.
//!
//! ## Operations
//! `KvGet`/`KvPut`/`KvDelete`/`KvScan`/`KvCas` — the wire shape of each, and its
//! response payload, is the `Method::Kv*` match in [`try_handle`].

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

use eg_storage::{KvOwner, OwnedStoreHandle, ScopedRead, StorageKernel};
use eg_transaction::{AdmittedOwnerWrite, Begin, MaintenanceBatch, MutationKernel};
use eg_types::MutationScopeIdentity;
use parking_lot::Mutex;
use redb::{ReadableTable, ReadableTableMetadata, TableDefinition};
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::RwLock;

use super::state::ServerState;
use crate::mutation_batch::{DurabilityDomain, MutationBatch, MutationSurface};
use crate::protocol::{Method, Response, ResultPayload};
use crate::server::access::CarrierAuthority;
use crate::server::mutation_batch::COMPILED_BATCH_INCARNATION;

/// The single KV table: `(namespace, key) -> value bytes`. Composite key so one file
/// holds every namespace and a prefix scan of one is a contiguous range. `eg-storage`
/// declares it as an owner table of `OwnerLayout::Kv`; this module only names it
/// (RF-RULING-004). That layout's OTHER owner table, `eg_kvcache_cold`, is eg-kvcache's.
const KV: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("kv");
/// Operator-facing identity of the ONE physical `kv.redb` owner file — the physical
/// authority boundary, independent of any logical serving scope.
pub(crate) const KV_PHYSICAL_STORE: &str = "epistemic-graph:kv";
const MAX_KV_NAMESPACE_BYTES: usize = 256;
const MAX_KV_KEY_BYTES: usize = 4 * 1024;
const MAX_KV_VALUE_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_KV_SCAN_LIMIT: usize = 10_000;
const MAX_KV_SCAN_LIMIT: usize = 100_000;
const MAX_KV_BATCH_RESULT_BYTES: usize = 1024 * 1024;

/// The row one plain KV write acts on, as it appears in the durable batch id: a
/// content digest of `(namespace, key)`. Digested rather than spelled out because a KV
/// key is arbitrary caller bytes ([`validate_key`] bounds only its length and rejects
/// NUL), and a batch id may carry no control character and no surrounding whitespace
/// (`MutationBatch::validate`). The digest is deterministic, so an auditor reading the
/// ledger can still recompute which row a batch wrote.
fn row_subject(namespace: &str, key: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(namespace.as_bytes());
    digest.update([0]);
    digest.update(key.as_bytes());
    hex::encode(digest.finalize())
}

fn validate_key(namespace: &str, key: &str) -> Result<(), String> {
    if namespace.is_empty()
        || namespace.len() > MAX_KV_NAMESPACE_BYTES
        || namespace.contains('\0')
        || key.len() > MAX_KV_KEY_BYTES
        || key.contains('\0')
    {
        return Err("KV key exceeds resource limits".to_string());
    }
    Ok(())
}

fn validate_value(value: &[u8]) -> Result<(), String> {
    if value.len() > MAX_KV_VALUE_BYTES {
        Err("KV value exceeds resource limits".to_string())
    } else {
        Ok(())
    }
}

/// The store's bootstrap AND serving scope (CONCEPT:EG-KG.storage.namespaced-kv-surface):
/// what cross-namespace reads (`get`/`scan`) and caller-less writes (`put`/`delete`/`cas`)
/// run on. `KvStore` serves arbitrarily many DYNAMIC namespaces out of one physical file,
/// each with its own identity from `kv_scope_identity` bound lazily via `mutation_version`;
/// this fixed human-readable resource can never collide with those, which are always the
/// opaque `"kv-scope:<sha256-hex>"` form `opaque_coordinator_key` mints.
const KV_BOOTSTRAP_TENANT: &str = "kv-store";
const KV_BOOTSTRAP_RESOURCE: &str = "kv-store-bootstrap";

fn kv_bootstrap_identity() -> Result<MutationScopeIdentity, String> {
    kv_scope_identity(KV_BOOTSTRAP_TENANT, KV_BOOTSTRAP_RESOURCE)
}

/// Build the native KV mutation-scope identity for one (tenant, resource) pair.
/// `resource` must be EXACTLY the string `compile_kv_batch` passes as
/// `CompileBatch::graph` for the same write: `crate::server::mutation_batch::finish_batch`
/// builds the batch's authoritative `identity` from that identical (tenant, graph) pair,
/// the SAME `DurabilityDomain::KvStore` domain (also the only native domain
/// `OwnerLayout::Kv` accepts) and the SAME `COMPILED_BATCH_INCARNATION`. The kernel's
/// binding validator rejects any mismatch, so drift on any of the three fails closed.
fn kv_scope_identity(tenant: &str, resource: &str) -> Result<MutationScopeIdentity, String> {
    let tenant = eg_types::ScopeTenantId::new(tenant)?;
    let resource = eg_types::LogicalName::new(resource)?;
    let incarnation_id = eg_types::IncarnationId::new(COMPILED_BATCH_INCARNATION)
        .map_err(|e| format!("invalid KV scope incarnation id: {e}"))?;
    MutationScopeIdentity::native(tenant, DurabilityDomain::KvStore, resource, incarnation_id)
}

/// Accumulate one scanned row under the response-size bound; `false` once `limit` rows
/// are collected and the scan must stop.
fn push_scan_row(
    out: &mut Vec<(String, Vec<u8>)>,
    bytes: &mut usize,
    limit: usize,
    key: &str,
    value: &[u8],
) -> Result<bool, String> {
    validate_value(value)?;
    *bytes = bytes
        .checked_add(key.len())
        .and_then(|total| total.checked_add(value.len()))
        .filter(|total| *total <= MAX_KV_VALUE_BYTES)
        .ok_or_else(|| "KV scan response exceeds resource limits".to_string())?;
    out.push((key.to_string(), value.to_vec()));
    Ok(out.len() < limit)
}

/// A bound serving scope. `OwnedStoreHandle` is not `Clone` (it IS a capability), so the
/// cache owns one per scope and hands out `Arc` clones.
type KvHandle = Arc<OwnedStoreHandle<KvOwner>>;

/// Authenticate and bind ONE logical serving scope on `kv.redb`. The proof bytes are
/// the composition root's; this module supplies only the identity and the layout.
fn bind_scope(kernel: &StorageKernel, scope: &MutationScopeIdentity) -> Result<KvHandle, String> {
    crate::redb_store::shard::bind_scope::<KvOwner>(kernel, scope)
}

/// Compare-and-swap `(namespace, key)` INSIDE one admitted owner write: the current
/// value is read back through the write's OWN table handle, so check and swap are the
/// same transaction, serialized against every concurrent writer.
fn swap_in_owner_write(
    owner_write: &AdmittedOwnerWrite<'_, KvOwner>,
    namespace: &str,
    key: &str,
    expected: Option<&[u8]>,
    new: Option<&[u8]>,
) -> Result<bool, String> {
    let mut table = owner_write.open_table(KV)?;
    let current: Option<Vec<u8>> = table
        .get((namespace, key))
        .map_err(|e| e.to_string())?
        .map(|value| {
            validate_value(value.value())?;
            Ok::<Vec<u8>, String>(value.value().to_vec())
        })
        .transpose()?;
    if current.as_deref() != expected {
        return Ok(false);
    }
    match new {
        Some(value) => table.insert((namespace, key), value).map(|_| ()),
        None => table.remove((namespace, key)).map(|_| ()),
    }
    .map_err(|e| e.to_string())?;
    Ok(true)
}

/// Write one KV row inside an admitted owner write; `Some` inserts, `None` removes.
/// Reports whether a value was there before.
fn write_kv_row(
    owner_write: &AdmittedOwnerWrite<'_, KvOwner>,
    namespace: &str,
    key: &str,
    new: Option<&[u8]>,
) -> Result<bool, String> {
    let mut table = owner_write.open_table(KV)?;
    let previous = match new {
        Some(value) => table.insert((namespace, key), value),
        None => table.remove((namespace, key)),
    }
    .map_err(|e| e.to_string())?;
    Ok(previous.is_some())
}

/// A namespaced key→bytes store. Durable (a kernel-owned `kv.redb`) when a persist dir
/// is configured, else an in-memory ordered map.
pub struct KvStore {
    backend: Backend,
}

enum Backend {
    /// `{persist_dir}/kv.redb` — ONE physical owner file under
    /// `eg_storage::OwnerLayout::Kv`, served through the two kernels. Boxed: far larger
    /// than the in-memory variant.
    Redb(Box<RedbBackend>),
    /// In-memory ordered map (no persist dir) — ephemeral scratch KV. The single mutex
    /// keeps compare-and-swap genuinely atomic and this path is not perf-critical.
    Memory(Mutex<BTreeMap<(String, String), Vec<u8>>>),
}

/// The kernel-owned durable backend: the storage kernel that owns `kv.redb`, the ONE
/// mutation kernel it issued, the bootstrap scope every cross-namespace read and
/// caller-less write runs on, and the per-namespace scopes bound on first use.
struct RedbBackend {
    kernel: StorageKernel,
    mutations: MutationKernel,
    bootstrap: KvHandle,
    /// Bound serving scopes, keyed by `identity.binding_digest().to_hex()`.
    scopes: Mutex<HashMap<String, KvHandle>>,
}

impl RedbBackend {
    /// Open one private adapter file with its own physical authority name.
    ///
    /// The KV substrate is shared by several adapters, but their files are not
    /// interchangeable stores.  Keeping the physical name at the opener makes
    /// an accidental adoption of (for example) an S3 index as the main KV
    /// store fail at the storage manifest boundary before any scope is bound.
    fn open_named(path: &Path, physical_name: &str) -> Result<Self, String> {
        let (kernel, mutations, bootstrap) = crate::redb_store::shard::open_kernel_owned_store::<
            KvOwner,
        >(
            path, physical_name, &kv_bootstrap_identity()?
        )?;
        Ok(Self {
            kernel,
            mutations,
            bootstrap,
            scopes: Mutex::new(HashMap::new()),
        })
    }

    /// The cross-namespace snapshot `get`/`scan` read: a kernel-issued scoped read on
    /// the bootstrap scope over the one `KV` table, whose composite key IS the partition.
    fn read(&self) -> Result<ScopedRead<'_, KvOwner>, String> {
        self.kernel.read_scope(&self.bootstrap)
    }

    /// One namespace's serving scope: bound on FIRST use, cached for every later call.
    fn scope_handle(&self, identity: &MutationScopeIdentity) -> Result<KvHandle, String> {
        let key = identity.binding_digest().to_hex();
        if let Some(handle) = self.scopes.lock().get(&key) {
            return Ok(Arc::clone(handle));
        }
        let handle = bind_scope(&self.kernel, identity)?;
        self.scopes.lock().insert(key, Arc::clone(&handle));
        Ok(handle)
    }

    /// The ALREADY bound handle for a batch's own scope. Every `*_batch` caller reached
    /// its OCC expectation through `mutation_version`, which binds the scope, so an
    /// unbound identity is a broken precondition — reported, not silently bound.
    fn bound_scope(&self, identity: &MutationScopeIdentity) -> Result<KvHandle, String> {
        self.scopes
            .lock()
            .get(&identity.binding_digest().to_hex())
            .map(Arc::clone)
            .ok_or_else(|| "KV batch scope was never bound by mutation_version".to_string())
    }

    /// One plain KV write as an owner MAINTENANCE mutation (RF-RULING-005) on the
    /// bootstrap scope: no caller identity, but ledgered, fenced and version-bumping
    /// like any other write — no un-ledgered owner-write path exists any more.
    ///
    /// `subject` is the row the write acts on, so `kv_put` on two different keys are
    /// two different durable batches. The version they fence on is resolved INSIDE the
    /// write transaction by `admit_current`: reading it from a snapshot first let two
    /// concurrent `put`s at one observed version build byte-identical batches, and the
    /// second then replayed the first's recorded result — a success ack, over the S3
    /// and Redis wire surfaces, for a write that never happened.
    fn maintain<T, F>(&self, event: &str, subject: &str, apply: F) -> Result<T, String>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce(&AdmittedOwnerWrite<'_, KvOwner>) -> Result<(T, bool), String>,
    {
        let write = MaintenanceBatch::new(DurabilityDomain::KvStore, event, subject);
        let bootstrap = self.bootstrap.as_ref();
        let (txn, batch, begun) = self.mutations.admit_current(bootstrap, |version| {
            write.for_scope_version(bootstrap, version)
        })?;
        let now = crate::server::dispatch::authoritative_now_ms();
        self.complete_write(bootstrap, txn, &batch, begun, now, apply)
    }

    /// One caller-identified `*_batch` write, admitted under the batch's OWN bound
    /// scope. Always commits: a failed `cas_batch` comparison is still terminal and
    /// replayable, and must persist its verdict.
    fn admit_batch<T, F>(&self, batch: &MutationBatch, at_ms: u64, apply: F) -> Result<T, String>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce(&AdmittedOwnerWrite<'_, KvOwner>) -> Result<T, String>,
    {
        let owner = self.bound_scope(&batch.identity)?;
        let owner = owner.as_ref();
        let (txn, begun) = self.mutations.admit(owner, batch)?;
        self.complete_write(owner, txn, batch, begun, at_ms, |owner_write| {
            apply(owner_write).map(|value| (value, true))
        })
    }

    /// Stage an ALREADY admitted batch's owner rows, persist the exact verdict as the
    /// replayable result and commit — ONE transaction, so a KV row and its terminal
    /// MutationBatch metadata can never disagree. `apply`'s `bool` says whether to
    /// commit at all: a `cas` that failed its comparison answers `false` and aborts,
    /// leaving no trace. Shared by both admission classes; only the admission itself
    /// differs, which is why it is the caller's.
    fn complete_write<T, F>(
        &self,
        owner: &OwnedStoreHandle<KvOwner>,
        write: eg_transaction::AdmittedMutation<'_, KvOwner>,
        batch: &MutationBatch,
        begun: Begin,
        at_ms: u64,
        apply: F,
    ) -> Result<T, String>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce(&AdmittedOwnerWrite<'_, KvOwner>) -> Result<(T, bool), String>,
    {
        let source_version = match begun {
            // Terminally committed already: the recorded verdict IS the answer, and
            // re-applying it would double the effect.
            Begin::Replay(record) => {
                let replayed = decode_batch_result(&record)?;
                self.mutations.commit(write, batch)?;
                return Ok(replayed);
            }
            Begin::Apply { source_version } => source_version,
        };
        let owner_write = write.owner_rows(owner, batch)?;
        let staged = apply(&owner_write);
        // Dropping the owner capability unfinished poisons the write; always close it.
        owner_write.finish_owner()?;
        let (value, commit) = match staged {
            Ok(outcome) => outcome,
            Err(error) => {
                write.abort()?;
                return Err(error);
            }
        };
        if !commit {
            return write.abort().map(|()| value);
        }
        let result = rmp_serde::to_vec_named(&value).map_err(|e| e.to_string())?;
        self.mutations
            .finish(&write, batch, Some(result), at_ms, source_version)?;
        self.mutations.commit(write, batch)?;
        Ok(value)
    }
}

impl KvStore {
    /// Open the KV store. `Some(dir)` ⇒ durable `{dir}/kv.redb`; `None` ⇒ in-memory.
    pub fn open(persist_dir: Option<&str>) -> Result<Self, String> {
        Self::open_named(persist_dir, KV_PHYSICAL_STORE)
    }

    /// Open a private adapter KV file with an explicit physical authority name.
    ///
    /// This remains crate-private: public callers use [`Self::open`], while
    /// Redis and S3 each select a distinct manifest identity for their own
    /// subordinate file.  `None` is still process-local memory and therefore
    /// has no physical identity to stamp.
    pub(crate) fn open_named(
        persist_dir: Option<&str>,
        physical_name: &str,
    ) -> Result<Self, String> {
        let backend = match persist_dir {
            Some(dir) => {
                std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
                let path = Path::new(dir).join("kv.redb");
                Backend::Redb(Box::new(RedbBackend::open_named(&path, physical_name)?))
            }
            None => Backend::Memory(Mutex::new(BTreeMap::new())),
        };
        Ok(Self { backend })
    }

    /// `true` if writes land durably on disk.
    pub fn is_durable(&self) -> bool {
        matches!(self.backend, Backend::Redb(_))
    }

    /// Fetch the value bytes for `(namespace, key)`, or `None` if absent.
    pub fn get(&self, namespace: &str, key: &str) -> Result<Option<Vec<u8>>, String> {
        validate_key(namespace, key)?;
        match &self.backend {
            Backend::Redb(store) => {
                let read = store.read()?;
                let table = read.open_owner_table(KV)?;
                table
                    .get((namespace, key))
                    .map_err(|e| e.to_string())?
                    .map(|g| {
                        validate_value(g.value())?;
                        Ok::<Vec<u8>, String>(g.value().to_vec())
                    })
                    .transpose()
            }
            Backend::Memory(m) => {
                let guard = m.lock();
                let value = guard.get(&(namespace.to_string(), key.to_string()));
                if let Some(value) = value {
                    validate_value(value)?;
                }
                Ok(value.cloned())
            }
        }
    }

    /// Store `value` at `(namespace, key)` (overwrite). Durable commit-before-ack, as
    /// ONE ledgered owner-maintenance mutation.
    pub fn put(&self, namespace: &str, key: &str, value: Vec<u8>) -> Result<(), String> {
        validate_key(namespace, key)?;
        validate_value(&value)?;
        match &self.backend {
            Backend::Redb(store) => {
                store.maintain("kv_put", &row_subject(namespace, key), |owner_write| {
                    write_kv_row(owner_write, namespace, key, Some(&value)).map(|_| ((), true))
                })
            }
            Backend::Memory(m) => {
                m.lock()
                    .insert((namespace.to_string(), key.to_string()), value);
                Ok(())
            }
        }
    }

    /// Delete `(namespace, key)`. Returns whether the key existed.
    pub fn delete(&self, namespace: &str, key: &str) -> Result<bool, String> {
        validate_key(namespace, key)?;
        match &self.backend {
            Backend::Redb(store) => {
                store.maintain("kv_delete", &row_subject(namespace, key), |owner_write| {
                    write_kv_row(owner_write, namespace, key, None).map(|existed| (existed, true))
                })
            }
            Backend::Memory(m) => Ok(m
                .lock()
                .remove(&(namespace.to_string(), key.to_string()))
                .is_some()),
        }
    }

    /// Ordered `(key, value)` pairs in `namespace` whose key starts with `prefix` (empty
    /// prefix ⇒ the whole namespace). `limit == 0` uses a safe default; scans stay bounded.
    pub fn scan(
        &self,
        namespace: &str,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<(String, Vec<u8>)>, String> {
        validate_key(namespace, prefix)?;
        let limit = if limit == 0 {
            DEFAULT_KV_SCAN_LIMIT
        } else {
            limit.min(MAX_KV_SCAN_LIMIT)
        };
        let mut out = Vec::new();
        let mut response_bytes = 0usize;
        match &self.backend {
            Backend::Redb(store) => {
                let read = store.read()?;
                let table = read.open_owner_table(KV)?;
                // Range from (namespace, prefix): all prefix matches are a contiguous
                // sorted block right after this bound, so we stop as soon as the
                // namespace changes or a key no longer carries the prefix.
                for entry in table
                    .range((namespace, prefix)..)
                    .map_err(|e| e.to_string())?
                {
                    let (k, v) = entry.map_err(|e| e.to_string())?;
                    let (ns, key) = k.value();
                    if ns != namespace || !key.starts_with(prefix) {
                        break;
                    }
                    if !push_scan_row(&mut out, &mut response_bytes, limit, key, v.value())? {
                        break;
                    }
                }
            }
            Backend::Memory(m) => {
                let guard = m.lock();
                let start = (namespace.to_string(), prefix.to_string());
                for ((ns, key), v) in guard.range(start..) {
                    if ns != namespace || !key.starts_with(prefix) {
                        break;
                    }
                    if !push_scan_row(&mut out, &mut response_bytes, limit, key, v)? {
                        break;
                    }
                }
            }
        }
        Ok(out)
    }

    /// Atomic compare-and-swap: if the CURRENT value equals `expected` (both absent
    /// ⇒ the key must not exist) set it to `new` (`None` ⇒ delete) and return `true`;
    /// otherwise leave it untouched and return `false`. The durable path does the
    /// read+compare+write inside ONE admitted write transaction, so it is atomic against
    /// concurrent writers, and a failed comparison ABORTS that write.
    pub fn cas(
        &self,
        namespace: &str,
        key: &str,
        expected: Option<&[u8]>,
        new: Option<Vec<u8>>,
    ) -> Result<bool, String> {
        validate_key(namespace, key)?;
        if let Some(value) = expected {
            validate_value(value)?;
        }
        if let Some(value) = new.as_deref() {
            validate_value(value)?;
        }
        match &self.backend {
            Backend::Redb(store) => {
                store.maintain("kv_cas", &row_subject(namespace, key), |owner_write| {
                    swap_in_owner_write(owner_write, namespace, key, expected, new.as_deref())
                        .map(|swapped| (swapped, swapped))
                })
            }
            Backend::Memory(m) => {
                let mut guard = m.lock();
                let mk = (namespace.to_string(), key.to_string());
                let current = guard.get(&mk).map(|v| v.as_slice());
                if current == expected {
                    match new {
                        Some(v) => {
                            guard.insert(mk, v);
                        }
                        None => {
                            guard.remove(&mk);
                        }
                    }
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
        }
    }

    /// Current universal MutationBatch version for one namespace scope. Authenticates
    /// the scope against the composition root's verifier and binds it on FIRST use (the
    /// mutation kernel admits no batch for an unbound scope), then serves the cache.
    pub fn mutation_version(&self, tenant: &str, graph: &str) -> Result<u64, String> {
        match &self.backend {
            Backend::Redb(store) => {
                let owner = store.scope_handle(&kv_scope_identity(tenant, graph)?)?;
                eg_transaction::version(&store.kernel.read_scope(&owner)?)
            }
            Backend::Memory(_) => Ok(0),
        }
    }

    /// Atomically write a KV value and its terminal MutationBatch metadata. Precondition
    /// (upheld by every caller — `compile_kv_batch`, below): `batch`'s identity is already
    /// bound, as a side effect of the `mutation_version` call that gave it its OCC
    /// expectation.
    pub fn put_batch(
        &self,
        namespace: &str,
        key: &str,
        value: Vec<u8>,
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<(), String> {
        validate_key(namespace, key)?;
        validate_value(&value)?;
        match &self.backend {
            Backend::Redb(store) => store.admit_batch(batch, committed_at_ms, |owner_write| {
                write_kv_row(owner_write, namespace, key, Some(&value)).map(|_| ())
            }),
            // The ephemeral backend keeps no MutationBatch bookkeeping, so a `*_batch`
            // write IS the plain write.
            Backend::Memory(_) => self.put(namespace, key, value),
        }
    }

    /// Atomically delete a KV value and its terminal MutationBatch metadata.
    /// Same binding precondition as [`KvStore::put_batch`].
    pub fn delete_batch(
        &self,
        namespace: &str,
        key: &str,
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<bool, String> {
        validate_key(namespace, key)?;
        match &self.backend {
            Backend::Redb(store) => store.admit_batch(batch, committed_at_ms, |owner_write| {
                write_kv_row(owner_write, namespace, key, None)
            }),
            // See `put_batch`'s memory arm.
            Backend::Memory(_) => self.delete(namespace, key),
        }
    }

    /// Atomically compare/swap a KV value and persist the exact verdict in the same
    /// MutationBatch transaction. A failed comparison is still a terminal, replayable
    /// request and therefore commits its record. Binding precondition: see `put_batch`.
    pub fn cas_batch(
        &self,
        namespace: &str,
        key: &str,
        expected: Option<&[u8]>,
        new: Option<Vec<u8>>,
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<bool, String> {
        validate_key(namespace, key)?;
        if let Some(value) = expected {
            validate_value(value)?;
        }
        if let Some(value) = new.as_deref() {
            validate_value(value)?;
        }
        match &self.backend {
            Backend::Redb(store) => store.admit_batch(batch, committed_at_ms, |owner_write| {
                swap_in_owner_write(owner_write, namespace, key, expected, new.as_deref())
            }),
            // See `put_batch`'s memory arm.
            Backend::Memory(_) => self.cas(namespace, key, expected, new),
        }
    }
}

fn decode_batch_result<T: serde::de::DeserializeOwned>(
    record: &crate::mutation_batch::MutationBatchRecord,
) -> Result<T, String> {
    let bytes = record
        .result_msgpack
        .as_deref()
        .ok_or_else(|| "committed native MutationBatch has no result".to_string())?;
    eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_KV_BATCH_RESULT_BYTES,
            1_024,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "committed KV result is invalid or exceeds resource limits".to_string())
}

/// Route a `Method::Kv*` op through the KV store on `ServerState`. Mirrors the
/// blob/tsdb self-routing handlers: `Err(method)` for a non-KV method (the caller then
/// falls through), `Ok(Response)` otherwise. Classification (`requires_write`) lives in
/// `server::access` alongside the graph-op classifier.
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    authority: &CarrierAuthority,
    method: Method,
) -> Result<Response, Method> {
    // The store is small + the ops are microsecond-cheap (single-key redb get/put,
    // commit fsync coalesced by redb), so they run inline like the tsdb append path.
    let store = { state.read().await.kv.clone() };
    let store = match store {
        Some(s) => s,
        None => {
            // KV feature compiled but no store on state (should not happen once main
            // wires it) — surface a clear error rather than a mis-route.
            if is_kv_method(&method) {
                return Ok(Response::err(
                    req_id,
                    "KV surface not available (no kv store configured)",
                ));
            }
            return Err(method);
        }
    };

    let original_method = method.clone();
    let resp = match method {
        Method::KvGet { namespace, key } => {
            let namespace = authority.namespace("kv-namespace", &namespace);
            match store.get(&namespace, &key) {
                Ok(Some(v)) => Response::ok(req_id, ResultPayload::Raw(v)),
                Ok(None) => Response::ok(req_id, ResultPayload::Json(serde_json::Value::Null)),
                Err(e) => Response::err(req_id, format!("KvGet error: {e}")),
            }
        }
        Method::KvPut {
            namespace,
            key,
            value,
        } => {
            let namespace = authority.namespace("kv-namespace", &namespace);
            match compile_kv_batch(&store, req_id, authority, &namespace, &original_method)
                .and_then(|(batch, now)| store.put_batch(&namespace, &key, value, &batch, now))
            {
                Ok(()) => Response::ok(req_id, ResultPayload::String("ok".to_string())),
                Err(e) => Response::err(req_id, format!("KvPut error: {e}")),
            }
        }
        Method::KvDelete { namespace, key } => {
            let namespace = authority.namespace("kv-namespace", &namespace);
            match compile_kv_batch(&store, req_id, authority, &namespace, &original_method)
                .and_then(|(batch, now)| store.delete_batch(&namespace, &key, &batch, now))
            {
                Ok(existed) => Response::ok(req_id, ResultPayload::Bool(existed)),
                Err(e) => Response::err(req_id, format!("KvDelete error: {e}")),
            }
        }
        Method::KvScan {
            namespace,
            prefix,
            limit,
        } => {
            let namespace = authority.namespace("kv-namespace", &namespace);
            match store.scan(&namespace, &prefix, limit) {
                Ok(pairs) => {
                    // `[(key, value-bytes)]` straight to MessagePack (value rides as a bin).
                    let wire: Vec<(String, serde_bytes::ByteBuf)> = pairs
                        .into_iter()
                        .map(|(k, v)| (k, serde_bytes::ByteBuf::from(v)))
                        .collect();
                    Response::ok(req_id, ResultPayload::raw(&wire))
                }
                Err(e) => Response::err(req_id, format!("KvScan error: {e}")),
            }
        }
        Method::KvCas {
            namespace,
            key,
            expected,
            new,
        } => {
            let namespace = authority.namespace("kv-namespace", &namespace);
            match compile_kv_batch(&store, req_id, authority, &namespace, &original_method)
                .and_then(|(batch, now)| {
                    store.cas_batch(&namespace, &key, expected.as_deref(), new, &batch, now)
                }) {
                Ok(swapped) => Response::ok(req_id, ResultPayload::Bool(swapped)),
                Err(e) => Response::err(req_id, format!("KvCas error: {e}")),
            }
        }
        other => return Err(other),
    };
    Ok(resp)
}

fn compile_kv_batch(
    store: &KvStore,
    req_id: u64,
    authority: &CarrierAuthority,
    namespace: &str,
    method: &Method,
) -> Result<(MutationBatch, u64), String> {
    if namespace.trim().is_empty() {
        return Err("KV namespace must not be empty".to_string());
    }
    let scope = crate::server::mutation_batch::opaque_coordinator_key(
        "kv-scope",
        authority.owner_scope(),
        namespace,
    );
    // The batch id is part of the canonical outbox key.  It must identify the
    // verified operation across transport retries, while the request id and
    // nonce identify only this attempt.  Derive it from the same authenticated
    // tenant/scope/actor/idempotency tuple the mutation kernel uses for replay;
    // a fresh nonce with the same stable key therefore replays the stored KV
    // verdict instead of creating a second outbox event.
    let batch_id = crate::server::mutation_batch::opaque_idempotency_key_for_context(
        "kv",
        authority.tenant_scope(),
        &scope,
        Some(authority.actor_scope()),
        authority.idempotency_key(),
    );
    let expected = store.mutation_version(authority.tenant_scope(), &scope)?;
    let now = crate::server::dispatch::authoritative_now_ms();
    // CarrierAuthority is the verified request context's handoff into this
    // self-routed surface. Preserve the transport nonce and stable caller key
    // here so KV retries use the kernel's one operation replay authority. The
    // batch id above intentionally has the same stable operation lifetime; the
    // nonce remains the separate attempt-replay guard.
    let batch = crate::server::mutation_batch::compile_opaque_method(
        crate::server::mutation_batch::CompileBatch {
            batch_id: &batch_id,
            request_id: req_id,
            attempt_nonce: authority.attempt_nonce(),
            principal: Some(authority.actor_scope()),
            tenant: authority.tenant_scope(),
            graph: &scope,
            placement_epoch: 0,
            idempotency_key: authority.idempotency_key(),
            expected_graph_version: Some(expected),
            fencing_token: None,
            created_at_ms: now,
            default_surface: MutationSurface::Other,
            authoritative_state: None,
        },
        method,
        MutationSurface::Other,
        DurabilityDomain::KvStore,
        "kv_operation",
    )?;
    Ok((batch, now))
}

/// Whether a method is one of the KV ops (used only for the no-store error path).
fn is_kv_method(method: &Method) -> bool {
    matches!(
        method,
        Method::KvGet { .. }
            | Method::KvPut { .. }
            | Method::KvDelete { .. }
            | Method::KvScan { .. }
            | Method::KvCas { .. }
    )
}

/// Bundle the durable namespaced KV surface (CONCEPT:EG-KG.backend.networked-shared-kv) into
/// an online backup.
///
/// `kv.redb` holds real acknowledged user writes (`KvPut`/`KvCas`) and the fleet-shared
/// KV-cache blocks, in its OWN durability domain off the graph shards.
#[cfg(feature = "redb")]
impl crate::server::persistence::durable_stores::BundledStoreSource for KvStore {
    fn file_name(&self) -> &'static str {
        "kv.redb"
    }

    fn copy_into(&self, destination: &std::path::Path) -> Result<u64, String> {
        let Backend::Redb(source) = &self.backend else {
            return Err("KV store is in-memory; nothing to bundle".to_string());
        };
        // Counted off the kernel's own snapshot BEFORE the copy: `backup_recovery_store`
        // copies the declared owner tables itself but reports only ledger counts.
        let rows = source
            .read()?
            .open_owner_table(KV)?
            .len()
            .map_err(|e| e.to_string())?;
        // The KV domain's own MutationBatch bookkeeping lives in the SAME file, so a
        // restored store must keep its idempotency/OCC/fence boundary rather than
        // re-admitting an acknowledged write. The WHOLE copy — ledger plus the layout's
        // declared owner tables — is the storage kernel's, which re-stamps
        // `SCOPE_BINDINGS` for the destination's incarnation and creates the destination
        // file itself. Hand-listing those tables out here produced BUG-PE-054:
        // `VERSIONS` was copied while `STORE_ROOT`/`SCOPE_BINDINGS` were not.
        let counts = eg_storage::backup_recovery_store(&source.kernel, destination)?;
        Ok(rows
            .saturating_add(counts.batches)
            .saturating_add(counts.maintenance_claims)
            .saturating_add(counts.versions)
            .saturating_add(counts.fences)
            .saturating_add(counts.outbox)
            .saturating_add(counts.encrypted_private_payloads))
    }

    fn is_durable(&self) -> bool {
        KvStore::is_durable(self)
    }
}

#[cfg(test)]
mod dispatch_tests;
#[cfg(test)]
mod tests;
