//! Generic namespaced Key→Value surface (CONCEPT:EG-KG.storage.namespaced-kv-surface).
//!
//! A drop-in KV store layered over the SAME durable substrate the rest of the
//! engine uses (redb). It is NOT graph-scoped — a KV pair is keyed by
//! `(namespace, key)` and lives entirely off the node/edge graph — so, exactly like
//! the BLOB substrate and the TSDB series store, the `Kv*` methods self-route at the
//! top of dispatch BEFORE the per-graph chain and read their store off
//! [`ServerState`](crate::server::ServerState).
//!
//! ## Durability
//!
//! When a persist dir is configured the store owns `{persist_dir}/kv.redb` and every
//! mutation (`put`/`delete`/`cas`) commits with `redb::Durability::Immediate` —
//! commit-before-ack, the SAME durability barrier the redb-authoritative graph write
//! path gives: a `KvPut` that returned `Ok` survives a `kill -9`. With NO persist dir
//! the store is an in-memory ordered map (a scratch KV), matching the in-memory-only
//! philosophy of the blob substrate (no durable place ⇒ ephemeral).
//!
//! ## Operations (wire `Method::Kv*`)
//!   * `KvGet   { namespace, key }`               → the value bytes, or null if absent
//!   * `KvPut   { namespace, key, value }`        → "ok" (durable commit-before-ack)
//!   * `KvDelete{ namespace, key }`               → bool (whether the key existed)
//!   * `KvScan  { namespace, prefix, limit }`     → ordered bounded `[(key, value)]`
//!   * `KvCas   { namespace, key, expected, new }`→ bool (swapped iff current == expected)

use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;
use redb::{Durability, ReadableDatabase, ReadableTable, TableDefinition};
use tokio::sync::RwLock;

use super::state::ServerState;
use crate::mutation_batch::{MutationBatch, MutationDomain, MutationSurface};
use crate::protocol::{Method, Response, ResultPayload};
use crate::server::access::CarrierAuthority;
use crate::server::mutation_batch::COMPILED_BATCH_INCARNATION;

/// The single KV table: `(namespace, key) -> value bytes`. Composite key so one redb
/// file holds every namespace, and a prefix scan over a namespace is a contiguous
/// range (tuples order lexicographically by namespace then key).
const KV: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("kv");
const MAX_KV_NAMESPACE_BYTES: usize = 256;
const MAX_KV_KEY_BYTES: usize = 4 * 1024;
const MAX_KV_VALUE_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_KV_SCAN_LIMIT: usize = 10_000;
const MAX_KV_SCAN_LIMIT: usize = 100_000;
const MAX_KV_BATCH_RESULT_BYTES: usize = 1024 * 1024;

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

/// Fixed store-private scope used ONLY to bootstrap the physical `kv.redb` root and
/// materialize the `KV` table on first open (CONCEPT:EG-KG.storage.namespaced-kv-surface,
/// MutationBatch v1 store-ownership migration — see `crates/eg-core/src/rbac_persist.rs`
/// for the worked reference this mirrors). Unlike `RbacStore`, `KvStore` serves
/// arbitrarily many DYNAMIC namespaces out of one physical file rather than a single
/// fixed scope, so this bootstrap identity is never used for a real write — every real
/// namespace gets its own identity, built by `kv_scope_identity` below and bound lazily
/// on first use via `mutation_version`. The resource name is fixed human-readable text
/// and can never collide with a real namespace's resource, which is always the opaque
/// `"kv-scope:<sha256-hex>"` form `opaque_coordinator_key` mints in `compile_kv_batch`.
const KV_BOOTSTRAP_TENANT: &str = "kv-store";
const KV_BOOTSTRAP_RESOURCE: &str = "kv-store-bootstrap";

fn kv_bootstrap_identity() -> Result<eg_types::MutationScopeIdentity, String> {
    kv_scope_identity(KV_BOOTSTRAP_TENANT, KV_BOOTSTRAP_RESOURCE)
}

/// Build the native KV mutation-scope identity for one (tenant, resource) pair.
/// `resource` must be EXACTLY the string `compile_kv_batch` passes as
/// `CompileBatch::graph` for the same write: `crate::server::mutation_batch::
/// finish_batch` builds the batch's authoritative `identity` from that identical
/// (tenant, graph) pair, the SAME `MutationDomain::KvStore` domain (the first — and
/// only — operation `compile_opaque_method` compiles for a KV write), and the SAME
/// `COMPILED_BATCH_INCARNATION` constant. `eg_mutation_store`'s scope-binding
/// validator rejects any mismatch (`binding.identity != *identity`), so drifting
/// from any one of those three fields here would make every KV write on that
/// namespace fail closed with "mutation scope binding identity mismatch".
fn kv_scope_identity(tenant: &str, resource: &str) -> Result<eg_types::MutationScopeIdentity, String> {
    let tenant = eg_types::TenantId::new(tenant)?;
    let resource = eg_types::LogicalName::new(resource)?;
    let incarnation_id = eg_types::IncarnationId::new(COMPILED_BATCH_INCARNATION)
        .map_err(|e| format!("invalid KV scope incarnation id: {e}"))?;
    eg_types::MutationScopeIdentity::native(tenant, MutationDomain::KvStore, resource, incarnation_id)
}

/// A namespaced key→bytes store. Durable (redb) when a persist dir is configured,
/// else an in-memory ordered map.
pub struct KvStore {
    backend: Backend,
}

enum Backend {
    /// `{persist_dir}/kv.redb` — durable, commit-before-ack. Owns the
    /// `eg_mutation_store::MutationStore` for that physical file rather than a bare
    /// `redb::Database` (MutationBatch v1: `MutationWrite` — and so every durable
    /// commit — can only be minted off a `MutationStore`, never a raw `Database`).
    /// Plain (non-`_batch`) reads/writes still reach the same physical file directly
    /// through `MutationStore::database()`, unchanged from the old `Database` path.
    Redb(eg_mutation_store::MutationStore),
    /// In-memory ordered map (no persist dir) — ephemeral scratch KV. A single mutex
    /// keeps compare-and-swap genuinely atomic; the non-durable path is not perf-
    /// critical so the coarse lock is fine.
    Memory(Mutex<std::collections::BTreeMap<(String, String), Vec<u8>>>),
}

impl KvStore {
    /// Open the KV store. `Some(dir)` ⇒ durable `{dir}/kv.redb`; `None` ⇒ in-memory.
    pub fn open(persist_dir: Option<&str>) -> Result<Self, String> {
        match persist_dir {
            Some(dir) => {
                std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
                let path = Path::new(dir).join("kv.redb");
                let identity = kv_bootstrap_identity()?;
                // `initialize` creates/opens the physical file, establishes/validates
                // its `StoreIncarnation` root, and binds the bootstrap scope at
                // `initial_version: 0`. The bootstrap closure runs only on the FIRST
                // bind (a fresh file) and just materializes the `KV` table so a later
                // `get`/`scan` on a fresh DB never sees a "no such table" error —
                // identical purpose to the old explicit `wtx.open_table(KV)` + commit.
                let mutation_store =
                    eg_mutation_store::initialize(&path, &identity, 0, None, |wtx| {
                        wtx.open_table(KV).map_err(|e| e.to_string())?;
                        Ok(())
                    })?;
                Ok(Self {
                    backend: Backend::Redb(mutation_store),
                })
            }
            None => Ok(Self {
                backend: Backend::Memory(Mutex::new(std::collections::BTreeMap::new())),
            }),
        }
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
                let rtx = store.database().begin_read().map_err(|e| e.to_string())?;
                let table = rtx.open_table(KV).map_err(|e| e.to_string())?;
                let v = table
                    .get((namespace, key))
                    .map_err(|e| e.to_string())?
                    .map(|g| {
                        validate_value(g.value())?;
                        Ok::<Vec<u8>, String>(g.value().to_vec())
                    })
                    .transpose()?;
                Ok(v)
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

    /// Store `value` at `(namespace, key)` (overwrite). Durable commit-before-ack.
    pub fn put(&self, namespace: &str, key: &str, value: Vec<u8>) -> Result<(), String> {
        validate_key(namespace, key)?;
        validate_value(&value)?;
        match &self.backend {
            Backend::Redb(store) => {
                let mut wtx = store.database().begin_write().map_err(|e| e.to_string())?;
                wtx.set_durability(Durability::Immediate)
                    .map_err(|e| e.to_string())?;
                {
                    let mut table = wtx.open_table(KV).map_err(|e| e.to_string())?;
                    table
                        .insert((namespace, key), value.as_slice())
                        .map_err(|e| e.to_string())?;
                }
                wtx.commit().map_err(|e| e.to_string())?;
                Ok(())
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
                let mut wtx = store.database().begin_write().map_err(|e| e.to_string())?;
                wtx.set_durability(Durability::Immediate)
                    .map_err(|e| e.to_string())?;
                let existed = {
                    let mut table = wtx.open_table(KV).map_err(|e| e.to_string())?;
                    // Bind the removed-value guard to a named local so the `?`
                    // temporary is resolved before the block tail; `removed` (the
                    // borrow) then drops before `table`.
                    let removed = table.remove((namespace, key)).map_err(|e| e.to_string())?;
                    removed.is_some()
                };
                wtx.commit().map_err(|e| e.to_string())?;
                Ok(existed)
            }
            Backend::Memory(m) => Ok(m
                .lock()
                .remove(&(namespace.to_string(), key.to_string()))
                .is_some()),
        }
    }

    /// Ordered `(key, value)` pairs in `namespace` whose key starts with `prefix`
    /// (empty prefix ⇒ every key in the namespace). `limit == 0` uses a safe
    /// default; every scan remains bounded.
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
                let rtx = store.database().begin_read().map_err(|e| e.to_string())?;
                let table = rtx.open_table(KV).map_err(|e| e.to_string())?;
                // Range from (namespace, prefix): all prefix matches are a contiguous
                // sorted block right after this bound, so we stop as soon as the
                // namespace changes or a key no longer carries the prefix.
                let iter = table
                    .range((namespace, prefix)..)
                    .map_err(|e| e.to_string())?;
                for entry in iter {
                    let (k, v) = entry.map_err(|e| e.to_string())?;
                    let (ns, key) = k.value();
                    if ns != namespace || !key.starts_with(prefix) {
                        break;
                    }
                    validate_value(v.value())?;
                    response_bytes = response_bytes
                        .checked_add(key.len())
                        .and_then(|total| total.checked_add(v.value().len()))
                        .filter(|total| *total <= MAX_KV_VALUE_BYTES)
                        .ok_or_else(|| "KV scan response exceeds resource limits".to_string())?;
                    out.push((key.to_string(), v.value().to_vec()));
                    if out.len() >= limit {
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
                    validate_value(v)?;
                    response_bytes = response_bytes
                        .checked_add(key.len())
                        .and_then(|total| total.checked_add(v.len()))
                        .filter(|total| *total <= MAX_KV_VALUE_BYTES)
                        .ok_or_else(|| "KV scan response exceeds resource limits".to_string())?;
                    out.push((key.clone(), v.clone()));
                    if out.len() >= limit {
                        break;
                    }
                }
            }
        }
        Ok(out)
    }

    /// Atomic compare-and-swap: if the CURRENT value equals `expected` (both absent
    /// ⇒ the key must not exist) set it to `new` (`None` ⇒ delete) and return `true`;
    /// otherwise leave it untouched and return `false`. The redb path does the
    /// read+compare+write inside ONE durable write transaction, so it is atomic
    /// against concurrent writers.
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
                let mut wtx = store.database().begin_write().map_err(|e| e.to_string())?;
                wtx.set_durability(Durability::Immediate)
                    .map_err(|e| e.to_string())?;
                let swapped = {
                    let mut table = wtx.open_table(KV).map_err(|e| e.to_string())?;
                    let current: Option<Vec<u8>> = table
                        .get((namespace, key))
                        .map_err(|e| e.to_string())?
                        .map(|g| {
                            validate_value(g.value())?;
                            Ok::<Vec<u8>, String>(g.value().to_vec())
                        })
                        .transpose()?;
                    if current.as_deref() == expected {
                        match &new {
                            Some(v) => {
                                table
                                    .insert((namespace, key), v.as_slice())
                                    .map_err(|e| e.to_string())?;
                            }
                            None => {
                                table.remove((namespace, key)).map_err(|e| e.to_string())?;
                            }
                        }
                        true
                    } else {
                        false
                    }
                };
                if swapped {
                    wtx.commit().map_err(|e| e.to_string())?;
                } else {
                    // No-op: abort the (empty) txn rather than commit a durability flush.
                    wtx.abort().map_err(|e| e.to_string())?;
                }
                Ok(swapped)
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

    /// Current universal MutationBatch version for one namespace scope. Binds the
    /// scope's identity on first use — `KvStore` serves arbitrarily many dynamic
    /// namespaces out of one physical file (unlike `RbacStore`'s single fixed
    /// scope), so each namespace must be registered with `eg_mutation_store`
    /// before `version`/`begin` will accept it. `eg_mutation_store::bind_scope` is
    /// idempotent — re-binding the identical identity on every subsequent call is a
    /// cheap no-op, not a re-registration.
    pub fn mutation_version(&self, tenant: &str, graph: &str) -> Result<u64, String> {
        match &self.backend {
            Backend::Redb(store) => {
                let identity = kv_scope_identity(tenant, graph)?;
                eg_mutation_store::bind_scope(store, &identity, 0, |_| Ok(()))?;
                eg_mutation_store::version(store, &identity)
            }
            Backend::Memory(_) => Ok(0),
        }
    }

    /// Atomically write a KV value and its terminal MutationBatch metadata.
    /// Precondition (upheld by every caller — `compile_kv_batch`, below): `batch`'s
    /// identity must already be bound, which happens as a side effect of the prior
    /// `mutation_version` call every write path uses to compute its OCC expectation.
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
            Backend::Redb(store) => {
                // `MutationStore::write()` already opens with `Durability::Immediate`
                // (the same durability the old code set by hand on the raw
                // `WriteTransaction`), so nothing further to set here.
                let write = store.write()?;
                match eg_mutation_store::begin(&write, batch)? {
                    eg_mutation_store::Begin::Replay(record) => {
                        decode_batch_result::<()>(&record)?;
                        // `MutationWrite::abort` is crate-private to
                        // `eg_mutation_store`; returning here without calling
                        // `.commit()` drops `write` un-committed, which redb's own
                        // `Drop for WriteTransaction` aborts automatically (same
                        // reasoning as `crates/eg-core/src/rbac_persist.rs::save`).
                        Ok(())
                    }
                    eg_mutation_store::Begin::Apply { source_version } => {
                        {
                            let mut table = write
                                .owner_rows()
                                .open_table(KV)
                                .map_err(|e| e.to_string())?;
                            table
                                .insert((namespace, key), value.as_slice())
                                .map_err(|e| e.to_string())?;
                        }
                        let result = rmp_serde::to_vec_named(&()).map_err(|e| e.to_string())?;
                        eg_mutation_store::finish(
                            &write,
                            batch,
                            Some(result),
                            committed_at_ms,
                            source_version,
                        )?;
                        eg_mutation_store::commit(write, batch)
                    }
                }
            }
            Backend::Memory(m) => {
                m.lock()
                    .insert((namespace.to_string(), key.to_string()), value);
                Ok(())
            }
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
            Backend::Redb(store) => {
                let write = store.write()?;
                match eg_mutation_store::begin(&write, batch)? {
                    eg_mutation_store::Begin::Replay(record) => {
                        let result = decode_batch_result(&record)?;
                        // See `put_batch`'s Replay arm for why no explicit abort.
                        Ok(result)
                    }
                    eg_mutation_store::Begin::Apply { source_version } => {
                        let existed = {
                            let mut table = write
                                .owner_rows()
                                .open_table(KV)
                                .map_err(|e| e.to_string())?;
                            let removed =
                                table.remove((namespace, key)).map_err(|e| e.to_string())?;
                            removed.is_some()
                        };
                        let result =
                            rmp_serde::to_vec_named(&existed).map_err(|e| e.to_string())?;
                        eg_mutation_store::finish(
                            &write,
                            batch,
                            Some(result),
                            committed_at_ms,
                            source_version,
                        )?;
                        eg_mutation_store::commit(write, batch)?;
                        Ok(existed)
                    }
                }
            }
            Backend::Memory(m) => Ok(m
                .lock()
                .remove(&(namespace.to_string(), key.to_string()))
                .is_some()),
        }
    }

    /// Atomically compare/swap a KV value and persist the exact verdict in the
    /// same MutationBatch transaction. A failed comparison is still a terminal,
    /// replayable request and therefore commits its status/outbox record.
    /// Same binding precondition as [`KvStore::put_batch`].
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
            Backend::Redb(store) => {
                let write = store.write()?;
                match eg_mutation_store::begin(&write, batch)? {
                    eg_mutation_store::Begin::Replay(record) => {
                        let result = decode_batch_result(&record)?;
                        // See `put_batch`'s Replay arm for why no explicit abort.
                        Ok(result)
                    }
                    eg_mutation_store::Begin::Apply { source_version } => {
                        let swapped = {
                            let mut table = write
                                .owner_rows()
                                .open_table(KV)
                                .map_err(|e| e.to_string())?;
                            let current = table
                                .get((namespace, key))
                                .map_err(|e| e.to_string())?
                                .map(|value| {
                                    validate_value(value.value())?;
                                    Ok::<Vec<u8>, String>(value.value().to_vec())
                                })
                                .transpose()?;
                            if current.as_deref() == expected {
                                match &new {
                                    Some(value) => {
                                        table
                                            .insert((namespace, key), value.as_slice())
                                            .map_err(|e| e.to_string())?;
                                    }
                                    None => {
                                        table
                                            .remove((namespace, key))
                                            .map_err(|e| e.to_string())?;
                                    }
                                }
                                true
                            } else {
                                false
                            }
                        };
                        let result =
                            rmp_serde::to_vec_named(&swapped).map_err(|e| e.to_string())?;
                        eg_mutation_store::finish(
                            &write,
                            batch,
                            Some(result),
                            committed_at_ms,
                            source_version,
                        )?;
                        eg_mutation_store::commit(write, batch)?;
                        Ok(swapped)
                    }
                }
            }
            Backend::Memory(m) => {
                let mut guard = m.lock();
                let map_key = (namespace.to_string(), key.to_string());
                if guard.get(&map_key).map(Vec::as_slice) == expected {
                    match new {
                        Some(value) => {
                            guard.insert(map_key, value);
                        }
                        None => {
                            guard.remove(&map_key);
                        }
                    }
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
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
/// blob/tsdb self-routing handlers: returns `Err(method)` for a method that is not a
/// KV op (so the caller falls through), `Ok(Response)` otherwise. KV writes are
/// durable commit-before-ack; classification (`requires_write`) lives in
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
                Ok(Some(v)) => Response::ok(req_id, ResultPayload::PropertiesMsgpack(v)),
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
    let batch_id = crate::server::mutation_batch::opaque_request_key("kv", &scope, req_id, method);
    let expected = store.mutation_version(authority.tenant_scope(), &scope)?;
    let now = crate::server::dispatch::authoritative_now_ms();
    let batch = crate::server::mutation_batch::compile_opaque_method(
        crate::server::mutation_batch::CompileBatch {
            batch_id: &batch_id,
            request_id: req_id,
            principal: Some(authority.actor_scope()),
            tenant: authority.tenant_scope(),
            graph: &scope,
            placement_epoch: 0,
            idempotency_key: &batch_id,
            expected_graph_version: Some(expected),
            fencing_token: None,
            created_at_ms: now,
            default_surface: MutationSurface::Other,
            authoritative_state: None,
        },
        method,
        MutationSurface::Other,
        MutationDomain::KvStore,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_dir(tag: &str) -> std::path::PathBuf {
        crate::test_support::temp_dir("eg-kv", tag)
    }

    /// put → get → scan → delete → cas round-trip over the durable store.
    #[test]
    fn kv_roundtrip_put_get_scan_delete_cas() {
        let dir = tmp_dir("rt");
        let store = KvStore::open(Some(dir.to_str().unwrap())).unwrap();
        assert!(store.is_durable());

        // put → get
        store.put("ns", "a", b"alpha".to_vec()).unwrap();
        store.put("ns", "ab", b"alphabet".to_vec()).unwrap();
        store.put("ns", "b", b"bravo".to_vec()).unwrap();
        store.put("other", "a", b"x".to_vec()).unwrap();
        assert_eq!(
            store.get("ns", "a").unwrap().as_deref(),
            Some(&b"alpha"[..])
        );
        assert_eq!(store.get("ns", "missing").unwrap(), None);

        // scan(prefix) is namespace-bounded + prefix-bounded + ordered.
        let hits = store.scan("ns", "a", 0).unwrap();
        assert_eq!(
            hits,
            vec![
                ("a".to_string(), b"alpha".to_vec()),
                ("ab".to_string(), b"alphabet".to_vec()),
            ],
            "prefix 'a' in 'ns' matches a, ab — not b, not the other namespace"
        );
        // Empty prefix → whole namespace; limit caps.
        assert_eq!(store.scan("ns", "", 2).unwrap().len(), 2);
        assert_eq!(store.scan("ns", "", 0).unwrap().len(), 3);

        // delete
        assert!(store.delete("ns", "a").unwrap());
        assert!(!store.delete("ns", "a").unwrap());
        assert_eq!(store.get("ns", "a").unwrap(), None);

        // cas: wrong expected fails, right expected swaps; absent-expected create.
        assert!(!store
            .cas("ns", "b", Some(b"WRONG"), Some(b"new".to_vec()))
            .unwrap());
        assert_eq!(
            store.get("ns", "b").unwrap().as_deref(),
            Some(&b"bravo"[..])
        );
        assert!(store
            .cas("ns", "b", Some(b"bravo"), Some(b"BRAVO".to_vec()))
            .unwrap());
        assert_eq!(
            store.get("ns", "b").unwrap().as_deref(),
            Some(&b"BRAVO"[..])
        );
        // create-if-absent: expected None on a non-existent key.
        assert!(store.cas("ns", "fresh", None, Some(b"v".to_vec())).unwrap());
        assert_eq!(
            store.get("ns", "fresh").unwrap().as_deref(),
            Some(&b"v"[..])
        );
        // cas-delete: expected current, new None.
        assert!(store.cas("ns", "fresh", Some(b"v"), None).unwrap());
        assert_eq!(store.get("ns", "fresh").unwrap(), None);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A durable put survives a store close + reopen (persistence across reopen).
    #[test]
    fn kv_persists_across_reopen() {
        let dir = tmp_dir("reopen");
        {
            let store = KvStore::open(Some(dir.to_str().unwrap())).unwrap();
            store.put("cfg", "version", b"42".to_vec()).unwrap();
        }
        // Reopen the SAME file — the value is still there.
        let store = KvStore::open(Some(dir.to_str().unwrap())).unwrap();
        assert_eq!(
            store.get("cfg", "version").unwrap().as_deref(),
            Some(&b"42"[..]),
            "durable KV value must survive reopen"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The in-memory backend (no persist dir) honors the same contract (ephemeral).
    #[test]
    fn kv_in_memory_roundtrip() {
        let store = KvStore::open(None).unwrap();
        assert!(!store.is_durable());
        store.put("ns", "k", b"v".to_vec()).unwrap();
        assert_eq!(store.get("ns", "k").unwrap().as_deref(), Some(&b"v"[..]));
        assert!(store
            .cas("ns", "k", Some(b"v"), Some(b"v2".to_vec()))
            .unwrap());
        assert_eq!(store.scan("ns", "", 0).unwrap().len(), 1);
        assert!(store.delete("ns", "k").unwrap());
    }
}

/// Wire-level proof: drive the `Method::Kv*` ops through the SAME `dispatch`
/// entrypoint a real request hits (auth → top-level routing → handler → store),
/// over a `ServerState` carrying a durable KV store.
#[cfg(test)]
mod dispatch_tests {
    use crate::protocol::{Method, Request, ResultPayload};
    use crate::server::{
        auth::{build_shared_test_request, dispatch_test_on_heap as dispatch_on_heap},
        ServerState,
    };
    use std::sync::Arc;
    use tokio::sync::RwLock;

    const SECRET: &str = "kv-test-secret";
    const TEST_AGENT: &str = "unit-test-agent";

    fn state_with_kv(dir: &str) -> Arc<RwLock<ServerState>> {
        let kv = Arc::new(super::KvStore::open(Some(dir)).unwrap());
        let mut state = ServerState::new_for_test(SECRET, ServerState::test_isolation(TEST_AGENT));
        state.persist_dir = Some(dir.to_string());
        #[cfg(feature = "kv")]
        {
            state.kv = Some(kv);
        }
        #[cfg(not(feature = "kv"))]
        let _ = kv;
        Arc::new(RwLock::new(state))
    }

    fn req(id: u64, method: Method) -> Request {
        build_shared_test_request(SECRET, id, "__commons__", TEST_AGENT, method)
    }

    #[tokio::test]
    async fn kv_dispatch_put_get_scan_delete_cas() {
        let dir = std::env::temp_dir().join(format!("eg-kv-dispatch-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let state = state_with_kv(&dir.to_string_lossy());

        // KvPut → "ok"
        let r = dispatch_on_heap(
            &state,
            req(
                1,
                Method::KvPut {
                    namespace: "cfg".into(),
                    key: "k".into(),
                    value: b"v1".to_vec(),
                },
            ),
        )
        .await;
        assert!(
            matches!(r.result, Some(ResultPayload::String(s)) if s == "ok"),
            "{:?}",
            r.error
        );

        // KvGet → the bytes (PropertiesMsgpack carries the opaque value verbatim).
        let r = dispatch_on_heap(
            &state,
            req(
                2,
                Method::KvGet {
                    namespace: "cfg".into(),
                    key: "k".into(),
                },
            ),
        )
        .await;
        match r.result {
            Some(ResultPayload::PropertiesMsgpack(v)) => assert_eq!(v, b"v1"),
            other => panic!("KvGet: {other:?} / {:?}", r.error),
        }

        // KvScan → ordered [(key, value)].
        dispatch_on_heap(
            &state,
            req(
                3,
                Method::KvPut {
                    namespace: "cfg".into(),
                    key: "k2".into(),
                    value: b"v2".to_vec(),
                },
            ),
        )
        .await;
        let r = dispatch_on_heap(
            &state,
            req(
                4,
                Method::KvScan {
                    namespace: "cfg".into(),
                    prefix: "k".into(),
                    limit: 0,
                },
            ),
        )
        .await;
        let pairs: Vec<(String, serde_bytes::ByteBuf)> = match r.result {
            Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
            other => panic!("KvScan: {other:?} / {:?}", r.error),
        };
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0, "k");

        // KvCas → swaps only on match.
        let r = dispatch_on_heap(
            &state,
            req(
                5,
                Method::KvCas {
                    namespace: "cfg".into(),
                    key: "k".into(),
                    expected: Some(b"v1".to_vec()),
                    new: Some(b"V1".to_vec()),
                },
            ),
        )
        .await;
        assert!(matches!(r.result, Some(ResultPayload::Bool(true))));

        // KvDelete → existed.
        let r = dispatch_on_heap(
            &state,
            req(
                6,
                Method::KvDelete {
                    namespace: "cfg".into(),
                    key: "k2".into(),
                },
            ),
        )
        .await;
        assert!(matches!(r.result, Some(ResultPayload::Bool(true))));

        // Bad auth is rejected before routing.
        let mut bad = req(
            7,
            Method::KvGet {
                namespace: "cfg".into(),
                key: "k".into(),
            },
        );
        bad.auth_token = "bogus".into();
        let r = dispatch_on_heap(&state, bad).await;
        assert_eq!(r.error.as_deref(), Some("Authentication failed"));

        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// Bundle the durable namespaced KV surface (CONCEPT:EG-KG.backend.networked-shared-kv) into
/// an online backup.
///
/// `kv.redb` holds real acknowledged user writes (`KvPut`/`KvCas`) and the fleet-shared
/// KV-cache blocks, in its OWN durability domain off the graph shards. It was omitted
/// from every bundle before this, so a restore silently came up with an empty KV surface.
#[cfg(feature = "redb")]
impl crate::server::persistence::durable_stores::BundledStoreSource for KvStore {
    fn file_name(&self) -> &'static str {
        "kv.redb"
    }

    fn copy_into(&self, destination: &std::path::Path) -> Result<u64, String> {
        let Backend::Redb(source) = &self.backend else {
            return Err("KV store is in-memory; nothing to bundle".to_string());
        };
        // The KV domain's own MutationBatch bookkeeping lives in the SAME file, so a
        // restored store must keep its idempotency/OCC/fence boundary rather than
        // re-admitting an already-acknowledged write. That half is delegated to
        // `backup_recovery_store`, which owns the complete table set and re-stamps
        // `SCOPE_BINDINGS` for the destination's own incarnation.
        //
        // Hand-listing those tables here is what produced BUG-PE-054: `VERSIONS` was
        // copied while `STORE_ROOT`/`SCOPE_BINDINGS` were not, so reopening a restored
        // bundle failed with "mutation version row exists without a scope binding".
        // The list cannot be kept correct from outside the crate that defines it.
        let counts = eg_mutation_store::backup_recovery_store(source, destination)
            .map_err(|error| error.to_string())?;

        // `KV` is this store's own table, appended with plain redb: it is not part of
        // the mutation-store contract, and adding a table does not disturb an
        // incarnation bound to (dev, ino).
        let rtx = source.database().begin_read().map_err(|e| e.to_string())?;
        let target = redb::Database::create(destination).map_err(|e| e.to_string())?;
        let mut wtx = target.begin_write().map_err(|e| e.to_string())?;
        wtx.set_durability(Durability::Immediate)
            .map_err(|e| e.to_string())?;
        let mut rows = 0u64;
        crate::copy_bundled_table!(rtx, wtx, rows, KV);
        wtx.commit().map_err(|e| e.to_string())?;
        Ok(rows
            .saturating_add(counts.batches)
            .saturating_add(counts.idempotency)
            .saturating_add(counts.versions)
            .saturating_add(counts.fences)
            .saturating_add(counts.outbox)
            .saturating_add(counts.encrypted_private_payloads))
    }

    fn is_durable(&self) -> bool {
        KvStore::is_durable(self)
    }
}
