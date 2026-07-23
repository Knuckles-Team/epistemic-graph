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
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use tokio::sync::RwLock;

use super::state::ServerState;
use crate::mutation_batch::{MutationBatch, MutationDomain, MutationSurface};
use crate::protocol::{Method, Response, ResultPayload};
use crate::server::access::CarrierAuthority;

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

/// A namespaced key→bytes store. Durable (redb) when a persist dir is configured,
/// else an in-memory ordered map.
pub struct KvStore {
    backend: Backend,
}

enum Backend {
    /// `{persist_dir}/kv.redb` — durable, commit-before-ack.
    Redb(Database),
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
                let db = Database::create(&path).map_err(|e| e.to_string())?;
                // Ensure the table exists so a fresh DB's read path doesn't error on a
                // missing table (identical bootstrap to the other redb stores).
                {
                    let wtx = db.begin_write().map_err(|e| e.to_string())?;
                    wtx.open_table(KV).map_err(|e| e.to_string())?;
                    wtx.commit().map_err(|e| e.to_string())?;
                }
                eg_mutation_store::initialize(&db)?;
                Ok(Self {
                    backend: Backend::Redb(db),
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
            Backend::Redb(db) => {
                let rtx = db.begin_read().map_err(|e| e.to_string())?;
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
            Backend::Redb(db) => {
                let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
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
            Backend::Redb(db) => {
                let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
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
            Backend::Redb(db) => {
                let rtx = db.begin_read().map_err(|e| e.to_string())?;
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
            Backend::Redb(db) => {
                let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
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

    /// Current universal MutationBatch version for one namespace scope.
    pub fn mutation_version(&self, tenant: &str, graph: &str) -> Result<u64, String> {
        match &self.backend {
            Backend::Redb(db) => eg_mutation_store::version(db, tenant, graph),
            Backend::Memory(_) => Ok(0),
        }
    }

    /// Atomically write a KV value and its terminal MutationBatch metadata.
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
            Backend::Redb(db) => {
                let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
                wtx.set_durability(Durability::Immediate)
                    .map_err(|e| e.to_string())?;
                match eg_mutation_store::begin(&wtx, batch)? {
                    eg_mutation_store::Begin::Replay(record) => {
                        decode_batch_result::<()>(&record)?;
                        wtx.abort().map_err(|e| e.to_string())?;
                        Ok(())
                    }
                    eg_mutation_store::Begin::Apply { source_version } => {
                        {
                            let mut table = wtx.open_table(KV).map_err(|e| e.to_string())?;
                            table
                                .insert((namespace, key), value.as_slice())
                                .map_err(|e| e.to_string())?;
                        }
                        let result = rmp_serde::to_vec_named(&()).map_err(|e| e.to_string())?;
                        eg_mutation_store::finish(
                            &wtx,
                            batch,
                            Some(result),
                            committed_at_ms,
                            source_version,
                        )?;
                        eg_mutation_store::commit(wtx, batch)
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
    pub fn delete_batch(
        &self,
        namespace: &str,
        key: &str,
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<bool, String> {
        validate_key(namespace, key)?;
        match &self.backend {
            Backend::Redb(db) => {
                let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
                wtx.set_durability(Durability::Immediate)
                    .map_err(|e| e.to_string())?;
                match eg_mutation_store::begin(&wtx, batch)? {
                    eg_mutation_store::Begin::Replay(record) => {
                        let result = decode_batch_result(&record)?;
                        wtx.abort().map_err(|e| e.to_string())?;
                        Ok(result)
                    }
                    eg_mutation_store::Begin::Apply { source_version } => {
                        let existed = {
                            let mut table = wtx.open_table(KV).map_err(|e| e.to_string())?;
                            let removed =
                                table.remove((namespace, key)).map_err(|e| e.to_string())?;
                            removed.is_some()
                        };
                        let result =
                            rmp_serde::to_vec_named(&existed).map_err(|e| e.to_string())?;
                        eg_mutation_store::finish(
                            &wtx,
                            batch,
                            Some(result),
                            committed_at_ms,
                            source_version,
                        )?;
                        eg_mutation_store::commit(wtx, batch)?;
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
            Backend::Redb(db) => {
                let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
                wtx.set_durability(Durability::Immediate)
                    .map_err(|e| e.to_string())?;
                match eg_mutation_store::begin(&wtx, batch)? {
                    eg_mutation_store::Begin::Replay(record) => {
                        let result = decode_batch_result(&record)?;
                        wtx.abort().map_err(|e| e.to_string())?;
                        Ok(result)
                    }
                    eg_mutation_store::Begin::Apply { source_version } => {
                        let swapped = {
                            let mut table = wtx.open_table(KV).map_err(|e| e.to_string())?;
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
                            &wtx,
                            batch,
                            Some(result),
                            committed_at_ms,
                            source_version,
                        )?;
                        eg_mutation_store::commit(wtx, batch)?;
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
        let d = std::env::temp_dir().join(format!(
            "eg-kv-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&d);
        d
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
    use crate::acl::{AgentIdentity, AgentRole, RequestContextClaims};
    use crate::channels::ChannelManager;
    use crate::isolation::IsolationLayer;
    use crate::protocol::{Method, Request, ResultPayload};
    use crate::registry::GraphRegistry;
    use crate::server::{
        compute_verified_envelope_token, dispatch, ServerState, VerifiedEnvelopeParams,
    };
    use dashmap::DashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::{RwLock, Semaphore};

    const SECRET: &str = "kv-test-secret";
    const TEST_AGENT: &str = "unit-test-agent";
    static NONCE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

    fn current_isolation() -> IsolationLayer {
        let mut isolation = IsolationLayer::new();
        isolation.register_agent(AgentIdentity {
            agent_id: TEST_AGENT.to_string(),
            role: AgentRole::System,
            teams: Vec::new(),
            roles: Vec::new(),
        });
        isolation
    }

    fn state_with_kv(dir: &str) -> Arc<RwLock<ServerState>> {
        let kv = Arc::new(super::KvStore::open(Some(dir)).unwrap());
        Arc::new(RwLock::new(ServerState {
            #[cfg(feature = "redb")]
            cold_tracker: std::sync::Arc::new(
                crate::server::persistence::cold_offload::ColdTenantTracker::new(),
            ),
            registry: GraphRegistry::new(),
            isolation: current_isolation(),
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: Some(dir.to_string()),
            persistence: None,
            max_in_flight: Arc::new(Semaphore::new(16)),
            read_admission: Arc::new(Semaphore::new(16)),
            per_graph_inflight: Arc::new(DashMap::new()),
            per_graph_inflight_limit: 8,
            write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::new()),
            open_txns: Arc::new(DashMap::new()),
            txn_id_gen: Arc::new(crate::server::txn::TxnIdGen),
            txn_ttl_secs: 300,
            txn_max_per_graph: 256,
            txn_max_per_agent: 256,
            #[cfg(feature = "blob")]
            blob: None,
            #[cfg(feature = "blob")]
            blob_cursor_ttl_secs: 300,
            #[cfg(feature = "raft")]
            raft: None,
            #[cfg(feature = "raft")]
            multi_raft: None,
            #[cfg(feature = "tsdb")]
            tsdb_store: None,
            #[cfg(feature = "streaming")]
            cdc: Some(Arc::new(crate::server::cdc::CdcHub::new())),
            #[cfg(feature = "wasm-udf")]
            udf_registry: Arc::new(eg_wasm::UdfRegistry::new()),
            #[cfg(feature = "compute-dist")]
            matviews: Arc::new(parking_lot::Mutex::new(
                crate::raft::pregel::MatViewStore::new(),
            )),
            #[cfg(feature = "federation")]
            foreign_sources: Arc::new(DashMap::new()),
            #[cfg(feature = "kv")]
            kv: Some(kv),
            #[cfg(feature = "lake")]
            lake: std::sync::Arc::new(crate::server::lake::LakeManager::new()),
        }))
    }

    fn req(id: u64, method: Method) -> Request {
        std::env::set_var("EPISTEMIC_GRAPH_AUDIENCE", "epistemic-graph-test");
        std::env::set_var("EPISTEMIC_GRAPH_TENANT", "tenant-shared");
        std::env::set_var("EPISTEMIC_GRAPH_POLICY_VERSION", "policy-test");
        std::env::set_var(
            "EPISTEMIC_GRAPH_SECURITY_STATE_DIR",
            std::env::temp_dir().join(format!("epistemic-graph-unit-auth-{}", std::process::id())),
        );
        let context = RequestContextClaims {
            principal: TEST_AGENT.to_string(),
            tenant: "tenant-shared".to_string(),
            audience: "epistemic-graph-test".to_string(),
            agent_id: TEST_AGENT.to_string(),
            roles: Vec::new(),
            scopes: vec!["*".to_string()],
            policy_version: "policy-test".to_string(),
            delegation: Vec::new(),
        };
        let mut request = Request {
            id,
            graph: "__commons__".into(),
            auth_token: String::new(),
            agent_id: Some(TEST_AGENT.to_string()),
            method,
        };
        let sequence = NONCE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let issued_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the system clock is after the Unix epoch");
        let nonce = format!(
            "kv-{}-{id}-{sequence}-{}",
            std::process::id(),
            issued_at.as_nanos()
        );
        let idempotency_key = format!("kv-request-{id}-{sequence}");
        request.auth_token = compute_verified_envelope_token(
            SECRET,
            &request,
            &VerifiedEnvelopeParams {
                context: &context,
                timestamp: issued_at.as_secs(),
                nonce: &nonce,
                idempotency_key: &idempotency_key,
            },
        );
        request
    }

    #[tokio::test]
    async fn kv_dispatch_put_get_scan_delete_cas() {
        let dir = std::env::temp_dir().join(format!("eg-kv-dispatch-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let state = state_with_kv(&dir.to_string_lossy());

        // KvPut → "ok"
        let r = dispatch(
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
        let r = dispatch(
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
        dispatch(
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
        let r = dispatch(
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
        let r = dispatch(
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
        let r = dispatch(
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
        let r = dispatch(&state, bad).await;
        assert_eq!(r.error.as_deref(), Some("Authentication failed"));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
