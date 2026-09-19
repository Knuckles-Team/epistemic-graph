//! Per-`Method::Kv*` dispatch bodies for [`super::try_handle`]. Split out of `kv.rs`
//! itself so the parent file's KISS `lines_per_file`/`functions_per_file` budget has
//! room for the store implementation; these are pure request/response glue over the
//! store methods `kv.rs` still owns.

use super::*;

/// Row accumulator shared by [`scan_redb_rows`] and [`scan_memory_rows`]: both walk a
/// sorted range and stop under the same [`push_scan_row`] row-count/response-size cap —
/// only how they obtain each `(key, value)` pair differs.
struct ScanAccumulator {
    rows: Vec<(String, Vec<u8>)>,
    bytes: usize,
}

impl ScanAccumulator {
    fn new() -> Self {
        Self {
            rows: Vec::new(),
            bytes: 0,
        }
    }

    /// Push one row; `false` once `limit` rows are collected and the scan must stop.
    fn push(&mut self, limit: usize, key: &str, value: &[u8]) -> Result<bool, String> {
        push_scan_row(&mut self.rows, &mut self.bytes, limit, key, value)
    }

    fn into_rows(self) -> Vec<(String, Vec<u8>)> {
        self.rows
    }
}

/// The durable-backend half of [`KvStore::scan`]: range from `(namespace, prefix)` —
/// all prefix matches are a contiguous sorted block right after this bound, so this
/// stops as soon as the namespace changes or a key no longer carries the prefix.
pub(super) fn scan_redb_rows(
    store: &RedbBackend,
    namespace: &str,
    prefix: &str,
    limit: usize,
) -> Result<Vec<(String, Vec<u8>)>, String> {
    let mut acc = ScanAccumulator::new();
    let read = store.read()?;
    let table = read.open_owner_table(KV)?;
    for entry in table
        .range((namespace, prefix)..)
        .map_err(|e| e.to_string())?
    {
        let (k, v) = entry.map_err(|e| e.to_string())?;
        let (ns, key) = k.value();
        if ns != namespace || !key.starts_with(prefix) {
            break;
        }
        if !acc.push(limit, key, v.value())? {
            break;
        }
    }
    Ok(acc.into_rows())
}

/// The ephemeral-backend half of [`KvStore::scan`]: same contiguous-range walk over the
/// in-memory ordered map.
pub(super) fn scan_memory_rows(
    m: &Mutex<BTreeMap<(String, String), Vec<u8>>>,
    namespace: &str,
    prefix: &str,
    limit: usize,
) -> Result<Vec<(String, Vec<u8>)>, String> {
    let mut acc = ScanAccumulator::new();
    let guard = m.lock();
    let start = (namespace.to_string(), prefix.to_string());
    for ((ns, key), v) in guard.range(start..) {
        if ns != namespace || !key.starts_with(prefix) {
            break;
        }
        if !acc.push(limit, key, v)? {
            break;
        }
    }
    Ok(acc.into_rows())
}

/// Resolve the KV store off `state`, or the final routing/error outcome when it isn't
/// configured (feature compiled but no store wired — should not happen once main wires
/// it): a KV method gets an explicit error response, any other method falls through the
/// dispatch chain via `Err(method)` (routing convention).
pub(super) fn resolve_kv_store(
    store: Option<Arc<KvStore>>,
    method: &Method,
    req_id: u64,
) -> Result<Arc<KvStore>, Box<Result<Response, Method>>> {
    match store {
        Some(store) => Ok(store),
        None if is_kv_method(method) => Err(Box::new(Ok(Response::err(
            req_id,
            "KV surface not available (no kv store configured)",
        )))),
        None => Err(Box::new(Err(method.clone()))),
    }
}

pub(super) fn handle_kv_get(req_id: u64, store: &KvStore, namespace: &str, key: &str) -> Response {
    match store.get(namespace, key) {
        Ok(value) => Response::ok(
            req_id,
            ResultPayload::of_encoded_or_null::<results::KvGet>(value),
        ),
        Err(e) => Response::err(req_id, format!("KvGet error: {e}")),
    }
}

pub(super) fn handle_kv_put(
    req_id: u64,
    store: &KvStore,
    authority: &CarrierAuthority,
    namespace: &str,
    key: &str,
    value: Vec<u8>,
    original_method: &Method,
) -> Response {
    match compile_kv_batch(store, req_id, authority, namespace, original_method)
        .and_then(|(batch, now)| store.put_batch(namespace, key, value, &batch, now))
    {
        Ok(()) => Response::ok(
            req_id,
            ResultPayload::scalar::<results::KvPut>("ok".to_string()),
        ),
        Err(e) => Response::err(req_id, format!("KvPut error: {e}")),
    }
}

pub(super) fn handle_kv_delete(
    req_id: u64,
    store: &KvStore,
    authority: &CarrierAuthority,
    namespace: &str,
    key: &str,
    original_method: &Method,
) -> Response {
    match compile_kv_batch(store, req_id, authority, namespace, original_method)
        .and_then(|(batch, now)| store.delete_batch(namespace, key, &batch, now))
    {
        Ok(existed) => Response::ok(req_id, ResultPayload::scalar::<results::KvDelete>(existed)),
        Err(e) => Response::err(req_id, format!("KvDelete error: {e}")),
    }
}

pub(super) fn handle_kv_cas(
    req_id: u64,
    store: &KvStore,
    authority: &CarrierAuthority,
    namespace: &str,
    key: &str,
    (expected, new): (Option<Vec<u8>>, Option<Vec<u8>>),
    original_method: &Method,
) -> Response {
    match compile_kv_batch(store, req_id, authority, namespace, original_method).and_then(
        |(batch, now)| store.cas_batch(namespace, key, expected.as_deref(), new, &batch, now),
    ) {
        Ok(swapped) => Response::ok(req_id, ResultPayload::scalar::<results::KvCas>(swapped)),
        Err(e) => Response::err(req_id, format!("KvCas error: {e}")),
    }
}
