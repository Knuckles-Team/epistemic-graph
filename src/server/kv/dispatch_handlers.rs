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

/// One served KV call's verified context.
#[derive(Clone, Copy)]
pub(super) struct KvCall<'a> {
    pub(super) req_id: u64,
    pub(super) store: &'a KvStore,
    pub(super) authority: &'a CarrierAuthority,
    /// The verified request graph; part of every served key's scope.
    pub(super) graph: &'a str,
    /// Whether the method named the tenant+graph shared namespace.
    pub(super) shared: bool,
}

impl KvCall<'_> {
    fn namespace(&self, local: &str) -> String {
        super::wire::wire_namespace(self.authority, self.graph, local, self.shared)
    }

    /// Who owns the kernel scope (and so the version sequence) of this call's
    /// namespace: the tenant for a shared namespace, else tenant+principal.
    fn scope_owner(&self) -> &str {
        if self.shared {
            self.authority.tenant_scope()
        } else {
            self.authority.owner_scope()
        }
    }

    /// Compile, admit and commit one served mutation, then emit its audit event.
    fn mutate<T>(
        &self,
        namespace: &str,
        method: &Method,
        apply: impl FnOnce(&super::wire::WireWrite<'_>) -> Result<T, String>,
    ) -> Result<T, String> {
        let (batch, at_ms, expected) = compile_kv_batch(
            self.store,
            self.req_id,
            self.authority,
            (self.scope_owner(), namespace),
            method,
        )?;
        let write = super::wire::WireWrite {
            batch: &batch,
            at_ms,
            stamp: expected.saturating_add(1),
        };
        let outcome = apply(&write)?;
        if let Some(line) = medulla_kernel::audit::audit_line(method) {
            tracing::info!(
                target: "eg::audit::kv",
                line = %line,
                batch_id = %batch.batch_id,
                tenant = %self.authority.tenant_scope(),
                actor = %self.authority.actor_scope(),
                shared = self.shared,
                "kv mutation committed"
            );
        }
        Ok(outcome)
    }
}

/// Whether a KV method names the tenant+graph shared namespace.
fn names_shared(method: &Method) -> bool {
    matches!(
        method,
        Method::KvGet { shared: true, .. }
            | Method::KvPut { shared: true, .. }
            | Method::KvDelete { shared: true, .. }
            | Method::KvScan { shared: true, .. }
            | Method::KvCas { shared: true, .. }
    )
}

/// Serve one `Method::Kv*` call. The caller has already checked `is_kv_method`.
pub(super) fn handle(call: &KvCall<'_>, method: Method) -> Response {
    let call = &KvCall {
        shared: names_shared(&method),
        ..*call
    };
    let original = method.clone();
    match method {
        Method::KvGet { namespace, key, .. } => kv_get(call, &namespace, &key),
        Method::KvPut {
            namespace,
            key,
            value,
            ..
        } => kv_put(call, &original, (&namespace, &key), value),
        Method::KvDelete { namespace, key, .. } => kv_delete(call, &original, &namespace, &key),
        Method::KvScan {
            namespace,
            prefix,
            limit,
            cursor,
            ..
        } => kv_scan(call, &namespace, &prefix, (cursor.as_deref(), limit)),
        Method::KvCas {
            namespace,
            key,
            expected_version,
            new,
            ..
        } => kv_cas(call, &original, (&namespace, &key), expected_version, new),
        _ => Response::err(call.req_id, "kv dispatch routing error"),
    }
}

fn kv_get(call: &KvCall<'_>, namespace: &str, key: &str) -> Response {
    match call.store.get_versioned(&call.namespace(namespace), key) {
        Ok(entry) => Response::ok(call.req_id, ResultPayload::of_ref::<results::KvGet>(&entry)),
        Err(e) => Response::err(call.req_id, format!("KvGet error: {e}")),
    }
}

fn kv_put(
    call: &KvCall<'_>,
    method: &Method,
    (namespace, key): (&str, &str),
    value: Vec<u8>,
) -> Response {
    let namespace = call.namespace(namespace);
    let put = call.mutate(&namespace, method, |write| {
        call.store
            .write_versioned((&namespace, key), None, Some(value), write)
    });
    match put {
        Ok(outcome) => Response::ok(
            call.req_id,
            ResultPayload::of_ref::<results::KvPut>(&medulla_types::storage_wire::KvWriteReceipt {
                version: outcome.version,
            }),
        ),
        Err(e) => Response::err(call.req_id, format!("KvPut error: {e}")),
    }
}

fn kv_delete(call: &KvCall<'_>, method: &Method, namespace: &str, key: &str) -> Response {
    let namespace = call.namespace(namespace);
    let deleted = call.mutate(&namespace, method, |write| {
        call.store
            .delete_batch(&namespace, key, write.batch, write.at_ms)
    });
    match deleted {
        Ok(existed) => Response::ok(
            call.req_id,
            ResultPayload::scalar::<results::KvDelete>(existed),
        ),
        Err(e) => Response::err(call.req_id, format!("KvDelete error: {e}")),
    }
}

fn kv_scan(
    call: &KvCall<'_>,
    namespace: &str,
    prefix: &str,
    (cursor, limit): (Option<&str>, usize),
) -> Response {
    let namespace = call.namespace(namespace);
    match call.store.scan_versioned(&namespace, prefix, cursor, limit) {
        Ok(page) => Response::ok(call.req_id, ResultPayload::of_ref::<results::KvScan>(&page)),
        Err(e) => Response::err(call.req_id, format!("KvScan error: {e}")),
    }
}

fn kv_cas(
    call: &KvCall<'_>,
    method: &Method,
    (namespace, key): (&str, &str),
    expected_version: u64,
    new: Option<Vec<u8>>,
) -> Response {
    let namespace = call.namespace(namespace);
    let swapped = call.mutate(&namespace, method, |write| {
        call.store
            .write_versioned((&namespace, key), Some(expected_version), new, write)
    });
    match swapped {
        Ok(outcome) => Response::ok(
            call.req_id,
            ResultPayload::of_ref::<results::KvCas>(&outcome),
        ),
        Err(e) => Response::err(call.req_id, format!("KvCas error: {e}")),
    }
}
