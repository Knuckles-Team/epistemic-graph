use std::sync::Arc;

use eg_capabilities::DurabilityDomain;

use crate::graph::GraphCore;
use crate::isolation::IsolationLayer;
use crate::protocol::{GraphType, Method, Response};
use crate::server::persistence::PersistenceBackend;

use super::MutationPlan;

/// Everything [`commit_mutation`] needs beyond the plan + the apply closure.
/// Borrowed, not owned — built fresh per request from already-resolved
/// `dispatch_graph_op` locals (the registry lock is never re-acquired here).
pub struct MutationCtx<'a> {
    pub req_id: u64,
    pub caller: Option<&'a str>,
    /// Authenticated transport nonce; consumed by the mutation envelope's
    /// kernel replay ledger when this context commits a durable write.
    pub attempt_nonce: Option<eg_types::contract::Nonce>,
    /// Stable caller retry identity from the authenticated request envelope.
    /// Unlike `req_id` and `attempt_nonce`, this is identical across attempts.
    pub idempotency_key: &'a str,
    /// Opaque tenant authority derived from the verified request context.
    pub tenant_scope: &'a str,
    pub graph_name: &'a str,
    pub graph_type: GraphType,
    pub owner: Option<&'a str>,
    pub isolation: &'a IsolationLayer,
    pub core: &'a Arc<GraphCore>,
    pub persistence: Option<&'a Arc<dyn PersistenceBackend>>,
    #[cfg(feature = "streaming")]
    pub cdc: Option<&'a Arc<crate::server::cdc::CdcHub>>,
    /// Generation-owned completeness/freshness watermark for this resident core.
    /// The registry replaces or invalidates this handle on lifecycle transitions,
    /// so a stale gateway completion cannot publish into a same-name recreation.
    pub materialization_manifest:
        Option<&'a Arc<std::sync::RwLock<crate::registry::MaterializationManifest>>>,
    /// Per-graph routed-write coalescer registry (CONCEPT:EG-KG.sharding.per-graph-write-coalescer,
    /// L18 rewrite). When present, a coalescable routed mutation
    /// (`AddNode`/`RemoveNode`/`AddEdge`/`RemoveEdge`/`CompareAndSetNodeFields`), routed through
    /// [`commit_coalescable_mutation`] rather than [`commit_mutation`], has its
    /// WHOLE prepare→durable-commit→RAM-publish sequence queued onto this
    /// graph's single worker, which runs a flushed batch's sequences
    /// back-to-back inside ONE `lock_graph` acquisition instead of one per op
    /// — see `server::routed_write_coalescer` and
    /// `commit_coalescable_mutation`'s doc for the invariant this preserves.
    /// `None` ⇒ [`commit_coalescable_mutation`] falls back to the ordinary
    /// [`commit_mutation`] single-call path, unchanged.
    pub write_coalescer:
        Option<&'a Arc<crate::server::routed_write_coalescer::RoutedWriteCoalescerRegistry>>,
}

/// Which graph-lifecycle request this is — the `CreateGraph`/`DeleteGraph`
/// sibling of [`MutationCtx`]'s retry identity.
///
/// `action` + `graph` + `principal` + `idempotency_key` are exactly what
/// `mutation_batch::lifecycle_batch_id` hashes into the durable batch id, and
/// `request_id` + `attempt_nonce` are the per-attempt halves the kernel's replay
/// ledger consumes. Neither half addresses a lifecycle commit on its own: the
/// durable commit and the replay probe that asks whether that same commit
/// already landed must be told about the identical request, so they are handed
/// one statement of it rather than six positional arguments that can drift
/// apart between the two calls.
///
/// It lives beside [`MutationCtx`] rather than in `mutation_batch::commit`
/// because the lifecycle dispatcher constructs it and the commit lane consumes
/// it, and `mutation_batch`'s submodules are private to that namespace.
pub(crate) struct LifecycleAttempt<'a> {
    /// The lifecycle verb (`"create"` / `"delete"`) the batch id is keyed on.
    pub(crate) action: &'a str,
    /// The durable graph name, NOT the sanitized on-disk file name.
    pub(crate) graph: &'a str,
    pub(crate) request_id: u64,
    /// Authenticated per-attempt transport nonce; see [`MutationCtx`].
    pub(crate) attempt_nonce: Option<eg_types::contract::Nonce>,
    pub(crate) principal: Option<&'a str>,
    /// Stable across attempts, unlike `request_id`/`attempt_nonce`.
    pub(crate) idempotency_key: &'a str,
}

/// Publish the resident freshness watermark only after an authoritative gateway
/// success. The manifest owns monotonic/out-of-order and lifecycle-phase fencing;
/// this gateway owns the exact durable-commit boundary that makes the version
/// authoritative.
pub(super) fn advance_authoritative_manifest(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    response: &Response,
) {
    if response.error.is_some()
        || !plan.mutates
        || matches!(plan.durability_domain, DurabilityDomain::None)
    {
        return;
    }
    let Some(manifest) = ctx.materialization_manifest else {
        return;
    };
    if let Ok(mut manifest) = manifest.write() {
        manifest.advance_committed_version(ctx.core.version());
    }
}

/// Bounded process-global idempotency-replay cache (CONCEPT:EG-P0-2). Keyed by a
/// deterministic `(method, graph, identity-args)` string (see [`idempotency_key`]).
///
/// Bounded, not TTL/LRU, for this increment: past [`MAX_IDEMPOTENCY_ENTRIES`] a new
/// key is simply not cached (fails OPEN — the mutation still applies correctly, it
/// just stops being replay-dedup-eligible until the process restarts or an entry
/// frees up) rather than evicting an arbitrary entry and risking a wrong cache hit
/// for a DIFFERENT request that happens to reuse a key. A real bounded LRU/TTL
/// policy is outside the graph gateway because its native coordinator owns it.
pub struct IdempotencyStore {
    seen: dashmap::DashMap<String, Response>,
}

/// Cap on live idempotency-cache entries (see [`IdempotencyStore`] docs).
const MAX_IDEMPOTENCY_ENTRIES: usize = 10_000;

impl IdempotencyStore {
    pub fn new() -> Self {
        IdempotencyStore {
            seen: dashmap::DashMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Option<Response> {
        self.seen.get(key).map(|r| r.clone())
    }

    pub fn insert(&self, key: String, response: Response) {
        if self.seen.len() < MAX_IDEMPOTENCY_ENTRIES {
            self.seen.insert(key, response);
        }
    }

    #[cfg(test)]
    pub fn clear(&self) {
        self.seen.clear();
    }
}

impl Default for IdempotencyStore {
    fn default() -> Self {
        Self::new()
    }
}

/// The process-global idempotency cache. A `OnceLock` singleton (matching the
/// `max_response_nodes()` precedent in `state.rs`) rather than a new `ServerState`
/// field, so wiring this gateway in touches none of the many `ServerState`
/// construction sites across the codebase.
pub(super) fn idempotency_store() -> &'static IdempotencyStore {
    static STORE: std::sync::OnceLock<IdempotencyStore> = std::sync::OnceLock::new();
    STORE.get_or_init(IdempotencyStore::new)
}

/// Derive a deterministic replay-dedup key for an idempotent method, scoped to
/// `graph_name` (so the SAME node id in two different graphs never collides). Only
/// meaningful when `MutationPlan::idempotent` is true; the fallback `Debug`-based
/// arm covers a future idempotent addition to [`GATEWAY_ROUTED`] generically
/// (correct but not as precise as a bespoke key).
pub(super) fn idempotency_key(graph_name: &str, method: &Method, req_id: u64) -> String {
    match method {
        // `ClearGraph`/`ClearLedger` carry NO distinguishing fields, so the
        // generic `{other:?}|{graph_name}` key below is IDENTICAL across every
        // call ever made against a graph, for the life of the process. Found
        // via a hang repro (two `ClearGraph` calls, the second never
        // returning -- fixed separately by re-stamping a cache hit's `id`,
        // see `commit_prepare`): unlike `RemoveNode`/`RemoveEdge` (whose keys
        // are scoped to the target id, so a repeat on a DIFFERENT id never
        // collides), a content-free method's "idempotent" policy flag only
        // means repeated application converges to the same state -- it does
        // NOT mean a later call is safe to skip and answer from a stale
        // cached result, because a graph that was cleared and then written to
        // again genuinely needs the SECOND `ClearGraph` to run. Folding
        // `req_id` in makes every call its own cache entry (this client's
        // ids are connection-scoped monotonic, never reused for a retry), so
        // the op always re-executes -- the correct semantics for a command
        // whose effect depends on the CURRENT graph state, not just its own
        // (empty) input.
        Method::ClearGraph => format!("ClearGraph|{graph_name}|{req_id}"),
        Method::ClearLedger => format!("ClearLedger|{graph_name}|{req_id}"),
        // `PublishIdempotent`'s policy (`eg_capabilities::policy`) marks it
        // `idempotent: true` because its OWN handler already de-duplicates by
        // `(producer_id, seq)` -- "replays are idempotent by construction", per
        // that policy's own comment. But the generic fallback key below is
        // content-addressed on the method's full Debug repr, so a second call
        // with the SAME `(exchange, routing_key, payload, producer_id, seq,
        // now_ms)` -- the exact scenario `PublishIdempotent` exists to detect
        // -- produces the IDENTICAL dedup key and gets the FIRST call's cached
        // `Response` replayed verbatim, including its `duplicate: false`. The
        // caller can never observe the handler's own correct `duplicate: true`
        // answer; the outer cache masks it before the handler runs a second
        // time. Same root cause as `ClearGraph`/`ClearLedger` above (a later
        // call's CORRECT response genuinely differs from the first's, so it is
        // unsound to skip re-executing it), same fix: fold `req_id` in so
        // every call reaches the handler, whose own producer/seq bookkeeping
        // is the actual source of truth for `duplicate`.
        Method::PublishIdempotent { .. } => {
            format!("PublishIdempotent|{graph_name}|{req_id}")
        }
        Method::RemoveNode { node_id } => format!("RemoveNode|{graph_name}|{node_id}"),
        Method::RemoveEdge {
            source_id,
            target_id,
        } => format!("RemoveEdge|{graph_name}|{source_id}|{target_id}"),
        #[cfg(feature = "modality-serving")]
        Method::ServedModality {
            op:
                eg_types::ServedModalityOp::Ingest {
                    modality,
                    idempotency_ref,
                    target_occurrence_id,
                    expected_version,
                    bundle_msgpack,
                    source_bytes,
                },
        } => served_modality_ingest_key(
            graph_name,
            modality,
            idempotency_ref,
            target_occurrence_id,
            *expected_version,
            bundle_msgpack,
            source_bytes,
        ),
        #[cfg(feature = "modality-serving")]
        Method::ServedModality {
            op:
                eg_types::ServedModalityOp::Delete {
                    modality,
                    idempotency_ref,
                    occurrence_id,
                    expected_version,
                },
        } => format!(
            "ServedModalityDelete|{graph_name}|{}|{idempotency_ref}|{occurrence_id}|{expected_version}",
            modality_discriminator(modality)
        ),
        #[cfg(feature = "modality-serving")]
        Method::ServedModality {
            op: eg_types::ServedModalityOp::IngestStream { modality, items },
        } => served_modality_ingest_stream_key(graph_name, modality, items),
        other => format!("{other:?}|{graph_name}"),
    }
}

/// The `modality`/discriminator half of every `ServedModality` idempotency key.
#[cfg(feature = "modality-serving")]
fn modality_discriminator(value: &eg_types::ServedModalityKind) -> &'static str {
    match value {
        eg_types::ServedModalityKind::Document => "document",
        eg_types::ServedModalityKind::Image => "image",
        eg_types::ServedModalityKind::Audio => "audio",
        eg_types::ServedModalityKind::Video => "video",
    }
}

/// The `ServedModalityOp::Ingest` arm of [`idempotency_key`]: content-addressed on
/// every field that determines the write's effect.
#[cfg(feature = "modality-serving")]
#[allow(clippy::too_many_arguments)]
fn served_modality_ingest_key(
    graph_name: &str,
    modality: &eg_types::ServedModalityKind,
    idempotency_ref: &str,
    target_occurrence_id: &str,
    expected_version: Option<u64>,
    bundle_msgpack: &[u8],
    source_bytes: &[u8],
) -> String {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"served-modality-idempotency-v2");
    digest.update(modality_discriminator(modality).as_bytes());
    for value in [idempotency_ref.as_bytes(), target_occurrence_id.as_bytes()] {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value);
    }
    digest.update(expected_version.unwrap_or(u64::MAX).to_be_bytes());
    digest.update(Sha256::digest(bundle_msgpack));
    digest.update(Sha256::digest(source_bytes));
    format!(
        "ServedModality|{graph_name}|{}",
        hex::encode(digest.finalize())
    )
}

/// The `ServedModalityOp::IngestStream` arm of [`idempotency_key`]: content-addressed
/// over every item in the bounded stream, in order.
#[cfg(feature = "modality-serving")]
fn served_modality_ingest_stream_key(
    graph_name: &str,
    modality: &eg_types::ServedModalityKind,
    items: &[eg_types::ServedModalityIngestItem],
) -> String {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"served-modality-stream-idempotency-v2");
    digest.update(modality_discriminator(modality).as_bytes());
    digest.update((items.len() as u64).to_be_bytes());
    for item in items {
        for value in [
            item.idempotency_ref.as_bytes(),
            item.target_occurrence_id.as_bytes(),
        ] {
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value);
        }
        digest.update(item.expected_version.unwrap_or(u64::MAX).to_be_bytes());
        digest.update(Sha256::digest(&item.bundle_msgpack));
        digest.update(Sha256::digest(&item.source_bytes));
    }
    format!(
        "ServedModalityStream|{graph_name}|{}",
        hex::encode(digest.finalize())
    )
}

/// Source bodies belong only to the live native decoder. State-backed batches
/// persist a digest of categorical operation data plus already-opaque references;
/// neither source bytes nor their direct content hash enter the receipt. The
/// complete effect is independently authenticated by `MutationStateDescriptor`
/// and the encrypted affected-row payload.
pub(crate) fn durable_receipt_method(method: &Method) -> Method {
    #[cfg(feature = "modality-serving")]
    if let Method::ServedModality { op } = method {
        if let Some(digest) = served_modality_receipt_digest(op) {
            return Method::ApplyMutation {
                event_type: "served_modality_v1".to_string(),
                query: format!("sha256:{digest}"),
            };
        }
    }
    method.clone()
}

#[cfg(feature = "modality-serving")]
fn served_modality_receipt_digest(op: &eg_types::ServedModalityOp) -> Option<String> {
    use sha2::{Digest, Sha256};

    let mut digest = Sha256::new();
    modality_receipt_field(&mut digest, b"served-modality-receipt-v1");
    if !append_served_modality_receipt_fields(&mut digest, op) {
        return None;
    }
    Some(hex::encode(digest.finalize()))
}

#[cfg(feature = "modality-serving")]
fn append_served_modality_receipt_fields(
    digest: &mut sha2::Sha256,
    op: &eg_types::ServedModalityOp,
) -> bool {
    use eg_types::ServedModalityOp;

    match op {
        ServedModalityOp::Ingest {
            modality,
            idempotency_ref,
            target_occurrence_id,
            expected_version,
            ..
        } => append_ingest_receipt_fields(
            digest,
            modality,
            idempotency_ref,
            target_occurrence_id,
            *expected_version,
        ),
        ServedModalityOp::IngestStream { modality, items } => {
            append_ingest_stream_receipt_fields(digest, modality, items)
        }
        ServedModalityOp::Delete {
            modality,
            idempotency_ref,
            occurrence_id,
            expected_version,
        } => append_delete_receipt_fields(
            digest,
            modality,
            idempotency_ref,
            occurrence_id,
            *expected_version,
        ),
        ServedModalityOp::MoveToCold {
            modality,
            occurrence_id,
        } => append_occurrence_receipt_fields(digest, b"move-to-cold", modality, occurrence_id),
        ServedModalityOp::Restore {
            modality,
            occurrence_id,
        } => append_occurrence_receipt_fields(digest, b"restore", modality, occurrence_id),
        ServedModalityOp::CollectTombstones {
            modality,
            through_event_sequence,
        } => append_tombstone_receipt_fields(digest, modality, *through_event_sequence),
        _ => false,
    }
}

#[cfg(feature = "modality-serving")]
fn modality_receipt_field(digest: &mut sha2::Sha256, value: &[u8]) {
    use sha2::Digest;

    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value);
}

#[cfg(feature = "modality-serving")]
fn append_modality_receipt_kind(
    digest: &mut sha2::Sha256,
    modality: &eg_types::ServedModalityKind,
) {
    modality_receipt_field(digest, modality_discriminator(modality).as_bytes());
}

#[cfg(feature = "modality-serving")]
fn append_ingest_receipt_fields(
    digest: &mut sha2::Sha256,
    modality: &eg_types::ServedModalityKind,
    idempotency_ref: &str,
    target_occurrence_id: &str,
    expected_version: Option<u64>,
) -> bool {
    modality_receipt_field(digest, b"ingest");
    append_modality_receipt_kind(digest, modality);
    modality_receipt_field(digest, idempotency_ref.as_bytes());
    modality_receipt_field(digest, target_occurrence_id.as_bytes());
    modality_receipt_field(digest, &expected_version.unwrap_or(u64::MAX).to_be_bytes());
    true
}

#[cfg(feature = "modality-serving")]
fn append_ingest_stream_receipt_fields(
    digest: &mut sha2::Sha256,
    modality: &eg_types::ServedModalityKind,
    items: &[eg_types::ServedModalityIngestItem],
) -> bool {
    modality_receipt_field(digest, b"ingest-stream");
    append_modality_receipt_kind(digest, modality);
    modality_receipt_field(digest, &(items.len() as u64).to_be_bytes());
    for item in items {
        modality_receipt_field(digest, item.idempotency_ref.as_bytes());
        modality_receipt_field(digest, item.target_occurrence_id.as_bytes());
        modality_receipt_field(
            digest,
            &item.expected_version.unwrap_or(u64::MAX).to_be_bytes(),
        );
    }
    true
}

#[cfg(feature = "modality-serving")]
fn append_delete_receipt_fields(
    digest: &mut sha2::Sha256,
    modality: &eg_types::ServedModalityKind,
    idempotency_ref: &str,
    occurrence_id: &str,
    expected_version: u64,
) -> bool {
    modality_receipt_field(digest, b"delete");
    append_modality_receipt_kind(digest, modality);
    modality_receipt_field(digest, idempotency_ref.as_bytes());
    modality_receipt_field(digest, occurrence_id.as_bytes());
    modality_receipt_field(digest, &expected_version.to_be_bytes());
    true
}

#[cfg(feature = "modality-serving")]
fn append_occurrence_receipt_fields(
    digest: &mut sha2::Sha256,
    tag: &[u8],
    modality: &eg_types::ServedModalityKind,
    occurrence_id: &str,
) -> bool {
    modality_receipt_field(digest, tag);
    append_modality_receipt_kind(digest, modality);
    modality_receipt_field(digest, occurrence_id.as_bytes());
    true
}

#[cfg(feature = "modality-serving")]
fn append_tombstone_receipt_fields(
    digest: &mut sha2::Sha256,
    modality: &eg_types::ServedModalityKind,
    through_event_sequence: u64,
) -> bool {
    modality_receipt_field(digest, b"collect-tombstones");
    append_modality_receipt_kind(digest, modality);
    modality_receipt_field(digest, &through_event_sequence.to_be_bytes());
    true
}
