//! Server-side compiler and serialization lane for canonical MutationBatch.
//!
//! Public mutation surfaces lower their already-validated [`Method`] values here;
//! persistence owns the atomic commit and handlers publish RAM only afterward.

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::change_envelope::ChangeEnvelope;
use crate::graph::GraphCore;
use crate::mutation_batch::{
    MutationBatch, MutationDomain, MutationOperation, MutationOutboxIntent, MutationRequestContext,
    MutationStateDescriptor, MutationSurface, MUTATION_BATCH_VERSION,
};
use crate::protocol::ResultPayload;
use crate::protocol::{CypherMode, Method};
use crate::server::persistence::PersistenceBackend;

/// Resolve the current graph version without treating RAM as a substitute for a
/// missing durable authority. Version zero is the sole implicit bootstrap state;
/// once RAM has advanced, the durable version row must exist and agree with it.
pub(crate) async fn authoritative_graph_version(
    persistence: &Arc<dyn PersistenceBackend>,
    graph_fname: &str,
    core: &GraphCore,
) -> Result<u64, String> {
    let projected_version = core.version();
    match persistence.read_mutation_graph_version(graph_fname).await? {
        Some(version) if version == projected_version => Ok(version),
        Some(version) => Err(format!(
            "authoritative graph version {version} does not match the serving projection {projected_version}"
        )),
        None if projected_version == 0 => Ok(0),
        None => Err(format!(
            "authoritative graph version is missing while the serving projection is at {projected_version}"
        )),
    }
}

/// Fields known by a mutation surface after authz/placement/OCC planning.
pub(crate) struct CompileBatch<'a> {
    pub batch_id: &'a str,
    pub request_id: u64,
    pub principal: Option<&'a str>,
    pub tenant: &'a str,
    pub graph: &'a str,
    pub placement_epoch: u64,
    pub idempotency_key: &'a str,
    pub expected_graph_version: Option<u64>,
    pub fencing_token: Option<u64>,
    pub created_at_ms: u64,
    pub default_surface: MutationSurface,
    /// Optional authenticated staged material used by runtime-result mutations.
    pub authoritative_state: Option<MutationStateDescriptor>,
}

/// Compile an ordered engine write-set into the universal durable unit.
pub(crate) fn compile_methods(
    ctx: CompileBatch<'_>,
    methods: Vec<Method>,
) -> Result<MutationBatch, String> {
    #[cfg(feature = "epistemic-tms")]
    let reasoning_events = eg_epistemic::ReasoningProjectionWakeup::events_for_methods(&methods);
    let state_backed = ctx.authoritative_state.is_some();
    let operations = methods
        .into_iter()
        .enumerate()
        .map(|(ordinal, method)| {
            let surface = surface_for(&method).unwrap_or(ctx.default_surface);
            let domain = domain_for(&method, surface);
            let method = if state_backed {
                opaque_state_operation(&method)?
            } else {
                lower_canonical_operation(method)
            };
            Ok::<_, String>(MutationOperation {
                ordinal: ordinal as u32,
                surface,
                domain,
                method,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let batch = finish_batch(ctx, operations)?;
    #[cfg(feature = "epistemic-tms")]
    let batch = {
        let mut batch = batch;
        install_reasoning_wakeup(&mut batch, reasoning_events)?;
        batch
    };
    Ok(batch)
}

/// Compile one payload-bearing surface operation as an opaque digest. SQL catalog
/// statements and other independently-staged domains must retain a canonical
/// status/idempotency/outbox record without persisting query text, bound parameters,
/// repository paths, document bodies, or caller identifiers.
pub(crate) fn compile_opaque_method(
    ctx: CompileBatch<'_>,
    method: &Method,
    surface: MutationSurface,
    domain: MutationDomain,
    event_type: &str,
) -> Result<MutationBatch, String> {
    let encoded = rmp_serde::to_vec_named(method).map_err(|e| e.to_string())?;
    use sha2::{Digest, Sha256};
    let operation = MutationOperation {
        ordinal: 0,
        surface,
        domain,
        method: Method::ApplyMutation {
            event_type: event_type.to_string(),
            query: format!("sha256:{}", hex::encode(Sha256::digest(encoded))),
        },
    };
    let batch = finish_batch(ctx, vec![operation])?;
    #[cfg(feature = "epistemic-tms")]
    let batch = {
        let mut batch = batch;
        install_reasoning_wakeup(
            &mut batch,
            vec![eg_epistemic::IncrementalReasoningEvent::InvalidateAll],
        )?;
        batch
    };
    Ok(batch)
}

/// Compile a coordinator operation already represented by a SHA-256 digest.  This
/// is the binding used for encrypted private recovery material: only the digest is
/// retained in the canonical batch/outbox, while ciphertext is stored out-of-line
/// by the coordinator's native transaction.
pub(crate) fn compile_opaque_digest(
    ctx: CompileBatch<'_>,
    digest: &str,
    surface: MutationSurface,
    domain: MutationDomain,
    event_type: &str,
) -> Result<MutationBatch, String> {
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(
            "opaque coordinator digest must be a 64-character SHA-256 hex value".to_string(),
        );
    }
    let operation = MutationOperation {
        ordinal: 0,
        surface,
        domain,
        method: Method::ApplyMutation {
            event_type: event_type.to_string(),
            query: format!("sha256:{}", digest.to_ascii_lowercase()),
        },
    };
    let batch = finish_batch(ctx, vec![operation])?;
    #[cfg(feature = "epistemic-tms")]
    let batch = {
        let mut batch = batch;
        install_reasoning_wakeup(
            &mut batch,
            vec![eg_epistemic::IncrementalReasoningEvent::InvalidateAll],
        )?;
        batch
    };
    Ok(batch)
}

/// Compile a cross-modal coordinator record. A digest-only operation/manifest binds
/// graph methods and vector/blob/time-series payloads into idempotency and outbox
/// identity without copying those potentially sensitive values into coordinator
/// metadata; authoritative rows remain the recovery source.
pub(crate) fn compile_crossmodal(
    ctx: CompileBatch<'_>,
    modality_payload: &[u8],
    graph_method_count: usize,
    vector_count: usize,
    blob_ref_count: usize,
    measurement_count: usize,
) -> Result<MutationBatch, String> {
    use sha2::{Digest, Sha256};
    let digest = hex::encode(Sha256::digest(modality_payload));
    let operation = MutationOperation {
        ordinal: 0,
        surface: MutationSurface::Transaction,
        domain: MutationDomain::CrossModal,
        method: Method::ApplyMutation {
            event_type: "crossmodal_operation".to_string(),
            query: format!("sha256:{digest}"),
        },
    };
    let mut batch = finish_batch(ctx, vec![operation])?;
    #[cfg(feature = "epistemic-tms")]
    install_reasoning_wakeup(
        &mut batch,
        vec![eg_epistemic::IncrementalReasoningEvent::InvalidateAll],
    )?;
    let manifest = serde_json::json!({
        "schema": "epistemic.crossmodal.manifest.v1",
        "payload_sha256": digest,
        "graph_methods": graph_method_count,
        "vectors": vector_count,
        "blob_refs": blob_ref_count,
        "measurements": measurement_count,
    });
    batch.outbox.push(MutationOutboxIntent {
        topic: "engine.crossmodal.committed".to_string(),
        key: batch.batch_id.clone(),
        payload: rmp_serde::to_vec_named(&manifest).map_err(|e| e.to_string())?,
        headers: BTreeMap::new(),
    });
    batch.validate()?;
    Ok(batch)
}

#[cfg(feature = "epistemic-tms")]
fn install_reasoning_wakeup(
    batch: &mut MutationBatch,
    events: Vec<eg_epistemic::IncrementalReasoningEvent>,
) -> Result<(), String> {
    use sha2::{Digest, Sha256};

    let operations =
        rmp_serde::to_vec_named(&batch.operations).map_err(|error| error.to_string())?;
    let wakeup = eg_epistemic::ReasoningProjectionWakeup::new(
        batch.operations.len(),
        hex::encode(Sha256::digest(operations)),
        events,
    )?;
    let payload = rmp_serde::to_vec_named(&wakeup).map_err(|error| error.to_string())?;
    let intent = batch
        .outbox
        .iter_mut()
        .find(|intent| intent.topic == "engine.projection.rebuild")
        .ok_or_else(|| "MutationBatch has no reasoning projection wake-up".to_string())?;
    intent.payload = payload;
    batch.validate()?;
    Ok(())
}

fn finish_batch(
    ctx: CompileBatch<'_>,
    operations: Vec<MutationOperation>,
) -> Result<MutationBatch, String> {
    #[cfg(feature = "raft")]
    let (placement_epoch, fencing_token) =
        crate::server::dispatch::replicated_placement_authority()
            .unwrap_or((ctx.placement_epoch, ctx.fencing_token));
    #[cfg(not(feature = "raft"))]
    let (placement_epoch, fencing_token) = (ctx.placement_epoch, ctx.fencing_token);
    // Projection wake-ups need correlation and integrity, not a second copy of node
    // properties, query text, document bodies, or identifiers. Bind the outbox row to
    // the canonical operation list with a digest-only manifest; the authoritative
    // batch/state remains the recovery source.
    let encoded_operations = rmp_serde::to_vec_named(&operations).map_err(|e| e.to_string())?;
    use sha2::{Digest, Sha256};
    let summary = rmp_serde::to_vec_named(&serde_json::json!({
        "schema": "epistemic.mutation.projection.v1",
        "operations": operations.len(),
        "operations_sha256": hex::encode(Sha256::digest(&encoded_operations)),
    }))
    .map_err(|e| e.to_string())?;
    let mut scope_digest = Sha256::new();
    scope_digest.update(ctx.tenant.as_bytes());
    scope_digest.update([0]);
    scope_digest.update(ctx.graph.as_bytes());
    let scope_digest = hex::encode(scope_digest.finalize());
    let principal =
        principal_fingerprint(ctx.principal.ok_or_else(|| {
            "durable mutation authority requires a verified principal".to_string()
        })?)?;
    let batch = MutationBatch {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: ctx.batch_id.to_string(),
        context: MutationRequestContext {
            request_id: ctx.request_id,
            principal,
            purpose: None,
            policy_fingerprint: None,
            trace_id: None,
        },
        tenant: ctx.tenant.to_string(),
        graph: ctx.graph.to_string(),
        placement_epoch,
        idempotency_key: ctx.idempotency_key.to_string(),
        expected_graph_version: ctx.expected_graph_version,
        fencing_token,
        authoritative_state: ctx.authoritative_state,
        operations,
        outbox: vec![MutationOutboxIntent {
            topic: "engine.projection.rebuild".to_string(),
            key: ctx.batch_id.to_string(),
            payload: summary,
            headers: BTreeMap::from([("scope_sha256".to_string(), scope_digest)]),
        }],
        created_at_ms: ctx.created_at_ms,
    };
    batch.validate()?;
    Ok(batch)
}

/// Durable pseudonym used by every native coordinator retry check.
pub(crate) fn principal_fingerprint(principal: &str) -> Result<String, String> {
    use sha2::{Digest, Sha256};
    let principal = principal.trim();
    if principal.is_empty() {
        return Err("durable mutation authority requires a verified principal".to_string());
    }
    if principal
        .strip_prefix("principal:sha256:")
        .is_some_and(|digest| {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        })
    {
        return Ok(principal.to_string());
    }
    Ok(format!(
        "principal:sha256:{}",
        hex::encode(Sha256::digest(principal.as_bytes()))
    ))
}

/// State-backed commits deliberately retain no caller payload, repository path,
/// query text, document body, or identifiers from the original operation. The
/// authoritative snapshot or row delta is supplied out-of-line and digest-verified; this
/// opaque operation fingerprint is sufficient for idempotency/audit correlation.
fn opaque_state_operation(method: &Method) -> Result<Method, String> {
    use sha2::{Digest, Sha256};
    let encoded = rmp_serde::to_vec_named(method).map_err(|e| e.to_string())?;
    Ok(Method::ApplyMutation {
        event_type: "authoritative_state_operation".to_string(),
        query: format!("sha256:{}", hex::encode(Sha256::digest(encoded))),
    })
}

/// Exhaustive durability-domain classifier for mutating methods accepted by a
/// batch adapter. Public mutation inventory tests ensure no mutating method lacks
/// a domain. Methods that coordinate child batches are explicitly `MultiGraph` or
/// `CrossModal`; they are never mistaken for a local graph-row write.
pub(crate) fn domain_for(method: &Method, surface: MutationSurface) -> MutationDomain {
    match method {
        Method::CreateGraph { .. } | Method::DeleteGraph { .. } => MutationDomain::Lifecycle,
        Method::MultiGraphBatchUpdate { .. } => MutationDomain::MultiGraph,
        Method::Commit { .. } => MutationDomain::CrossModal,
        #[cfg(feature = "blob")]
        Method::BlobBegin { .. }
        | Method::BlobChunkPut { .. }
        | Method::BlobCommit { .. }
        | Method::BlobRef { .. }
        | Method::BlobUnref { .. }
        | Method::BlobGc => MutationDomain::BlobStore,
        #[cfg(feature = "kv")]
        Method::KvPut { .. } | Method::KvDelete { .. } | Method::KvCas { .. } => {
            MutationDomain::KvStore
        }
        #[cfg(feature = "tsdb")]
        Method::TsAppend { .. } => MutationDomain::TimeSeries,
        #[cfg(feature = "jobs")]
        Method::AnalyticsJob { .. } => MutationDomain::AnalyticsJob,
        Method::ClaimWorkItem { .. }
        | Method::RenewWorkItemLease { .. }
        | Method::CommitWorkItemResult { .. }
        | Method::CancelWorkItem { .. }
        | Method::DeferWorkItem { .. } => MutationDomain::ControlPlane,
        #[cfg(feature = "query")]
        Method::Sql { .. } => MutationDomain::SqlCatalog,
        #[cfg(feature = "rdf")]
        Method::AddTriples { .. } | Method::RemoveTriples { .. } | Method::DropNamedGraph => {
            MutationDomain::RdfDataset
        }
        #[cfg(feature = "broker")]
        Method::DeclareExchange { .. }
        | Method::DeleteExchange { .. }
        | Method::BindQueue { .. }
        | Method::UnbindQueue { .. }
        | Method::Publish { .. }
        | Method::DeclareQueue { .. }
        | Method::PublishEx { .. }
        | Method::BrokerConsume { .. }
        | Method::BrokerAck { .. }
        | Method::BrokerReject { .. }
        | Method::SweepExpired { .. }
        | Method::StreamDeclare { .. }
        | Method::StreamPublish { .. }
        | Method::StreamTrim { .. }
        | Method::StreamCommitOffset { .. }
        | Method::PublishConfirmed { .. }
        | Method::PublishIdempotent { .. }
        | Method::BrokerAckTag { .. }
        | Method::BrokerNackTag { .. }
        | Method::BrokerRenewTag { .. } => MutationDomain::Broker,
        _ if matches!(surface, MutationSurface::Transaction) => MutationDomain::GraphRows,
        _ => MutationDomain::GraphSnapshot,
    }
}

/// Safe one-to-one canonical operations lowered before persistence. More complex RDF
/// additions/removals and query-language DML require their parser/planner to emit
/// explicit graph-row methods; they are intentionally not guessed here.
fn lower_canonical_operation(method: Method) -> Method {
    match method {
        #[cfg(feature = "rdf")]
        Method::DropNamedGraph => Method::ClearGraph,
        other => other,
    }
}

/// Query/RDF/lifecycle adapters are classified here, not in persistence.  Their
/// operations still use exactly the same Method payload and commit machinery.
fn surface_for(method: &Method) -> Option<MutationSurface> {
    match method {
        Method::Sql { .. }
        | Method::CypherQuery {
            mode: CypherMode::Write,
            ..
        } => Some(MutationSurface::Query),
        #[cfg(feature = "graphql")]
        Method::GraphQl { .. } => Some(MutationSurface::Query),
        #[cfg(feature = "rdf")]
        Method::AddTriples { .. } | Method::RemoveTriples { .. } | Method::DropNamedGraph => {
            Some(MutationSurface::Rdf)
        }
        Method::CreateGraph { .. } | Method::DeleteGraph { .. } => Some(MutationSurface::Lifecycle),
        #[cfg(feature = "jobs")]
        Method::AnalyticsJob { .. } => Some(MutationSurface::Job),
        #[cfg(feature = "broker")]
        Method::DeclareExchange { .. }
        | Method::DeleteExchange { .. }
        | Method::BindQueue { .. }
        | Method::UnbindQueue { .. }
        | Method::Publish { .. }
        | Method::DeclareQueue { .. }
        | Method::PublishEx { .. }
        | Method::BrokerConsume { .. }
        | Method::BrokerAck { .. }
        | Method::BrokerReject { .. }
        | Method::SweepExpired { .. }
        | Method::StreamDeclare { .. }
        | Method::StreamPublish { .. }
        | Method::StreamTrim { .. }
        | Method::StreamCommitOffset { .. }
        | Method::PublishConfirmed { .. }
        | Method::PublishIdempotent { .. }
        | Method::BrokerAckTag { .. }
        | Method::BrokerNackTag { .. }
        | Method::BrokerRenewTag { .. } => Some(MutationSurface::Broker),
        _ => None,
    }
}

/// One deterministic async serialization lane for each logical graph. Transaction
/// Commit and the ordinary mutation gateway both acquire it, so OCC validation
/// cannot race a gateway write while its durable-before-RAM batch is in flight. A
/// fixed number of deterministic stripes bounds memory across create/delete churn; a hash
/// collision only serializes two unrelated graphs and cannot weaken correctness.
pub(crate) async fn lock_graph(graph: &str) -> OwnedMutexGuard<()> {
    const STRIPES: usize = 1024;
    static LOCKS: OnceLock<Vec<Arc<Mutex<()>>>> = OnceLock::new();
    let locks = LOCKS.get_or_init(|| (0..STRIPES).map(|_| Arc::new(Mutex::new(()))).collect());
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in graph.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let lock = Arc::clone(&locks[(hash as usize) % STRIPES]);
    lock.lock_owned().await
}

/// Stable lifecycle batch id used both before the first commit and by a retry
/// reconciling a crash after durability but before registry publication.
pub(crate) fn lifecycle_batch_id(action: &str, graph: &str, request_id: u64) -> String {
    use sha2::{Digest, Sha256};
    let material = format!("{action}\0{graph}\0{request_id}");
    format!(
        "lifecycle:{}",
        hex::encode(Sha256::digest(material.as_bytes()))
    )
}

pub(crate) fn is_work_item_method(method: &Method) -> bool {
    matches!(
        method,
        Method::ClaimWorkItem { .. }
            | Method::RenewWorkItemLease { .. }
            | Method::CommitWorkItemResult { .. }
            | Method::CancelWorkItem { .. }
            | Method::DeferWorkItem { .. }
    )
}

pub(crate) fn opaque_request_key(
    namespace: &str,
    graph: &str,
    request_id: u64,
    method: &Method,
) -> String {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(namespace.as_bytes());
    digest.update([0]);
    digest.update(graph.as_bytes());
    digest.update([0]);
    digest.update(request_id.to_be_bytes());
    digest.update(rmp_serde::to_vec_named(method).unwrap_or_default());
    format!("{namespace}:{}", hex::encode(digest.finalize()))
}

/// Durable identity for one WorkItem transition.
///
/// Terminal transitions carry an explicit caller-stable idempotency key. Their
/// durable identity must therefore be independent of the transport request id:
/// a retry after an uncertain acknowledgement arrives under a fresh request id,
/// but must reconstruct the exact same batch/context/outbox identity. The digest
/// is scope-bound so equal caller keys in different tenants or graphs cannot
/// collide in the shard-global batch table. Raw scope/key material is never
/// copied into the derived identifiers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkItemBatchIdentity {
    pub(crate) batch_id: String,
    pub(crate) idempotency_key: String,
    pub(crate) durable_request_id: u64,
    pub(crate) uses_native_row_cas: bool,
}

pub(crate) fn work_item_batch_identity(
    graph: &str,
    tenant: &str,
    transport_request_id: u64,
    method: &Method,
) -> Result<WorkItemBatchIdentity, String> {
    let terminal_key = match method {
        Method::CommitWorkItemResult {
            idempotency_key, ..
        }
        | Method::CancelWorkItem {
            idempotency_key, ..
        }
        | Method::DeferWorkItem {
            idempotency_key, ..
        } => Some(idempotency_key.as_str()),
        Method::ClaimWorkItem { .. } | Method::RenewWorkItemLease { .. } => None,
        _ => {
            return Err("WorkItem identity requires a WorkItem operation".to_string());
        }
    };

    if let Some(terminal_key) = terminal_key {
        if terminal_key.trim().is_empty() {
            return Err("terminal WorkItem mutation requires idempotency_key".to_string());
        }
        use sha2::{Digest, Sha256};
        let mut digest = Sha256::new();
        digest.update(b"epistemic-graph.work-item-terminal.v1");
        for field in [graph.as_bytes(), tenant.as_bytes(), terminal_key.as_bytes()] {
            digest.update((field.len() as u64).to_be_bytes());
            digest.update(field);
        }
        let digest: [u8; 32] = digest.finalize().into();
        let mut request_bytes = [0u8; 8];
        request_bytes.copy_from_slice(&digest[..8]);
        let durable_request_id = u64::from_be_bytes(request_bytes).max(1);
        let digest = hex::encode(digest);
        return Ok(WorkItemBatchIdentity {
            batch_id: format!("work:{digest}"),
            idempotency_key: format!("work-idem:{digest}"),
            durable_request_id,
            uses_native_row_cas: true,
        });
    }

    let batch_id = opaque_request_key("work", graph, transport_request_id, method);
    use sha2::{Digest, Sha256};
    let idempotency_key = format!(
        "work-idem:{}",
        hex::encode(Sha256::digest(batch_id.as_bytes()))
    );
    Ok(WorkItemBatchIdentity {
        batch_id,
        idempotency_key,
        durable_request_id: transport_request_id,
        uses_native_row_cas: false,
    })
}

/// Stable privacy-safe identity for a multi-request/native coordinator. The raw
/// transaction/session id is never written to durable status or outbox rows.
pub(crate) fn opaque_coordinator_key(namespace: &str, graph: &str, coordinator_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(namespace.as_bytes());
    digest.update([0]);
    digest.update(graph.as_bytes());
    digest.update([0]);
    digest.update(coordinator_id.as_bytes());
    format!("{namespace}:{}", hex::encode(digest.finalize()))
}

/// Commit an engine-internal graph write-set (for example an asynchronous job
/// result) through the same staged-state MutationBatch authority as public
/// runtime-result mutations.  Payload-bearing graph methods are represented by
/// opaque digests in coordinator metadata; their values exist only in the
/// authoritative graph image, avoiding a second PII-bearing copy in status/outbox
/// tables.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn commit_internal_graph_methods(
    persistence: Option<&Arc<dyn PersistenceBackend>>,
    core: &Arc<GraphCore>,
    request_id: u64,
    principal: Option<&str>,
    graph: &str,
    batch_id: &str,
    methods: Vec<Method>,
    result: &ResultPayload,
) -> Result<crate::mutation_batch::MutationBatchCommit, String> {
    let persistence = persistence.ok_or_else(|| {
        "internal graph write requires an authoritative MutationBatch backend".to_string()
    })?;
    let _guard = lock_graph(graph).await;
    let fname = crate::persist::sanitize(graph);
    let expected_principal = principal_fingerprint(
        principal
            .ok_or_else(|| "internal graph write requires a verified principal".to_string())?,
    )?;

    if let Some(record) = persistence.read_mutation_batch(&fname, batch_id).await? {
        use sha2::{Digest, Sha256};
        let operations_match = record.batch.operations.len() == methods.len()
            && record
                .batch
                .operations
                .iter()
                .zip(methods.iter())
                .enumerate()
                .all(|(ordinal, (operation, method))| {
                    let encoded = rmp_serde::to_vec_named(method).ok();
                    let expected = encoded
                        .map(|bytes| format!("sha256:{}", hex::encode(Sha256::digest(bytes))));
                    operation.ordinal == ordinal as u32
                        && matches!(
                            &operation.method,
                            Method::ApplyMutation { event_type, query }
                                if event_type == "authoritative_state_operation"
                                    && expected.as_deref() == Some(query.as_str())
                        )
                });
        if record.status != crate::mutation_batch::MutationBatchStatus::Committed
            || record.batch.batch_id != batch_id
            || record.batch.graph != graph
            || record.batch.tenant != graph
            || record.batch.context.principal != expected_principal
            || !operations_match
        {
            return Err("internal child receipt does not match its parent scope".to_string());
        }
        let expected_result = rmp_serde::to_vec_named(result).map_err(|error| error.to_string())?;
        if record.result_msgpack.as_deref() != Some(expected_result.as_slice()) {
            return Err("internal child receipt has a conflicting terminal result".to_string());
        }
        let (snapshot, version) = persistence
            .read_authoritative_graph_snapshot(&fname)
            .await?
            .ok_or_else(|| "committed internal graph image is missing".to_string())?;
        core.install_committed_snapshot(snapshot, version)?;
        return Ok(crate::mutation_batch::MutationBatchCommit {
            record,
            replayed: true,
        });
    }

    let (base_snapshot, source_version) = match persistence
        .read_authoritative_graph_snapshot(&fname)
        .await?
    {
        Some(value) => value,
        None => (
            core.snapshot(),
            authoritative_graph_version(persistence, &fname, core).await?,
        ),
    };
    let base_snapshot_for_delta = base_snapshot.clone();
    let staged = GraphCore::from_snapshot(base_snapshot, source_version)?;
    for method in &methods {
        apply_projectable_method(&staged, method)?;
    }
    let staged_snapshot = staged.snapshot();
    let row_delta =
        crate::graph_delta::GraphRowDelta::between(&base_snapshot_for_delta, &staged_snapshot)?;
    let state_msgpack = row_delta.to_msgpack()?;
    use sha2::{Digest, Sha256};
    let target_graph_version = source_version
        .checked_add(1)
        .ok_or_else(|| "authoritative graph version overflow".to_string())?;
    let descriptor = MutationStateDescriptor {
        algorithm: crate::graph_delta::ROW_DELTA_ALGORITHM.to_string(),
        digest: hex::encode(Sha256::digest(&state_msgpack)),
        source_graph_version: source_version,
        target_graph_version,
    };
    let created_at_ms = crate::server::dispatch::authoritative_now_ms();
    let batch = compile_methods(
        CompileBatch {
            batch_id,
            request_id,
            principal,
            tenant: graph,
            graph,
            placement_epoch: 0,
            idempotency_key: batch_id,
            expected_graph_version: Some(source_version),
            fencing_token: None,
            created_at_ms,
            default_surface: MutationSurface::Job,
            authoritative_state: Some(descriptor),
        },
        methods,
    )?;
    let result_msgpack = rmp_serde::to_vec_named(result).map_err(|error| error.to_string())?;
    let committed = persistence
        .commit_mutation_batch_state(
            &fname,
            &batch,
            state_msgpack,
            Some(&result_msgpack),
            created_at_ms,
            // Preserves this function's existing (pre-existing, out of scope here)
            // behavior exactly: every `methods` list this internal coordinator sees
            // today (Txn/2PC child write-sets, multi-graph commit slices, job-claim
            // provenance) is policy-audited == true. NOTE: `handlers::query.rs`'s
            // `RecomputeMaterialization` caller is a known exception (policy
            // `audited: false`) that this `true` does NOT correctly honor -- same
            // root cause as the TouchNodes fix elsewhere in this changeset, but
            // untested here and out of scope for this fix; left as a follow-up.
            true,
        )
        .await?;
    if committed.replayed {
        let (snapshot, version) = persistence
            .read_authoritative_graph_snapshot(&fname)
            .await?
            .ok_or_else(|| "committed internal graph image is missing".to_string())?;
        core.install_committed_snapshot(snapshot, version)?;
    } else {
        crate::server::mutation::publish_committed_row_delta(
            persistence,
            &fname,
            core,
            &row_delta,
            source_version,
        )
        .await?;
        if row_delta.preserves_node_derived_indexes() {
            core.mark_dirty_preserving_indexes();
        } else {
            core.mark_dirty();
        }
    }
    Ok(committed)
}

fn apply_projectable_method(core: &GraphCore, method: &Method) -> Result<(), String> {
    match method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => {
            core.add_node(node_id.clone(), properties_msgpack.clone());
            Ok(())
        }
        Method::RemoveNode { node_id } => {
            core.remove_node(node_id.clone());
            Ok(())
        }
        Method::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
        } => core.add_edge(
            source_id.clone(),
            target_id.clone(),
            properties_msgpack.clone(),
        ),
        Method::RemoveEdge {
            source_id,
            target_id,
        } => {
            core.remove_edge(source_id.clone(), target_id.clone());
            Ok(())
        }
        Method::CompareAndSetNodeFields {
            node_id,
            conditions_msgpack,
            updates_msgpack,
        } => {
            // Match the staged-transaction contract: malformed maps or a failed
            // predicate are a no-op CAS, not a partial child-batch failure.
            if let (Ok(conditions), Ok(updates)) = (
                eg_types::msgpack::decode_property_object(conditions_msgpack),
                eg_types::msgpack::decode_property_object(updates_msgpack),
            ) {
                let _ = core.compare_and_set_fields(node_id, &conditions, &updates);
            }
            Ok(())
        }
        Method::ClearGraph => {
            core.clear();
            Ok(())
        }
        Method::FromMsgpack { msgpack } => core.from_msgpack(msgpack),
        #[cfg(feature = "epistemic")]
        Method::RecomputeMaterialization { .. } => Ok(()),
        _ => Err("internal graph MutationBatch contains a non-projectable method".to_string()),
    }
}

/// Execute a WorkItem claim/renew/result transition inside the redb
/// MutationBatch transaction and then refresh every affected in-memory node from
/// the authoritative store. No selection or transition runs in RAM first.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn commit_work_item(
    persistence: Option<&Arc<dyn PersistenceBackend>>,
    core: &Arc<GraphCore>,
    request_id: u64,
    principal: Option<&str>,
    graph: &str,
    placement_epoch: u64,
    placement_fencing_token: Option<u64>,
    method: Method,
) -> Result<ResultPayload, String> {
    let persistence = persistence.ok_or_else(|| {
        "WorkItem mutation requires an authoritative persistence backend".to_string()
    })?;
    let tenant = match &method {
        Method::ClaimWorkItem { request } => request.tenant_ref.clone(),
        Method::RenewWorkItemLease { tenant, .. }
        | Method::CommitWorkItemResult { tenant, .. }
        | Method::CancelWorkItem { tenant, .. }
        | Method::DeferWorkItem { tenant, .. } => tenant.clone(),
        _ => return Err("commit_work_item received a non-WorkItem operation".to_string()),
    };
    if tenant.trim().is_empty() {
        return Err("WorkItem mutation requires a non-empty tenant".to_string());
    }
    // WorkItem rows live in the same authoritative graph image and advance the
    // same graph version as every other MutationBatch. Keep version discovery,
    // durable commit, and RAM publication inside the shared per-graph lane so a
    // ChangeEnvelope cannot pass its version fence and then lose a redb race to
    // a background claim/renew/result transition (or vice versa).
    let _mutation_guard = lock_graph(graph).await;
    let identity = work_item_batch_identity(graph, &tenant, request_id, &method)?;
    let created_at_ms = crate::server::dispatch::authoritative_now_ms();
    let fname = crate::persist::sanitize(graph);
    // Terminal WorkItem methods carry their own lease epoch/fencing CAS and are
    // applied inside the same redb transaction as their result/status. A graph-wide
    // OCC version would make an otherwise identical retry differ after the first
    // commit. Claim/renew still use graph OCC and their transport request identity.
    let expected_graph_version = if identity.uses_native_row_cas {
        None
    } else {
        Some(authoritative_graph_version(persistence, &fname, core).await?)
    };
    let batch = compile_methods(
        CompileBatch {
            batch_id: &identity.batch_id,
            request_id: identity.durable_request_id,
            principal,
            tenant: &tenant,
            graph,
            placement_epoch,
            idempotency_key: &identity.idempotency_key,
            expected_graph_version,
            fencing_token: placement_fencing_token,
            created_at_ms,
            default_surface: MutationSurface::Job,
            authoritative_state: None,
        },
        vec![method],
    )?;
    let committed = persistence
        .commit_mutation_batch(&fname, &batch, None, created_at_ms)
        .await?;
    let bytes = committed
        .record
        .result_msgpack
        .as_deref()
        .ok_or_else(|| "committed WorkItem batch has no durable result".to_string())?;
    let result: ResultPayload = eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(64 * 1024 * 1024, 1_000_000, 64),
    )
    .map_err(|_| "committed WorkItem result is corrupt".to_string())?;

    if !committed.replayed {
        for node_id in changed_work_item_ids(&result)? {
            let props = persistence
                .read_node(&fname, &node_id)
                .await?
                .ok_or_else(|| format!("committed WorkItem projection '{}' is missing", node_id))?;
            core.add_node(node_id, props);
        }
        core.mark_dirty();
    }
    Ok(result)
}

fn changed_work_item_ids(result: &ResultPayload) -> Result<Vec<String>, String> {
    fn from_json(value: &serde_json::Value) -> Result<Vec<String>, String> {
        value
            .get("changed_work_item_ids")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| "committed WorkItem result has no changed_work_item_ids".to_string())?
            .iter()
            .map(|value| {
                value.as_str().map(str::to_string).ok_or_else(|| {
                    "committed WorkItem result has a non-string changed id".to_string()
                })
            })
            .collect()
    }

    match result {
        ResultPayload::Json(value) => from_json(value),
        // ``ResultPayload::raw`` is wire-identical to ``PropertiesMsgpack``.
        // Because ResultPayload is untagged, decoding the durable outer payload
        // can legitimately select either byte variant. Both carry the same
        // typed WorkItem result and must refresh the resident graph projection.
        ResultPayload::Raw(bytes) | ResultPayload::PropertiesMsgpack(bytes) => {
            let value: serde_json::Value = eg_types::msgpack::decode_bounded(
                bytes,
                eg_types::msgpack::MsgpackLimits::new(1024 * 1024, 10_000, 32),
            )
            .map_err(|_| "committed WorkItem inner result is corrupt".to_string())?;
            from_json(&value)
        }
        _ => Err("committed WorkItem result has an invalid payload shape".to_string()),
    }
}

/// Publish the graph-row projection of a durably committed ChangeEnvelope.
/// Durable redb state remains authoritative; a failure here is repaired from the
/// transactional `engine.projection.rebuild` outbox rather than rolling back or
/// pretending the envelope did not commit.
pub(crate) fn publish_change_envelope_projection(
    core: &Arc<GraphCore>,
    envelope: &ChangeEnvelope,
) -> Result<(), String> {
    // Project into an isolated copy first. A late CAS failure or missing edge
    // endpoint must never leave the live cache with only the earlier operations
    // applied after the authoritative redb transaction committed atomically.
    // The final snapshot swap is the one publication point observed by readers.
    let source_version = core.version();
    let staged = Arc::new(GraphCore::new());
    staged.install_committed_snapshot(core.snapshot(), source_version)?;
    for operation in &envelope.mutation.operations {
        match &operation.method {
            Method::AddNode {
                node_id,
                properties_msgpack,
            } => staged.add_node(node_id.clone(), properties_msgpack.clone()),
            Method::RemoveNode { node_id } => staged.remove_node(node_id.clone()),
            Method::CompareAndSetNodeFields {
                node_id,
                conditions_msgpack,
                updates_msgpack,
            } => {
                let conditions = eg_types::msgpack::decode_property_object(conditions_msgpack)
                    .map_err(|_| "invalid committed CAS conditions".to_string())?;
                let updates = eg_types::msgpack::decode_property_object(updates_msgpack)
                    .map_err(|_| "invalid committed CAS updates".to_string())?;
                if !staged.compare_and_set_fields(node_id, &conditions, &updates) {
                    return Err(format!(
                        "committed CAS projection for '{}' no longer matches RAM",
                        node_id
                    ));
                }
            }
            Method::AddEdge {
                source_id,
                target_id,
                properties_msgpack,
            } => staged.add_edge(
                source_id.clone(),
                target_id.clone(),
                properties_msgpack.clone(),
            )?,
            Method::RemoveEdge {
                source_id,
                target_id,
            } => staged.remove_edge(source_id.clone(), target_id.clone()),
            Method::ClearGraph => staged.clear(),
            other => {
                return Err(format!(
                    "ChangeEnvelope contains a non-projectable operation in domain {:?}",
                    crate::server::mutation_batch::domain_for(other, operation.surface)
                ));
            }
        }
    }
    if core.version() != source_version {
        return Err(format!(
            "ChangeEnvelope projection raced another write: expected version {source_version}, current {}",
            core.version()
        ));
    }
    let target_graph_version = source_version
        .checked_add(1)
        .ok_or_else(|| "authoritative graph version overflow".to_string())?;
    core.install_committed_snapshot(staged.snapshot(), target_graph_version)
}

/// Commit one CreateGraph/DeleteGraph batch before the caller mutates the in-RAM
/// registry.  The redb kernel applies graph_meta/purge, status, idempotency and
/// outbox atomically; `replayed` lets the caller finish a post-commit RAM publish.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn commit_lifecycle(
    persistence: &Arc<dyn PersistenceBackend>,
    action: &str,
    request_id: u64,
    principal: Option<&str>,
    graph: &str,
    method: Method,
    result: &ResultPayload,
) -> Result<crate::mutation_batch::MutationBatchCommit, String> {
    let batch_id = lifecycle_batch_id(action, graph, request_id);
    let created_at_ms = crate::server::dispatch::authoritative_now_ms();
    let batch = compile_methods(
        CompileBatch {
            batch_id: &batch_id,
            request_id,
            principal,
            tenant: graph,
            graph,
            placement_epoch: 0,
            idempotency_key: &batch_id,
            expected_graph_version: None,
            fencing_token: None,
            created_at_ms,
            default_surface: MutationSurface::Lifecycle,
            authoritative_state: None,
        },
        vec![method],
    )?;
    let encoded_result = rmp_serde::to_vec_named(result).map_err(|e| e.to_string())?;
    let fname = crate::persist::sanitize(graph);
    persistence
        .commit_mutation_batch(&fname, &batch, Some(&encoded_result), created_at_ms)
        .await
}

/// Did this exact lifecycle request already reach its durable commit point? Used
/// when the registry already reflects Create (or no longer reflects Delete) so a
/// network retry returns the committed outcome instead of creating a second batch.
pub(crate) async fn lifecycle_was_committed(
    persistence: &Arc<dyn PersistenceBackend>,
    action: &str,
    graph: &str,
    request_id: u64,
) -> Result<bool, String> {
    let fname = crate::persist::sanitize(graph);
    let batch_id = lifecycle_batch_id(action, graph, request_id);
    let record_exists = persistence
        .read_mutation_batch(&fname, &batch_id)
        .await?
        .is_some();
    if !record_exists {
        return Ok(false);
    }
    Ok(persistence
        .read_mutation_lifecycle_head(&fname)
        .await?
        .as_deref()
        == Some(batch_id.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct LockProbePersistence {
        entered_commit: tokio::sync::Notify,
    }

    #[async_trait::async_trait]
    impl PersistenceBackend for LockProbePersistence {
        async fn load_all(
            &self,
            _state: &Arc<tokio::sync::RwLock<crate::server::ServerState>>,
        ) -> Result<usize, String> {
            Ok(0)
        }

        async fn record_durable(&self, _graph_fname: &str, _method: &Method) -> Result<(), String> {
            Ok(())
        }

        async fn read_mutation_graph_version(
            &self,
            _graph_fname: &str,
        ) -> Result<Option<u64>, String> {
            Ok(Some(0))
        }

        async fn commit_mutation_batch(
            &self,
            _graph_fname: &str,
            _batch: &MutationBatch,
            _result_msgpack: Option<&[u8]>,
            _committed_at_ms: u64,
        ) -> Result<crate::mutation_batch::MutationBatchCommit, String> {
            self.entered_commit.notify_one();
            Err("lock-probe-stop".to_string())
        }

        fn shutdown(&self) {}
    }

    #[tokio::test]
    async fn work_item_commit_waits_for_shared_graph_mutation_lane() {
        use std::time::Duration;

        let graph = "work-item-lock-probe";
        let held = lock_graph(graph).await;
        let probe = Arc::new(LockProbePersistence {
            entered_commit: tokio::sync::Notify::new(),
        });
        let persistence: Arc<dyn PersistenceBackend> = probe.clone();
        let core = Arc::new(GraphCore::new());
        let wait_for_commit = probe.entered_commit.notified();
        tokio::pin!(wait_for_commit);

        let task = tokio::spawn(async move {
            commit_work_item(
                Some(&persistence),
                &core,
                7,
                Some("principal:synthetic"),
                graph,
                0,
                None,
                Method::RenewWorkItemLease {
                    tenant: "tenant:synthetic".into(),
                    work_item_id: "work:synthetic".into(),
                    worker_id: "worker:synthetic".into(),
                    lease_epoch: 1,
                    fencing_token: 1,
                    now_ms: 1,
                    lease_ms: 1_000,
                },
            )
            .await
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut wait_for_commit)
                .await
                .is_err(),
            "WorkItem persistence entered while the graph mutation lane was held"
        );
        drop(held);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), &mut wait_for_commit)
                .await
                .is_ok(),
            "WorkItem persistence did not enter after the graph mutation lane released"
        );
        let outcome = task.await.expect("lock-probe task panicked");
        assert_eq!(outcome.unwrap_err(), "lock-probe-stop");
    }

    #[test]
    fn work_item_result_bytes_preserve_projection_refresh_ids() {
        let value = serde_json::json!({
            "status": "leased",
            "changed_work_item_ids": ["work:one", "work:two"],
        });
        let bytes = rmp_serde::to_vec_named(&value).unwrap();
        let expected = vec!["work:one".to_string(), "work:two".to_string()];

        assert_eq!(
            changed_work_item_ids(&ResultPayload::Raw(bytes.clone())).unwrap(),
            expected
        );
        assert_eq!(
            changed_work_item_ids(&ResultPayload::PropertiesMsgpack(bytes)).unwrap(),
            expected
        );
        assert_eq!(
            changed_work_item_ids(&ResultPayload::Json(value)).unwrap(),
            expected
        );
    }

    #[test]
    fn terminal_work_item_retry_identity_is_transport_independent_and_scope_bound() {
        let method = Method::CommitWorkItemResult {
            tenant: "tenant-a".into(),
            work_item_id: "work-1".into(),
            worker_id: "worker-a".into(),
            lease_epoch: 2,
            fencing_token: 9,
            idempotency_key: "terminal-key".into(),
            outcome: "succeeded".into(),
            result_ref: Some("result:sha256:one".into()),
            error_ref: None,
            retryable: false,
            now_ms: 1_000,
        };

        let first = work_item_batch_identity("graph-a", "tenant-a", 10, &method).unwrap();
        let retry = work_item_batch_identity("graph-a", "tenant-a", 11, &method).unwrap();
        assert_eq!(first, retry);
        assert!(first.uses_native_row_cas);
        assert_ne!(first.durable_request_id, 0);
        assert_eq!(first.batch_id.len(), "work:".len() + 64);
        assert_eq!(first.idempotency_key.len(), "work-idem:".len() + 64);

        let other_tenant = work_item_batch_identity("graph-a", "tenant-b", 11, &method).unwrap();
        let other_graph = work_item_batch_identity("graph-b", "tenant-a", 11, &method).unwrap();
        assert_ne!(first.batch_id, other_tenant.batch_id);
        assert_ne!(first.batch_id, other_graph.batch_id);

        let renew = Method::RenewWorkItemLease {
            tenant: "tenant-a".into(),
            work_item_id: "work-1".into(),
            worker_id: "worker-a".into(),
            lease_epoch: 2,
            fencing_token: 9,
            now_ms: 1_000,
            lease_ms: 10_000,
        };
        let renew_first = work_item_batch_identity("graph-a", "tenant-a", 10, &renew).unwrap();
        let renew_retry = work_item_batch_identity("graph-a", "tenant-a", 11, &renew).unwrap();
        assert!(!renew_first.uses_native_row_cas);
        assert_ne!(renew_first.batch_id, renew_retry.batch_id);
    }

    #[test]
    fn transaction_compiler_preserves_order_and_fences() {
        let batch = compile_methods(
            CompileBatch {
                batch_id: "txn-1",
                request_id: 1,
                principal: Some("agent:a"),
                tenant: "tenant-a",
                graph: "graph-a",
                placement_epoch: 8,
                idempotency_key: "idem-a",
                expected_graph_version: Some(4),
                fencing_token: Some(11),
                created_at_ms: 99,
                default_surface: MutationSurface::Transaction,
                authoritative_state: None,
            },
            vec![
                Method::RemoveNode {
                    node_id: "a".into(),
                },
                Method::RemoveNode {
                    node_id: "b".into(),
                },
            ],
        )
        .unwrap();
        assert_eq!(batch.operations[0].ordinal, 0);
        assert_eq!(batch.operations[1].ordinal, 1);
        assert_eq!(batch.expected_graph_version, Some(4));
        assert_eq!(batch.fencing_token, Some(11));
        assert_eq!(batch.outbox.len(), 1);
    }

    #[test]
    fn change_envelope_projection_failure_never_partially_publishes() {
        use crate::change_envelope::{
            ContentVersion, ContentVersionPosition, PrivacyAttestation, CHANGE_ENVELOPE_VERSION,
        };

        let core = Arc::new(GraphCore::new());
        core.add_node(
            "existing".into(),
            rmp_serde::to_vec_named(&serde_json::json!({"value": 0})).unwrap(),
        );
        core.mark_dirty();
        let source_version = core.version();
        let conditions =
            rmp_serde::to_vec_named(serde_json::json!({"value": 999}).as_object().unwrap())
                .unwrap();
        let updates =
            rmp_serde::to_vec_named(serde_json::json!({"value": 1}).as_object().unwrap()).unwrap();
        let mutation = compile_methods(
            CompileBatch {
                batch_id: "projection-atomic",
                request_id: 9,
                principal: Some("test-principal"),
                tenant: "tenant-a",
                graph: "graph-a",
                placement_epoch: 0,
                idempotency_key: "projection-atomic",
                expected_graph_version: Some(source_version),
                fencing_token: None,
                created_at_ms: 1,
                default_surface: MutationSurface::Graph,
                authoritative_state: None,
            },
            vec![
                Method::AddNode {
                    node_id: "early".into(),
                    properties_msgpack: rmp_serde::to_vec_named(
                        &serde_json::json!({"value": "must-not-publish"}),
                    )
                    .unwrap(),
                },
                Method::CompareAndSetNodeFields {
                    node_id: "existing".into(),
                    conditions_msgpack: conditions,
                    updates_msgpack: updates,
                },
            ],
        )
        .unwrap();
        let envelope = ChangeEnvelope {
            schema_version: CHANGE_ENVELOPE_VERSION,
            envelope_id: "projection-atomic-envelope".into(),
            mutation,
            content_version: ContentVersion {
                object_id: "existing".into(),
                digest_algorithm: "sha256".into(),
                digest: "a".repeat(64),
                previous_digest: None,
                source_version: ContentVersionPosition::Sequence(1),
            },
            cursor: None,
            blobs: Vec::new(),
            features: Vec::new(),
            evidence: Vec::new(),
            policies: Vec::new(),
            lineage: Vec::new(),
            privacy: PrivacyAttestation {
                policy_version: "privacy-v1".into(),
                sanitizer_version: "sanitizer-v1".into(),
                sanitized_payload_digest: "b".repeat(64),
            },
        };

        assert!(publish_change_envelope_projection(&core, &envelope).is_err());
        assert!(!core.has_node("early"));
        assert_eq!(core.version(), source_version);
        let existing: serde_json::Value = rmp_serde::from_slice(
            &core
                .get_node_properties("existing")
                .expect("existing node properties"),
        )
        .unwrap();
        assert_eq!(existing["value"], 0);
    }

    #[cfg(feature = "rdf")]
    #[test]
    fn rdf_adapter_marks_rdf_surface() {
        let batch = compile_methods(
            CompileBatch {
                batch_id: "rdf-1",
                request_id: 2,
                principal: Some("test-principal"),
                tenant: "tenant-a",
                graph: "graph-a",
                placement_epoch: 0,
                idempotency_key: "rdf-idem",
                expected_graph_version: Some(0),
                fencing_token: None,
                created_at_ms: 100,
                default_surface: MutationSurface::Other,
                authoritative_state: None,
            },
            vec![Method::DropNamedGraph],
        )
        .unwrap();
        assert_eq!(batch.operations[0].surface, MutationSurface::Rdf);
        assert!(matches!(batch.operations[0].method, Method::ClearGraph));
    }
}
