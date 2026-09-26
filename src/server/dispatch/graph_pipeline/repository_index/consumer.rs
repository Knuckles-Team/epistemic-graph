//! EH-557: validate a durable enrichment intent and plan one funded native page.
//!
//! The outbox lease is acknowledged only by the serving drain after a native
//! WorkItem commit has atomically reserved its total compute budget. This
//! module does not infer a checkpoint from memory or spend a new page budget.

use std::collections::BTreeMap;
#[cfg(feature = "blob")]
use std::{future::Future, sync::Arc};

use eg_types::epistemic_operations::RequestContext;
use eg_types::mutation_batch::MutationOutboxIntent;
#[cfg(feature = "blob")]
use eg_types::mutation_batch::MutationOutboxLease;
#[cfg(feature = "blob")]
use eg_types::native_control::EnrichmentBudgetPark;
use eg_types::native_control::{
    EnrichmentBudgetCheckpoint as DurableBudgetCheckpoint, EnrichmentBudgetReservation,
    NativeControlSchemaVersion, SubmitWorkItemsRequest, MAX_SUBMIT_BATCH,
};
use serde::Deserialize;

use crate::parser::enrichment_admission::{
    to_native_submit_batch, AdmissionPlan, EnrichmentCandidate, EnrichmentStage, QueueBinding,
    UnitKey,
};
use crate::parser::enrichment_snapshot::{
    EligibleSnapshot, EligibleUnit, EnrichmentBudgetCheckpoint, EnrichmentWorkStage,
};
use crate::server::persistence::PersistenceBackend;

pub(super) const PENDING_TOPIC: &str = "repository.enrichment.pending";
#[cfg(feature = "blob")]
pub(super) const CONSUMER: &str = "repository-enrichment-v1";
const MAX_PENDING_INTENT_BYTES: usize = 4 * 1024 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingSnapshot {
    budget_status: PendingBudgetStatus,
    snapshot: EligibleSnapshot,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum PendingBudgetStatus {
    Unreserved,
}

/// Decode only the server-owned, path-free snapshot for this exact outbox key.
/// The caller must obtain the intent from a held durable outbox lease.
pub(super) fn decode_pending_intent(
    intent: &MutationOutboxIntent,
) -> Result<EligibleSnapshot, String> {
    if intent.topic != PENDING_TOPIC || intent.payload.len() > MAX_PENDING_INTENT_BYTES {
        return Err("CONFLICT: repository enrichment outbox intent is invalid".into());
    }
    let pending: PendingSnapshot = eg_types::msgpack::decode_bounded(
        &intent.payload,
        eg_types::msgpack::MsgpackLimits::new(MAX_PENDING_INTENT_BYTES, 100_000, 64),
    )
    .map_err(|_| "CONFLICT: repository enrichment outbox payload is invalid")?;
    let PendingBudgetStatus::Unreserved = pending.budget_status;
    pending
        .snapshot
        .validate()
        .map_err(|_| "CONFLICT: repository enrichment snapshot is invalid")?;
    if pending.snapshot.source_envelope != intent.key {
        return Err("CONFLICT: repository enrichment source identity mismatch".into());
    }
    Ok(pending.snapshot)
}

/// One exact page to admit through a single atomic `SubmitWorkItems` and budget
/// reservation commit. A retry reads the authoritative checkpoint again; it
/// never advances this value in RAM after an uncertain response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PagePlan {
    pub start_index: usize,
    pub end_index: usize,
    pub page_key: String,
    pub reserve_units: u64,
    pub units: Vec<EligibleUnit>,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum PageDecision {
    Ready(PagePlan),
    Complete,
    DeferredBudget {
        required_units: u64,
        remaining_units: u64,
    },
}

/// Accept only a checkpoint read from the native authority for this exact
/// immutable source. A missing row must remain a refusal: the source commit
/// must seed budget authority before any page can be submitted.
pub(super) fn checkpoint_from_durable(
    snapshot: &EligibleSnapshot,
    durable: &DurableBudgetCheckpoint,
) -> Result<EnrichmentBudgetCheckpoint, String> {
    if durable.tenant_id != snapshot.tenant_id
        || durable.source_envelope != snapshot.source_envelope
        || durable.total_budget_units != snapshot.budget_units
    {
        return Err("CONFLICT: repository enrichment budget owner mismatch".into());
    }
    let checkpoint = EnrichmentBudgetCheckpoint {
        schema_version: durable.schema_version,
        snapshot_digest: durable.snapshot_digest.clone(),
        next_index: durable.next_index as usize,
        page_number: durable.page_number,
        reserved_units: durable.reserved_units,
        spent_units: durable.spent_units,
        remaining_units: durable.remaining_units,
        last_batch_key: (!durable.last_page_key.is_empty()).then(|| durable.last_page_key.clone()),
    };
    checkpoint
        .validate(snapshot)
        .map_err(|_| "CONFLICT: repository enrichment budget checkpoint is invalid")?;
    Ok(checkpoint)
}

/// Read the source's authoritative native budget row. A missing row is never
/// synthesized from the snapshot: producer pre-seeding must have committed it
/// with the source envelope before the consumer can admit a page.
pub(super) async fn read_checkpoint(
    persistence: &dyn PersistenceBackend,
    graph_fname: &str,
    snapshot: &EligibleSnapshot,
) -> Result<EnrichmentBudgetCheckpoint, String> {
    if graph_fname != snapshot.graph {
        return Err("CONFLICT: repository enrichment graph scope mismatch".into());
    }
    let durable = persistence
        .read_enrichment_budget_checkpoint(graph_fname, &snapshot.source_envelope)
        .await?
        .ok_or("CONFLICT: repository enrichment budget was not seeded")?;
    checkpoint_from_durable(snapshot, &durable)
}

/// Select a prefix of the remaining immutable order within the ONE global
/// budget. An underfunded first unit remains pending; skipping it would let a
/// later cheap unit violate demand ordering and hide the unfunded candidate.
pub(super) fn plan_next_page(
    snapshot: &EligibleSnapshot,
    checkpoint: &EnrichmentBudgetCheckpoint,
) -> Result<PageDecision, String> {
    checkpoint
        .validate(snapshot)
        .map_err(|_| "CONFLICT: repository enrichment checkpoint is invalid")?;
    let start = checkpoint.next_index;
    if start == snapshot.units.len() {
        return Ok(PageDecision::Complete);
    }
    let mut reserve_units = 0_u64;
    let mut end = start;
    for unit in snapshot.units.iter().skip(start).take(MAX_SUBMIT_BATCH) {
        let Some(next) = reserve_units.checked_add(unit.compute_units) else {
            break;
        };
        if next > checkpoint.remaining_units {
            break;
        }
        reserve_units = next;
        end += 1;
    }
    if end == start {
        return Ok(PageDecision::DeferredBudget {
            required_units: snapshot.units[start].compute_units,
            remaining_units: checkpoint.remaining_units,
        });
    }
    let page_key = checkpoint
        .page_key(snapshot, end)
        .map_err(|_| "CONFLICT: repository enrichment page identity is invalid")?;
    Ok(PageDecision::Ready(PagePlan {
        start_index: start,
        end_index: end,
        page_key,
        reserve_units,
        units: snapshot.units[start..end].to_vec(),
    }))
}

/// Derive the exact park row from immutable source evidence and the durable
/// checkpoint. `parked_at_ms` must be a command timestamp when this transition
/// is replicated; followers must never sample their own wall clocks. The
/// returned row is still only a plan, with no local delivery side effect.
#[cfg(feature = "blob")]
pub(super) fn plan_underfunded_park(
    snapshot: &EligibleSnapshot,
    checkpoint: &EnrichmentBudgetCheckpoint,
    parked_at_ms: u64,
) -> Result<EnrichmentBudgetPark, String> {
    let PageDecision::DeferredBudget {
        required_units,
        remaining_units,
    } = plan_next_page(snapshot, checkpoint)?
    else {
        return Err("CONFLICT: repository enrichment source is not underfunded".into());
    };
    if parked_at_ms == 0 {
        return Err("CONFLICT: repository enrichment park timestamp is invalid".into());
    }
    Ok(EnrichmentBudgetPark {
        schema_version: 1,
        source_envelope: snapshot.source_envelope.clone(),
        snapshot_digest: checkpoint.snapshot_digest.clone(),
        next_index: u32::try_from(checkpoint.next_index)
            .map_err(|_| "CONFLICT: repository enrichment cursor is invalid")?,
        page_number: checkpoint.page_number,
        remaining_units,
        required_units,
        policy_digest: snapshot.policy_digest.clone(),
        parked_at_ms,
    })
}

/// Bind an internal park proposal to one exact held local event and the latest
/// durable budget row. No caller-supplied snapshot, cursor, or cost is trusted.
#[cfg(feature = "blob")]
pub(crate) async fn plan_held_underfunded_park(
    persistence: &dyn PersistenceBackend,
    graph_fname: &str,
    lease: &MutationOutboxLease,
    committed_at_ms: u64,
) -> Result<(EligibleSnapshot, EnrichmentBudgetPark), String> {
    lease.record.validate()?;
    if lease.consumer != CONSUMER
        || lease
            .record
            .identity
            .scope()
            .graph_name()
            .map(|name| name.as_str())
            != Some(graph_fname)
        || lease.record.commit_sequence.is_none()
        || committed_at_ms >= lease.lease_until_ms
    {
        return Err("CONFLICT: repository enrichment park lease is invalid".into());
    }
    let snapshot = decode_pending_intent(&lease.record.intent)?;
    if snapshot.graph != graph_fname {
        return Err("CONFLICT: repository enrichment park graph changed".into());
    }
    let digest = snapshot
        .digest()
        .map_err(|_| "CONFLICT: repository enrichment park snapshot is invalid")?;
    if persistence
        .read_enrichment_supersession(graph_fname, &snapshot.source_envelope, &digest)
        .await?
    {
        return Err("CONFLICT: repository enrichment source was superseded".into());
    }
    let checkpoint = read_checkpoint(persistence, graph_fname, &snapshot).await?;
    let park = plan_underfunded_park(&snapshot, &checkpoint, committed_at_ms)?;
    Ok((snapshot, park))
}

/// Recheck every page input against the CAS holder before native submission.
/// The serving adapter must run this bounded read on a blocking worker, not on
/// the async reactor. Source hash and manifest hash are distinct identities.
#[cfg(feature = "blob")]
pub(super) fn verify_page_cas(
    store: &dyn crate::server::blob::store::ChunkStore,
    snapshot: &EligibleSnapshot,
    page: &PagePlan,
) -> Result<(), String> {
    for unit in &page.units {
        crate::server::blob::engine_bodies::read_repository_content_ref(
            store,
            &snapshot.tenant_id,
            &snapshot.repository_id,
            &unit.input_ref,
            &unit.content_digest,
            unit.content_length,
        )
        .map_err(|error| format!("CONFLICT: repository enrichment CAS binding failed: {error}"))?;
    }
    Ok(())
}

/// Lower an exact checkpoint page to the native atomic budget and WorkItem
/// command. The serving adapter must supply a freshly verified service
/// authority whose encoded RequestContext is stable for retries of this page.
pub(super) fn lower_native_page(
    snapshot: &EligibleSnapshot,
    checkpoint: &EnrichmentBudgetCheckpoint,
    page: &PagePlan,
    service_context: RequestContext,
) -> Result<SubmitWorkItemsRequest, String> {
    if !matches!(plan_next_page(snapshot, checkpoint)?, PageDecision::Ready(expected) if expected == *page)
    {
        return Err("CONFLICT: repository enrichment page does not match checkpoint".into());
    }
    if service_context.tenant_id != snapshot.tenant_id
        || service_context.graph != snapshot.graph
        || service_context.request_id != page.page_key
        || !service_context
            .scopes
            .iter()
            .any(|scope| scope == "work:submit")
        || !service_context
            .scopes
            .iter()
            .any(|scope| scope == "repository:enrichment:submit")
    {
        return Err("ACCESS_DENIED: repository enrichment service authority is invalid".into());
    }
    let mut input_refs = BTreeMap::new();
    let candidates: Vec<_> = page
        .units
        .iter()
        .map(|unit| {
            let key = UnitKey {
                content_digest: unit.content_digest.clone(),
                parser_capability_digest: unit.parser_capability_digest.clone(),
            };
            input_refs.insert(key.clone(), unit.input_ref.clone());
            EnrichmentCandidate {
                unit: key,
                stage: match unit.stage {
                    EnrichmentWorkStage::Classical => EnrichmentStage::Classical,
                    EnrichmentWorkStage::Embedding => EnrichmentStage::Embedding,
                    EnrichmentWorkStage::Llm => EnrichmentStage::Llm,
                },
                demanded: unit.demanded,
                reserved_compute_units: unit.compute_units,
            }
        })
        .collect();
    let plan = AdmissionPlan {
        candidates,
        reserved_compute_units: page.reserve_units,
        deferred_for_budget: 0,
        deferred_for_capacity: 0,
    };
    let binding = QueueBinding {
        context: service_context,
        input_refs,
        policy_digest: snapshot.policy_digest.clone(),
        catalog_digest: snapshot.catalog_digest.clone(),
        model_digest: snapshot.model_digest.clone(),
        source_commit_ref: snapshot.source_commit_ref.clone(),
    };
    let mut request = to_native_submit_batch(&plan, &binding)
        .map_err(|_| "CONFLICT: repository enrichment native page is invalid")?;
    for child in &mut request.requests {
        if !child.provenance_refs.contains(&snapshot.source_envelope) {
            child.provenance_refs.push(snapshot.source_envelope.clone());
        }
    }
    request.idempotency_key = page.page_key.clone();
    request.enrichment_budget = Some(EnrichmentBudgetReservation {
        schema_version: NativeControlSchemaVersion::V1,
        snapshot_digest: snapshot
            .digest()
            .map_err(|_| "CONFLICT: repository enrichment snapshot digest is invalid")?,
        source_envelope: snapshot.source_envelope.clone(),
        page_key: page.page_key.clone(),
        expected_next_index: u32::try_from(page.start_index)
            .map_err(|_| "CONFLICT: repository enrichment page index is invalid")?,
        end_index: u32::try_from(page.end_index)
            .map_err(|_| "CONFLICT: repository enrichment page index is invalid")?,
        expected_page_number: checkpoint.page_number,
        expected_remaining_units: checkpoint.remaining_units,
        reserve_units: page.reserve_units,
        total_budget_units: snapshot.budget_units,
    });
    Ok(request)
}

/// One bounded durable outbox sweep. The caller must provide a trusted service
/// authority for each page and a submit function that returns only after the
/// native WorkItem+budget commit receipt. Neither identity nor budget authority
/// is reconstructed from the outbox body.
#[cfg(feature = "blob")]
pub(crate) async fn drain_once<A, AF, S, F, P, PF>(
    persistence: &dyn PersistenceBackend,
    graph_fname: &str,
    store: Arc<dyn crate::server::blob::store::ChunkStore>,
    mut authority_for_page: A,
    mut submit: S,
    mut park_source: P,
) -> Result<DrainOutcome, String>
where
    A: FnMut(String, String, String) -> AF,
    AF: Future<
        Output = Result<(crate::server::auth::VerifiedRequestContext, RequestContext), String>,
    >,
    S: FnMut(crate::server::auth::VerifiedRequestContext, SubmitWorkItemsRequest) -> F,
    F: Future<Output = Result<(), String>>,
    P: FnMut(MutationOutboxLease, EnrichmentBudgetPark) -> PF,
    PF: Future<Output = Result<(), String>>,
{
    // The park is graph-wide and durable. Read it before subscribing/claiming
    // on every sweep, including the first sweep after process restart.
    if persistence
        .read_enrichment_budget_park(graph_fname)
        .await?
        .is_some()
    {
        return Ok(DrainOutcome::Parked);
    }
    persistence
        .subscribe_mutation_outbox(graph_fname, CONSUMER, PENDING_TOPIC)
        .await?;
    let now_ms = crate::server::dispatch::authoritative_now_ms();
    let mut budget = eg_transaction::OutboxClaimBudget::new(1, 60 * 60 * 1_000, now_ms)?;
    let outcome = persistence
        .claim_mutation_outbox(graph_fname, CONSUMER, &mut budget)
        .await?;
    if !outcome.dead_lettered.is_empty() {
        // Claim may report a dead letter and a later held event together.
        // Do not strand that fresh lease for the hour-long claim TTL while
        // the dead letter awaits the operator's ordered rewind.
        for lease in &outcome.claims {
            persistence
                .release_mutation_outbox(graph_fname, lease)
                .await?;
        }
        return Err(
            classify_dead_letter(persistence, graph_fname, &outcome.dead_lettered[0]).await?,
        );
    }
    let Some(lease) = outcome.claims.into_iter().next() else {
        return Ok(DrainOutcome::Idle);
    };
    if persistence
        .read_enrichment_budget_park(graph_fname)
        .await?
        .is_some()
    {
        persistence
            .release_mutation_outbox(graph_fname, &lease)
            .await?;
        return Ok(DrainOutcome::Parked);
    }
    process_held_lease(
        persistence,
        graph_fname,
        store,
        &lease,
        &mut authority_for_page,
        &mut submit,
        &mut park_source,
    )
    .await
}

#[cfg(feature = "blob")]
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum DrainOutcome {
    Idle,
    Parked,
    Acknowledged { pages: u32 },
}

/// Classify only the first ordered dead letter using its immutable outbox row
/// and replicated supersession marker. An operator can then rewind from the
/// exact position; the worker never drops a dead letter or rewinds a stream on
/// its own authority, since rewind re-delivers every later event as well.
#[cfg(feature = "blob")]
async fn classify_dead_letter(
    persistence: &dyn PersistenceBackend,
    graph_fname: &str,
    position: &eg_transaction::OutboxPosition,
) -> Result<String, String> {
    let records = persistence
        .read_mutation_outbox(graph_fname, &position.batch_id)
        .await?;
    let record = records
        .into_iter()
        .find(|record| record.ordinal == position.ordinal)
        .ok_or("CORRUPT_OUTBOX: repository enrichment dead letter has no event")?;
    if record.batch_id != position.batch_id
        || record.created_at_ms != position.created_at_ms
        || record.commit_sequence != Some(position.sequence)
    {
        return Err("CORRUPT_OUTBOX: repository enrichment dead letter changed".into());
    }
    let snapshot = decode_pending_intent(&record.intent)?;
    if snapshot.graph != graph_fname {
        return Err("CORRUPT_OUTBOX: repository enrichment dead letter graph changed".into());
    }
    let digest = snapshot
        .digest()
        .map_err(|_| "CORRUPT_OUTBOX: repository enrichment dead letter snapshot changed")?;
    let superseded = persistence
        .read_enrichment_supersession(graph_fname, &snapshot.source_envelope, &digest)
        .await?;
    let condition = if superseded {
        "superseded source; ordered rewind and stale-event ACK required"
    } else {
        "source still active; repair cause before ordered rewind"
    };
    Ok(format!(
        "CONFLICT: repository enrichment outbox dead-letter at {}:{} ({condition})",
        position.batch_id, position.ordinal
    ))
}

#[cfg(feature = "blob")]
async fn process_held_lease<A, AF, S, F, P, PF>(
    persistence: &dyn PersistenceBackend,
    graph_fname: &str,
    store: Arc<dyn crate::server::blob::store::ChunkStore>,
    lease: &MutationOutboxLease,
    authority_for_page: &mut A,
    submit: &mut S,
    park_source: &mut P,
) -> Result<DrainOutcome, String>
where
    A: FnMut(String, String, String) -> AF,
    AF: Future<
        Output = Result<(crate::server::auth::VerifiedRequestContext, RequestContext), String>,
    >,
    S: FnMut(crate::server::auth::VerifiedRequestContext, SubmitWorkItemsRequest) -> F,
    F: Future<Output = Result<(), String>>,
    P: FnMut(MutationOutboxLease, EnrichmentBudgetPark) -> PF,
    PF: Future<Output = Result<(), String>>,
{
    lease.record.validate()?;
    if lease.consumer != CONSUMER
        || lease
            .record
            .identity
            .scope()
            .graph_name()
            .map(|name| name.as_str())
            != Some(graph_fname)
        || lease.record.commit_sequence.is_none()
    {
        return Err("CONFLICT: repository enrichment lease consumer mismatch".into());
    }
    let snapshot = decode_pending_intent(&lease.record.intent)?;
    if snapshot.graph != graph_fname {
        return Err("CONFLICT: repository enrichment lease graph mismatch".into());
    }
    let snapshot_digest = snapshot
        .digest()
        .map_err(|_| "CONFLICT: repository enrichment snapshot digest is invalid")?;
    if persistence
        .read_enrichment_supersession(graph_fname, &snapshot.source_envelope, &snapshot_digest)
        .await?
    {
        // A Raft follower or former leader may retain its own old delivery
        // row after the replicated replacement commit. The owner-row marker
        // is the durable proof that no more units may be charged to this
        // snapshot. Resolve only this exact held local lease; the replacement
        // intent will be claimed next in the same ordered consumer stream.
        persistence
            .ack_mutation_outbox(
                graph_fname,
                lease,
                crate::server::dispatch::authoritative_now_ms(),
            )
            .await?;
        return Ok(DrainOutcome::Acknowledged { pages: 0 });
    }
    let mut checkpoint = read_checkpoint(persistence, graph_fname, &snapshot).await?;
    let mut pages = 0_u32;
    loop {
        let now_ms = crate::server::dispatch::authoritative_now_ms();
        if now_ms >= lease.lease_until_ms {
            return Err("STALE_OUTBOX_LEASE: repository enrichment lease expired".into());
        }
        match plan_next_page(&snapshot, &checkpoint)? {
            PageDecision::Complete => {
                persistence
                    .ack_mutation_outbox(graph_fname, lease, now_ms)
                    .await?;
                return Ok(DrainOutcome::Acknowledged { pages });
            }
            PageDecision::DeferredBudget { .. } => {
                let park = plan_underfunded_park(&snapshot, &checkpoint, now_ms)?;
                // The adapter owns the local or replicated park barrier and
                // releases this exact held lease only after that barrier.
                park_source(lease.clone(), park).await?;
                return Ok(DrainOutcome::Parked);
            }
            PageDecision::Ready(page) => {
                let owned_snapshot = snapshot.clone();
                let owned_page = page.clone();
                let owned_store = Arc::clone(&store);
                tokio::task::spawn_blocking(move || {
                    verify_page_cas(owned_store.as_ref(), &owned_snapshot, &owned_page)
                })
                .await
                .map_err(|_| "CONFLICT: repository enrichment CAS worker failed")??;
                let (verified, wire) = authority_for_page(
                    snapshot.tenant_id.clone(),
                    snapshot.graph.clone(),
                    page.page_key.clone(),
                )
                .await?;
                if verified.idempotency_key() != page.page_key
                    || !verified.allows_action("work:submit")
                    || !verified.allows_action("repository:enrichment:submit")
                    || !crate::server::handlers::delegation::context_matches_verified_authority(
                        &wire, &verified,
                    )
                    || wire.subject_id.trim().is_empty()
                    || wire.trace_id.trim().is_empty()
                    || wire
                        .scopes
                        .iter()
                        .any(|scope| scope.trim().is_empty() || !verified.allows_action(scope))
                    || wire.issued_at_ms > now_ms
                    || wire.expires_at_ms <= now_ms
                {
                    return Err(
                        "ACCESS_DENIED: repository enrichment service authority is invalid".into(),
                    );
                }
                let request = lower_native_page(&snapshot, &checkpoint, &page, wire)?;
                submit(verified, request).await?;
                let advanced = read_checkpoint(persistence, graph_fname, &snapshot).await?;
                if advanced.next_index < page.end_index
                    || (advanced.next_index == page.end_index
                        && advanced.last_batch_key.as_deref() != Some(page.page_key.as_str()))
                {
                    return Err(
                        "CONFLICT: repository enrichment native receipt did not advance budget"
                            .into(),
                    );
                }
                checkpoint = advanced;
                pages = pages.saturating_add(1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "blob")]
    use crate::server::blob::store::ChunkStore;
    use eg_types::epistemic_operations::{
        RequestContextAuthenticationMethod, RequestContextSchemaVersion,
    };

    fn service_context(tenant: &str, graph: &str, request_id: &str) -> RequestContext {
        RequestContext {
            schema_version: RequestContextSchemaVersion::V2,
            request_id: request_id.into(),
            subject_id: "principal:service".into(),
            tenant_id: tenant.into(),
            agent_id: "repository-enrichment".into(),
            scopes: vec!["work:submit".into(), "repository:enrichment:submit".into()],
            audience: "graph".into(),
            authentication_method: RequestContextAuthenticationMethod::LocalProcess,
            policy_version: "policy:v1".into(),
            graph: graph.into(),
            placement_epoch: Some(0),
            trace_id: "repository-index:one".into(),
            issued_at_ms: 1,
            expires_at_ms: 60_001,
        }
    }

    fn snapshot(count: usize, budget_units: u64) -> EligibleSnapshot {
        EligibleSnapshot {
            schema_version: 1,
            tenant_id: "tenant".into(),
            graph: "code".into(),
            repository_id: "repo".into(),
            source_envelope: "repository-index:one".into(),
            source_commit_ref: "repository-index:one".into(),
            policy_digest: "a".repeat(64),
            catalog_digest: "b".repeat(64),
            model_digest: "c".repeat(64),
            budget_units,
            units: (0..count)
                .map(|index| EligibleUnit {
                    content_digest: format!("sha256:{index:064x}"),
                    parser_capability_digest: "grammar:v1".into(),
                    stage: EnrichmentWorkStage::Classical,
                    input_ref: format!("cas:sha256:{:064x}", index + 1),
                    content_length: 1,
                    compute_units: 1,
                    demanded: false,
                })
                .collect(),
        }
    }

    #[test]
    fn pending_intent_requires_exact_source_and_bounded_valid_snapshot() {
        let source = snapshot(2, 2);
        let payload = rmp_serde::to_vec_named(&serde_json::json!({
            "budget_status": "unreserved",
            "snapshot": source
        }))
        .unwrap();
        let mut intent = MutationOutboxIntent {
            topic: PENDING_TOPIC.into(),
            key: "repository-index:one".into(),
            payload,
            headers: BTreeMap::new(),
        };
        assert_eq!(decode_pending_intent(&intent).unwrap().units.len(), 2);
        intent.key = "repository-index:other".into();
        assert!(decode_pending_intent(&intent).is_err());
        intent.key = "repository-index:one".into();
        intent.topic = "other.topic".into();
        assert!(decode_pending_intent(&intent).is_err());
    }

    #[test]
    fn maximum_eligible_snapshot_fits_inline_intent_bound() {
        let mut source = snapshot(
            crate::parser::enrichment_snapshot::MAX_ELIGIBLE_UNITS,
            u64::MAX,
        );
        source.tenant_id = "t".repeat(512);
        source.graph = "g".repeat(512);
        source.repository_id = "r".repeat(512);
        source.source_envelope = "e".repeat(512);
        source.source_commit_ref = source.source_envelope.clone();
        for (index, unit) in source.units.iter_mut().enumerate() {
            unit.parser_capability_digest = format!("{}{:012}", "p".repeat(500), index);
            unit.content_length = crate::parser::enrichment_snapshot::MAX_REPOSITORY_CONTENT_BYTES;
            unit.compute_units = u64::MAX;
        }
        source.validate().unwrap();
        let payload = rmp_serde::to_vec_named(&serde_json::json!({
            "budget_status": "unreserved",
            "snapshot": source
        }))
        .unwrap();
        assert!(
            payload.len() <= MAX_PENDING_INTENT_BYTES,
            "maximal valid snapshot encoded to {} bytes",
            payload.len()
        );
        let intent = MutationOutboxIntent {
            topic: PENDING_TOPIC.into(),
            key: "e".repeat(512),
            payload,
            headers: BTreeMap::new(),
        };
        assert_eq!(
            decode_pending_intent(&intent).unwrap().units.len(),
            crate::parser::enrichment_snapshot::MAX_ELIGIBLE_UNITS
        );
    }

    #[test]
    fn funded_pages_cover_one_hundred_thirty_units_without_renewing_budget() {
        let source = snapshot(130, 130);
        let mut checkpoint = EnrichmentBudgetCheckpoint::initial(&source).unwrap();
        let PageDecision::Ready(first) = plan_next_page(&source, &checkpoint).unwrap() else {
            panic!("first page should be funded");
        };
        assert_eq!(first.end_index, MAX_SUBMIT_BATCH);
        assert_eq!(first.reserve_units, MAX_SUBMIT_BATCH as u64);
        checkpoint.next_index = first.end_index;
        checkpoint.page_number = 1;
        checkpoint.reserved_units = first.reserve_units;
        checkpoint.remaining_units -= first.reserve_units;
        checkpoint.last_batch_key = Some(first.page_key);
        let PageDecision::Ready(second) = plan_next_page(&source, &checkpoint).unwrap() else {
            panic!("second page should be funded");
        };
        assert_eq!(second.start_index, MAX_SUBMIT_BATCH);
        assert_eq!(second.end_index, 130);
        assert_eq!(second.reserve_units, 2);
        assert_ne!(second.page_key, checkpoint.last_batch_key.unwrap());
    }

    #[test]
    fn underfunded_unit_stays_pending_at_the_same_cursor() {
        let source = snapshot(2, 1);
        let checkpoint = EnrichmentBudgetCheckpoint::initial(&source).unwrap();
        let PageDecision::Ready(first) = plan_next_page(&source, &checkpoint).unwrap() else {
            panic!("first page should fit");
        };
        let mut exhausted = checkpoint;
        exhausted.next_index = first.end_index;
        exhausted.page_number = 1;
        exhausted.reserved_units = 1;
        exhausted.remaining_units = 0;
        exhausted.last_batch_key = Some(first.page_key);
        assert_eq!(
            plan_next_page(&source, &exhausted).unwrap(),
            PageDecision::DeferredBudget {
                required_units: 1,
                remaining_units: 0
            }
        );
        #[cfg(feature = "blob")]
        {
            let park = plan_underfunded_park(&source, &exhausted, 42).unwrap();
            assert_eq!(park.source_envelope, source.source_envelope);
            assert_eq!(park.snapshot_digest, source.digest().unwrap());
            assert_eq!(park.next_index, 1);
            assert_eq!(park.page_number, 1);
            assert_eq!(park.required_units, 1);
            assert_eq!(park.remaining_units, 0);
            assert_eq!(park.parked_at_ms, 42);
            assert!(plan_underfunded_park(&source, &exhausted, 0).is_err());
            assert!(plan_underfunded_park(
                &source,
                &EnrichmentBudgetCheckpoint::initial(&source).unwrap(),
                42
            )
            .is_err());
        }
    }

    #[test]
    fn page_sum_overflow_stops_at_funded_prefix() {
        let mut source = snapshot(2, u64::MAX);
        source.units[0].compute_units = u64::MAX;
        let checkpoint = EnrichmentBudgetCheckpoint::initial(&source).unwrap();
        let PageDecision::Ready(page) = plan_next_page(&source, &checkpoint).unwrap() else {
            panic!("first unit should fit");
        };
        assert_eq!(page.end_index, 1);
        assert_eq!(page.reserve_units, u64::MAX);
    }

    #[test]
    fn durable_checkpoint_must_match_source_authority() {
        let source = snapshot(1, 1);
        let mut row = DurableBudgetCheckpoint {
            schema_version: 1,
            tenant_id: source.tenant_id.clone(),
            snapshot_digest: source.digest().unwrap(),
            source_envelope: source.source_envelope.clone(),
            next_index: 0,
            page_number: 0,
            reserved_units: 0,
            spent_units: 0,
            remaining_units: 1,
            total_budget_units: 1,
            last_page_key: String::new(),
        };
        assert_eq!(
            checkpoint_from_durable(&source, &row).unwrap().next_index,
            0
        );
        row.source_envelope = "repository-index:other".into();
        assert!(checkpoint_from_durable(&source, &row).is_err());
    }

    #[test]
    fn native_page_binds_atomic_budget_and_stable_page_identity() {
        let source = snapshot(2, 2);
        let checkpoint = EnrichmentBudgetCheckpoint::initial(&source).unwrap();
        let PageDecision::Ready(page) = plan_next_page(&source, &checkpoint).unwrap() else {
            panic!("page should be funded");
        };
        let context = service_context(&source.tenant_id, &source.graph, &page.page_key);
        let first = lower_native_page(&source, &checkpoint, &page, context.clone()).unwrap();
        let retry = lower_native_page(&source, &checkpoint, &page, context).unwrap();
        assert_eq!(first, retry);
        assert_eq!(first.idempotency_key, page.page_key);
        assert_eq!(first.requests.len(), 2);
        assert!(first.requests.iter().all(|child| {
            child.provenance_refs.contains(&source.source_envelope)
                && child.metadata.contains_key("reserved_compute_units")
        }));
        let debit = first.enrichment_budget.unwrap();
        assert_eq!(debit.expected_next_index, 0);
        assert_eq!(debit.end_index, 2);
        assert_eq!(debit.reserve_units, 2);
        assert_eq!(debit.total_budget_units, 2);
        assert_eq!(debit.page_key, page.page_key);
    }

    #[test]
    fn native_page_refuses_unscoped_service_or_stale_checkpoint() {
        let source = snapshot(1, 1);
        let checkpoint = EnrichmentBudgetCheckpoint::initial(&source).unwrap();
        let PageDecision::Ready(page) = plan_next_page(&source, &checkpoint).unwrap() else {
            panic!("page should be funded");
        };
        let wrong = service_context("other-tenant", &source.graph, &page.page_key);
        assert!(lower_native_page(&source, &checkpoint, &page, wrong).is_err());
        let mut ordinary_worker = service_context(&source.tenant_id, &source.graph, &page.page_key);
        ordinary_worker
            .scopes
            .retain(|scope| scope != "repository:enrichment:submit");
        assert!(lower_native_page(&source, &checkpoint, &page, ordinary_worker).is_err());
        let mut stale = checkpoint.clone();
        stale.remaining_units = 0;
        assert!(lower_native_page(
            &source,
            &stale,
            &page,
            service_context(&source.tenant_id, &source.graph, &page.page_key)
        )
        .is_err());
    }

    #[cfg(feature = "blob")]
    #[test]
    fn consumer_rechecks_exact_cas_holder_digest_and_length() {
        use sha2::{Digest, Sha256};

        let store = crate::server::blob::store::RedbChunkStore::open_temp().unwrap();
        let body = b"verified repository source";
        let source_digest = eg_types::contract::Digest256::from_bytes(Sha256::digest(body).into());
        let stored = store
            .put_repository_bodies(
                "tenant",
                "repo",
                &[crate::server::blob::engine_bodies::EngineBody {
                    sha256: source_digest,
                    body: body.to_vec(),
                }],
                1,
            )
            .unwrap();
        let mut source = snapshot(1, 1);
        source.units[0].content_digest = format!("sha256:{}", source_digest.to_hex());
        source.units[0].input_ref = format!("cas:sha256:{}", stored[0].manifest_digest);
        source.units[0].content_length = body.len() as u64;
        let checkpoint = EnrichmentBudgetCheckpoint::initial(&source).unwrap();
        let PageDecision::Ready(page) = plan_next_page(&source, &checkpoint).unwrap() else {
            panic!("source should form one page");
        };
        verify_page_cas(&store, &source, &page).unwrap();
        let mut wrong_length = page;
        wrong_length.units[0].content_length += 1;
        assert!(verify_page_cas(&store, &source, &wrong_length).is_err());
        wrong_length.units[0].content_length -= 1;
        wrong_length.units[0].content_digest = format!("sha256:{}", "f".repeat(64));
        assert!(verify_page_cas(&store, &source, &wrong_length).is_err());
        wrong_length.units[0].content_digest = source.units[0].content_digest.clone();
        let mut wrong_owner = source;
        wrong_owner.tenant_id = "other-tenant".into();
        assert!(verify_page_cas(&store, &wrong_owner, &wrong_length).is_err());
    }
}
