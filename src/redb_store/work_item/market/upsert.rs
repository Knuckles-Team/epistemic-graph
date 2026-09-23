//! `GapUpsert`: one canonical Gap and its native WorkItem, in one transaction.
//!
//! A new Gap, or a closed one that new evidence reopens, admits its generation's
//! WorkItem through the ordinary native submission ([`apply_submit_work_item_rows`]):
//! the same quota, command sequence, row shape and outbox as `SubmitWorkItem`,
//! under an engine-synthesised request context -- the body names no principal,
//! and the verified carrier tenant was already bound to it by dispatch. If that
//! admission fails (quota, bounds) the whole batch fails and the Gap is not
//! written either.

use std::collections::BTreeMap;

use eg_types::epistemic_operations::{
    RequestContext, RequestContextAuthenticationMethod, RequestContextSchemaVersion,
};
use eg_types::native_control::{NativeControlSchemaVersion, SubmitWorkItemRequest};
use eg_types::result_contract::coordination::GapUpsert;
use eg_types::work_market::gap::GapMerge;
use eg_types::work_market::{
    gap_row_key, gap_work_item_id, scoped_digest, GapUpsertOutcome, GapUpsertRequest, GapUpserted,
    GapView,
};

use super::*;

/// The identity the engine admits a Gap's WorkItem under. Not a principal: a
/// label that says the row was admitted by the market kernel.
const GAP_MARKET_AGENT: &str = "eg:work-market";
/// The policy/catalog/model identity of a market-admitted WorkItem.
const GAP_MARKET_POLICY: &str = "eg/work-market/v1";

/// The tables a `GapUpsert` writes.
pub(super) struct GapUpsertTables<'args, 'table> {
    pub(super) graph: &'args str,
    pub(super) nodes: &'args mut NodeRows<'table>,
    pub(super) edges: &'args mut ScopedOwnerTableMut<
        'table,
        (&'static str, &'static str, &'static str, u32),
        &'static [u8],
    >,
    pub(super) command_sequences: &'args mut ScopedOwnerTableMut<'table, &'static str, u64>,
}

/// Apply one `GapUpsert` and encode its typed result.
pub(super) fn apply_gap_upsert_rows(
    request: &GapUpsertRequest,
    tables: GapUpsertTables<'_, '_>,
    scope: WorkItemCommitScope<'_, '_>,
) -> Result<crate::protocol::ResultPayload, String> {
    request.validate()?;
    let GapUpsertTables {
        graph,
        nodes,
        edges,
        command_sequences,
    } = tables;
    let (crypto, now_ms) = (scope.crypto, scope.authoritative_now_ms);
    let tenant = request.tenant.as_str();
    let (mut gap, outcome) = match load_gap(nodes, graph, tenant, &request.gap_id, crypto)? {
        None => (
            request.fresh_gap(gap_work_item_id(tenant, &request.gap_id, 1), now_ms),
            GapUpsertOutcome::Created,
        ),
        Some(mut gap) => {
            let merge = gap.merge(
                request,
                |generation| gap_work_item_id(tenant, &request.gap_id, generation),
                now_ms,
            );
            (gap, upsert_outcome(merge))
        }
    };
    if outcome == GapUpsertOutcome::Unchanged {
        return encode(outcome, gap, Vec::new());
    }
    let admits = matches!(
        outcome,
        GapUpsertOutcome::Created | GapUpsertOutcome::Reopened
    );
    let mut changed = vec![gap_row_key(tenant, &gap.gap_id)];
    if admits {
        let submission = gap_submission(graph, request, &gap, now_ms);
        let admitted = apply_submit_work_item_rows(
            graph,
            &submission,
            nodes,
            edges,
            command_sequences,
            scope,
        )?;
        changed.extend(admitted.changed_work_item_ids);
    }
    gap = store_gap(nodes, graph, tenant, &gap, crypto)?;
    encode(outcome, gap, changed)
}

fn upsert_outcome(merge: GapMerge) -> GapUpsertOutcome {
    match merge {
        GapMerge::Unchanged => GapUpsertOutcome::Unchanged,
        GapMerge::Merged => GapUpsertOutcome::Merged,
        GapMerge::Reopened => GapUpsertOutcome::Reopened,
    }
}

fn encode(
    outcome: GapUpsertOutcome,
    gap: GapView,
    changed_work_item_ids: Vec<String>,
) -> Result<crate::protocol::ResultPayload, String> {
    let work_item_created = matches!(
        outcome,
        GapUpsertOutcome::Created | GapUpsertOutcome::Reopened
    );
    crate::protocol::ResultPayload::of::<GapUpsert>(GapUpserted {
        outcome,
        gap,
        work_item_created,
        changed_work_item_ids,
    })
}

/// The native submission of `gap`'s current-generation WorkItem. Every field
/// is a function of the request, the Gap and the batch's authoritative time,
/// so a replicated apply rebuilds it byte-for-byte.
fn gap_submission(
    graph: &str,
    request: &GapUpsertRequest,
    gap: &GapView,
    now_ms: u64,
) -> SubmitWorkItemRequest {
    let metadata: BTreeMap<String, serde_json::Value> = BTreeMap::from([
        ("gap_id".to_string(), gap.gap_id.clone().into()),
        ("gap_generation".to_string(), gap.generation.into()),
    ]);
    SubmitWorkItemRequest {
        schema_version: NativeControlSchemaVersion::V1,
        context: gap_market_context(graph, request, now_ms),
        work_item_id: Some(gap.work_item_id.clone()),
        idempotency_key: gap.work_item_id.clone(),
        command_digest: gap_command_digest(&request.tenant, gap),
        kind: gap.work.kind.clone(),
        priority: i64::from(gap.priority_bucket),
        depends_on: Vec::new(),
        input_ref: gap.gap_id.clone(),
        policy_digest: GAP_MARKET_POLICY.to_string(),
        catalog_digest: GAP_MARKET_POLICY.to_string(),
        model_digest: GAP_MARKET_POLICY.to_string(),
        max_attempts: gap.work.max_attempts,
        deadline_unix: None,
        metadata,
        provenance_refs: request
            .evidence
            .iter()
            .map(|evidence| evidence.digest.clone())
            .collect(),
        max_tenant_in_flight: 0,
    }
}

fn gap_market_context(graph: &str, request: &GapUpsertRequest, now_ms: u64) -> RequestContext {
    RequestContext {
        schema_version: RequestContextSchemaVersion::V2,
        request_id: request.idempotency_key.clone(),
        subject_id: GAP_MARKET_AGENT.to_string(),
        tenant_id: request.tenant.clone(),
        agent_id: GAP_MARKET_AGENT.to_string(),
        scopes: Vec::new(),
        audience: GAP_MARKET_AGENT.to_string(),
        authentication_method: RequestContextAuthenticationMethod::LocalProcess,
        policy_version: GAP_MARKET_POLICY.to_string(),
        graph: graph.to_string(),
        placement_epoch: None,
        trace_id: request.gap_id.clone(),
        issued_at_ms: now_ms,
        expires_at_ms: now_ms,
    }
}

/// SHA-256 over the Gap's identity and generation: the WorkItem's command.
fn gap_command_digest(tenant: &str, gap: &GapView) -> String {
    scoped_digest(
        b"eg/gap-market-command/v1",
        &[
            tenant.as_bytes(),
            gap.gap_id.as_bytes(),
            &gap.generation.to_be_bytes(),
        ],
    )
}
