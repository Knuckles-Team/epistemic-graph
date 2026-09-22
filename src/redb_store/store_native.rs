use super::store_prelude::*;
use super::*;

/// GOC-19/GOC-20 (BUG-015 "B9"): a `CommitWorkItemResult` batch may
/// additionally carry co-committed `AddNode` provenance operations
/// (RunTrace/ToolCall/OutcomeEvaluation) so a WorkItem's terminal status
/// and its provenance land in the SAME redb write transaction -- never
/// one durable without the other. Validated ONCE, before any row is
/// touched below, so a disallowed shape is refused without partially
/// applying anything in this transaction. Every other WorkItem method
/// (Claim/Renew/Cancel/Defer/CasMetadata) keeps the original strict
/// single-operation rule enforced further down -- this relaxation is
/// deliberately narrow to `CommitWorkItemResult` alone, which is the one
/// terminal transition `crates/eg-types/src/outcome_bundle.rs`'s
/// `CommitOutcomeBundle` (GOC-20) is designed to accompany.
pub(crate) fn validate_native_operations_commit_work_item_result_shape(
    batch: &MutationBatch,
) -> Result<(), String> {
    let work_item_result_ops = batch
        .operations
        .iter()
        .filter(|op| matches!(&op.method, Method::CommitWorkItemResult { .. }))
        .count();
    if work_item_result_ops > 1 {
        return Err(
            "a MutationBatch may contain at most one CommitWorkItemResult operation".to_string(),
        );
    }
    let Some((terminal_index, extension)) =
        batch
            .operations
            .iter()
            .enumerate()
            .find_map(|(index, operation)| match &operation.method {
                Method::CommitWorkItemResult {
                    outcome_extension, ..
                } => Some((index, outcome_extension.as_deref())),
                _ => None,
            })
    else {
        return Ok(());
    };
    if terminal_index != 0 {
        return Err("CommitWorkItemResult must be the first operation".to_string());
    }
    match extension {
        Some(extension) => validate_terminal_outcome_extension(batch, extension),
        None => validate_terminal_provenance_shape(batch),
    }
}

fn validate_terminal_provenance_shape(batch: &MutationBatch) -> Result<(), String> {
    if batch.operations.iter().any(|op| {
        !matches!(
            &op.method,
            Method::CommitWorkItemResult { .. } | Method::AddNode { .. }
        )
    }) {
        return Err(
            "a CommitWorkItemResult MutationBatch may only carry additional AddNode \
             (provenance) operations"
                .to_string(),
        );
    }
    Ok(())
}

fn validate_terminal_outcome_extension(
    batch: &MutationBatch,
    extension: &eg_types::outcome_bundle::TerminalOutcomeExtension,
) -> Result<(), String> {
    if batch.operations.len() != 1 {
        return Err(
            "a terminal outcome extension owns receipt rows and must be the only operation"
                .to_string(),
        );
    }
    let event_intent = terminal_run_event_intent(batch)?;
    let event: eg_types::outcome_bundle::RunEvent = rmp_serde::from_slice(&event_intent.payload)
        .map_err(|_| "terminal run-event outbox payload is not a valid RunEvent".to_string())?;
    event.validate_for_bundle(&extension.outcome_bundle)?;
    validate_terminal_event_headers(batch, extension, event_intent)
}

fn terminal_run_event_intent(batch: &MutationBatch) -> Result<&MutationOutboxIntent, String> {
    let event_intents: Vec<_> = batch
        .outbox
        .iter()
        .filter(|intent| intent.topic == eg_types::outcome_bundle::RUN_EVENT_OUTBOX_TOPIC)
        .collect();
    if event_intents.len() != 1 {
        return Err(
            "a terminal outcome extension must carry exactly one run-event outbox intent"
                .to_string(),
        );
    }
    let event_intent = event_intents[0];
    if event_intent.key != batch.batch_id {
        return Err("terminal run-event outbox key must equal the mutation batch id".to_string());
    }
    Ok(event_intent)
}

fn validate_terminal_event_headers(
    batch: &MutationBatch,
    extension: &eg_types::outcome_bundle::TerminalOutcomeExtension,
    event_intent: &MutationOutboxIntent,
) -> Result<(), String> {
    let fence_token = extension.outcome_bundle.fence_token.to_string();
    let completeness = serde_json::to_value(extension.outcome_bundle.completeness)
        .map_err(|error| format!("outcome completeness encoding failed: {error}"))?
        .as_str()
        .ok_or_else(|| "outcome completeness encoding was not a string".to_string())?
        .to_string();
    let missing_refs = serde_json::to_string(&extension.outcome_bundle.missing_refs)
        .map_err(|error| format!("outcome missing_refs encoding failed: {error}"))?;
    let actor = batch
        .envelope
        .operation()
        .ok_or_else(|| "terminal batch envelope has no operation actor".to_string())?
        .authority
        .actor
        .as_str();
    let graph = batch
        .identity
        .scope()
        .graph_name()
        .ok_or_else(|| "terminal batch identity is not graph-scoped".to_string())?
        .as_str();
    let scope_digest = terminal_scope_digest(batch, graph);
    let headers = [
        ("batch_id", batch.batch_id.as_str()),
        (
            "delegation_id",
            extension.outcome_bundle.delegation_id.as_str(),
        ),
        (
            "delegator_id",
            extension.outcome_bundle.delegator_id.as_str(),
        ),
        (
            "selected_agent_id",
            extension.outcome_bundle.selected_agent_id.as_str(),
        ),
        (
            "executor_lease_actor",
            extension.outcome_bundle.executor_lease_actor.as_str(),
        ),
        ("outcome", extension.outcome_bundle.outcome.as_str()),
        (
            "work_item_id",
            extension.outcome_bundle.work_item_id.as_str(),
        ),
        ("run_id", extension.outcome_bundle.run_id.as_str()),
        ("fence_token", fence_token.as_str()),
        (
            "capability_digest",
            extension.outcome_bundle.capability_digest.as_str(),
        ),
        (
            "catalog_digest",
            extension.outcome_bundle.catalog_digest.as_str(),
        ),
        (
            "policy_digest",
            extension.outcome_bundle.policy_digest.as_str(),
        ),
        (
            "model_digest",
            extension.outcome_bundle.model_digest.as_str(),
        ),
        ("completeness", completeness.as_str()),
        ("missing_refs", missing_refs.as_str()),
        ("actor", actor),
        ("scope_sha256", scope_digest.as_str()),
    ];
    for (field, expected) in headers {
        if event_intent.headers.get(field).map(String::as_str) != Some(expected) {
            return Err(format!(
                "terminal run-event outbox header '{field}' is not bound"
            ));
        }
    }
    if extension.outcome_bundle.result_ref.as_deref()
        != event_intent.headers.get("result_ref").map(String::as_str)
    {
        return Err("terminal run-event outbox result_ref header is not bound".to_string());
    }
    Ok(())
}

fn terminal_scope_digest(batch: &MutationBatch, graph: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut scope_digest = Sha256::new();
    scope_digest.update(batch.identity.tenant().as_str().as_bytes());
    scope_digest.update([0]);
    scope_digest.update(graph.as_bytes());
    hex::encode(scope_digest.finalize())
}

pub(crate) fn apply_native_clear_or_delete_graph_rows(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    tables: &mut NativeOperationTables<'_>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    clear_graph_rows(
        graph_fname,
        &mut tables.graph.nodes,
        &mut tables.graph.edges,
        &mut tables.graph.ledger,
    )?;
    tables
        .graph
        .command_sequences
        .remove(graph_fname)
        .map_err(|e| e.to_string())?;
    clear_resource_rows_with_tables(graph_fname, &mut tables.resources, crypto)?;
    development_lane::clear_native_graph_rows_in_wtx_with_lane_tables(
        write,
        graph_fname,
        &mut tables.lane_holds,
        &mut tables.lane_work_item_index,
        &mut tables.lane_counters,
        &mut tables.lane_pressure_index,
        &mut tables.lane_policies,
        crypto,
    )?;
    capacity_lease::clear_graph_rows(write, graph_fname)?;
    work_item_capability::clear_graph_rows_with_native(
        write,
        graph_fname,
        &mut tables.graph.native_work_items,
    )?;
    Ok(())
}

/// The batch-level context both native WorkItem-submit operations need: the
/// batch being applied (its `batch_id` is the outbox id, and its operation count
/// carries the one-result-producing-operation invariant), the authoritative
/// commit timestamp, and the sealing handle. Bundled so the two operations stay
/// inside clippy's parameter cap; each field is the value the caller already
/// passed positionally.
#[derive(Clone, Copy)]
pub(crate) struct NativeSubmitScope<'batch, 'crypto> {
    batch: &'batch MutationBatch,
    committed_at_ms: u64,
    crypto: DurableCrypto<'crypto>,
}

/// The shared inputs for the row phases of one admitted graph member.
///
/// The table capability is already tied to the member's `ShardWrite`; keeping
/// it here prevents the operation loop from manufacturing a second transaction
/// or accidentally mixing a graph name with another member's tables. The
/// pre-row domain validation (`prepare_and_validate_mutation_batch`) is
/// addressed by the same four, for the same reason, so it reads this context
/// rather than restating its halves.
#[derive(Clone, Copy)]
pub(crate) struct MutationRowCtx<'a> {
    pub(crate) write: &'a ShardWrite<'a>,
    pub(crate) graph_fname: &'a str,
    pub(crate) batch: &'a MutationBatch,
    pub(crate) crypto: DurableCrypto<'a>,
}

pub(crate) struct NativeOperationTables<'txn> {
    graph: GraphRowTables<'txn>,
    resources: ResourceRowTables<'txn>,
    lane_holds: ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
    lane_work_item_index:
        ScopedOwnerTableMut<'txn, (&'static str, &'static str, u64), &'static str>,
    lane_counters: ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
    lane_pressure_index: ScopedOwnerTableMut<
        'txn,
        (
            &'static str,
            &'static str,
            &'static str,
            &'static str,
            u64,
            &'static str,
        ),
        u8,
    >,
    lane_policies: ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
}

impl<'txn> NativeOperationTables<'txn> {
    fn open(write: &'txn ShardWrite<'txn>, graph_fname: &str) -> Result<Self, String> {
        let graph = GraphRowTables::open(write.graph(graph_fname)?)?;
        let resources = ResourceRowTables::open(write, graph_fname)?;
        let (lane_holds, lane_work_item_index, lane_counters, lane_pressure_index, lane_policies) =
            open_native_operation_lane_tables(write, graph_fname)?;
        Ok(Self {
            graph,
            resources,
            lane_holds,
            lane_work_item_index,
            lane_counters,
            lane_pressure_index,
            lane_policies,
        })
    }
}

struct NativeOperationContext<'a, 'txn> {
    write: &'a ShardWrite<'txn>,
    graph_fname: &'a str,
    batch: &'a MutationBatch,
    operation: &'a MutationOperation,
    committed_at_ms: u64,
    crypto: DurableCrypto<'txn>,
    generated_result: &'a mut Option<Vec<u8>>,
    crossmodal_present: bool,
}

struct NativeOperationLoopInput<'a, 'txn> {
    write: &'a ShardWrite<'txn>,
    graph_fname: &'a str,
    batch: &'a MutationBatch,
    committed_at_ms: u64,
    crypto: DurableCrypto<'txn>,
    #[cfg(feature = "security")]
    staged_audit_tail: &'a mut AuditTailCache,
    generated_result: &'a mut Option<Vec<u8>>,
    crossmodal_present: bool,
}

pub(crate) struct NativeOperationOptions<'a> {
    pub(crate) committed_at_ms: u64,
    #[cfg(feature = "security")]
    pub(crate) staged_audit_tail: &'a mut AuditTailCache,
    pub(crate) generated_result: &'a mut Option<Vec<u8>>,
    pub(crate) crossmodal_present: bool,
}

/// Apply one native submit row operation inside the caller's existing write,
/// then encode its typed result. The row applier intentionally runs before the
/// result-slot and operation-count checks, preserving the transaction's
/// historical error order; an error still aborts the surrounding row phase.
fn apply_native_submit_operation<'batch, 'crypto, M, ApplyRows>(
    scope: NativeSubmitScope<'batch, 'crypto>,
    generated_result: &mut Option<Vec<u8>>,
    apply_rows: ApplyRows,
) -> Result<(), String>
where
    M: eg_types::result_contract::MethodResult,
    ApplyRows: FnOnce(WorkItemCommitScope<'crypto, 'batch>) -> Result<M::Body, String>,
{
    let NativeSubmitScope {
        batch,
        committed_at_ms,
        crypto,
    } = scope;
    let result = apply_rows(WorkItemCommitScope {
        crypto,
        authoritative_now_ms: committed_at_ms,
        outbox_id: &batch.batch_id,
    })?;
    if generated_result.is_some() || batch.operations.len() != 1 {
        return Err(format!(
            "{} MutationBatch must contain exactly one result-producing operation",
            M::METHOD
        ));
    }
    let payload = crate::protocol::ResultPayload::of::<M>(result)?;
    *generated_result = Some(rmp_serde::to_vec_named(&payload).map_err(|e| e.to_string())?);
    Ok(())
}

pub(crate) fn apply_native_work_item_family_operation(
    graph_fname: &str,
    method: &Method,
    tables: &mut NativeOperationTables<'_>,
    batch: &MutationBatch,
    generated_result: &mut Option<Vec<u8>>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let result = apply_work_item_rows(super::work_item::WorkItemApplyRequest {
        graph: graph_fname,
        batch_id: batch.batch_id.as_str(),
        method,
        nodes: &mut tables.graph.nodes,
        holds: &mut tables.lane_holds,
        work_item_index: &tables.lane_work_item_index,
        counters: &mut tables.lane_counters,
        pressure_index: &mut tables.lane_pressure_index,
        policies: &tables.lane_policies,
        native_work_items: &mut tables.graph.native_work_items,
        crypto,
    })?
    .ok_or_else(|| "WorkItem mutation produced no durable result".to_string())?;
    if generated_result.is_some() || batch.operations.len() != 1 {
        return Err(
            "WorkItem MutationBatch must contain exactly one result-producing operation"
                .to_string(),
        );
    }
    *generated_result = Some(rmp_serde::to_vec_named(&result).map_err(|e| e.to_string())?);
    Ok(())
}

// GOC-19/GOC-20 (BUG-015 "B9"): split out from the shared WorkItem-family
// helper above -- this is the ONE WorkItem terminal transition allowed to
// co-commit additional `AddNode` provenance operations in the same batch
// (validated once, before the operation loop, by
// `validate_native_operations_commit_work_item_result_shape`). The
// `batch.operations.len() != 1` sub-check is deliberately dropped here;
// `generated_result.is_some()` alone still guarantees at most one
// result-producing operation applies.
pub(crate) fn apply_native_commit_work_item_result_operation(
    graph_fname: &str,
    batch_id: &str,
    method: &Method,
    tables: &mut NativeOperationTables<'_>,
    generated_result: &mut Option<Vec<u8>>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let result = apply_work_item_rows(super::work_item::WorkItemApplyRequest {
        graph: graph_fname,
        batch_id,
        method,
        nodes: &mut tables.graph.nodes,
        holds: &mut tables.lane_holds,
        work_item_index: &tables.lane_work_item_index,
        counters: &mut tables.lane_counters,
        pressure_index: &mut tables.lane_pressure_index,
        policies: &tables.lane_policies,
        native_work_items: &mut tables.graph.native_work_items,
        crypto,
    })?
    .ok_or_else(|| "WorkItem mutation produced no durable result".to_string())?;
    if generated_result.is_some() {
        return Err(
            "WorkItem MutationBatch must contain exactly one result-producing operation"
                .to_string(),
        );
    }
    *generated_result = Some(rmp_serde::to_vec_named(&result).map_err(|e| e.to_string())?);
    Ok(())
}

pub(crate) fn apply_native_resource_reservation_operation(
    graph_fname: &str,
    method: &Method,
    tables: &mut NativeOperationTables<'_>,
    batch: &MutationBatch,
    generated_result: &mut Option<Vec<u8>>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let result = super::resource::apply_resource_reservation_rows(
        super::resource::ResourceReservationApplyRequest {
            graph: graph_fname,
            method,
            tables: super::resource::ResourceReservationTables {
                nodes: &mut tables.graph.nodes,
                reservations: &mut tables.resources.reservations,
                tenant_index: &mut tables.resources.tenant_index,
                attempts: &mut tables.resources.attempts,
                hosts: &mut tables.resources.hosts,
                exclusivity: &mut tables.resources.exclusivity,
                fairness: &mut tables.resources.fairness,
                concurrency: &mut tables.resources.concurrency,
                anti_affinity: &mut tables.resources.anti_affinity,
                disk_policies: &mut tables.resources.disk_policies,
                crypto,
            },
        },
    )?
    .ok_or_else(|| "resource reservation mutation produced no durable result".to_string())?;
    if generated_result.is_some() || batch.operations.len() != 1 {
        return Err(
            "resource reservation MutationBatch must contain exactly one result-producing operation"
                .to_string(),
        );
    }
    *generated_result = Some(rmp_serde::to_vec_named(&result).map_err(|e| e.to_string())?);
    Ok(())
}

/// True for the `ApplyMutation` row that merely carries a crossmodal projection
/// in a batch that already staged one; the projection itself is applied by
/// `apply_crossmodal_projection_rows`.
pub(crate) fn native_operation_is_crossmodal_carrier(
    method: &Method,
    crossmodal_present: bool,
) -> bool {
    crossmodal_present
        && matches!(
            method,
            Method::ApplyMutation { event_type, .. } if event_type == "crossmodal_operation"
        )
}

fn apply_one_native_operation_row(
    context: NativeOperationContext<'_, '_>,
    tables: &mut NativeOperationTables<'_>,
) -> Result<(), String> {
    let NativeOperationContext {
        write,
        graph_fname,
        batch,
        operation,
        committed_at_ms,
        crypto,
        generated_result,
        crossmodal_present,
    } = context;
    // Hoisted out of the match below (it was the arm immediately before the
    // wildcard, and no earlier arm can match `ApplyMutation`, so the dispatch
    // order is unchanged): a crossmodal envelope's carrier row is applied by
    // `apply_crossmodal_projection_rows`, not here.
    if native_operation_is_crossmodal_carrier(&operation.method, crossmodal_present) {
        return Ok(());
    }
    match &operation.method {
        Method::CreateGraph { .. } => Ok(()),
        Method::DeleteGraph { .. } | Method::ClearGraph => {
            apply_native_clear_or_delete_graph_rows(write, graph_fname, tables, crypto)
        }
        Method::ClearLedger => clear_ledger_rows(graph_fname, &mut tables.graph.ledger),
        Method::SubmitWorkItem { request } => apply_native_submit_operation::<
            eg_types::result_contract::coordination::SubmitWorkItem,
            _,
        >(
            NativeSubmitScope {
                batch,
                committed_at_ms,
                crypto,
            },
            generated_result,
            |commit_scope| {
                apply_submit_work_item_rows(
                    graph_fname,
                    request,
                    &mut tables.graph.nodes,
                    &mut tables.graph.edges,
                    &mut tables.graph.command_sequences,
                    commit_scope,
                )
            },
        ),
        Method::SubmitWorkItems { request } => apply_native_submit_operation::<
            eg_types::result_contract::coordination::SubmitWorkItems,
            _,
        >(
            NativeSubmitScope {
                batch,
                committed_at_ms,
                crypto,
            },
            generated_result,
            |commit_scope| {
                apply_submit_work_items_rows(
                    graph_fname,
                    request,
                    &mut tables.graph.nodes,
                    &mut tables.graph.edges,
                    &mut tables.graph.command_sequences,
                    commit_scope,
                )
            },
        ),
        method @ (Method::ClaimWorkItem { .. }
        | Method::RenewWorkItemLease { .. }
        | Method::CancelWorkItem { .. }
        | Method::DeferWorkItem { .. }
        | Method::CasWorkItemMetadata { .. }
        | Method::IssueControlLease { .. }
        | Method::TransitionControlLease { .. }) => apply_native_work_item_family_operation(
            graph_fname,
            method,
            tables,
            batch,
            generated_result,
            crypto,
        ),
        method @ Method::CommitWorkItemResult { .. } => {
            apply_native_commit_work_item_result_operation(
                graph_fname,
                batch.batch_id.as_str(),
                method,
                tables,
                generated_result,
                crypto,
            )
        }
        method @ (Method::ReserveWorkItemResources { .. }
        | Method::ReleaseWorkItemResources { .. }
        | Method::ReclaimWorkItemResources { .. }
        | Method::UpdateResourceHost { .. }) => apply_native_resource_reservation_operation(
            graph_fname,
            method,
            tables,
            batch,
            generated_result,
            crypto,
        ),
        method => {
            let mut tables = GraphRowTablesRef {
                nodes: &mut tables.graph.nodes,
                edges: &mut tables.graph.edges,
                ledger: &mut tables.graph.ledger,
                semantic: &mut tables.graph.semantic,
                native_work_items: &mut tables.graph.native_work_items,
            };
            apply_method_rows_ref(graph_fname, method, &mut tables, crypto)
        }
    }
}

fn apply_native_operation_rows_loop(
    input: NativeOperationLoopInput<'_, '_>,
    tables: &mut NativeOperationTables<'_>,
) -> Result<(), String> {
    let NativeOperationLoopInput {
        write,
        graph_fname,
        batch,
        committed_at_ms,
        crypto,
        #[cfg(feature = "security")]
        staged_audit_tail,
        generated_result,
        crossmodal_present,
    } = input;
    for operation in &batch.operations {
        apply_one_native_operation_row(
            NativeOperationContext {
                write,
                graph_fname,
                batch,
                operation,
                committed_at_ms,
                crypto,
                generated_result,
                crossmodal_present,
            },
            tables,
        )?;
        #[cfg(feature = "security")]
        append_audit_entry(
            &mut tables.graph.audit,
            staged_audit_tail,
            graph_fname,
            &operation.method,
        )?;
    }
    Ok(())
}

pub(crate) fn apply_native_operations(
    ctx: &MutationRowCtx<'_>,
    options: NativeOperationOptions<'_>,
) -> Result<(), String> {
    let MutationRowCtx {
        write,
        graph_fname,
        batch,
        crypto,
    } = *ctx;
    let NativeOperationOptions {
        committed_at_ms,
        #[cfg(feature = "security")]
        staged_audit_tail,
        generated_result,
        crossmodal_present,
    } = options;
    let mut tables = NativeOperationTables::open(write, graph_fname)?;
    validate_native_operations_commit_work_item_result_shape(batch)?;
    apply_native_operation_rows_loop(
        NativeOperationLoopInput {
            write,
            graph_fname,
            batch,
            committed_at_ms,
            crypto,
            #[cfg(feature = "security")]
            staged_audit_tail,
            generated_result,
            crossmodal_present,
        },
        &mut tables,
    )?;

    // The compact graph/control path can replace or remove a linked
    // WorkItem just as a snapshot/row-delta can.  Release the ordinary
    // table guards, then run the same lane lifecycle validator inside this
    // write transaction before status/outbox metadata is staged.
    drop(tables);
    development_lane::validate_current_lane_links_in_wtx(write, graph_fname, crypto)?;
    Ok(())
}
