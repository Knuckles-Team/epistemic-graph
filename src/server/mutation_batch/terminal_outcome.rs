//! Atomic lowering for the optional terminal WorkItem outcome extension.
//!
//! A terminal receipt stays one native `CommitWorkItemResult` operation. Its
//! bound RunTrace/ToolCall/OutcomeEvaluation rows are applied by the native
//! WorkItem authority only after that operation's lease CAS succeeds, and its
//! RunEvent becomes one conditional mutation outbox intent.

use std::collections::BTreeMap;

use crate::mutation_batch::MutationOutboxIntent;
use crate::protocol::Method;

/// Lower the optional GOC-20/RF-020 terminal extension into the existing native
/// terminal shape. The first operation remains `CommitWorkItemResult`; receipt
/// rows stay inside its extension so the native applier can gate them on the
/// actual WorkItem transition, and exactly one conditional run-event intent is
/// supplied to `finish_batch` before the envelope is minted.
///
/// `batch_id` is the mutation kernel's native outbox identity. Requiring the
/// bundle to carry it closes the gap between a caller-supplied receipt label and
/// the durable row that the transaction actually publishes.
pub(super) fn lower_terminal_outcome_extensions(
    batch_id: &str,
    mut methods: Vec<Method>,
) -> Result<(Vec<Method>, Vec<MutationOutboxIntent>), String> {
    methods
        .iter_mut()
        .for_each(|method| bind_engine_fields(batch_id, method));
    let Some(extension) = terminal_extension(&methods)? else {
        return Ok((methods, Vec::new()));
    };
    validate_terminal_extension(batch_id, &methods, extension)?;
    let terminal_intent = run_event_intent(batch_id, extension)?;
    Ok((methods, vec![terminal_intent]))
}

/// graph-os EG-4: the outbox id and receipt digests are engine-derived; a
/// caller leaves them empty and the engine fills them here, before validation.
fn bind_engine_fields(batch_id: &str, method: &mut Method) {
    if let Method::CommitWorkItemResult {
        outcome_extension: Some(extension),
        ..
    } = method
    {
        extension.bind_engine_fields(batch_id);
    }
}

fn terminal_extension(
    methods: &[Method],
) -> Result<Option<&eg_types::outcome_bundle::TerminalOutcomeExtension>, String> {
    if let Some(Method::CommitWorkItemResult {
        outcome_extension: Some(extension),
        ..
    }) = methods.first()
    {
        return Ok(Some(extension));
    }
    if methods.iter().any(|method| {
        matches!(
            method,
            Method::CommitWorkItemResult {
                outcome_extension: Some(_),
                ..
            }
        )
    }) {
        return Err(
            "terminal outcome extension must be the first operation in its mutation batch"
                .to_string(),
        );
    }
    Ok(None)
}

fn validate_terminal_extension(
    batch_id: &str,
    methods: &[Method],
    extension: &eg_types::outcome_bundle::TerminalOutcomeExtension,
) -> Result<(), String> {
    validate_terminal_binding(batch_id, methods, extension)
}

fn validate_terminal_binding(
    batch_id: &str,
    methods: &[Method],
    extension: &eg_types::outcome_bundle::TerminalOutcomeExtension,
) -> Result<(), String> {
    let (terminal_work_item_id, terminal_fence_token, terminal_result_ref, terminal_outcome) =
        match methods.first() {
            Some(Method::CommitWorkItemResult {
                work_item_id,
                fencing_token,
                result_ref,
                outcome,
                ..
            }) => (
                work_item_id.as_str(),
                *fencing_token,
                result_ref,
                outcome.as_str(),
            ),
            _ => unreachable!("the extension guard selected a terminal operation"),
        };
    extension.validate()?;
    let bundle = &extension.outcome_bundle;
    if bundle.outcome != terminal_outcome {
        return Err(
            "terminal outcome bundle outcome does not match the terminal method".to_string(),
        );
    }
    if bundle.work_item_id != terminal_work_item_id {
        return Err(
            "terminal outcome bundle WorkItem id does not match the CAS target".to_string(),
        );
    }
    if bundle.fence_token != terminal_fence_token {
        return Err("terminal outcome bundle fence does not match the CAS target".to_string());
    }
    if &bundle.result_ref != terminal_result_ref {
        return Err(
            "terminal outcome bundle result_ref does not match the terminal result".to_string(),
        );
    }
    if bundle.outbox_id != batch_id {
        return Err(
            "terminal outcome bundle outbox_id must equal the native mutation batch id".to_string(),
        );
    }
    if methods.len() != 1 {
        return Err(
            "terminal outcome extension owns its receipt rows and must be the only operation"
                .to_string(),
        );
    }
    Ok(())
}

fn run_event_intent(
    batch_id: &str,
    extension: &eg_types::outcome_bundle::TerminalOutcomeExtension,
) -> Result<MutationOutboxIntent, String> {
    let bundle = &extension.outcome_bundle;
    let payload = rmp_serde::to_vec_named(&extension.run_event)
        .map_err(|error| format!("run event encoding failed: {error}"))?;
    let completeness = serde_json::to_value(bundle.completeness)
        .map_err(|error| format!("outcome completeness encoding failed: {error}"))?
        .as_str()
        .ok_or_else(|| "outcome completeness encoding was not a string".to_string())?
        .to_string();
    let missing_refs = serde_json::to_string(&bundle.missing_refs)
        .map_err(|error| format!("outcome missing_refs encoding failed: {error}"))?;
    let mut headers = BTreeMap::from([
        ("batch_id".to_string(), batch_id.to_string()),
        ("delegation_id".to_string(), bundle.delegation_id.clone()),
        ("delegator_id".to_string(), bundle.delegator_id.clone()),
        (
            "selected_agent_id".to_string(),
            bundle.selected_agent_id.clone(),
        ),
        (
            "executor_lease_actor".to_string(),
            bundle.executor_lease_actor.clone(),
        ),
        ("outcome".to_string(), bundle.outcome.clone()),
        ("work_item_id".to_string(), bundle.work_item_id.clone()),
        ("run_id".to_string(), bundle.run_id.clone()),
        ("fence_token".to_string(), bundle.fence_token.to_string()),
        (
            "capability_digest".to_string(),
            bundle.capability_digest.clone(),
        ),
        ("catalog_digest".to_string(), bundle.catalog_digest.clone()),
        ("policy_digest".to_string(), bundle.policy_digest.clone()),
        ("model_digest".to_string(), bundle.model_digest.clone()),
        ("completeness".to_string(), completeness),
        ("missing_refs".to_string(), missing_refs),
    ]);
    if let Some(result_ref) = &bundle.result_ref {
        headers.insert("result_ref".to_string(), result_ref.clone());
    }
    Ok(MutationOutboxIntent {
        topic: eg_types::outcome_bundle::RUN_EVENT_OUTBOX_TOPIC.to_string(),
        key: batch_id.to_string(),
        payload,
        headers,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_types::outcome_bundle::{
        CommitOutcomeBundle, OutcomeCompleteness, ReceiptNode, ReceiptNodeKind, RunEvent,
        TerminalOutcomeExtension, OUTCOME_BUNDLE_VERSION,
    };
    use sha2::Digest;

    fn digest(byte: char) -> String {
        byte.to_string().repeat(64)
    }

    fn receipt_node(
        bundle: &CommitOutcomeBundle,
        kind: ReceiptNodeKind,
        node_id: &str,
    ) -> ReceiptNode {
        let kind_name = match kind {
            ReceiptNodeKind::RunTrace => "run_trace",
            ReceiptNodeKind::ToolCall => "tool_call",
            ReceiptNodeKind::OutcomeEvaluation => "outcome_evaluation",
        };
        let properties = serde_json::json!({
            "node_id": node_id,
            "kind": kind_name,
            "delegation_id": bundle.delegation_id,
            "delegator_id": bundle.delegator_id,
            "selected_agent_id": bundle.selected_agent_id,
            "executor_lease_actor": bundle.executor_lease_actor,
            "outcome": bundle.outcome,
            "work_item_id": bundle.work_item_id,
            "run_id": bundle.run_id,
            "fence_token": bundle.fence_token,
            "result_ref": bundle.result_ref,
            "result_digest": bundle.result_digest,
            "event_sequence": bundle.event_sequence,
            "completeness": bundle.completeness,
            "missing_refs": bundle.missing_refs,
            "outbox_id": bundle.outbox_id,
            "payload_ref": format!("cas:receipt:{node_id}"),
            "capability_digest": bundle.capability_digest,
            "catalog_digest": bundle.catalog_digest,
            "policy_digest": bundle.policy_digest,
            "model_digest": bundle.model_digest,
            "payload": {"fixture": true},
        });
        let properties_msgpack = rmp_serde::to_vec_named(&properties).unwrap();
        ReceiptNode {
            node_id: node_id.into(),
            kind,
            delegation_id: bundle.delegation_id.clone(),
            work_item_id: bundle.work_item_id.clone(),
            run_id: bundle.run_id.clone(),
            fence_token: bundle.fence_token,
            result_ref: bundle.result_ref.clone(),
            outbox_id: bundle.outbox_id.clone(),
            payload_ref: format!("cas:receipt:{node_id}"),
            payload_digest: hex::encode(sha2::Sha256::digest(&properties_msgpack)),
            properties_msgpack,
        }
    }

    fn extension(outbox_id: &str) -> TerminalOutcomeExtension {
        extension_with(outbox_id, |_| {})
    }

    /// Build a terminal extension whose receipt nodes and run event are derived
    /// from the bundle AFTER `shape` has mutated it.
    ///
    /// A receipt node carries the bundle's authority/currency fields inside its
    /// own durable `properties_msgpack`, and the run event repeats them, so
    /// `TerminalOutcomeExtension::validate` requires all three to agree. Mutating
    /// the bundle of an already-built extension therefore leaves the receipts and
    /// the event bound to the PREVIOUS bundle and is rejected before any
    /// assertion about lowering can be reached -- so the shape has to be decided
    /// here, before the derived rows are minted.
    fn extension_with(
        outbox_id: &str,
        shape: impl FnOnce(&mut CommitOutcomeBundle),
    ) -> TerminalOutcomeExtension {
        let mut bundle = CommitOutcomeBundle {
            schema_version: OUTCOME_BUNDLE_VERSION,
            delegation_id: "delegation:test".into(),
            delegator_id: "agent:delegator-a".into(),
            selected_agent_id: "agent:selected-b".into(),
            executor_lease_actor: "worker:test".into(),
            outcome: "succeeded".into(),
            work_item_id: "work:test".into(),
            fence_token: 7,
            run_id: "run:test".into(),
            result_ref: Some("cas:result:test".into()),
            result_digest: Some(digest('a')),
            artifacts: Vec::new(),
            trace_ref: "trace:test".into(),
            tool_call_refs: vec!["toolcall:test:0".into()],
            outcome_ref: "outcome:test".into(),
            capability_digest: digest('b'),
            catalog_digest: digest('c'),
            policy_digest: digest('d'),
            model_digest: digest('e'),
            event_sequence: 1,
            completeness: OutcomeCompleteness::Complete,
            missing_refs: Vec::new(),
            outbox_id: outbox_id.into(),
            langfuse_observation_refs: Vec::new(),
        };
        shape(&mut bundle);
        let bundle = bundle;
        let receipt_nodes = vec![
            receipt_node(&bundle, ReceiptNodeKind::RunTrace, &bundle.trace_ref),
            receipt_node(
                &bundle,
                ReceiptNodeKind::ToolCall,
                &bundle.tool_call_refs[0],
            ),
            receipt_node(
                &bundle,
                ReceiptNodeKind::OutcomeEvaluation,
                &bundle.outcome_ref,
            ),
        ];
        let run_event = RunEvent {
            schema_version: OUTCOME_BUNDLE_VERSION,
            // Every identity/currency field below is READ OFF the shaped
            // bundle, never restated: `RunEvent::validate_for_bundle`
            // compares all of them, and a literal here would silently
            // unbind the event the moment a test reshapes the bundle.
            delegation_id: bundle.delegation_id.clone(),
            delegator_id: bundle.delegator_id.clone(),
            selected_agent_id: bundle.selected_agent_id.clone(),
            executor_lease_actor: bundle.executor_lease_actor.clone(),
            outcome: bundle.outcome.clone(),
            work_item_id: bundle.work_item_id.clone(),
            run_id: bundle.run_id.clone(),
            fence_token: bundle.fence_token,
            outbox_id: bundle.outbox_id.clone(),
            result_ref: bundle.result_ref.clone(),
            capability_digest: bundle.capability_digest.clone(),
            catalog_digest: bundle.catalog_digest.clone(),
            policy_digest: bundle.policy_digest.clone(),
            model_digest: bundle.model_digest.clone(),
            event_sequence: bundle.event_sequence,
            completeness: bundle.completeness,
            missing_refs: bundle.missing_refs.clone(),
            // A degraded bundle completes through the `degraded` event kind;
            // both kinds bind the same `outcome_ref` (see
            // `validate_for_bundle`'s completion-reference match).
            kind: match bundle.completeness {
                OutcomeCompleteness::Complete => "outcome".into(),
                _ => "degraded".to_string(),
            },
            tool_call_ref: None,
            outcome_ref: Some(bundle.outcome_ref.clone()),
            payload_digest: digest('f'),
            timestamp_ms: 10,
            cursor_token: "cursor:test:1".into(),
            carrier_digest: digest('0'),
        };
        TerminalOutcomeExtension {
            outcome_bundle: bundle,
            receipt_nodes,
            run_event,
        }
    }

    fn terminal(extension: TerminalOutcomeExtension) -> Method {
        Method::CommitWorkItemResult {
            tenant: "tenant:test".into(),
            work_item_id: "work:test".into(),
            worker_id: "worker:test".into(),
            lease_epoch: 1,
            fencing_token: 7,
            idempotency_key: "terminal:test".into(),
            outcome: "succeeded".into(),
            result_ref: Some("cas:result:test".into()),
            outcome_extension: Some(Box::new(extension)),
            error_ref: None,
            retryable: false,
            now_ms: 10,
        }
    }

    #[test]
    fn terminal_extension_stays_native_and_lowers_one_conditional_run_event() {
        let (methods, outbox) = lower_terminal_outcome_extensions(
            "batch:test",
            vec![terminal(extension("batch:test"))],
        )
        .unwrap();
        assert_eq!(methods.len(), 1);
        assert!(matches!(methods[0], Method::CommitWorkItemResult { .. }));
        assert_eq!(outbox.len(), 1);
        assert_eq!(
            outbox[0].topic,
            eg_types::outcome_bundle::RUN_EVENT_OUTBOX_TOPIC
        );
        assert_eq!(outbox[0].key, "batch:test");
        assert_eq!(outbox[0].headers["fence_token"], "7");
        assert_eq!(outbox[0].headers["outcome"], "succeeded");
        assert_eq!(outbox[0].headers["completeness"], "complete");
        assert_eq!(outbox[0].headers["missing_refs"], "[]");
    }

    #[test]
    fn an_extension_left_blank_is_bound_to_the_native_batch_by_the_engine() {
        let mut blank = extension("");
        for receipt in &mut blank.receipt_nodes {
            receipt.payload_digest.clear();
        }
        let (methods, outbox) =
            lower_terminal_outcome_extensions("batch:test", vec![terminal(blank)]).unwrap();
        assert_eq!(outbox[0].key, "batch:test");
        let Method::CommitWorkItemResult {
            outcome_extension: Some(bound),
            ..
        } = &methods[0]
        else {
            panic!("the terminal operation keeps its extension");
        };
        assert_eq!(bound.outcome_bundle.outbox_id, "batch:test");
        assert_eq!(bound.run_event.outbox_id, "batch:test");
        assert!(bound
            .receipt_nodes
            .iter()
            .all(|receipt| receipt.outbox_id == "batch:test"));
    }

    #[test]
    fn terminal_extension_rejects_unbound_native_batch_id() {
        let error = lower_terminal_outcome_extensions(
            "batch:actual",
            vec![terminal(extension("batch:claimed"))],
        )
        .unwrap_err();
        assert!(error.contains("native mutation batch id"));
    }

    #[test]
    fn terminal_extension_publishes_failed_cancelled_and_degraded_currency() {
        for outcome in ["failed", "cancelled"] {
            let extension = extension_with("batch:test", |bundle| bundle.outcome = outcome.into());
            let mut method = terminal(extension);
            if let Method::CommitWorkItemResult {
                outcome: method_outcome,
                ..
            } = &mut method
            {
                *method_outcome = outcome.into();
            }
            let (_, outbox) =
                lower_terminal_outcome_extensions("batch:test", vec![method]).unwrap();
            assert_eq!(outbox[0].headers["outcome"], outcome);
        }

        let extension = extension_with("batch:test", |bundle| {
            bundle.completeness = OutcomeCompleteness::Degraded;
            bundle.missing_refs = vec!["tool_call:test:1".into()];
        });
        let (_, outbox) =
            lower_terminal_outcome_extensions("batch:test", vec![terminal(extension)]).unwrap();
        assert_eq!(outbox[0].headers["completeness"], "degraded");
        assert_eq!(outbox[0].headers["missing_refs"], "[\"tool_call:test:1\"]");
    }

    #[test]
    fn terminal_extension_rejects_method_outcome_mismatch() {
        // The bundle (and therefore its receipts and run event) says `failed`
        // while `terminal` still builds a `succeeded` CommitWorkItemResult, so
        // the ONLY disagreement left is the one this test names. Reshaping an
        // already-built extension instead would unbind its receipts first and
        // fail on that, never reaching the method-outcome check.
        let extension = extension_with("batch:test", |bundle| bundle.outcome = "failed".into());
        let error =
            lower_terminal_outcome_extensions("batch:test", vec![terminal(extension)]).unwrap_err();
        assert!(
            error.contains("does not match the terminal method"),
            "{error}"
        );
    }
}
