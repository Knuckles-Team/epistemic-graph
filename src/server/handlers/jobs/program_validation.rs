//! Private implementation module for the analytics job handler.

use super::prelude_jobs::*;
use super::prelude_server::*;
use super::prelude_std::*;
use super::*;

#[cfg(feature = "program-optimization")]
/// Whether a reference string is an opaque, governed reference id.
pub(super) fn governed_ref(value: &str) -> bool {
    OpaqueRef::new(value.to_string()).is_ok()
}

/// `field` is present, an array, and (unless `allow_empty`) non-empty, with
/// every element a governed reference string.
pub(super) fn row_valid_refs(
    row: &BTreeMap<String, serde_json::Value>,
    field: &str,
    allow_empty: bool,
) -> bool {
    row.get(field)
        .and_then(serde_json::Value::as_array)
        .is_some_and(|values| {
            (allow_empty || !values.is_empty())
                && values
                    .iter()
                    .all(|value| value.as_str().is_some_and(governed_ref))
        })
}

/// `field` is present and either JSON `null` or a governed reference string.
pub(super) fn row_valid_optional_ref(
    row: &BTreeMap<String, serde_json::Value>,
    field: &str,
) -> bool {
    row.get(field)
        .is_some_and(|value| value.is_null() || value.as_str().is_some_and(governed_ref))
}

/// `field` is present, an array, and empty.
pub(super) fn row_empty_list(row: &BTreeMap<String, serde_json::Value>, field: &str) -> bool {
    row.get(field)
        .and_then(serde_json::Value::as_array)
        .is_some_and(|values| values.is_empty())
}

/// `field` is present, an array with exactly one element, and that element is
/// one of `allowed`.
pub(super) fn row_single_label(
    row: &BTreeMap<String, serde_json::Value>,
    field: &str,
    allowed: &std::collections::BTreeSet<&str>,
) -> bool {
    row.get(field)
        .and_then(serde_json::Value::as_array)
        .is_some_and(|values| {
            values.len() == 1
                && values[0]
                    .as_str()
                    .is_some_and(|value| allowed.contains(value))
        })
}

/// `modalities` is present, a non-empty array, with every element one of the
/// known `ProgramModality` strings.
pub(super) fn row_valid_modalities(
    row: &BTreeMap<String, serde_json::Value>,
    modalities: &std::collections::BTreeSet<&str>,
) -> bool {
    row.get("modalities")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|values| {
            !values.is_empty()
                && values
                    .iter()
                    .all(|value| modality_value_allowed(value, modalities))
        })
}

fn modality_value_allowed(
    value: &serde_json::Value,
    modalities: &std::collections::BTreeSet<&str>,
) -> bool {
    value
        .as_str()
        .is_some_and(|value| modalities.contains(value))
}

/// The `composition_refs` shape a `candidate_role` requires: empty for a
/// `proposal`, empty + `selected == false` for an `ensemble_member`, and a
/// non-empty governed list for an `ensemble`.
pub(super) fn row_valid_candidate_role(
    row: &BTreeMap<String, serde_json::Value>,
    candidate_role: Option<&str>,
) -> bool {
    let selected = row.get("selected").and_then(serde_json::Value::as_bool);
    match candidate_role {
        Some("proposal") => row_empty_list(row, "composition_refs"),
        Some("ensemble_member") => {
            row_empty_list(row, "composition_refs") && selected == Some(false)
        }
        Some("ensemble") => row_valid_refs(row, "composition_refs", false),
        _ => false,
    }
}

/// The plan-step-kind/plan-executor pairing is one of the governed
/// combinations for a `program_optimization_plan_step` row.
pub(super) fn row_valid_step_executor(
    plan_step_kind: Option<&str>,
    plan_executor: Option<&str>,
) -> bool {
    matches!(
        (plan_step_kind, plan_executor),
        (Some("query_similarity"), Some("graph_similarity"))
            | (
                Some(
                    "propose_instruction"
                        | "compare_tool_use"
                        | "propose_rules"
                        | "reflect_on_trace"
                        | "pareto_reflect"
                ),
                Some("model_transport")
            )
            | (Some("compose_programs"), Some("native_kernel"))
            | (Some("train_weights"), Some("trainer"))
            | (Some("evaluate_candidates"), Some("evaluator"))
    )
}

/// The `tool_policy_ref`/`instruction_ref`/`artifact_refs` binding a row's
/// `optimizer` requires: `avatar` must bind a governed, artifact-listed tool
/// policy and a null `instruction_ref`; any other optimizer must have a null
/// `tool_policy_ref`; a missing optimizer is never valid.
pub(super) fn row_valid_tool_policy_binding(row: &BTreeMap<String, serde_json::Value>) -> bool {
    match row.get("optimizer").and_then(serde_json::Value::as_str) {
        Some("avatar") => {
            row.get("tool_policy_ref")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|reference| {
                    governed_ref(reference) && artifact_refs_contain(row, reference)
                })
                && row
                    .get("instruction_ref")
                    .is_some_and(serde_json::Value::is_null)
        }
        Some(_) => row
            .get("tool_policy_ref")
            .is_some_and(serde_json::Value::is_null),
        None => false,
    }
}

fn artifact_refs_contain(row: &BTreeMap<String, serde_json::Value>, reference: &str) -> bool {
    row.get("artifact_refs")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|values| values.iter().any(|value| value.as_str() == Some(reference)))
}

/// The `program_candidate` role/tool/plan-ref shape: kind, candidate role
/// membership, its role-specific `composition_refs` shape, tool-policy
/// binding, and a null `plan_ref`.
pub(super) fn row_candidate_role_shape(
    row: &BTreeMap<String, serde_json::Value>,
    kind: Option<&str>,
    candidate_role: Option<&str>,
    candidate_roles: &std::collections::BTreeSet<&str>,
) -> bool {
    kind == Some("program_candidate")
        && candidate_role.is_some_and(|value| candidate_roles.contains(value))
        && row_valid_candidate_role(row, candidate_role)
        && row_valid_tool_policy_binding(row)
        && row.get("plan_ref").is_some_and(serde_json::Value::is_null)
}

/// The `program_candidate` reference/plan-field shape: demonstration/artifact
/// refs, the plan-only fields all empty, and a null `max_operations`.
pub(super) fn row_candidate_field_shape(row: &BTreeMap<String, serde_json::Value>) -> bool {
    row_valid_refs(row, "demonstration_refs", false)
        && row_valid_refs(row, "artifact_refs", true)
        && row_valid_refs(row, "composition_refs", true)
        && row_empty_list(row, "plan_step_kinds")
        && row_empty_list(row, "plan_executors")
        && row_empty_list(row, "plan_input_refs")
        && row_empty_list(row, "plan_output_refs")
        && row_empty_list(row, "plan_depends_on")
        && row
            .get("max_operations")
            .is_some_and(serde_json::Value::is_null)
}

/// Whether a row is shaped as a governed `program_candidate`.
pub(super) fn row_candidate_shape(
    row: &BTreeMap<String, serde_json::Value>,
    kind: Option<&str>,
    candidate_role: Option<&str>,
    candidate_roles: &std::collections::BTreeSet<&str>,
) -> bool {
    row_candidate_role_shape(row, kind, candidate_role, candidate_roles)
        && row_candidate_field_shape(row)
}

/// The `program_optimization_plan_step` identity shape: kind, a null
/// candidate role, a governed `plan_ref`, and null instruction/tool/model refs.
pub(super) fn row_plan_identity_shape(
    row: &BTreeMap<String, serde_json::Value>,
    kind: Option<&str>,
) -> bool {
    kind == Some("program_optimization_plan_step")
        && row
            .get("candidate_role")
            .is_some_and(serde_json::Value::is_null)
        && row
            .get("plan_ref")
            .and_then(serde_json::Value::as_str)
            .is_some_and(governed_ref)
        && row
            .get("instruction_ref")
            .is_some_and(serde_json::Value::is_null)
        && row
            .get("tool_policy_ref")
            .is_some_and(serde_json::Value::is_null)
        && row
            .get("model_profile_ref")
            .is_some_and(serde_json::Value::is_null)
}

/// The `program_optimization_plan_step` step shape: candidate-only refs
/// empty, a single governed plan-step-kind/executor label each, and a
/// governed step/executor pairing.
pub(super) fn row_plan_step_shape(
    row: &BTreeMap<String, serde_json::Value>,
    plan_step_kinds: &std::collections::BTreeSet<&str>,
    plan_executors: &std::collections::BTreeSet<&str>,
) -> bool {
    let plan_step_kind = row
        .get("plan_step_kinds")
        .and_then(serde_json::Value::as_array)
        .and_then(|values| values.first())
        .and_then(serde_json::Value::as_str);
    let plan_executor = row
        .get("plan_executors")
        .and_then(serde_json::Value::as_array)
        .and_then(|values| values.first())
        .and_then(serde_json::Value::as_str);
    row_empty_list(row, "demonstration_refs")
        && row_empty_list(row, "artifact_refs")
        && row_empty_list(row, "composition_refs")
        && row_single_label(row, "plan_step_kinds", plan_step_kinds)
        && row_single_label(row, "plan_executors", plan_executors)
        && row_valid_step_executor(plan_step_kind, plan_executor)
}

/// The `program_optimization_plan_step` I/O shape: input/output/depends-on
/// refs, a positive `max_operations`, and `selected == false`.
pub(super) fn row_plan_io_shape(
    row: &BTreeMap<String, serde_json::Value>,
    selected: Option<bool>,
) -> bool {
    row_valid_refs(row, "plan_input_refs", false)
        && row_valid_refs(row, "plan_output_refs", false)
        && row_valid_refs(row, "plan_depends_on", true)
        && row
            .get("max_operations")
            .and_then(serde_json::Value::as_u64)
            .is_some_and(|value| value > 0)
        && selected == Some(false)
}

/// Whether a row is shaped as a governed `program_optimization_plan_step`.
pub(super) fn row_plan_shape(
    row: &BTreeMap<String, serde_json::Value>,
    kind: Option<&str>,
    selected: Option<bool>,
    plan_step_kinds: &std::collections::BTreeSet<&str>,
    plan_executors: &std::collections::BTreeSet<&str>,
) -> bool {
    row_plan_identity_shape(row, kind)
        && row_plan_step_shape(row, plan_step_kinds, plan_executors)
        && row_plan_io_shape(row, selected)
}

/// The id/program_ref/optimizer/execution/instruction/tool/model reference
/// checks every governed row must pass, independent of candidate/plan shape.
pub(super) fn row_governed_identity_and_type(
    row: &BTreeMap<String, serde_json::Value>,
    optimizers: &std::collections::BTreeSet<&str>,
    executions: &std::collections::BTreeSet<&str>,
) -> bool {
    row.get("id")
        .and_then(serde_json::Value::as_str)
        .is_some_and(governed_ref)
        && row
            .get("program_ref")
            .and_then(serde_json::Value::as_str)
            .is_some_and(governed_ref)
        && row
            .get("optimizer")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| optimizers.contains(value))
        && row
            .get("execution")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| executions.contains(value))
        && row_valid_optional_ref(row, "instruction_ref")
        && row_valid_optional_ref(row, "tool_policy_ref")
        && row_valid_optional_ref(row, "model_profile_ref")
}

/// The confidence/selected/evidence-and-proof-ref checks every governed row
/// must pass, independent of candidate/plan shape.
pub(super) fn row_governed_evidence_and_confidence(
    row: &BTreeMap<String, serde_json::Value>,
    selected: Option<bool>,
) -> bool {
    row.get("confidence")
        .and_then(serde_json::Value::as_f64)
        .is_some_and(|value| (0.0..=1.0).contains(&value))
        && selected.is_some()
        && row_valid_refs(row, "evidence_refs", false)
        && row_valid_refs(row, "source_refs", false)
        && row_valid_refs(row, "proof_ids", true)
        && row_valid_refs(row, "contradiction_ids", true)
}

/// Static governance vocabulary [`validate_program_result_row`] checks each
/// row against — computed once per [`validate_program_result_privacy`] call.
pub(super) struct RowGovernanceCtx<'a> {
    expected: &'a std::collections::BTreeSet<&'a str>,
    modalities: &'a std::collections::BTreeSet<&'a str>,
    optimizers: &'a std::collections::BTreeSet<&'a str>,
    executions: &'a std::collections::BTreeSet<&'a str>,
    candidate_roles: &'a std::collections::BTreeSet<&'a str>,
    plan_step_kinds: &'a std::collections::BTreeSet<&'a str>,
    plan_executors: &'a std::collections::BTreeSet<&'a str>,
    policy: &'a PolicyEnvelope,
}

/// Whether one program-result row VIOLATES governance (mirrors the original
/// inline `if <bad> { return Err }` condition verbatim, just relocated —
/// `true` means the caller should reject the whole result).
pub(super) fn validate_program_result_row(
    row: &BTreeMap<String, serde_json::Value>,
    ctx: &RowGovernanceCtx<'_>,
) -> bool {
    let row_fields = row
        .keys()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    let valid_modalities = row_valid_modalities(row, ctx.modalities);
    let kind = row.get("kind").and_then(serde_json::Value::as_str);
    let selected = row.get("selected").and_then(serde_json::Value::as_bool);
    let candidate_role = row
        .get("candidate_role")
        .and_then(serde_json::Value::as_str);
    let candidate_shape = row_candidate_shape(row, kind, candidate_role, ctx.candidate_roles);
    let plan_shape = row_plan_shape(row, kind, selected, ctx.plan_step_kinds, ctx.plan_executors);
    row_fields != *ctx.expected
        || (!candidate_shape && !plan_shape)
        || !row_governed_identity_and_type(row, ctx.optimizers, ctx.executions)
        || !row_governed_evidence_and_confidence(row, selected)
        || !row_valid_policy_binding(row, kind, ctx.policy)
        || !valid_modalities
}

/// The policy column is an immutable candidate binding. Candidate rows must
/// carry the exact policy that was rebound onto the program request; plan rows
/// cannot carry a policy because they are not executable candidates.
pub(super) fn row_valid_policy_binding(
    row: &BTreeMap<String, serde_json::Value>,
    kind: Option<&str>,
    expected_policy: &PolicyEnvelope,
) -> bool {
    match kind {
        Some("program_candidate") => {
            let Some(value) = row.get("policy") else {
                return false;
            };
            let Some(object) = value.as_object() else {
                return false;
            };
            let fields = object
                .keys()
                .map(String::as_str)
                .collect::<std::collections::BTreeSet<_>>();
            let expected_fields = std::collections::BTreeSet::from([
                "tenant_ref",
                "access_policy_ref",
                "classification",
                "retention_policy_ref",
                "deletion_policy_ref",
                "legal_hold_ref",
                "purpose_refs",
            ]);
            if fields != expected_fields {
                return false;
            }
            let Ok(policy) = serde_json::from_value::<PolicyEnvelope>(value.clone()) else {
                return false;
            };
            policy == *expected_policy
                && policy.tenant_ref.namespace() == "tenant"
                && policy.access_policy_ref.namespace() == "policy"
                && policy.retention_policy_ref.namespace() == "retention"
                && policy.deletion_policy_ref.namespace() == "deletion"
                && policy.purpose_refs.len() <= eg_program::MAX_PURPOSE_REFS
                && policy
                    .purpose_refs
                    .iter()
                    .all(|reference| reference.namespace() == "purpose")
        }
        Some("program_optimization_plan_step") => {
            row.get("policy").is_some_and(serde_json::Value::is_null)
        }
        _ => false,
    }
}

pub(super) fn validate_program_result_privacy(
    result: &TypedJobResult,
    expected_policy: &PolicyEnvelope,
) -> Result<(), String> {
    let expected = std::collections::BTreeSet::from([
        "id",
        "kind",
        "confidence",
        "evidence_refs",
        "source_refs",
        "proof_ids",
        "contradiction_ids",
        "program_ref",
        "optimizer",
        "execution",
        "candidate_role",
        "demonstration_refs",
        "artifact_refs",
        "composition_refs",
        "instruction_ref",
        "tool_policy_ref",
        "model_profile_ref",
        "policy",
        "modalities",
        "plan_ref",
        "plan_step_kinds",
        "plan_executors",
        "plan_input_refs",
        "plan_output_refs",
        "plan_depends_on",
        "max_operations",
        "selected",
        "promotion_identity",
    ]);
    let actual = result
        .schema
        .iter()
        .map(|column| column.name.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let modalities = ProgramModality::ALL
        .into_iter()
        .map(ProgramModality::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    let optimizers = eg_program::OptimizerKind::ALL
        .into_iter()
        .map(eg_program::OptimizerKind::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    let executions = eg_program::OptimizerKind::ALL
        .into_iter()
        .map(|optimizer| optimizer.execution().as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let candidate_roles = ["proposal", "ensemble_member", "ensemble"]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    let plan_step_kinds = eg_program::PlanStepKind::ALL
        .into_iter()
        .map(eg_program::PlanStepKind::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    let plan_executors = eg_program::PlanExecutor::ALL
        .into_iter()
        .map(eg_program::PlanExecutor::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    if actual != expected
        || !result.evidence_refs.iter().all(|value| governed_ref(value))
        || !result
            .counterexample_refs
            .iter()
            .all(|value| governed_ref(value))
    {
        return Err("program result schema/references are not governed".to_string());
    }
    let ctx = RowGovernanceCtx {
        expected: &expected,
        modalities: &modalities,
        optimizers: &optimizers,
        executions: &executions,
        candidate_roles: &candidate_roles,
        plan_step_kinds: &plan_step_kinds,
        plan_executors: &plan_executors,
        policy: expected_policy,
    };
    for row in &result.rows {
        if validate_program_result_row(row, &ctx) {
            return Err("program result contains non-governed row data".to_string());
        }
    }
    Ok(())
}
