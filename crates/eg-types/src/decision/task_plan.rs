//! Bounded, explicit dependency planning (MEDULLA-DECISION-ENGINE-R134.111).
//!
//! Prompts and edges are caller claims, never authorization or inferred policy.
//! The engine canonicalizes a DAG; Arbora admits it and AU executes it.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::contract::BoundedVec;

use super::digest::digest_text;

mod call;
pub use call::{
    task_bindings, TaskPlanBinding, TaskPlanCall, BINDING_KEY, MAX_BINDINGS_PER_CALL,
    MAX_BINDING_PATH_BYTES, MAX_BINDING_PATH_SEGMENTS, MAX_CALL_ARGUMENT_BYTES, MAX_TOOL_STEPS,
};

pub const TASK_PLAN_POLICY_REVISION: &str = "bounded-explicit-dag-v1";

/// The Agent Library component id a host publishes the canonical task-plan
/// feature schema under (MEDULLA-DECISION-ENGINE-R134.111.7).
pub const TASK_PLAN_FEATURE_SCHEMA_COMPONENT_ID: &str = "task-plan-features";

/// The canonical feature schema a `task_plan` Decide request pins.
///
/// A task-plan decision resolves by constraint and never builds a feature
/// matrix, but every Decide request pins a published `FeatureSchema`. One
/// engine-defined body means every host publishes the same bytes, so one
/// tenant's pin is reproducible and checkable at start-up. The single feature
/// reads each task's declared position and abstains when it is absent. Two
/// more describe a tool step's call and its binding count
/// (MEDULLA-DECISION-ENGINE-R134.111.12).
pub fn task_plan_feature_schema() -> super::statistical::features::FeatureSchemaBody {
    use super::statistical::features::{
        FeatureKind, FeatureSchemaBody, FeatureSpec, MissingValue, FEATURE_SCHEMA_VERSION,
    };
    FeatureSchemaBody {
        schema_version: FEATURE_SCHEMA_VERSION,
        features: BoundedVec::new(
            ["task_position", "task_call", "task_call_bindings"]
                .into_iter()
                .map(|name| FeatureSpec {
                    name: name.to_string(),
                    kind: FeatureKind::Number {
                        key: name.to_string(),
                    },
                    missing: MissingValue::Abstain,
                })
                .collect(),
        )
        .expect("three features are within the schema bound"),
    }
}
pub const MAX_TASKS: u16 = 64;
pub const MAX_CONCURRENCY: u16 = 8;
pub const MAX_PROMPT_BYTES: usize = 16 * 1024;
pub const MAX_TOTAL_PROMPT_BYTES: usize = 64 * 1024;

/// A prompt step uses the host's already-bound agent. It cannot select
/// credentials, models or a different principal. A task with `call` is a tool
/// step: no model runs it and `prompt` is only its label. The call is a caller
/// claim and grants no authority to invoke the tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TaskPlanTask {
    pub task_id: String,
    pub prompt: String,
    #[serde(default)]
    pub depends_on: BoundedVec<String, 64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub call: Option<TaskPlanCall>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TaskPlanProposal {
    pub tasks: BoundedVec<TaskPlanTask, 64>,
    pub output_task_id: String,
    pub max_concurrency: u16,
    pub max_steps: u16,
}

/// Canonical task order is a topological order with lexical ready-task ties.
/// The input digest retains the submitted order; the plan digest binds the
/// canonical executable body and policy, independent of declaration order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TaskPlan {
    pub tasks: BoundedVec<TaskPlanTask, 64>,
    pub output_task_id: String,
    pub max_concurrency: u16,
    pub max_steps: u16,
    pub total_tasks: u16,
    pub policy_revision: String,
    pub policy_digest: String,
    pub input_digest: String,
    pub plan_digest: String,
}

/// The policy digest includes every engine bound and ordering rule.
pub fn task_plan_policy_digest() -> String {
    digest_text(
        "eg/task-plan-policy/v1",
        &(
            TASK_PLAN_POLICY_REVISION,
            MAX_TASKS,
            MAX_CONCURRENCY,
            MAX_PROMPT_BYTES,
            MAX_TOTAL_PROMPT_BYTES,
            (
                MAX_TOOL_STEPS,
                MAX_BINDINGS_PER_CALL,
                MAX_CALL_ARGUMENT_BYTES,
                MAX_BINDING_PATH_SEGMENTS,
                MAX_BINDING_PATH_BYTES,
            ),
            "lexical_ready_ids;sorted_dependencies;single_output_sink;no_retries;\
             canonical_calls;direct_dependency_bindings;no_inferred_edges",
        ),
    )
}

fn error(code: &str, detail: impl std::fmt::Display) -> String {
    format!("{code}: {detail}")
}

fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.:-".contains(&b))
}

fn validate_plan_bounds(proposal: &TaskPlanProposal) -> Result<(), String> {
    if proposal.tasks.is_empty() {
        return Err(error("empty_plan", "at least one task is required"));
    }
    if proposal.max_steps == 0
        || proposal.max_steps > MAX_TASKS
        || proposal.tasks.len() > usize::from(proposal.max_steps)
        || proposal.max_concurrency == 0
        || proposal.max_concurrency > MAX_CONCURRENCY.min(proposal.max_steps)
    {
        return Err(error("plan_bounds", "invalid task or concurrency ceiling"));
    }
    Ok(())
}

fn canonical_task(task: &TaskPlanTask, bytes: &mut usize) -> Result<TaskPlanTask, String> {
    if !valid_id(&task.task_id) {
        return Err(error("invalid_task_id", &task.task_id));
    }
    *bytes += task.prompt.len(); // at most 64 bounded prompt lengths below
    if task.prompt.trim().is_empty()
        || task.prompt.len() > MAX_PROMPT_BYTES
        || *bytes > MAX_TOTAL_PROMPT_BYTES
    {
        return Err(error("plan_text_bounds", &task.task_id));
    }
    let mut canonical = task.clone();
    if let Some(raw) = &task.call {
        let (call, _, call_bytes) = call::canonical_call(&task.task_id, raw)?;
        *bytes += call_bytes;
        if *bytes > MAX_TOTAL_PROMPT_BYTES {
            return Err(error("plan_text_bounds", &task.task_id));
        }
        canonical.call = Some(call);
    }
    let mut deps = task.depends_on.as_slice().to_vec();
    deps.sort();
    if deps.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(error("duplicate_dependency", &task.task_id));
    }
    canonical.depends_on = BoundedVec::new(deps).expect("same bounded length");
    Ok(canonical)
}

fn validate_dependencies(tasks: &BTreeMap<String, TaskPlanTask>) -> Result<(), String> {
    for task in tasks.values() {
        for dependency in task.depends_on.iter() {
            if !tasks.contains_key(dependency) {
                return Err(error("missing_dependency", dependency));
            }
        }
    }
    Ok(())
}

fn index(proposal: &TaskPlanProposal) -> Result<BTreeMap<String, TaskPlanTask>, String> {
    validate_plan_bounds(proposal)?;
    let mut tasks = BTreeMap::new();
    let mut bytes = 0usize;
    for task in proposal.tasks.iter() {
        let canonical = canonical_task(task, &mut bytes)?;
        if tasks.insert(task.task_id.clone(), canonical).is_some() {
            return Err(error("duplicate_task_id", &task.task_id));
        }
    }
    validate_dependencies(&tasks)?;
    Ok(tasks)
}

fn ordered(tasks: &BTreeMap<String, TaskPlanTask>) -> Result<Vec<TaskPlanTask>, String> {
    let mut finished = BTreeSet::new();
    let mut output = Vec::with_capacity(tasks.len());
    while output.len() < tasks.len() {
        let next = tasks.values().find(|task| {
            !finished.contains(&task.task_id)
                && task.depends_on.iter().all(|id| finished.contains(id))
        });
        let Some(task) = next else {
            return Err(error("plan_cycle", "dependency graph is cyclic"));
        };
        finished.insert(task.task_id.clone());
        output.push(task.clone());
    }
    Ok(output)
}

fn check_output(tasks: &BTreeMap<String, TaskPlanTask>, output: &str) -> Result<(), String> {
    if !tasks.contains_key(output) {
        return Err(error("invalid_output_task", output));
    }
    let mut covered = BTreeSet::new();
    let mut pending = vec![output];
    while let Some(id) = pending.pop() {
        if covered.insert(id) {
            pending.extend(tasks[id].depends_on.iter().map(String::as_str));
        }
    }
    if covered.len() != tasks.len() {
        return Err(error(
            "disconnected_output",
            "every task must feed the output sink",
        ));
    }
    Ok(())
}

/// Derive a bounded executable DAG from explicit caller claims. No model,
/// external service, mutable state or authorization mechanism is consulted.
pub fn plan_explicit_tasks(proposal: &TaskPlanProposal) -> Result<TaskPlan, String> {
    let tasks = index(proposal)?;
    let canonical = ordered(&tasks)?;
    call::check_bindings(&tasks)?;
    check_output(&tasks, &proposal.output_task_id)?;
    let mut plan = TaskPlan {
        total_tasks: canonical.len() as u16,
        tasks: BoundedVec::new(canonical).expect("validated task bound"),
        output_task_id: proposal.output_task_id.clone(),
        max_concurrency: proposal.max_concurrency,
        max_steps: proposal.max_steps,
        policy_revision: TASK_PLAN_POLICY_REVISION.to_string(),
        policy_digest: task_plan_policy_digest(),
        input_digest: digest_text("eg/task-plan-input/v1", proposal),
        plan_digest: String::new(),
    };
    plan.plan_digest = digest_text(
        "eg/task-plan/v1",
        &(
            &plan.tasks,
            &plan.output_task_id,
            plan.max_concurrency,
            plan.max_steps,
            plan.total_tasks,
            &plan.policy_revision,
            &plan.policy_digest,
        ),
    );
    Ok(plan)
}

#[cfg(test)]
mod tests;
