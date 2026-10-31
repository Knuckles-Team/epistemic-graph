//! Evaluate-only explicit DAG planning. EG derives a plan; the host admits it.

use medulla_types::contract::BoundedVec;
use medulla_types::decision::statistical::{
    CandidateSource, DecideRequest, QuestionKind, QuestionSafety, StatisticalOutcome, TypedValue,
};
use medulla_types::decision::task_plan::{plan_explicit_tasks, task_bindings, TaskPlan, TaskPlanTask};
use medulla_types::decision::{PremiseClass, PremiseProvenance, PremiseRef};

use super::stat_support::invalid_parameter;

pub(super) fn task_plan(request: &DecideRequest) -> Result<Option<TaskPlan>, String> {
    if request.question.kind != QuestionKind::TaskPlan {
        if request
            .params
            .iter()
            .any(|p| matches!(p.value, TypedValue::TaskPlan(_)))
        {
            return Err(invalid_parameter(
                "task_plan parameter requires task_plan question",
            ));
        }
        return Ok(None);
    }
    if request.question.safety != QuestionSafety::Ordinary
        || request.head.is_some()
        || !request.belief_as_of.is_empty()
        || request.params.len() != 1
    {
        return Err(invalid_parameter(
            "task_plan needs ordinary safety, no head or belief slices, and one parameter",
        ));
    }
    let param = &request.params.as_slice()[0];
    let TypedValue::TaskPlan(proposal) = &param.value else {
        return Err(invalid_parameter(
            "task_plan requires a typed task_plan proposal",
        ));
    };
    if param.name != "task_plan" {
        return Err(invalid_parameter(
            "task_plan parameter must be named task_plan",
        ));
    }
    let plan = plan_explicit_tasks(proposal).map_err(invalid_parameter)?;
    let CandidateSource::Declared { options } = &request.candidates else {
        return Err(invalid_parameter("task_plan requires declared task ids"));
    };
    let mut task_ids: Vec<&str> = plan
        .tasks
        .iter()
        .map(|task| task.task_id.as_str())
        .collect();
    task_ids.sort();
    if options
        .iter()
        .map(|o| o.option_id.as_str())
        .collect::<Vec<_>>()
        != task_ids
        || options
            .iter()
            .any(|o| !o.classification.is_empty() || !o.numbers.is_empty() || !o.texts.is_empty())
    {
        return Err(invalid_parameter(
            "declared candidates must be exactly the sorted task ids without facts",
        ));
    }
    Ok(Some(plan))
}

pub(super) fn outcome(_plan: &TaskPlan) -> StatisticalOutcome {
    // A canonical plan is not a calibrated score or an authorization grant.
    StatisticalOutcome::Advisory {
        scores: BoundedVec::default(),
        calibrated: false,
    }
}

/// A tool step's call and each binding are caller claims; neither grants
/// authority to invoke the tool (MEDULLA-DECISION-ENGINE-R134.111.11).
fn call_premises(task: &TaskPlanTask) -> Vec<PremiseRef> {
    let Some(call) = &task.call else {
        return Vec::new();
    };
    let bindings = task_bindings(task);
    let claim = |fact: String, field: &str| PremiseRef {
        subject: task.task_id.clone(),
        fact,
        class: PremiseClass::Claim,
        provenance: PremiseProvenance::Request {
            field: field.to_string(),
        },
    };
    let references: Vec<&str> = bindings.iter().map(|b| b.reference.as_str()).collect();
    let mut out = vec![claim(
        format!(
            "task_plan_call:server={};tool={};bindings={}",
            call.server,
            call.tool,
            references.join(",")
        ),
        "params.task_plan.tasks.call",
    )];
    out.extend(bindings.iter().map(|binding| {
        claim(
            format!(
                "task_plan_binding:pointer={};source_task={};reference={}",
                binding.pointer, binding.source_task, binding.reference
            ),
            "params.task_plan.tasks.call.arguments",
        )
    }));
    out
}

pub(super) fn premises(plan: &TaskPlan) -> Vec<PremiseRef> {
    let mut out: Vec<PremiseRef> = plan
        .tasks
        .iter()
        .map(|task| PremiseRef {
            subject: task.task_id.clone(),
            fact: format!(
                "task_plan_claim:dependencies={};prompt_digest={}",
                task.depends_on.as_slice().join(","),
                medulla_types::decision::digest::digest_text("eg/task-plan-prompt/v1", &task.prompt)
            ),
            class: PremiseClass::Claim,
            provenance: PremiseProvenance::Request {
                field: "params.task_plan.tasks".to_string(),
            },
        })
        .collect();
    for task in plan.tasks.iter() {
        out.extend(call_premises(task));
    }
    out.push(PremiseRef {
        subject: plan.policy_revision.clone(),
        fact: format!(
            "bounded_explicit_dag:max_steps={};max_concurrency={};plan_digest={}",
            plan.max_steps, plan.max_concurrency, plan.plan_digest
        ),
        class: PremiseClass::Definition,
        provenance: PremiseProvenance::Policy {
            policy_digest: plan.policy_digest.clone(),
        },
    });
    out
}
