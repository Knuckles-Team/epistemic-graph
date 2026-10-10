//! Swarm topology through the assembly decision (SWARM-TOPOLOGY-DECIDE-DESIGN
//! §13): the plan, the planted refusals, and a brute-force oracle over every
//! (width, rounds) choice.

use eg_types::capacity_lease::CapacityResourceClass;
use eg_types::decision::{
    AbstainReason, CapacityHeadroom, DecisionErrorCode, DecisionOutcome, DecisionPolicy,
    EvidenceClass, SchemaAuthority, SubagentFallback, TemplateFacts, TopologyAdmission,
    TopologyFacts, TopologyInputs, TopologyPlan, TopologyPolicy, TopologyRequirements,
    TOPOLOGY_DECISION_RECORD_SCHEMA_VERSION,
};
use eg_types::test_support::topology::{facts, requirements, FAN_OUT_JOIN, INDEPENDENT_SUBTASKS};

use super::super::{assemble, inputs as build_inputs, replay_check, AssembleError, Assembly};
use super::fixture::*;
use super::template::{reference, template};

fn fan(topology: TopologyFacts) -> TemplateFacts {
    let mut fan = template("fan", &["lead", "worker"]);
    fan.topology = Some(topology);
    fan
}

fn admitted() -> Vec<TopologyAdmission> {
    vec![TopologyAdmission {
        task_class: INDEPENDENT_SUBTASKS.to_string(),
        topology_class: FAN_OUT_JOIN.to_string(),
        admit_class: format!("{FAN_OUT_JOIN}Admission"),
        source_key: "swarm-topology".to_string(),
        authority: SchemaAuthority::Admin,
        axioms: bounded(Vec::new()),
    }]
}

fn read(available: u64, admissions: Vec<TopologyAdmission>) -> TopologyInputs {
    TopologyInputs {
        schema_digest: format!("sha256:{}", "ab".repeat(32)),
        admissions: bounded(admissions),
        verify_required_by: bounded(Vec::new()),
        headroom: bounded(vec![CapacityHeadroom {
            cell_id: "cell-llm".to_string(),
            class: CapacityResourceClass::LlmGenerator,
            available,
            epoch: 3,
        }]),
    }
}

fn decide_with(
    topology: TopologyFacts,
    asked: TopologyRequirements,
    read: TopologyInputs,
    policy: DecisionPolicy,
) -> Result<Assembly, AssembleError> {
    let templates = vec![fan(topology)];
    let mut request = request(&["eg:task/research"], &[]);
    request.templates = bounded(templates.iter().map(reference).collect());
    request.requirements.topology = Some(asked);
    let mut inputs = build_inputs(request, research_library(), templates, policy)?;
    inputs.topology = Some(read);
    assemble(inputs, identity())
}

fn decide(asked: TopologyRequirements, read: TopologyInputs) -> Assembly {
    decide_with(facts(), asked, read, DecisionPolicy::engine_default()).expect("decides")
}

fn plan(assembly: &Assembly) -> &TopologyPlan {
    assembly
        .record
        .topology_plan()
        .unwrap_or_else(|| panic!("a topology plan: {:?}", assembly.record.outcome))
}

fn widths(plan: &TopologyPlan) -> Vec<(String, u8)> {
    plan.slots
        .iter()
        .map(|slot| (slot.node_id.clone(), slot.width))
        .collect()
}

fn infeasible_labels(assembly: &Assembly) -> Vec<String> {
    let DecisionOutcome::Abstained { reasons } = &assembly.record.outcome else {
        panic!("expected an abstention, got {:?}", assembly.record.outcome);
    };
    reasons
        .iter()
        .flat_map(|reason| match reason {
            AbstainReason::Infeasible { constraints } => constraints.as_slice().to_vec(),
            _ => Vec::new(),
        })
        .collect()
}

// spec: EG-DECISION-ENGINE-R046
#[test]
fn demand_sizes_the_fan_out_and_the_plan_leases_it() {
    let assembly = decide(requirements(4), read(10, admitted()));
    let plan = plan(&assembly);
    assert_eq!(
        widths(plan),
        vec![("lead".to_string(), 1), ("worker".to_string(), 4)]
    );
    assert_eq!(plan.lease.per_cell.len(), 1);
    assert_eq!(plan.lease.per_cell.as_slice()[0].amount, 5);
    assert_eq!(plan.makespan_ms, Some(100 + 200));
    assert_eq!(
        assembly.record.schema_version,
        TOPOLOGY_DECISION_RECORD_SCHEMA_VERSION
    );
    assert_eq!(assembly.record.evidence_class, EvidenceClass::Claim);
    replay_check(&assembly.record).expect("a topology record replays byte for byte");
}

// spec: EG-DECISION-ENGINE-R046
#[test]
fn headroom_below_demand_abstains_naming_the_cell_lease() {
    let assembly = decide(requirements(4), read(3, admitted()));
    let labels = infeasible_labels(&assembly);
    assert!(
        labels
            .iter()
            .any(|label| label == "lease:cell-llm:llm_generator"),
        "{labels:?}"
    );
}

#[test]
fn a_request_that_widens_the_policy_caps_is_refused() {
    let mut asked = requirements(4);
    asked.caps.max_width = TopologyPolicy::engine_default().caps.max_width + 1;
    let error = decide_with(
        facts(),
        asked,
        read(10, admitted()),
        DecisionPolicy::engine_default(),
    )
    .expect_err("refused");
    assert_eq!(error.code, DecisionErrorCode::PolicyLoosening);
}

// spec: EG-DECISION-ENGINE-R018
#[test]
fn an_unknown_p95_under_a_deadline_is_an_unknown_fact_not_zero() {
    let mut topology = facts();
    let mut slots = topology.slots.as_slice().to_vec();
    slots[1].p95_ms = None;
    topology.slots = bounded(slots);
    let mut asked = requirements(4);
    asked.deadline_ms = Some(10_000);
    let assembly = decide_with(
        topology,
        asked,
        read(10, admitted()),
        DecisionPolicy::engine_default(),
    )
    .expect("decides");
    let DecisionOutcome::Abstained { reasons } = &assembly.record.outcome else {
        panic!("abstains");
    };
    assert!(reasons.iter().any(|reason| matches!(
        reason,
        AbstainReason::UnknownFact { component_id, field }
            if component_id == "fan" && field == "topology.worker.p95_ms"
    )));
}

#[test]
fn a_deadline_the_template_cannot_meet_is_infeasible() {
    let mut asked = requirements(4);
    asked.deadline_ms = Some(250);
    let assembly = decide(asked, read(10, admitted()));
    assert!(infeasible_labels(&assembly)
        .iter()
        .any(|label| label == "deadline"));
}

#[test]
fn a_class_no_task_class_admits_is_refused_by_task_class() {
    let assembly = decide(requirements(4), read(10, Vec::new()));
    assert_eq!(
        infeasible_labels(&assembly),
        vec![format!("admissible:{INDEPENDENT_SUBTASKS}")]
    );
}

#[test]
fn a_task_needing_an_independent_check_needs_a_verifier_slot() {
    let mut observed = read(10, admitted());
    observed.verify_required_by = bounded(vec![INDEPENDENT_SUBTASKS.to_string()]);
    let assembly = decide(requirements(4), observed);
    assert_eq!(infeasible_labels(&assembly), vec!["verify".to_string()]);
}

// spec: EG-DECISION-ENGINE-R114
#[test]
fn a_harness_slot_gets_no_native_sub_agents_unless_its_harness_opts_in() {
    let mut topology = facts();
    let mut slots = topology.slots.as_slice().to_vec();
    slots[1].harness = Some("claude-code".to_string());
    topology.slots = bounded(slots);
    let disabled = decide_with(
        topology.clone(),
        requirements(2),
        read(10, admitted()),
        DecisionPolicy::engine_default(),
    )
    .expect("decides");
    let allowance = &plan(&disabled).allowances.as_slice()[0];
    assert_eq!(allowance.fallback, SubagentFallback::Disabled);
    assert_eq!(allowance.max_children, 1);

    let mut policy = DecisionPolicy::engine_default();
    let mut opt_in = TopologyPolicy::engine_default();
    opt_in.token_budget_harnesses = bounded(vec!["claude-code".to_string()]);
    policy.topology = Some(opt_in);
    let budgeted =
        decide_with(topology, requirements(2), read(10, admitted()), policy).expect("decides");
    assert_eq!(
        plan(&budgeted).allowances.as_slice()[0].fallback,
        SubagentFallback::TokenBudget { max_tokens: 2_000 }
    );
}

#[test]
fn a_tampered_plan_does_not_replay() {
    let assembly = decide(requirements(2), read(10, admitted()));
    let mut tampered = assembly.record.clone();
    let DecisionOutcome::Solved { topology, .. } = &mut tampered.outcome else {
        unreachable!()
    };
    let mut plan = *topology.clone().expect("a plan");
    let mut slots = plan.slots.as_slice().to_vec();
    slots[1].width = 3;
    plan.slots = bounded(slots);
    *topology = Some(Box::new(plan));
    let error = replay_check(&tampered).expect_err("refused");
    assert_eq!(error.code, DecisionErrorCode::DecisionReplayMismatch);
}

/// The brute-force oracle: every worker width, filtered by demand, lease and
/// deadline, ranked by (makespan, lease). The lead slot is fixed at one.
fn oracle(subtasks: u32, per_agent: u32, available: u64, deadline: Option<u64>) -> Option<u8> {
    let need = u64::from(subtasks).div_ceil(u64::from(per_agent));
    (1u8..=4)
        .filter(|width| u64::from(*width) >= need)
        .filter(|width| u64::from(*width) < available)
        .map(|width| {
            let makespan = 100 + 200 * u64::from(subtasks).div_ceil(u64::from(width));
            (makespan, 1 + u64::from(width), width)
        })
        .filter(|(makespan, _, _)| deadline.is_none_or(|limit| *makespan <= limit))
        .min()
        .map(|(_, _, width)| width)
}

#[test]
fn the_brute_force_oracle_agrees_with_every_plan() {
    let cases = [
        (1, 1, 10, None),
        (3, 1, 10, None),
        (4, 2, 10, None),
        (4, 2, 3, None),
        (4, 4, 10, Some(500)),
        (4, 4, 10, Some(400)),
        (8, 2, 5, None),
        (8, 2, 4, None),
        (2, 1, 2, None),
    ];
    for (subtasks, per_agent, available, deadline) in cases {
        let mut asked = requirements(subtasks);
        asked.per_agent_subtasks = per_agent;
        asked.deadline_ms = deadline.map(|ms| ms as u32);
        let assembly = decide(asked, read(available, admitted()));
        let decided = assembly
            .record
            .topology_plan()
            .map(|plan| plan.slots.as_slice()[1].width);
        assert_eq!(
            decided,
            oracle(subtasks, per_agent, available, deadline),
            "case {subtasks}/{per_agent}/{available}/{deadline:?}"
        );
    }
}
