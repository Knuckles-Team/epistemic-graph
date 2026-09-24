//! Swarm topology inside the assembly decision (SWARM-TOPOLOGY-DECIDE-DESIGN).
//!
//! A topology question is asked of one graph template at a time, inside the
//! existing template enumeration: [`pose`] refuses a template the question
//! cannot use (no topology facts, a class the attached schema does not admit
//! for every task class, too deep, or no verifier where one is required), and
//! [`Question::extend`] appends the one-hot width/rounds variables and the
//! lease, deadline, demand and cap rows to that template's own programme. The
//! same solver, certificate and no-good loop decide both halves at once, and
//! the winner's plan is read back from the certificate's selection.

mod admissible;
mod model;
mod plan;
pub(crate) mod tables;
pub(super) mod validate;

use eg_types::contract::BoundedVec;
use eg_types::decision::{
    AbstainReason, DecisionInputs, SlotRole, TemplateFacts, TopologyCaps, TopologyFacts,
    TopologyInputs, TopologyPolicy, TopologyRequirements,
};

pub(super) use tables::Choice;

/// A topology question over one template, with everything it reads.
pub(super) struct Question<'a> {
    pub requirements: &'a TopologyRequirements,
    pub read: &'a TopologyInputs,
    pub facts: &'a TopologyFacts,
    pub template: &'a TemplateFacts,
    pub policy: TopologyPolicy,
    /// The policy's caps, tightened by the request's.
    pub caps: TopologyCaps,
}

/// What a template is to the request's topology question.
pub(super) enum Posed<'a> {
    /// The request asks no topology question.
    Plain,
    Ask(Box<Question<'a>>),
    /// The template cannot answer it, and why.
    Refused(Vec<AbstainReason>),
}

fn infeasible(labels: Vec<String>) -> Vec<AbstainReason> {
    vec![AbstainReason::Infeasible {
        constraints: BoundedVec::new(labels.into_iter().take(64).collect())
            .expect("taken to the bound"),
    }]
}

/// Pose the request's topology question to `template`.
pub(super) fn pose<'a>(
    inputs: &'a DecisionInputs,
    template: Option<&'a TemplateFacts>,
) -> Posed<'a> {
    let Some(requirements) = inputs.request.requirements.topology.as_ref() else {
        return Posed::Plain;
    };
    let (Some(template), Some(read)) = (template, inputs.topology.as_ref()) else {
        return Posed::Refused(infeasible(vec!["topology:template".to_string()]));
    };
    let Some(facts) = template.topology.as_ref() else {
        return Posed::Refused(infeasible(vec![format!(
            "topology:facts:{}",
            template.graph_id
        )]));
    };
    let policy = inputs
        .policy
        .topology
        .clone()
        .unwrap_or_else(TopologyPolicy::engine_default);
    let caps = policy.caps.tightened_by(&requirements.caps);
    let question = Question {
        requirements,
        read,
        facts,
        template,
        policy,
        caps,
    };
    let refusals = question.refusals();
    match refusals.is_empty() {
        true => Posed::Ask(Box::new(question)),
        false => Posed::Refused(infeasible(refusals)),
    }
}

impl Question<'_> {
    /// Every hard rule the template fails before any variable exists:
    /// admissibility (`admissible:<task-class>`), depth (`cap:max_depth`) and
    /// the independent-check rule (`verify`).
    fn refusals(&self) -> Vec<String> {
        let mut out = admissible::refused_task_classes(self);
        if self.facts.depth > self.caps.max_depth {
            out.push("cap:max_depth".to_string());
        }
        let has_verifier = self
            .facts
            .slots
            .iter()
            .any(|slot| slot.role == SlotRole::Verifier);
        if !self.read.verify_required_by.is_empty() && !has_verifier {
            out.push("verify".to_string());
        }
        out
    }
}
