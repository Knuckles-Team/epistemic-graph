//! EH-470: the agent-orchestration shapes EG took over from agent-utilities flag what
//! they flagged there. Each case validates through the composed GraphSchema — the
//! shapes a graph uses when `ShaclValidate` omits `shapes` — so the proof covers the
//! served composition, not a shapes file read in isolation.

use std::collections::BTreeSet;

use eg_rdf::oxrdf::{NamedNode, Term, Triple};

use super::compose::validate_and_compose;
use crate::graph::GraphSchemaSources;

const PREFIX: &str = "@prefix : <http://knuckles.team/kg#> .\n\
                      @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n";

/// The focus nodes (N-Triples form) the composed core shapes report violations for.
fn flagged(data: &str) -> BTreeSet<String> {
    let composed = validate_and_compose(&GraphSchemaSources::default()).unwrap();
    let data = eg_shacl::graph_from_turtle(&format!("{PREFIX}{data}")).unwrap();
    let report = eg_shacl::validate(&composed.shapes, &data).unwrap();
    report
        .results
        .into_iter()
        .filter(|result| result.severity == eg_shacl::Severity::Violation)
        .map(|result| result.focus_node)
        .collect()
}

fn node(local: &str) -> String {
    format!("<http://knuckles.team/kg#{local}>")
}

fn assert_flags(data: &str, local: &str) {
    let focus = flagged(data);
    assert!(
        focus.contains(&node(local)),
        "{local} not flagged in {focus:?}"
    );
}

fn assert_clean(data: &str, local: &str) {
    let focus = flagged(data);
    assert!(
        !focus.contains(&node(local)),
        "{local} flagged in {focus:?}"
    );
}

#[test]
fn harness_concentration_needs_three_shipped_edits_in_one_window() {
    let two = ":dim a :HarnessDimension .\n\
               :e1 a :HarnessEdit ; :targetsDimension :dim ; :editStatus \"shipped\" ; :editRound 2 .\n\
               :e2 a :HarnessEdit ; :targetsDimension :dim ; :editStatus \"shipped\" ; :editRound 3 .\n";
    assert_clean(two, "dim");
    let three = format!(
        "{two}:e3 a :HarnessEdit ; :targetsDimension :dim ; :editStatus \"shipped\" ; :editRound 4 .\n"
    );
    assert_flags(&three, "dim");
}

#[test]
fn harness_seesaw_hook_singleton_and_reward_hacking_are_flagged() {
    let regression = ":v a :HarnessVariant ; :variantStatus \"accepted\" ; :appliesEdit :e1 .\n\
                      :e1 :causesRegression :task1 .\n";
    assert_flags(regression, "v");
    let hook = ":e4 a :HarnessEdit ; :atHook :step_end ; :modifiesField \"prompt\" .\n\
                :step_end a :HarnessHook ; :hookReadOnly true .\n";
    assert_flags(hook, "e4");
    let singleton = ":p1 a :Processor ; :attachedToHook :h ; :singletonGroup \"g\" ; :variantStatus \"accepted\" .\n\
                     :p2 a :Processor ; :attachedToHook :h ; :singletonGroup \"g\" ; :variantStatus \"accepted\" .\n";
    assert_flags(singleton, "p1");
    let hacking =
        ":e5 a :HarnessEdit ; :editStatus \"shipped\" ; :exhibitsPathology :pathology .\n\
                   :pathology :pathologyKind \"reward_hacking\" .\n";
    assert_flags(hacking, "e5");
}

#[test]
fn temporal_windows_and_superseded_beliefs_are_checked() {
    assert_flags(
        ":f a :TemporalFact ; :validFrom 300 ; :validUntil 100 .",
        "f",
    );
    assert_clean(
        ":f a :TemporalFact ; :validFrom 100 ; :validUntil 300 .",
        "f",
    );
    let open = ":f1 a :TemporalFact ; :validFrom 100 ; :validUntil 200 .\n\
                :f2 a :TemporalFact ; :validFrom 200 ; :supersedes :f1 .\n";
    assert_flags(open, "f1");
    let closed = ":f1 a :TemporalFact ; :validFrom 100 ; :validUntil 200 ; :txTo 200 .\n\
                  :f2 a :TemporalFact ; :validFrom 200 ; :supersedes :f1 .\n";
    assert_clean(closed, "f1");
}

#[test]
fn sdlc_merge_needs_a_pipeline_run() {
    assert_flags(":mr1 a :CodeChangeProposal .", "mr1");
    assert_clean(
        ":mr1 a :CodeChangeProposal ; :triggersPipeline :pr1 .\n:pr1 a :PipelineRun .",
        "mr1",
    );
}

#[test]
fn portfolio_verdicts_are_closed_and_assessments_scored() {
    assert_flags(
        ":r a :Recommendation ; :verdict \"maybe\" ; :rationale \"why\" .",
        "r",
    );
    assert_clean(
        ":r a :Recommendation ; :verdict \"adopt\" ; :rationale \"why\" .",
        "r",
    );
    assert_flags(":a a :Assessment .", "a");
    assert_clean(
        ":a a :Assessment ; :assessmentScore \"0.7\"^^xsd:float .",
        "a",
    );
}

#[test]
fn process_events_need_a_time_and_an_event_type() {
    assert_flags(":ev1 a :ProcessEvent .", "ev1");
    assert_clean(
        ":ev1 a :ProcessEvent ; :occurred_at \"2026-09-24T10:00:00Z\" ; :INSTANCE_OF_EVENT_TYPE :t1 .\n\
         :t1 a :ProcessEventType .",
        "ev1",
    );
}

#[test]
fn agent_governance_shapes_apply_to_every_graph() {
    assert_flags(":agent1 a :Agent .", "agent1");
    assert_clean(":agent1 a :Agent ; :name \"planner\" .", "agent1");
    assert_flags(":wf a :WorkflowDefinition .", "wf");
}

const SH: &str = "http://www.w3.org/ns/shacl#";

/// Every committed validation composes every core shape, so a core shape using a
/// construct EG's sh:sparql declines would fail EVERY graph's validation the moment
/// a focus node of its target exists. Instantiate one focus node per core target
/// (class, subjects-of, objects-of) and require the composed validation to run.
#[test]
fn every_core_shape_target_validates_without_error() {
    let composed = validate_and_compose(&GraphSchemaSources::default()).unwrap();
    let mut data = eg_shacl::Graph::new();
    let rdf_type = NamedNode::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    for (index, triple) in composed.shapes.iter().enumerate() {
        let Term::NamedNode(target) = triple.object.into_owned() else {
            continue;
        };
        let focus = NamedNode::new_unchecked(format!("http://example.org/focus#{index}"));
        let other = NamedNode::new_unchecked(format!("http://example.org/other#{index}"));
        match triple.predicate.as_str().strip_prefix(SH) {
            Some("targetClass") => data.insert(&Triple::new(focus, rdf_type.clone(), target)),
            Some("targetSubjectsOf") => data.insert(&Triple::new(focus, target, other)),
            Some("targetObjectsOf") => data.insert(&Triple::new(other, target, focus)),
            _ => false,
        };
    }
    assert!(
        data.len() > 40,
        "expected a focus node per core target, got {}",
        data.len()
    );
    eg_shacl::validate(&composed.shapes, &data).unwrap();
}
