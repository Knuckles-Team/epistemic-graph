//! EH-028 / EH-064: NL template slot filling without a model.

use eg_numeric::decision::nl::fill_slots;
use eg_types::decision::statistical::nl::{
    NlSlot, NlSlotType, NlTarget, NlTemplateBody, NL_TEMPLATE_SCHEMA_VERSION,
};
use eg_types::decision::statistical::TypedValue;

use super::common::bounded;

fn template() -> NlTemplateBody {
    NlTemplateBody {
        schema_version: NL_TEMPLATE_SCHEMA_VERSION,
        utterances: bounded(vec!["build an agent that can {capability}".to_string()]),
        labels: bounded(vec![]),
        slots: bounded(vec![
            NlSlot {
                name: "capability".to_string(),
                slot_type: NlSlotType::Iri {
                    under: "eg:capability".to_string(),
                },
                required: true,
            },
            NlSlot {
                name: "limit".to_string(),
                slot_type: NlSlotType::Int,
                required: false,
            },
            NlSlot {
                name: "topic".to_string(),
                slot_type: NlSlotType::Text {
                    after: "about".to_string(),
                },
                required: true,
            },
        ]),
        target: NlTarget::AgentAssemble,
    }
    .checked()
    .expect("valid template")
}

#[test]
fn slots_bind_by_exact_matching_and_unfilled_required_slots_are_named() {
    let fill = fill_slots(&template(), "build an agent for web search with 5 tools");
    let capability = fill
        .params
        .iter()
        .find(|p| p.name == "capability")
        .expect("linked");
    assert!(matches!(&capability.value, TypedValue::Iri(iri) if iri.starts_with("eg:capability")));
    let limit = fill
        .params
        .iter()
        .find(|p| p.name == "limit")
        .expect("integer");
    assert_eq!(limit.value, TypedValue::Int(5));
    assert_eq!(fill.unfilled, vec!["topic".to_string()]);
    let names: Vec<&str> = fill.params.iter().map(|p| p.name.as_str()).collect();
    let mut sorted = names.clone();
    sorted.sort_unstable();
    assert_eq!(names, sorted, "params are sorted by name");
}
