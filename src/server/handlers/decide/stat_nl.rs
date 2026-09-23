//! Routing an utterance to an `NlTemplate` and filling its slots (EH-028,
//! EH-064). The routing itself is the `Decide` answer over template
//! candidates; this binds the chosen template. When the engine abstained, a
//! caller-supplied LLM proposal (`llm_proposal_template`, `llm_producer`,
//! `llm_prompt_digest` parameters) is bound as a CLAIM, never as the decision.

use eg_numeric::decision::nl::fill_slots;
use eg_types::agent_component::{AgentComponentEntry, ComponentDependency};
use eg_types::contract::BoundedVec;
use eg_types::decision::statistical::body::decode_body;
use eg_types::decision::statistical::nl::{NlBinding, NlChoiceSource, NlTemplateBody};
use eg_types::decision::statistical::{
    DecideRequest, QuestionKind, StatisticalErrorCode, StatisticalOutcome, TypedValue,
};

use super::stat_support::refusal;

/// The utterance parameter a template-choice question reads.
pub(super) const UTTERANCE_PARAM: &str = "utterance";

fn text_param<'a>(request: &'a DecideRequest, name: &str) -> Option<&'a str> {
    request
        .params
        .iter()
        .find(|p| p.name == name)
        .and_then(|p| match &p.value {
            TypedValue::Text(text) => Some(text.as_str()),
            _ => None,
        })
}

fn chosen(outcome: &StatisticalOutcome) -> Option<&str> {
    match outcome {
        StatisticalOutcome::Acted { option_id, .. }
        | StatisticalOutcome::Explored { option_id, .. } => Some(option_id),
        StatisticalOutcome::Advisory { scores, .. } => scores
            .iter()
            .fold(
                None,
                |best: Option<&eg_types::decision::statistical::ScoredOption>, s| {
                    if best.is_none_or(|b| s.score.value > b.score.value) {
                        Some(s)
                    } else {
                        best
                    }
                },
            )
            .map(|s| s.option_id.as_str()),
        StatisticalOutcome::Abstained { .. } => None,
    }
}

fn dependency(entry: &AgentComponentEntry) -> ComponentDependency {
    ComponentDependency {
        component_id: entry.component_id.clone(),
        kind: entry.kind,
        definition_digest: entry.definition_digest.clone(),
    }
}

fn bind(
    entry: &AgentComponentEntry,
    utterance: &str,
    source: NlChoiceSource,
) -> Result<NlBinding, String> {
    let body: NlTemplateBody = decode_body(&entry.content_digest, &entry.attributes)
        .and_then(NlTemplateBody::checked)
        .map_err(|detail| refusal(StatisticalErrorCode::ParameterInvalid, detail))?;
    let fill = fill_slots(&body, utterance);
    let bounded = |detail: String| refusal(StatisticalErrorCode::ParameterInvalid, detail);
    Ok(NlBinding {
        template: dependency(entry),
        target: body.target,
        params: BoundedVec::new(fill.params).map_err(bounded)?,
        unfilled: BoundedVec::new(fill.unfilled).map_err(bounded)?,
        source,
    })
}

fn llm_proposal(request: &DecideRequest) -> Option<(&str, NlChoiceSource)> {
    let template = text_param(request, "llm_proposal_template")?;
    let producer = text_param(request, "llm_producer")?;
    let prompt_digest = text_param(request, "llm_prompt_digest")?;
    Some((
        template,
        NlChoiceSource::LlmProposal {
            producer: producer.to_string(),
            prompt_digest: prompt_digest.to_string(),
        },
    ))
}

/// The binding a template-choice record carries, if any.
pub(super) fn binding(
    request: &DecideRequest,
    outcome: &StatisticalOutcome,
    entries: &[AgentComponentEntry],
) -> Result<Option<NlBinding>, String> {
    if request.question.kind != QuestionKind::TemplateChoice {
        return Ok(None);
    }
    let utterance = text_param(request, UTTERANCE_PARAM).ok_or_else(|| {
        refusal(
            StatisticalErrorCode::ParameterInvalid,
            "a template choice needs a text `utterance`",
        )
    })?;
    let (id, source) = match (chosen(outcome), llm_proposal(request)) {
        (Some(id), _) => (id, NlChoiceSource::Engine),
        (None, Some((id, source))) => (id, source),
        (None, None) => return Ok(None),
    };
    let entry = entries
        .iter()
        .find(|e| e.component_id == id)
        .ok_or_else(|| {
            refusal(
                StatisticalErrorCode::ParameterInvalid,
                format!("{id} is not a visible template candidate"),
            )
        })?;
    bind(entry, utterance, source).map(Some)
}
