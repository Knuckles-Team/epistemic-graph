//! Filling an NL template's typed slots without a model (EH-028, EH-064).
//!
//! Template CHOICE is an ordinary `Decide` over `NlTemplate` components (their
//! utterances and labels are candidate text fields scored by candidate-local
//! BM25). This module does the second half: given the chosen template and the
//! utterance, bind each slot by exact matching -- an IRI slot to the longest
//! native ontology label found as a contiguous token run, an integer slot to
//! the first integer token, a text slot to the tokens after its marker word.
//! Nothing is generated; a slot that does not match stays unfilled and is
//! reported as such.

use eg_types::agent_ontology::{descendants, term};
use eg_types::decision::statistical::nl::{NlSlot, NlSlotType, NlTemplateBody};
use eg_types::decision::statistical::{TypedParam, TypedValue};

use super::bm25::tokens;

/// The bound slot values and the required slots left unfilled.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SlotFill {
    pub params: Vec<TypedParam>,
    pub unfilled: Vec<String>,
}

fn contains_run(haystack: &[String], needle: &[String]) -> bool {
    !needle.is_empty()
        && haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

fn link_iri(words: &[String], under: &str) -> Option<String> {
    let mut best: Option<(usize, &str)> = None;
    for iri in descendants(under) {
        let Some(entry) = term(iri) else { continue };
        let label = tokens(entry.label);
        let longer = best.is_none_or(|(len, _)| label.len() > len);
        if longer && contains_run(words, &label) {
            best = Some((label.len(), iri));
        }
    }
    best.map(|(_, iri)| iri.to_string())
}

fn first_integer(words: &[String]) -> Option<i64> {
    words.iter().find_map(|w| w.parse::<i64>().ok())
}

fn text_after(words: &[String], marker: &str) -> Option<String> {
    let marker = tokens(marker);
    let at = (0..words.len()).find(|&i| words[i..].starts_with(&marker))?;
    let rest = &words[at + marker.len()..];
    (!rest.is_empty()).then(|| rest.join(" "))
}

fn fill_one(words: &[String], slot: &NlSlot) -> Option<TypedValue> {
    match &slot.slot_type {
        NlSlotType::Iri { under } => link_iri(words, under).map(TypedValue::Iri),
        NlSlotType::Int => first_integer(words).map(TypedValue::Int),
        NlSlotType::Text { after } => text_after(words, after).map(TypedValue::Text),
    }
}

/// Bind every slot of `template` against `utterance`. Params are sorted by
/// name, the order a `Decide` request requires.
pub fn fill_slots(template: &NlTemplateBody, utterance: &str) -> SlotFill {
    let words = tokens(utterance);
    let mut fill = SlotFill::default();
    for slot in &template.slots {
        match fill_one(&words, slot) {
            Some(value) => fill.params.push(TypedParam {
                name: slot.name.clone(),
                value,
            }),
            None if slot.required => fill.unfilled.push(slot.name.clone()),
            None => {}
        }
    }
    fill.params.sort_by(|a, b| a.name.cmp(&b.name));
    fill
}
