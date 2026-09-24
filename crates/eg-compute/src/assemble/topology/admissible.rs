//! Stage 2 for topology: admissibility entailed under the attached schema.
//!
//! The engine's graph-schema reasoner has already entailed, for every
//! requested task class and every template class, whether the task class is
//! subsumed by the schema's `∃ admits . <class>` restriction (the facts and
//! their proof axioms are in the record's inputs, so replay reads the same
//! facts). Here a template is admissible only when EVERY task class admits
//! its class; each one that does not is named `admissible:<task-class>`.

use eg_types::decision::{
    PremiseClass, PremiseProvenance, PremiseRef, SchemaAuthority, TopologyAdmission,
};

use super::Question;

fn admits<'a>(question: &'a Question<'_>, task_class: &str) -> Option<&'a TopologyAdmission> {
    question.read.admissions.iter().find(|admission| {
        admission.task_class == task_class && admission.topology_class == question.facts.class_iri
    })
}

/// The task classes that do not admit the template's class.
pub(super) fn refused_task_classes(question: &Question<'_>) -> Vec<String> {
    question
        .requirements
        .task_classes
        .iter()
        .filter(|task_class| admits(question, task_class).is_none())
        .map(|task_class| format!("admissible:{task_class}"))
        .collect()
}

fn premise_class(authority: SchemaAuthority) -> PremiseClass {
    match authority {
        SchemaAuthority::Admin => PremiseClass::Definition,
        SchemaAuthority::Pack => PremiseClass::Claim,
    }
}

/// The admissibility premises an answer over this template rests on: one per
/// task class, classed by who attached the schema that entailed it.
pub(super) fn premises(question: &Question<'_>) -> Vec<PremiseRef> {
    question
        .requirements
        .task_classes
        .iter()
        .filter_map(|task_class| admits(question, task_class))
        .map(|admission| PremiseRef {
            subject: admission.topology_class.clone(),
            fact: format!("admitted-by:{}", admission.task_class),
            class: premise_class(admission.authority),
            provenance: PremiseProvenance::SchemaSource {
                source_key: admission.source_key.clone(),
                schema_digest: question.read.schema_digest.clone(),
            },
        })
        .collect()
}
