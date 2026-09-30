//! Precharge indexed shape/prefix parsing without charging unrelated graph data.
use crate::{
    budget::Budget,
    shapes::{as_subject_ref, nn},
    vocab,
};
use eg_rdf::oxrdf::{Graph, Term};
use std::collections::HashSet;

/// Allowance for the fixed set of indexed parameter lookups in `parse_shape`.
const SHAPE_LOOKUPS: usize = 64;

pub(crate) fn shape(graph: &Graph, id: &Term, budget: &Budget) -> Result<(), String> {
    budget.charge(SHAPE_LOOKUPS)?;
    let Some(subject) = as_subject_ref(id) else {
        return Ok(());
    };
    let (mut patterns, mut flags) = (0usize, 0usize);
    for triple in graph.triples_for_subject(subject) {
        budget.charge(2)?;
        let predicate = triple.predicate.as_str();
        patterns += usize::from(predicate == vocab::PATTERN);
        flags += usize::from(predicate == vocab::FLAGS);
        if list_parameter(predicate) {
            list(graph, triple.object.into_owned(), budget)?;
        }
    }
    // Optional flags instantiate every pattern/flag combination, before allocation.
    budget.charge(patterns.saturating_mul(flags.max(1)))
}

fn list_parameter(predicate: &str) -> bool {
    matches!(
        predicate,
        vocab::IN
            | vocab::AND
            | vocab::OR
            | vocab::XONE
            | vocab::LANGUAGE_IN
            | vocab::IGNORED_PROPERTIES
    )
}

fn list(graph: &Graph, mut head: Term, budget: &Budget) -> Result<(), String> {
    while let Some(subject) = as_subject_ref(&head) {
        budget.charge(2)?; // rdf:first and rdf:rest indexed lookups
        let Some(rest) = graph
            .objects_for_subject_predicate(subject, nn(vocab::RDF_REST))
            .next()
        else {
            break;
        };
        head = rest.into_owned();
    }
    Ok(())
}

pub(crate) fn sparql(graph: &Graph, id: &Term, budget: &Budget) -> Result<(), String> {
    let mut pending = vec![id.clone()];
    let mut seen = HashSet::new();
    while let Some(resource) = pending.pop() {
        budget.charge(1)?;
        if !seen.insert(resource.clone()) {
            continue;
        }
        append_prefix_references(graph, &resource, &mut pending, budget)?;
    }
    Ok(())
}

fn append_prefix_references(
    graph: &Graph,
    resource: &Term,
    pending: &mut Vec<Term>,
    budget: &Budget,
) -> Result<(), String> {
    let Some(subject) = as_subject_ref(resource) else {
        return Ok(());
    };
    for triple in graph.triples_for_subject(subject) {
        budget.charge(2)?;
        if matches!(
            triple.predicate.as_str(),
            vocab::PREFIXES | vocab::OWL_IMPORTS | vocab::DECLARE
        ) {
            pending.push(triple.object.into_owned());
        }
    }
    Ok(())
}
