//! Reject shape syntax this evaluator would otherwise silently discard.
use std::collections::HashSet;

use eg_rdf::oxrdf::{Graph, NamedNodeRef, Term, TermRef};

use crate::{budget::Budget, shapes::as_subject_ref, vocab};

pub(crate) fn check(graph: &Graph, budget: &Budget) -> Result<(), String> {
    let mut singletons = HashSet::new();
    for triple in graph.iter() {
        budget.charge(1)?;
        let p = triple.predicate.as_str();
        if p == vocab::RDF_TYPE
            && triple.object
                == TermRef::NamedNode(NamedNodeRef::new_unchecked(
                    "http://www.w3.org/ns/shacl#ConstraintComponent",
                ))
        {
            return Err("SHACL custom constraint components are not supported".into());
        }
        let Some(local) = p.strip_prefix(vocab::SH) else {
            continue;
        };
        if annotation(local) {
            continue;
        }
        if !supported(local) {
            return Err(format!("SHACL predicate is not supported: {p}"));
        }
        if singleton(local) && !singletons.insert((triple.subject, triple.predicate)) {
            return Err(format!("SHACL parameter has multiple values: {p}"));
        }
        check_value(local, triple.object)?;
        if matches!(
            local,
            "in" | "and" | "or" | "xone" | "languageIn" | "ignoredProperties"
        ) {
            check_list(graph, triple.object.into_owned(), local, budget)?;
        }
    }
    Ok(())
}

fn annotation(p: &str) -> bool {
    matches!(p, "name" | "description" | "order" | "group" | "defaultValue" | "message" | "severity")
        // A shapes graph can also contain expected validation reports (W3C fixtures).
        || matches!(p, "shapesGraphWellFormed" | "conforms" | "result" | "focusNode" | "resultPath" | "value" | "sourceShape" | "sourceConstraint" | "sourceConstraintComponent" | "resultSeverity" | "resultMessage" | "detail")
}

fn supported(p: &str) -> bool {
    matches!(
        p,
        "targetClass"
            | "targetNode"
            | "targetSubjectsOf"
            | "targetObjectsOf"
            | "path"
            | "property"
            | "node"
            | "deactivated"
            | "closed"
            | "ignoredProperties"
    ) || matches!(
        p,
        "minCount"
            | "maxCount"
            | "datatype"
            | "class"
            | "nodeKind"
            | "minInclusive"
            | "maxInclusive"
            | "minExclusive"
            | "maxExclusive"
            | "minLength"
            | "maxLength"
            | "pattern"
            | "flags"
            | "languageIn"
    ) || matches!(
        p,
        "in" | "hasValue"
            | "and"
            | "or"
            | "not"
            | "xone"
            | "sparql"
            | "select"
            | "prefixes"
            | "declare"
            | "prefix"
            | "namespace"
    )
}

// W3C SHACL syntax summary: only these supported semantic parameters are
// single-valued. Repeatable constraint parameters must be decoded in full.
const SINGLE_VALUE_PARAMETERS: &[&str] = &[
    "path",
    "deactivated",
    "datatype",
    "nodeKind",
    "minCount",
    "maxCount",
    "minInclusive",
    "maxInclusive",
    "minExclusive",
    "maxExclusive",
    "minLength",
    "maxLength",
    "languageIn",
    "in",
    "select",
    "prefix",
    "namespace",
];

fn singleton(p: &str) -> bool {
    SINGLE_VALUE_PARAMETERS.contains(&p)
}

fn check_value(p: &str, value: TermRef<'_>) -> Result<(), String> {
    let valid = match p {
        "path" | "targetClass" | "targetSubjectsOf" | "targetObjectsOf" | "datatype" | "class" => {
            matches!(value, TermRef::NamedNode(_))
        }
        "nodeKind" => {
            matches!(value, TermRef::NamedNode(n) if matches!(n.as_str(), vocab::K_BLANK_NODE | vocab::K_IRI | vocab::K_LITERAL | vocab::K_BLANK_NODE_OR_IRI | vocab::K_BLANK_NODE_OR_LITERAL | vocab::K_IRI_OR_LITERAL))
        }
        "minCount" | "maxCount" | "minLength" | "maxLength" => {
            matches!(value, TermRef::Literal(l) if l.datatype().as_str() == vocab::XSD_INTEGER && l.value().trim().parse::<usize>().is_ok())
        }
        "deactivated" | "closed" => {
            matches!(value, TermRef::Literal(l) if l.datatype().as_str() == vocab::XSD_BOOLEAN && matches!(l.value(), "true" | "false" | "0" | "1"))
        }
        "pattern" | "flags" | "select" | "prefix" | "namespace" => {
            matches!(value, TermRef::Literal(_))
        }
        "node" | "property" | "not" | "sparql" | "prefixes" | "declare" => {
            !matches!(value, TermRef::Literal(_))
        }
        _ => true,
    };
    if valid {
        Ok(())
    } else {
        Err(format!("SHACL parameter has an unsupported value: sh:{p}"))
    }
}

fn check_list(graph: &Graph, mut head: Term, p: &str, budget: &Budget) -> Result<(), String> {
    let mut seen = HashSet::new();
    while head != Term::NamedNode(NamedNodeRef::new_unchecked(vocab::RDF_NIL).into_owned()) {
        budget.charge(1)?;
        if !seen.insert(head.clone()) {
            return Err("SHACL parameter contains a cyclic RDF list".into());
        }
        let first = list_link(graph, &head, vocab::RDF_FIRST)?;
        check_list_item(p, first)?;
        if seen.len() > 100_000 {
            return Err(crate::WORK_EXCEEDED.into());
        }
        head = list_link(graph, &head, vocab::RDF_REST)?.into_owned();
    }
    Ok(())
}

fn list_link<'a>(graph: &'a Graph, head: &Term, p: &str) -> Result<TermRef<'a>, String> {
    let subject = as_subject_ref(head).ok_or("SHACL list cell is not a resource")?;
    let mut values = graph.objects_for_subject_predicate(subject, NamedNodeRef::new_unchecked(p));
    let first = values
        .next()
        .ok_or("SHACL parameter contains an incomplete RDF list")?;
    if values.next().is_some() {
        return Err("SHACL RDF list link has multiple values".into());
    }
    Ok(first)
}

fn check_list_item(p: &str, item: TermRef<'_>) -> Result<(), String> {
    let valid = match p {
        "languageIn" => matches!(item, TermRef::Literal(_)),
        "ignoredProperties" => matches!(item, TermRef::NamedNode(_)),
        "and" | "or" | "xone" => !matches!(item, TermRef::Literal(_)),
        _ => true,
    };
    if valid {
        Ok(())
    } else {
        Err(format!("SHACL list has an unsupported item: sh:{p}"))
    }
}
