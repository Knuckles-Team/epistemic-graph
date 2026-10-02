//! Typed triples to an RDF graph: `ShaclValidate.data_triples` lets a caller
//! that owns no RDF syntax send data as plain values; the engine alone turns them
//! into terms, so an IRI or language tag is checked here, once.

use eg_rdf::oxrdf::{Literal, NamedNode, Term, Triple};
use eg_types::ontology_inspection::{RdfObject, RdfTriple, MAX_TYPED_TRIPLES};

/// Build the data graph from typed triples, refusing the whole request on the first
/// malformed triple (named by its index) rather than validating a partial graph.
pub(super) fn graph_from_typed_triples(triples: &[RdfTriple]) -> Result<eg_shacl::Graph, String> {
    if triples.len() > MAX_TYPED_TRIPLES {
        return Err(format!(
            "{} triples exceed the maximum of {MAX_TYPED_TRIPLES}",
            triples.len()
        ));
    }
    let mut graph = eg_shacl::Graph::new();
    for (index, triple) in triples.iter().enumerate() {
        let triple = typed_triple(triple).map_err(|error| format!("triple {index}: {error}"))?;
        graph.insert(&triple);
    }
    Ok(graph)
}

fn typed_triple(triple: &RdfTriple) -> Result<Triple, String> {
    let subject = named(&triple.subject, "subject")?;
    let predicate = named(&triple.predicate, "predicate")?;
    Ok(Triple::new(
        subject,
        predicate,
        object_term(&triple.object)?,
    ))
}

fn named(iri: &str, role: &str) -> Result<NamedNode, String> {
    NamedNode::new(iri).map_err(|error| format!("{role} is not an absolute IRI: {error}"))
}

fn object_term(object: &RdfObject) -> Result<Term, String> {
    match object {
        RdfObject::Iri { iri } => named(iri, "object").map(Term::from),
        RdfObject::Literal {
            lexical,
            datatype,
            language,
        } => literal(lexical, datatype.as_deref(), language.as_deref()).map(Term::from),
    }
}

fn literal(
    lexical: &str,
    datatype: Option<&str>,
    language: Option<&str>,
) -> Result<Literal, String> {
    match (datatype, language) {
        (Some(_), Some(_)) => {
            Err("a literal carries a datatype or a language, not both".to_string())
        }
        (Some(datatype), None) => Ok(Literal::new_typed_literal(
            lexical,
            named(datatype, "datatype")?,
        )),
        (None, Some(language)) => Literal::new_language_tagged_literal(lexical, language)
            .map_err(|error| format!("invalid language tag: {error}")),
        (None, None) => Ok(Literal::new_simple_literal(lexical)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn iri(value: &str) -> RdfObject {
        RdfObject::Iri {
            iri: value.to_string(),
        }
    }

    fn text(value: &str) -> RdfObject {
        RdfObject::Literal {
            lexical: value.to_string(),
            datatype: None,
            language: None,
        }
    }

    fn triple(subject: &str, predicate: &str, object: RdfObject) -> RdfTriple {
        RdfTriple {
            subject: subject.to_string(),
            predicate: predicate.to_string(),
            object,
        }
    }

    #[test]
    fn typed_triples_are_the_same_graph_as_their_turtle() {
        let typed = [
            triple(
                "http://knuckles.team/kg#wf",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                iri("http://knuckles.team/kg#WorkflowDefinition"),
            ),
            triple(
                "http://knuckles.team/kg#wf",
                "http://knuckles.team/kg#name",
                text("deploy"),
            ),
            triple(
                "http://knuckles.team/kg#wf",
                "http://knuckles.team/kg#step_count",
                RdfObject::Literal {
                    lexical: "2".to_string(),
                    datatype: Some("http://www.w3.org/2001/XMLSchema#integer".to_string()),
                    language: None,
                },
            ),
            triple(
                "http://knuckles.team/kg#wf",
                "http://www.w3.org/2000/01/rdf-schema#label",
                RdfObject::Literal {
                    lexical: "Bereitstellen".to_string(),
                    datatype: None,
                    language: Some("de".to_string()),
                },
            ),
        ];
        let turtle = "@prefix kg: <http://knuckles.team/kg#> .\n\
                      kg:wf a kg:WorkflowDefinition ; kg:name \"deploy\" ; kg:step_count 2 ;\n\
                      <http://www.w3.org/2000/01/rdf-schema#label> \"Bereitstellen\"@de .\n";
        let from_typed = graph_from_typed_triples(&typed).unwrap();
        let from_turtle = eg_shacl::graph_from_turtle(turtle).unwrap();
        assert_eq!(from_typed, from_turtle);
    }

    #[test]
    fn a_malformed_triple_refuses_the_request_by_index() {
        let typed = [
            triple(
                "http://knuckles.team/kg#a",
                "http://knuckles.team/kg#p",
                text("ok"),
            ),
            triple("not an iri", "http://knuckles.team/kg#p", text("x")),
        ];
        let error = graph_from_typed_triples(&typed).unwrap_err();
        assert!(
            error.starts_with("triple 1: subject is not an absolute IRI"),
            "{error}"
        );

        let both = RdfObject::Literal {
            lexical: "x".to_string(),
            datatype: Some("http://www.w3.org/2001/XMLSchema#string".to_string()),
            language: Some("en".to_string()),
        };
        let error = graph_from_typed_triples(&[triple(
            "http://knuckles.team/kg#a",
            "http://knuckles.team/kg#p",
            both,
        )])
        .unwrap_err();
        assert!(error.contains("not both"), "{error}");
    }

    #[test]
    fn the_wire_shape_is_tagged_and_closed() {
        let parsed: RdfTriple = serde_json::from_str(
            r#"{"subject":"http://x/a","predicate":"http://x/p","object":{"kind":"literal","lexical":"v"}}"#,
        )
        .unwrap();
        assert_eq!(parsed.object, text("v"));
        assert!(serde_json::from_str::<RdfTriple>(
            r#"{"subject":"http://x/a","predicate":"http://x/p","object":{"kind":"iri","iri":"http://x/o"},"extra":1}"#,
        )
        .is_err());
    }
}
