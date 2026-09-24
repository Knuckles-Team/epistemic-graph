//! `Method::GraphSchemaClasses` (EH-389): the declared vocabulary of the
//! request graph's composed schema.
//!
//! Terms are read from the same keyed sources composition reads, in the same
//! order (core before dynamic, then by key), so a term's `source_id` is the
//! source that owns it: a dynamic source may only restate a core declaration
//! verbatim (`compose::validate_dynamic_declarations`), never take it over.

use std::collections::BTreeMap;
use std::sync::Arc;

use eg_rdf::oxrdf::{NamedOrBlankNode, Term, Triple};
use eg_types::graph_schema::{
    term_local_name, validate_term_page_limit, GraphSchemaClassesView, GraphSchemaTermKind,
    GraphSchemaTermPosition, GraphSchemaTermView, GRAPH_SCHEMA_RESULT_SCHEMA_VERSION,
};

use crate::graph::{GraphCore, GraphSchemaSources};
use crate::protocol::{Response, ResultPayload};

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

/// One page request, as the method carries it.
pub(crate) struct TermPageRequest<'a> {
    pub(crate) kind: Option<GraphSchemaTermKind>,
    pub(crate) cursor: Option<&'a str>,
    pub(crate) limit: u32,
}

/// Answer one `GraphSchemaClasses` page for the request graph.
pub(crate) async fn handle_classes(
    req_id: u64,
    graph_name: &str,
    core: &Arc<GraphCore>,
    request: TermPageRequest<'_>,
) -> Response {
    match classes_page(graph_name, &super::disclosed_sources(core), &request) {
        Ok(view) => Response::ok(
            req_id,
            ResultPayload::of::<eg_types::result_contract::reasoning::GraphSchemaClasses>(view),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

fn classes_page(
    graph_name: &str,
    sources: &GraphSchemaSources,
    request: &TermPageRequest<'_>,
) -> Result<GraphSchemaClassesView, String> {
    let limit = validate_term_page_limit(request.limit)?;
    let after = request
        .cursor
        .map(GraphSchemaTermPosition::parse)
        .transpose()?;
    let terms = declared_terms(sources)?;
    let matching: Vec<(&GraphSchemaTermPosition, &String)> = terms
        .iter()
        .filter(|(position, _)| request.kind.is_none_or(|kind| kind == position.kind))
        .collect();
    let total_terms = matching.len() as u64;
    let mut page: Vec<GraphSchemaTermView> = matching
        .into_iter()
        .filter(|(position, _)| after.as_ref().is_none_or(|after| *position > after))
        .take(limit + 1)
        .map(|(position, source_id)| term_view(position, source_id))
        .collect();
    let next_cursor = (page.len() > limit).then(|| {
        page.truncate(limit);
        let last = page.last().expect("a page over its limit is non-empty");
        GraphSchemaTermPosition {
            iri: last.iri.clone(),
            kind: last.kind,
        }
        .cursor()
    });
    Ok(GraphSchemaClassesView {
        schema_version: GRAPH_SCHEMA_RESULT_SCHEMA_VERSION,
        graph: graph_name.to_string(),
        composed_digest: sources.composed_digest().to_hex(),
        total_terms,
        terms: eg_types::contract::BoundedVec::new(page)?,
        next_cursor,
    })
}

fn term_view(position: &GraphSchemaTermPosition, source_id: &str) -> GraphSchemaTermView {
    GraphSchemaTermView {
        iri: position.iri.clone(),
        local_name: term_local_name(&position.iri).to_string(),
        kind: position.kind,
        source_id: source_id.to_string(),
    }
}

/// Every named term the sources' ontology documents declare, keyed by
/// `(iri, kind)`, mapped to the first source declaring it.
fn declared_terms(
    sources: &GraphSchemaSources,
) -> Result<BTreeMap<GraphSchemaTermPosition, String>, String> {
    let mut terms = BTreeMap::new();
    for (source_id, document) in sources.ontologies() {
        let triples = eg_rdf::mapping::parse_turtle(document)
            .map_err(|error| format!("ONTOLOGY_INVALID: source '{source_id}': {error}"))?;
        for position in triples.iter().filter_map(declared_position) {
            terms
                .entry(position)
                .or_insert_with(|| source_id.to_string());
        }
    }
    Ok(terms)
}

fn declared_position(triple: &Triple) -> Option<GraphSchemaTermPosition> {
    if triple.predicate.as_str() != RDF_TYPE {
        return None;
    }
    let (NamedOrBlankNode::NamedNode(subject), Term::NamedNode(object)) =
        (&triple.subject, &triple.object)
    else {
        return None;
    };
    Some(GraphSchemaTermPosition {
        iri: subject.as_str().to_string(),
        kind: GraphSchemaTermKind::declared_by(object.as_str())?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{GraphSchemaSource, SchemaSourceOrigin};

    const ADMIN_ONTOLOGY: &str = "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n\
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\
        <https://example.org/pack#Widget> a owl:Class .\n\
        <https://example.org/pack#Gadget> a rdfs:Class .\n\
        <https://example.org/pack#partOf> a owl:ObjectProperty .\n";

    fn with_admin_source() -> GraphSchemaSources {
        let source = GraphSchemaSource::new(
            SchemaSourceOrigin::Admin {
                name: "widgets".to_string(),
            },
            None,
            Some(Arc::from(ADMIN_ONTOLOGY)),
            0,
        )
        .expect("a bounded admin source");
        GraphSchemaSources::with_dynamic(BTreeMap::from([("admin:widgets".to_string(), source)]))
            .expect("the admin source composes with the core catalog")
    }

    fn request(
        kind: Option<GraphSchemaTermKind>,
        cursor: Option<&str>,
        limit: u32,
    ) -> TermPageRequest<'_> {
        TermPageRequest {
            kind,
            cursor,
            limit,
        }
    }

    fn walk(
        sources: &GraphSchemaSources,
        kind: Option<GraphSchemaTermKind>,
        limit: u32,
    ) -> Vec<GraphSchemaClassesView> {
        let mut pages = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let page = classes_page("g", sources, &request(kind, cursor.as_deref(), limit))
                .expect("every page of a valid walk answers");
            cursor = page.next_cursor.clone();
            pages.push(page);
            if cursor.is_none() {
                return pages;
            }
        }
    }

    #[test]
    fn the_core_catalog_declares_the_canonical_classes_under_core_sources() {
        let sources = GraphSchemaSources::default();
        let terms = declared_terms(&sources).expect("the core catalog parses");
        let classes: Vec<_> = terms
            .iter()
            .filter(|(position, _)| position.kind == GraphSchemaTermKind::Class)
            .collect();
        assert!(classes.len() > 100, "the core catalog declares its classes");
        assert!(classes
            .iter()
            .all(|(_, source)| source.starts_with("core:")));
        assert!(classes
            .iter()
            .any(|(position, _)| term_local_name(&position.iri) == "Document"));
    }

    #[test]
    fn an_attached_source_contributes_its_terms_with_its_own_source_id() {
        let sources = with_admin_source();
        let page = classes_page(
            "g",
            &sources,
            &request(Some(GraphSchemaTermKind::Class), None, 1000),
        )
        .expect("a first page");
        let widget = page
            .terms
            .as_slice()
            .iter()
            .find(|term| term.local_name == "Widget")
            .expect("the attached class is listed");
        assert_eq!(widget.source_id, "admin:widgets");
        assert!(page
            .terms
            .as_slice()
            .iter()
            .any(|term| term.iri == "https://example.org/pack#Gadget"));
        assert!(page
            .terms
            .as_slice()
            .iter()
            .all(|term| term.kind == GraphSchemaTermKind::Class));
        assert_eq!(page.composed_digest, sources.composed_digest().to_hex());
    }

    #[test]
    fn a_paged_walk_returns_every_term_once_in_order_at_one_digest() {
        let sources = with_admin_source();
        let pages = walk(&sources, None, 97);
        assert!(
            pages.len() > 1,
            "a 97-term page cannot hold the core catalog"
        );
        let walked: Vec<_> = pages
            .iter()
            .flat_map(|page| page.terms.as_slice().iter().cloned())
            .collect();
        let total = pages[0].total_terms;
        assert_eq!(walked.len() as u64, total);
        assert!(pages.iter().all(|page| page.total_terms == total));
        assert!(pages
            .iter()
            .all(|page| page.composed_digest == pages[0].composed_digest));
        let keys: Vec<_> = walked
            .iter()
            .map(|term| (term.iri.clone(), term.kind))
            .collect();
        let mut sorted = keys.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(
            keys, sorted,
            "strictly ordered by (iri, kind), no duplicates"
        );
        let classes: u64 = walk(&sources, Some(GraphSchemaTermKind::Class), 1000)
            .iter()
            .map(|page| page.terms.len() as u64)
            .sum();
        let walked_classes = walked
            .iter()
            .filter(|term| term.kind == GraphSchemaTermKind::Class)
            .count() as u64;
        assert_eq!(
            classes, walked_classes,
            "the kind filter keeps exactly one kind"
        );
    }

    #[test]
    fn a_foreign_cursor_and_an_unbounded_limit_are_refused_by_code() {
        let sources = GraphSchemaSources::default();
        let cursor =
            classes_page("g", &sources, &request(None, Some("not a cursor"), 10)).unwrap_err();
        assert!(cursor.starts_with("SCHEMA_TERM_CURSOR_INVALID"), "{cursor}");
        let limit = classes_page("g", &sources, &request(None, None, 0)).unwrap_err();
        assert!(limit.starts_with("SCHEMA_TERM_PAGE_INVALID"), "{limit}");
    }
}
