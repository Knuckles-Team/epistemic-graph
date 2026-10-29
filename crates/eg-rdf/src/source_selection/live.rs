//! Live read of authority rows from the composed graph schema sources
//! (MEDULLA-SRC-AUTH-R004.5): the same [`CoverageFixture`] the unit tests build by
//! hand, parsed from `vg:SourceAuthority` rows, `vg:SourceConnection` facts and
//! `rdfs:subClassOf` axioms. Each row carries the origin of the schema source it
//! was read from, so a pack or ingestion row stays a proposal (R003.4).

use std::collections::{BTreeMap, BTreeSet};

use eg_core::graph::{GraphSchemaSources, SchemaSourceOrigin};
use oxrdf::{Term, Triple};

use super::{AuthorityOrigin, AuthorityRow, CoverageFixture, Materialisation, SourceRole};

const VG: &str = "http://knuckles.team/kg/virtual-graph#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDFS_LABEL: &str = "http://www.w3.org/2000/01/rdf-schema#label";
const RDFS_SUBCLASS: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";

/// Subject -> predicate -> objects, rendered as plain strings (an IRI's text
/// or a literal's lexical value).
#[derive(Default)]
struct Index(BTreeMap<String, BTreeMap<String, Vec<String>>>);

impl Index {
    fn from_triples(triples: &[Triple]) -> Self {
        let mut index = Self::default();
        for triple in triples {
            let subject = match &triple.subject {
                oxrdf::NamedOrBlankNode::NamedNode(node) => node.as_str().to_string(),
                oxrdf::NamedOrBlankNode::BlankNode(node) => format!("_:{}", node.as_str()),
            };
            let object = match &triple.object {
                Term::NamedNode(node) => node.as_str().to_string(),
                Term::BlankNode(node) => format!("_:{}", node.as_str()),
                Term::Literal(literal) => literal.value().to_string(),
                // A quoted triple (RDF-star builds) is not a source-selection value;
                // index its text form.
                #[allow(unreachable_patterns)]
                other => other.to_string(),
            };
            index
                .0
                .entry(subject)
                .or_default()
                .entry(triple.predicate.as_str().to_string())
                .or_default()
                .push(object);
        }
        index
    }

    fn first(&self, subject: &str, predicate: &str) -> Option<&str> {
        self.all(subject, predicate).first().map(String::as_str)
    }

    fn all(&self, subject: &str, predicate: &str) -> &[String] {
        self.0
            .get(subject)
            .and_then(|predicates| predicates.get(predicate))
            .map_or(&[], Vec::as_slice)
    }

    fn vg(&self, subject: &str, local: &str) -> Option<&str> {
        self.first(subject, &format!("{VG}{local}"))
    }

    fn subjects_of_type(&self, class: &str) -> Vec<&str> {
        self.0
            .iter()
            .filter(|(_, predicates)| {
                predicates
                    .get(RDF_TYPE)
                    .is_some_and(|types| types.iter().any(|t| t == class))
            })
            .map(|(subject, _)| subject.as_str())
            .collect()
    }

    /// The name a connection is selected by: its foreign catalog name, its
    /// label, or its IRI.
    fn source_name(&self, connection: &str) -> String {
        self.vg(connection, "foreignSourceName")
            .or_else(|| self.first(connection, RDFS_LABEL))
            .unwrap_or(connection)
            .to_string()
    }
}

fn row_from(
    index: &Index,
    subject: &str,
    origin: AuthorityOrigin,
    generation: u64,
) -> Option<AuthorityRow> {
    let connection = index.vg(subject, "authoritativeSource")?;
    let class = index.vg(subject, "authorityFor")?;
    let rank = index.vg(subject, "authorityRank")?.parse().ok()?;
    let mapping = index.vg(subject, "authorityMapping").unwrap_or_default();
    let approved = !mapping.is_empty() && index.vg(mapping, "approved") == Some("true");
    let materialisation = match index.vg(subject, "materialisation") {
        Some("accelerated") => Materialisation::Accelerated {
            reason: index
                .vg(subject, "materialisationReason")
                .unwrap_or_default()
                .to_string(),
        },
        _ => Materialisation::Virtual,
    };
    Some(AuthorityRow {
        row: subject.to_string(),
        source: index.source_name(connection),
        class: class.to_string(),
        mapping_name: mapping.to_string(),
        facet: index.vg(subject, "authorityFacet").map(str::to_string),
        rank,
        scope: index.vg(subject, "scopeTenant").map(str::to_string),
        partition: index.vg(subject, "scopePartition").map(str::to_string),
        origin: origin.as_str().to_string(),
        generation,
        allow_fallback: index.vg(subject, "allowFallback") == Some("true"),
        approved,
        declared: true,
        materialisation,
        freshness_max_age_seconds: index
            .vg(subject, "freshnessMaxAgeSeconds")
            .and_then(|v| v.parse().ok()),
        contract: index.vg(subject, "authorityContract").map(str::to_string),
    })
}

fn parse_role(value: &str) -> Option<SourceRole> {
    serde_json::from_value(serde_json::Value::String(value.to_string())).ok()
}

/// Add the rows, connection facts and subclass axioms of one document.
fn absorb(
    mut fixture: CoverageFixture,
    triples: &[Triple],
    origin: AuthorityOrigin,
    generation: u64,
) -> CoverageFixture {
    let index = Index::from_triples(triples);
    for connection in index.subjects_of_type(&format!("{VG}SourceConnection")) {
        let name = index.source_name(connection);
        if let Some(role) = index.vg(connection, "sourceRole").and_then(parse_role) {
            fixture = fixture.with_source_role(&name, role);
        }
        if let Some(kind) = index.vg(connection, "sourceKind") {
            fixture = fixture.with_source_kind(&name, kind);
        }
        if index
            .all(connection, &format!("{VG}declaredCapability"))
            .iter()
            .any(|c| c == "copy")
        {
            fixture = fixture.with_copy_capability(&name, true);
        }
    }
    for (subject, predicates) in &index.0 {
        for superclass in predicates.get(RDFS_SUBCLASS).into_iter().flatten() {
            fixture = fixture.with_subclass(subject, superclass);
        }
    }
    for subject in index.subjects_of_type(&format!("{VG}SourceAuthority")) {
        if let Some(row) = row_from(&index, subject, origin, generation) {
            fixture = fixture.with_row(row);
        }
    }
    fixture
}

/// Parse the authority rows of one Turtle document read under `origin`.
pub fn authority_rows_from_turtle(
    document: &str,
    origin: AuthorityOrigin,
) -> Result<Vec<AuthorityRow>, String> {
    let triples = crate::mapping::parse_turtle(document)?;
    Ok(absorb(CoverageFixture::new(), &triples, origin, 0)
        .rows()
        .cloned()
        .collect())
}

fn origin_of(origin: &SchemaSourceOrigin) -> AuthorityOrigin {
    match origin {
        SchemaSourceOrigin::Core { .. } => AuthorityOrigin::Core,
        SchemaSourceOrigin::Operator => AuthorityOrigin::Operator,
        SchemaSourceOrigin::Admin { .. } => AuthorityOrigin::Admin,
        SchemaSourceOrigin::Approved { .. } => AuthorityOrigin::Approved,
        SchemaSourceOrigin::Pack { .. } => AuthorityOrigin::Pack,
        SchemaSourceOrigin::Ingestion { .. } => AuthorityOrigin::Ingestion,
    }
}

/// Build the selection fixture from every ontology document of the composed
/// graph schema sources; each row keeps the origin of its source.
pub fn fixture_from_schema_sources(
    sources: &GraphSchemaSources,
) -> Result<CoverageFixture, String> {
    let mut fixture = CoverageFixture::new();
    for source in sources.core.values().chain(sources.dynamic.values()) {
        let Some(document) = source.ontology_ttl.as_deref() else {
            continue;
        };
        let triples = crate::mapping::parse_turtle(document)?;
        fixture = absorb(
            fixture,
            &triples,
            origin_of(&source.origin),
            source.attached_at_ms,
        );
    }
    Ok(fixture)
}

/// The sources the documents declare connected: a `vg:SourceConnection` with a
/// `vg:MetadataContract` carrying an approved `vg:VirtualMapping`. A connection
/// is named by its foreign catalog name, `dc:identifier`, label, or IRI.
pub fn connected_sources_from_turtle(documents: &[&str]) -> Result<BTreeSet<String>, String> {
    const DC_IDENTIFIER: &str = "http://purl.org/dc/terms/identifier";
    let mut triples = Vec::new();
    for document in documents {
        triples.extend(crate::mapping::parse_turtle(document)?);
    }
    let index = Index::from_triples(&triples);
    let mut connected = BTreeSet::new();
    for connection in index.subjects_of_type(&format!("{VG}SourceConnection")) {
        let has_approved_mapping = index
            .subjects_of_type(&format!("{VG}MetadataContract"))
            .into_iter()
            .filter(|contract| index.vg(contract, "contractSource") == Some(connection))
            .any(|contract| {
                index
                    .subjects_of_type(&format!("{VG}VirtualMapping"))
                    .into_iter()
                    .any(|mapping| {
                        index.vg(mapping, "mapsContract") == Some(contract)
                            && index.vg(mapping, "approved") == Some("true")
                    })
            });
        if has_approved_mapping {
            let name = index
                .vg(connection, "foreignSourceName")
                .or_else(|| index.first(connection, DC_IDENTIFIER))
                .map_or_else(|| index.source_name(connection), str::to_string);
            connected.insert(name);
        }
    }
    Ok(connected)
}
