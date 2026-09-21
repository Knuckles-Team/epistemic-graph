//! Bounded ConnectorPack archive and entry validation.

use std::collections::{BTreeMap, BTreeSet};

use eg_types::connector_pack::{
    ConnectorPackImportRequest, PackEntry, PackEntryKind, PackViolation, PackViolationCode,
    PackWarning, PackWarningCode,
};
use eg_types::contract::Digest256;
use sha2::{Digest, Sha256};

use crate::server::persistence::connector_pack::decode_schema_mappings;

use super::facts::uri_prefix;
use super::json::parse_bounded_json;

pub(super) fn validate_index(
    request: &ConnectorPackImportRequest,
    archive: &[u8],
    all: &[&PackEntry],
    violations: &mut Vec<PackViolation>,
    warnings: &mut Vec<PackWarning>,
) {
    let mut validator = IndexValidator::new(request, archive, all, violations, warnings);
    validate_header(request, archive, validator.violations);
    for entry in all {
        validator.validate_entry(entry);
    }
    finish_validation(&mut validator);
}

fn validate_header(
    request: &ConnectorPackImportRequest,
    archive: &[u8],
    violations: &mut Vec<PackViolation>,
) {
    if request.index.server.kind != PackEntryKind::McpServer
        || archive.len() as u64 != request.index.archive.length
    {
        violations.push(violation(
            PackViolationCode::MalformedIndex,
            None,
            "pack must name exactly one MCP server and the declared archive length",
        ));
    }
    if request
        .index
        .entries
        .windows(2)
        .any(|pair| pair[0].uri >= pair[1].uri)
    {
        violations.push(violation(
            PackViolationCode::MalformedIndex,
            None,
            "pack entries must be sorted by unique URI",
        ));
    }
    validate_catalog_binding(request, violations);
}

fn validate_catalog_binding(
    request: &ConnectorPackImportRequest,
    violations: &mut Vec<PackViolation>,
) {
    if request.index.catalog.catalog_generation == 0
        || request.index.catalog.child_connection_generation == 0
    {
        violations.push(violation(
            PackViolationCode::MalformedIndex,
            None,
            "catalog and child connection generations must be non-zero",
        ));
    }
}

struct IndexValidator<'a> {
    request: &'a ConnectorPackImportRequest,
    archive: &'a [u8],
    violations: &'a mut Vec<PackViolation>,
    warnings: &'a mut Vec<PackWarning>,
    uris: BTreeSet<String>,
    ids: BTreeSet<String>,
    ranges: Vec<(u64, u64)>,
    ontologies: Vec<String>,
    shapes: Vec<String>,
    ontology_iris: BTreeSet<&'a str>,
    shape_owners: BTreeMap<String, String>,
}

impl<'a> IndexValidator<'a> {
    fn new(
        request: &'a ConnectorPackImportRequest,
        archive: &'a [u8],
        all: &[&'a PackEntry],
        violations: &'a mut Vec<PackViolation>,
        warnings: &'a mut Vec<PackWarning>,
    ) -> Self {
        Self {
            request,
            archive,
            violations,
            warnings,
            uris: BTreeSet::new(),
            ids: BTreeSet::new(),
            ranges: Vec::new(),
            ontologies: Vec::new(),
            shapes: Vec::new(),
            ontology_iris: all
                .iter()
                .filter(|entry| entry.kind == PackEntryKind::Ontology)
                .map(|entry| entry.uri.as_str())
                .collect(),
            shape_owners: BTreeMap::new(),
        }
    }

    fn validate_entry(&mut self, entry: &PackEntry) {
        self.validate_identity(entry);
        self.validate_sections(entry);
        let body = section_bytes(self.archive, &entry.body).unwrap_or_default();
        self.validate_body(entry, body);
        self.validate_schemas(entry);
        self.validate_annotations(entry);
        if entry_description(entry, body).is_none() {
            self.warnings.push(PackWarning {
                code: PackWarningCode::EmptyDescription,
                uri: Some(entry.uri.clone()),
                detail: "entry description is empty".into(),
            });
        }
    }

    fn validate_identity(&mut self, entry: &PackEntry) {
        if !canonical_identity(entry) {
            self.reject(
                PackViolationCode::MalformedIndex,
                Some(&entry.uri),
                "entry URI and name must be non-empty canonical text",
            );
        }
        if !self.uris.insert(entry.uri.clone()) {
            self.reject(
                PackViolationCode::MalformedIndex,
                Some(&entry.uri),
                "entry URI is duplicated",
            );
        }
        validate_uri_kind(self, entry);
        let expected_server = format!("mcp-server://{}", self.request.index.connector.as_str());
        if entry.kind == PackEntryKind::McpServer && entry.uri != expected_server {
            self.reject(
                PackViolationCode::MalformedIndex,
                Some(&entry.uri),
                "server URI must name the pack connector exactly",
            );
        }
        let id = eg_types::connector_pack::pack_component_id(
            self.request.index.connector.as_str(),
            entry.kind,
            &entry.name,
        );
        if !self.ids.insert(id) {
            self.reject(
                PackViolationCode::DuplicateComponentId,
                Some(&entry.uri),
                "two entries mint the same component id",
            );
        }
    }

    fn validate_sections(&mut self, entry: &PackEntry) {
        let sections = [(&entry.body, eg_types::connector_pack::MAX_PACK_BODY_BYTES)]
            .into_iter()
            .chain(entry.input_schema.iter().map(|section| {
                (
                    section,
                    eg_types::connector_pack::MAX_PACK_SCHEMA_SECTION_BYTES,
                )
            }))
            .chain(entry.output_schema.iter().map(|section| {
                (
                    section,
                    eg_types::connector_pack::MAX_PACK_SCHEMA_SECTION_BYTES,
                )
            }));
        for (section, limit) in sections {
            self.validate_section(entry, section, limit);
        }
    }

    fn validate_section(
        &mut self,
        entry: &PackEntry,
        section: &eg_types::connector_pack::PackSection,
        limit: u64,
    ) {
        let Ok(bytes) = section_bytes(self.archive, section) else {
            self.reject(
                PackViolationCode::MalformedSections,
                Some(&entry.uri),
                "section is out of range or exceeds its bound",
            );
            return;
        };
        if section.length > limit {
            self.reject(
                PackViolationCode::MalformedSections,
                Some(&entry.uri),
                "section is out of range or exceeds its bound",
            );
            return;
        }
        if Digest256::from_bytes(Sha256::digest(bytes).into()) != section.sha256 {
            self.reject(
                PackViolationCode::ArchiveDigestMismatch,
                Some(&entry.uri),
                "section digest differs from archive bytes",
            );
        }
        self.ranges.push((
            section.offset,
            section.offset.saturating_add(section.length),
        ));
    }

    fn validate_body(&mut self, entry: &PackEntry, body: &[u8]) {
        if text_kind(entry.kind) {
            match std::str::from_utf8(body) {
                Ok(text) if !text.starts_with('\u{feff}') => self.validate_text(entry, text),
                _ => self.reject(
                    PackViolationCode::MalformedBody,
                    Some(&entry.uri),
                    "text body must be UTF-8 without BOM",
                ),
            }
        }
        if json_kind(entry.kind) && parse_bounded_json(body).is_err() {
            self.reject(
                PackViolationCode::MalformedBody,
                Some(&entry.uri),
                "entry body is not bounded JSON",
            );
        }
        if entry.kind == PackEntryKind::Manifest && decode_schema_mappings(body).is_err() {
            self.reject(
                PackViolationCode::MalformedBody,
                Some(&entry.uri),
                "connector manifest schema mappings are invalid",
            );
        }
        if entry.kind == PackEntryKind::Skill && !valid_skill_frontmatter(body, &entry.name) {
            self.reject(
                PackViolationCode::MalformedBody,
                Some(&entry.uri),
                "skill front matter is absent, unsafe, or does not name the entry",
            );
        }
    }

    fn validate_text(&mut self, entry: &PackEntry, text: &str) {
        if !matches!(entry.kind, PackEntryKind::Ontology | PackEntryKind::Shapes) {
            return;
        }
        if text
            .lines()
            .any(|line| line.trim_start().starts_with("@base"))
        {
            let code = rdf_invalid_code(entry.kind);
            self.reject(
                code,
                Some(&entry.uri),
                "RDF @base declarations are forbidden",
            );
        }
        match parse_rdf_bounded(entry.kind, text) {
            Ok(()) if entry.kind == PackEntryKind::Ontology => self.validate_ontology(entry, text),
            Ok(()) => self.validate_shapes(entry, text),
            Err((code, detail)) => self.reject(code, Some(&entry.uri), detail),
        }
    }

    fn validate_ontology(&mut self, entry: &PackEntry, text: &str) {
        if let Err(detail) = validate_ontology_imports(text, &entry.uri, &self.ontology_iris) {
            self.reject(PackViolationCode::OntologyInvalid, Some(&entry.uri), detail);
        } else {
            self.ontologies.push(text.to_string());
        }
    }

    fn validate_shapes(&mut self, entry: &PackEntry, text: &str) {
        for shape in declared_shape_iris(text) {
            if let Some(first) = self.shape_owners.insert(shape.clone(), entry.uri.clone()) {
                self.warnings.push(PackWarning {
                    code: PackWarningCode::DuplicateShapeIri,
                    uri: Some(entry.uri.clone()),
                    detail: format!("shape IRI {shape} is also declared by {first}")
                        .chars()
                        .take(1024)
                        .collect(),
                });
            }
        }
        self.shapes.push(text.to_string());
    }

    fn validate_schemas(&mut self, entry: &PackEntry) {
        if entry.kind == PackEntryKind::Tool && entry.input_schema.is_none() {
            self.reject(
                PackViolationCode::MissingToolSchema,
                Some(&entry.uri),
                "tool input_schema is required",
            );
        }
        validate_mcp_schemas(self, entry);
        for section in entry.input_schema.iter().chain(entry.output_schema.iter()) {
            self.validate_schema(entry, section);
        }
    }

    fn validate_schema(
        &mut self,
        entry: &PackEntry,
        section: &eg_types::connector_pack::PackSection,
    ) {
        let Ok(bytes) = section_bytes(self.archive, section) else {
            return;
        };
        let Ok(value) = parse_bounded_json(bytes) else {
            self.reject(
                PackViolationCode::MalformedBody,
                Some(&entry.uri),
                "schema is not bounded JSON",
            );
            return;
        };
        let is_input = entry
            .input_schema
            .as_ref()
            .is_some_and(|input| std::ptr::eq(section, input));
        if entry.kind == PackEntryKind::Tool
            && is_input
            && value.get("type").and_then(|value| value.as_str()) != Some("object")
        {
            self.reject(
                PackViolationCode::MalformedBody,
                Some(&entry.uri),
                "tool input schema root type must be object",
            );
        }
    }

    fn validate_annotations(&mut self, entry: &PackEntry) {
        for iri in entry
            .annotations
            .provides
            .iter()
            .chain(entry.annotations.requires_capabilities.iter())
        {
            if !valid_iri(iri) {
                self.reject(
                    PackViolationCode::InvalidAnnotation,
                    Some(&entry.uri),
                    "capability is not an absolute IRI",
                );
            }
        }
    }

    fn reject(&mut self, code: PackViolationCode, uri: Option<&str>, detail: &str) {
        self.violations.push(violation(code, uri, detail));
    }
}

fn validate_uri_kind(validator: &mut IndexValidator<'_>, entry: &PackEntry) {
    if matches!(
        entry.kind,
        PackEntryKind::Resource | PackEntryKind::ResourceTemplate
    ) {
        if !generic_mcp_uri(&entry.uri) {
            validator.reject(
                PackViolationCode::MalformedIndex,
                Some(&entry.uri),
                "MCP resource URI/template must have an absolute URI scheme",
            );
        }
    } else if !entry.uri.starts_with(uri_prefix(entry.kind)) {
        validator.reject(
            PackViolationCode::MalformedIndex,
            Some(&entry.uri),
            "entry URI scheme does not match its kind",
        );
    }
}

fn validate_mcp_schemas(validator: &mut IndexValidator<'_>, entry: &PackEntry) {
    if entry.kind == PackEntryKind::Resource && entry.output_schema.is_none() {
        validator.reject(
            PackViolationCode::MalformedBody,
            Some(&entry.uri),
            "resource content schema is required",
        );
    }
    if entry.kind == PackEntryKind::ResourceTemplate
        && (entry.input_schema.is_none() || entry.output_schema.is_none())
    {
        validator.reject(
            PackViolationCode::MalformedBody,
            Some(&entry.uri),
            "resource template argument and result schemas are required",
        );
    }
}

fn canonical_identity(entry: &PackEntry) -> bool {
    entry.uri.trim() == entry.uri
        && !entry.uri.is_empty()
        && entry.name.trim() == entry.name
        && !entry.name.is_empty()
        && entry.name.len() <= 256
        && !entry.name.chars().any(char::is_control)
}

fn generic_mcp_uri(uri: &str) -> bool {
    let scheme_end = uri.find(':').unwrap_or_default();
    scheme_end > 0
        && uri[..scheme_end].bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_alphabetic()
                || (index > 0 && (byte.is_ascii_digit() || matches!(byte, b'+' | b'-' | b'.')))
        })
}

fn finish_validation(validator: &mut IndexValidator<'_>) {
    validator.ranges.sort_unstable();
    let mut cursor = 0;
    let mut malformed = false;
    for (start, end) in validator.ranges.clone() {
        if start != cursor || end < start {
            validator.reject(
                PackViolationCode::MalformedSections,
                None,
                "archive sections must be non-overlapping and cover the archive exactly",
            );
            malformed = true;
            break;
        }
        cursor = end;
    }
    if malformed || cursor != validator.archive.len() as u64 {
        validator.reject(
            PackViolationCode::MalformedSections,
            None,
            "archive sections do not cover the complete archive",
        );
    }
    if let Err((code, detail)) = validate_rdf_unions(&validator.ontologies, &validator.shapes) {
        validator.reject(code, None, detail);
    }
}

fn json_kind(kind: PackEntryKind) -> bool {
    matches!(
        kind,
        PackEntryKind::McpServer
            | PackEntryKind::Tool
            | PackEntryKind::ModelProfile
            | PackEntryKind::A2aCard
    )
}

fn rdf_invalid_code(kind: PackEntryKind) -> PackViolationCode {
    if kind == PackEntryKind::Ontology {
        PackViolationCode::OntologyInvalid
    } else {
        PackViolationCode::ShapesInvalid
    }
}

fn text_kind(kind: PackEntryKind) -> bool {
    !matches!(
        kind,
        PackEntryKind::A2aCard
            | PackEntryKind::ModelProfile
            | PackEntryKind::Resource
            | PackEntryKind::ResourceTemplate
    )
}

#[cfg(feature = "rdf")]
fn parse_rdf_bounded(
    kind: PackEntryKind,
    text: &str,
) -> Result<(), (PackViolationCode, &'static str)> {
    let code = rdf_invalid_code(kind);
    let triples =
        eg_rdf::mapping::parse_turtle(text).map_err(|_| (code, "RDF body is not valid Turtle"))?;
    if triples.len() > 100_000 {
        return Err((
            PackViolationCode::ValidationBudgetExceeded,
            "RDF graph exceeds the triple validation budget",
        ));
    }
    for triple in &triples {
        validate_rdf_triple(code, triple)?;
    }
    Ok(())
}

#[cfg(feature = "rdf")]
fn validate_rdf_triple(
    code: PackViolationCode,
    triple: &eg_rdf::oxrdf::Triple,
) -> Result<(), (PackViolationCode, &'static str)> {
    if triple.subject.to_string().len() > 4 * 1024 || triple.predicate.as_str().len() > 4 * 1024 {
        return Err((code, "RDF graph contains an IRI above the served bound"));
    }
    match &triple.object {
        eg_rdf::oxrdf::Term::Literal(literal) if literal.value().len() > 256 * 1024 => {
            Err((code, "RDF graph contains a literal above the served bound"))
        }
        eg_rdf::oxrdf::Term::NamedNode(node) if node.as_str().len() > 4 * 1024 => {
            Err((code, "RDF graph contains an IRI above the served bound"))
        }
        _ => Ok(()),
    }
}

#[cfg(feature = "rdf")]
pub(super) fn validate_ontology_imports(
    text: &str,
    current: &str,
    allowed: &BTreeSet<&str>,
) -> Result<(), &'static str> {
    use eg_rdf::oxrdf::Term;

    const OWL_IMPORTS: &str = "http://www.w3.org/2002/07/owl#imports";
    let triples = eg_rdf::mapping::parse_turtle(text)
        .map_err(|_| "ontology import policy could not parse its graph")?;
    for triple in triples {
        if triple.predicate.as_str() != OWL_IMPORTS {
            continue;
        }
        let Term::NamedNode(imported) = &triple.object else {
            return Err("owl:imports must name an ontology IRI from this pack");
        };
        if imported.as_str() == current || !allowed.contains(imported.as_str()) {
            return Err("owl:imports may name only an ontology entry in this pack");
        }
    }
    Ok(())
}

#[cfg(not(feature = "rdf"))]
pub(super) fn validate_ontology_imports(
    _text: &str,
    _current: &str,
    _allowed: &BTreeSet<&str>,
) -> Result<(), &'static str> {
    Err("RDF validation is unavailable in this build")
}

#[cfg(feature = "rdf")]
pub(super) fn declared_shape_iris(text: &str) -> BTreeSet<String> {
    use eg_rdf::oxrdf::{NamedOrBlankNode, Term};

    const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    const NODE_SHAPE: &str = "http://www.w3.org/ns/shacl#NodeShape";
    const PROPERTY_SHAPE: &str = "http://www.w3.org/ns/shacl#PropertyShape";
    eg_rdf::mapping::parse_turtle(text)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|triple| {
            if triple.predicate.as_str() != RDF_TYPE
                || !matches!(&triple.object, Term::NamedNode(node) if matches!(node.as_str(), NODE_SHAPE | PROPERTY_SHAPE))
            {
                return None;
            }
            match triple.subject {
                NamedOrBlankNode::NamedNode(node) => Some(node.as_str().to_string()),
                NamedOrBlankNode::BlankNode(_) => None,
                #[allow(unreachable_patterns)]
                _ => None,
            }
        })
        .collect()
}

#[cfg(not(feature = "rdf"))]
pub(super) fn declared_shape_iris(_text: &str) -> BTreeSet<String> {
    BTreeSet::new()
}

#[cfg(not(feature = "rdf"))]
fn parse_rdf_bounded(
    _kind: PackEntryKind,
    _text: &str,
) -> Result<(), (PackViolationCode, &'static str)> {
    Err((
        PackViolationCode::ValidationBudgetExceeded,
        "RDF validation is unavailable in this build",
    ))
}

#[cfg(all(feature = "owl", feature = "shacl"))]
fn validate_rdf_unions(
    ontologies: &[String],
    shapes: &[String],
) -> Result<(), (PackViolationCode, &'static str)> {
    if ontologies.is_empty() && shapes.is_empty() {
        return Ok(());
    }
    let ontology = ontologies.join("\n");
    let shape_graph = shapes.join("\n");
    let triples = eg_rdf::mapping::parse_turtle(&ontology).map_err(|_| {
        (
            PackViolationCode::OntologyInvalid,
            "ontology union is not valid Turtle",
        )
    })?;
    if triples.len() > 100_000 {
        return Err((
            PackViolationCode::ValidationBudgetExceeded,
            "ontology union exceeds the triple validation budget",
        ));
    }
    const MAX_REASONING_STEPS: usize = 25_000_000;
    if triples.len().saturating_mul(triples.len()) > MAX_REASONING_STEPS {
        return Err((
            PackViolationCode::ValidationBudgetExceeded,
            "ontology classification exceeds the deterministic derivation budget",
        ));
    }
    let mut reasoner = eg_rdf::owl::Reasoner::from_triples(&triples);
    if !reasoner.classify().consistent {
        return Err((
            PackViolationCode::OntologyInconsistent,
            "ontology union contains an unsatisfiable named class",
        ));
    }
    if shapes.is_empty() {
        return Ok(());
    }
    let shape_triples = eg_rdf::mapping::parse_turtle(&shape_graph).map_err(|_| {
        (
            PackViolationCode::ShapesInvalid,
            "shapes union is not valid Turtle",
        )
    })?;
    const MAX_SHACL_STEPS: usize = 10_000_000;
    if shape_triples.len().saturating_mul(triples.len().max(1)) > MAX_SHACL_STEPS {
        return Err((
            PackViolationCode::ValidationBudgetExceeded,
            "SHACL validation exceeds the deterministic evaluation budget",
        ));
    }
    let upper = shape_graph.to_ascii_uppercase();
    if upper.contains("SERVICE") {
        return Err((
            PackViolationCode::ShapesInvalid,
            "SHACL SPARQL SERVICE constraints are forbidden",
        ));
    }
    eg_shacl::IcvPolicy::from_turtle(&shape_graph).map_err(|_| {
        (
            PackViolationCode::ShapesInvalid,
            "shapes union is not a supported ICV policy",
        )
    })?;
    let report = eg_shacl::validate_icv_turtle(&shape_graph, &ontology).map_err(|_| {
        (
            PackViolationCode::ShapesInvalid,
            "SHACL validation could not evaluate the shapes union",
        )
    })?;
    if !report.conforms {
        return Err((
            PackViolationCode::ShaclViolation,
            "ontology union does not conform to the shapes union",
        ));
    }
    Ok(())
}

#[cfg(not(all(feature = "owl", feature = "shacl")))]
fn validate_rdf_unions(
    ontologies: &[String],
    shapes: &[String],
) -> Result<(), (PackViolationCode, &'static str)> {
    if ontologies.is_empty() && shapes.is_empty() {
        Ok(())
    } else {
        Err((
            PackViolationCode::ValidationBudgetExceeded,
            "OWL and SHACL validation are unavailable in this build",
        ))
    }
}

pub(super) fn entry_description(entry: &PackEntry, body: &[u8]) -> Option<String> {
    if entry.kind == PackEntryKind::Skill {
        let text = std::str::from_utf8(body).ok()?;
        let rest = text
            .strip_prefix("---\n")
            .or_else(|| text.strip_prefix("---\r\n"))?;
        let end = rest.find("\n---\n").or_else(|| rest.find("\r\n---\r\n"))?;
        return rest[..end].lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            (key.trim() == "description")
                .then(|| value.split_whitespace().collect::<Vec<_>>().join(" "))
                .filter(|value| !value.is_empty())
        });
    }
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()?
        .get("description")?
        .as_str()
        .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|value| !value.is_empty())
}

pub(super) fn valid_skill_frontmatter(body: &[u8], expected_name: &str) -> bool {
    let Ok(text) = std::str::from_utf8(body) else {
        return false;
    };
    let Some(rest) = text
        .strip_prefix("---\n")
        .or_else(|| text.strip_prefix("---\r\n"))
    else {
        return false;
    };
    let end = rest.find("\n---\n").or_else(|| rest.find("\r\n---\r\n"));
    let Some(end) = end.filter(|end| *end <= 16 * 1024) else {
        return false;
    };
    let front = &rest[..end];
    if front.lines().any(|line| {
        line.contains('&')
            || line.contains('*')
            || line.contains('!')
            || line.contains('{')
            || line.contains('}')
            || line.trim() == "---"
            || line.trim() == "..."
    }) {
        return false;
    }
    let mut name = None;
    let mut description = None;
    for line in front.lines() {
        let line = line.trim_end_matches('\r');
        if line.chars().next().is_some_and(char::is_whitespace)
            || line.trim_start().starts_with('-')
        {
            continue;
        }
        let Some((key, value)) = line.split_once(':') else {
            return false;
        };
        let value = value.trim();
        match key.trim() {
            "name" => name = Some(value.trim_matches(&['\'', '"'][..])),
            "description" => description = Some(value),
            _ => {}
        }
    }
    name == Some(expected_name) && description.is_some_and(|value| !value.is_empty())
}
pub(super) fn section_bytes<'a>(
    archive: &'a [u8],
    section: &eg_types::connector_pack::PackSection,
) -> Result<&'a [u8], String> {
    let start =
        usize::try_from(section.offset).map_err(|_| "section offset overflow".to_string())?;
    let end = usize::try_from(
        section
            .offset
            .checked_add(section.length)
            .ok_or_else(|| "section range overflow".to_string())?,
    )
    .map_err(|_| "section end overflow".to_string())?;
    archive
        .get(start..end)
        .ok_or_else(|| "section outside archive".to_string())
}

#[cfg(test)]
mod mcp_resource_uri_tests {
    use super::generic_mcp_uri;

    #[test]
    fn accepts_generic_absolute_resource_uris_and_templates() {
        assert!(generic_mcp_uri("file:///srv/catalog/item.json"));
        assert!(generic_mcp_uri("https://example.test/items/{item_id}"));
        assert!(generic_mcp_uri("company+graph://tenant/{kind}/{id}"));
    }

    #[test]
    fn rejects_relative_or_malformed_resource_uris() {
        assert!(!generic_mcp_uri("resources/item.json"));
        assert!(!generic_mcp_uri(":missing-scheme"));
        assert!(!generic_mcp_uri("1invalid://item"));
    }
}
fn valid_iri(value: &str) -> bool {
    value.contains(':')
        && !value
            .bytes()
            .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
}

fn violation(code: PackViolationCode, uri: Option<&str>, detail: &str) -> PackViolation {
    PackViolation {
        code,
        uri: uri.map(str::to_string),
        detail: detail.chars().take(1024).collect(),
    }
}
