//! Bounded ConnectorPack archive and entry validation.

use std::collections::{BTreeMap, BTreeSet};

use eg_types::connector_pack::{
    ConnectorPackImportRequest, PackEntry, PackEntryKind, PackViolation, PackViolationCode,
    PackWarning, PackWarningCode,
};
use eg_types::contract::Digest256;
use sha2::{Digest, Sha256};

use crate::server::persistence::connector_pack::{
    decode_relationship_mappings, decode_schema_mappings,
};

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
        // An in-range section covers its bytes whatever else is wrong with it,
        // so an oversized section is reported as G2 alone, never also as a G4
        // coverage gap it did not cause.
        self.ranges.push((
            section.offset,
            section.offset.saturating_add(section.length),
        ));
        if section.length > limit {
            self.reject(
                PackViolationCode::PackTooLarge,
                Some(&entry.uri),
                "section exceeds its served size bound",
            );
            return;
        }
        if Digest256::from_bytes(Sha256::digest(bytes).into()) != section.sha256 {
            self.reject(
                PackViolationCode::PackDigestMismatch,
                Some(&entry.uri),
                "section digest differs from archive bytes",
            );
        }
    }

    fn validate_body(&mut self, entry: &PackEntry, body: &[u8]) {
        self.validate_text_body(entry, body);
        self.validate_json_body(entry, body);
        self.validate_manifest_body(entry, body);
        self.validate_skill_body(entry, body);
        if let Err(detail) = super::skill_files::validate_skill_file(entry) {
            self.reject(PackViolationCode::MalformedIndex, Some(&entry.uri), detail);
        }
    }

    fn validate_text_body(&mut self, entry: &PackEntry, body: &[u8]) {
        if !text_kind(entry.kind) {
            return;
        }
        match std::str::from_utf8(body) {
            Ok(text) if !text.starts_with('\u{feff}') => self.validate_text(entry, text),
            _ => self.reject(
                PackViolationCode::MalformedBody,
                Some(&entry.uri),
                "text body must be UTF-8 without BOM",
            ),
        }
    }

    fn validate_json_body(&mut self, entry: &PackEntry, body: &[u8]) {
        if json_kind(entry.kind) && parse_bounded_json(body).is_err() {
            self.reject(
                PackViolationCode::MalformedBody,
                Some(&entry.uri),
                "entry body is not bounded JSON",
            );
        }
    }

    fn validate_manifest_body(&mut self, entry: &PackEntry, body: &[u8]) {
        if entry.kind != PackEntryKind::Manifest {
            return;
        }
        if decode_schema_mappings(body).is_err() || decode_relationship_mappings(body).is_err() {
            self.reject(
                PackViolationCode::MalformedBody,
                Some(&entry.uri),
                "connector manifest schema mappings are invalid",
            );
        }
    }

    fn validate_skill_body(&mut self, entry: &PackEntry, body: &[u8]) {
        if entry.kind != PackEntryKind::Skill {
            return;
        }
        if !valid_skill_frontmatter(body, &entry.name) {
            self.reject(
                PackViolationCode::MalformedBody,
                Some(&entry.uri),
                "skill front matter is absent, unsafe, or does not name the entry",
            );
        } else if super::front_matter::declared_skill_type(body).is_err() {
            self.reject(
                PackViolationCode::MalformedBody,
                Some(&entry.uri),
                "skill front-matter type is not skill, workflow, graph or mcp_skill",
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
        let findings = super::annotations::check_annotations(entry.kind, &entry.annotations);
        for (code, detail) in findings.violations {
            self.reject(code, Some(&entry.uri), detail);
        }
        for (code, detail) in findings.warnings {
            self.warnings.push(PackWarning {
                code,
                uri: Some(entry.uri.clone()),
                detail,
            });
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

/// Kinds whose body is SDK-serialized JSON (G8): the server identity, a tool
/// descriptor, a prompt's rendered `prompts/get` capture, a model profile and
/// an A2A card. Every one goes through the one bounded parser.
fn json_kind(kind: PackEntryKind) -> bool {
    matches!(
        kind,
        PackEntryKind::McpServer
            | PackEntryKind::Tool
            | PackEntryKind::Prompt
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
            | PackEntryKind::SkillFile
    )
}

#[cfg(feature = "rdf")]
pub(super) use eg_rdf::pack::{declared_shape_iris, validate_ontology_imports};

#[cfg(feature = "rdf")]
fn parse_rdf_bounded(
    kind: PackEntryKind,
    text: &str,
) -> Result<(), (PackViolationCode, &'static str)> {
    eg_rdf::pack::validate_document(rdf_invalid_code(kind), text)
}

#[cfg(not(feature = "rdf"))]
pub(super) fn validate_ontology_imports(
    _text: &str,
    _current: &str,
    _allowed: &BTreeSet<&str>,
) -> Result<(), &'static str> {
    Err("RDF validation is unavailable in this build")
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
    let ontology = eg_rdf::pack::scoped_union(ontologies).map_err(|_| {
        (
            PackViolationCode::OntologyInvalid,
            "an ontology file is not valid Turtle",
        )
    })?;
    let triples = ontology.triples;
    if triples.len() > 100_000 {
        return Err((
            PackViolationCode::ValidationBudgetExceeded,
            "ontology union exceeds the triple validation budget",
        ));
    }
    // G14 (EH-119, EH-355 ruling (c)): the EL+/RL classification and then the
    // full-ABox tableau, each inside one deterministic step budget -- the same
    // verdict on every host, and the same budget the schema attach uses.
    const MAX_REASONING_STEPS: u64 = 10_000_000;
    match eg_rdf::tableau::check_pack_ontology(&triples, MAX_REASONING_STEPS) {
        Ok(()) => {}
        Err(eg_rdf::tableau::BoundedCheckRefusal::Inconsistent { .. }) => {
            return Err((
                PackViolationCode::OntologyInconsistent,
                "ontology union is inconsistent: an unsatisfiable class or individual",
            ));
        }
        Err(eg_rdf::tableau::BoundedCheckRefusal::BudgetExceeded { .. }) => {
            return Err((
                PackViolationCode::ValidationBudgetExceeded,
                "ontology reasoning exceeds the deterministic step budget",
            ));
        }
    }
    if shapes.is_empty() {
        return Ok(());
    }
    validate_shapes_union(shapes, &ontology.ntriples, triples.len())
}

/// G15/G16 over the file-scoped shapes union, against the ontology union.
#[cfg(all(feature = "owl", feature = "shacl"))]
fn validate_shapes_union(
    shapes: &[String],
    ontology: &str,
    ontology_triples: usize,
) -> Result<(), (PackViolationCode, &'static str)> {
    let shape_union = eg_rdf::pack::scoped_union(shapes).map_err(|_| {
        (
            PackViolationCode::ShapesInvalid,
            "a shapes file is not valid Turtle",
        )
    })?;
    let shape_triples = shape_union.triples;
    let shape_graph = shape_union.ntriples;
    // The ICV parser represents unsupported paths but ignores them during
    // evaluation. A pack must refuse one rather than silently drop a declared
    // constraint (G15): this build supports only predicate paths.
    const SH_PATH: &str = "http://www.w3.org/ns/shacl#path";
    if shape_triples.iter().any(|triple| {
        triple.predicate.as_str() == SH_PATH
            && !matches!(&triple.object, eg_rdf::oxrdf::Term::NamedNode(_))
    }) {
        return Err((
            PackViolationCode::ShapesInvalid,
            "SHACL property paths must be predicate IRIs",
        ));
    }
    const MAX_SHACL_STEPS: usize = 10_000_000;
    if shape_triples.len().saturating_mul(ontology_triples.max(1)) > MAX_SHACL_STEPS {
        return Err((
            PackViolationCode::ValidationBudgetExceeded,
            "SHACL validation exceeds the deterministic evaluation budget",
        ));
    }
    if shapes
        .iter()
        .any(|document| document.to_ascii_uppercase().contains("SERVICE"))
    {
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
    let report = eg_shacl::validate_icv_turtle(&shape_graph, ontology).map_err(|_| {
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
        return super::front_matter::front_matter_value(body, "description");
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

pub(super) fn violation(code: PackViolationCode, uri: Option<&str>, detail: &str) -> PackViolation {
    PackViolation {
        code,
        uri: uri.map(str::to_string),
        detail: detail.chars().take(1024).collect(),
    }
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
