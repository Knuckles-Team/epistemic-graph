//! Durable, keyed schema authority for one graph.
//!
//! Core sources are compiled into the engine and replaced only as one catalog
//! during an engine upgrade. Dynamic sources are graph-local operator, pack, or
//! ingestion attachments. Keeping the maps separate makes the 32-source quota
//! apply only to tenant/operator material and makes it impossible for a generic
//! detach request to remove an engine-owned source.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, OnceLock};

use eg_types::contract::Digest256;

use super::IntegrityPolicyV2;

/// Closed upper bound for one binary's immutable catalog.  The current catalog
/// contains the aggregate document, foundation, 28 domain TBoxes, and the core
/// governance-shape slice.  It is deliberately independent of the dynamic
/// 32-source tenant quota.
pub const MAX_CORE_SCHEMA_SOURCES: usize = 32;
pub const MAX_TOTAL_DYNAMIC_SCHEMA_BYTES: usize = 8 << 20;
pub const MAX_SCHEMA_DOCUMENT_TRIPLES: usize = 100_000;
pub const CORE_SOURCE_PREFIX: &str = "core:";
pub const OPERATOR_SOURCE_ID: &str = "operator";
const COMPOSED_DIGEST_DOMAIN: &[u8] = b"eg/graph-schema-sources/v1";
const CORE_SET_DIGEST_DOMAIN: &[u8] = b"eg/core-schema-catalog/v1";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "origin", rename_all = "snake_case", deny_unknown_fields)]
pub enum SchemaSourceOrigin {
    Core {
        module: String,
        version: u32,
        set_digest: Digest256,
    },
    Operator,
    Admin {
        name: String,
    },
    Pack {
        connector: String,
        record_id: String,
    },
    Ingestion {
        mapping: String,
        revision: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphSchemaSource {
    pub origin: SchemaSourceOrigin,
    pub shapes_ttl: Option<Arc<str>>,
    pub ontology_ttl: Option<Arc<str>>,
    pub shapes_sha256: Option<Digest256>,
    pub ontology_sha256: Option<Digest256>,
    /// Deterministic provenance time. Replicated handlers must never read the
    /// local wall clock; until the commit envelope exposes a consensus time,
    /// live attachments use the documented sentinel `0`.
    pub attached_at_ms: u64,
}

impl GraphSchemaSource {
    pub fn new(
        origin: SchemaSourceOrigin,
        shapes_ttl: Option<Arc<str>>,
        ontology_ttl: Option<Arc<str>>,
        attached_at_ms: u64,
    ) -> Result<Self, String> {
        // A pack head that intentionally withdraws its last semantic member is
        // retained as an empty Pack-origin tombstone.  Its record id is the
        // high-water mark that prevents a stale reproject from resurrecting
        // withdrawn schema.  Generic/admin/operator sources still need a real
        // document; an empty request is never their detach alias.
        if shapes_ttl.is_none()
            && ontology_ttl.is_none()
            && !matches!(&origin, SchemaSourceOrigin::Pack { .. })
        {
            return Err("a schema source needs shapes, ontology, or both".to_string());
        }
        let shapes_sha256 = shapes_ttl
            .as_deref()
            .map(|document| Digest256::sha256(document.as_bytes()));
        let ontology_sha256 = ontology_ttl
            .as_deref()
            .map(|document| Digest256::sha256(document.as_bytes()));
        Ok(Self {
            origin,
            shapes_ttl,
            ontology_ttl,
            shapes_sha256,
            ontology_sha256,
            attached_at_ms,
        })
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.shapes_ttl.is_none()
            && self.ontology_ttl.is_none()
            && !matches!(&self.origin, SchemaSourceOrigin::Pack { .. })
        {
            return Err("schema source has no document".to_string());
        }
        for (label, document) in [
            ("shapes", self.shapes_ttl.as_deref()),
            ("ontology", self.ontology_ttl.as_deref()),
        ] {
            if document.is_some_and(|value| {
                value.len() > eg_types::graph_schema::MAX_SCHEMA_DOCUMENT_BYTES
            }) {
                return Err(format!(
                    "schema source {label} exceeds {} bytes",
                    eg_types::graph_schema::MAX_SCHEMA_DOCUMENT_BYTES
                ));
            }
        }
        validate_document_digest("shapes", self.shapes_ttl.as_deref(), self.shapes_sha256)?;
        validate_document_digest(
            "ontology",
            self.ontology_ttl.as_deref(),
            self.ontology_sha256,
        )
    }

    pub fn documents_equal(&self, other: &Self) -> bool {
        // Attachment time is provenance, not document identity. Compare each
        // document together with its recorded digest so this remains distinct
        // from full-record equality.
        self.origin == other.origin
            && same_schema_document(
                self.shapes_ttl.as_deref(),
                self.shapes_sha256,
                other.shapes_ttl.as_deref(),
                other.shapes_sha256,
            )
            && same_schema_document(
                self.ontology_ttl.as_deref(),
                self.ontology_sha256,
                other.ontology_ttl.as_deref(),
                other.ontology_sha256,
            )
    }

    pub fn byte_len(&self) -> usize {
        self.shapes_ttl.as_deref().map_or(0, str::len)
            + self.ontology_ttl.as_deref().map_or(0, str::len)
    }
}

fn same_schema_document(
    left_document: Option<&str>,
    left_digest: Option<Digest256>,
    right_document: Option<&str>,
    right_digest: Option<Digest256>,
) -> bool {
    left_document == right_document && left_digest == right_digest
}

fn validate_document_digest(
    label: &str,
    document: Option<&str>,
    digest: Option<Digest256>,
) -> Result<(), String> {
    match (document, digest) {
        (None, None) => Ok(()),
        (Some(document), Some(digest)) if Digest256::sha256(document.as_bytes()) == digest => {
            Ok(())
        }
        (Some(_), Some(_)) => Err(format!("schema source {label} digest mismatch")),
        _ => Err(format!(
            "schema source {label} document/digest presence differs"
        )),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphSchemaSources {
    /// Immutable engine-owned catalog. Not counted against `dynamic`'s quota.
    pub core: BTreeMap<String, GraphSchemaSource>,
    /// Operator/admin/pack/ingestion sources, bounded to 32 entries.
    pub dynamic: BTreeMap<String, GraphSchemaSource>,
}

impl Default for GraphSchemaSources {
    fn default() -> Self {
        Self {
            core: current_core_catalog().clone(),
            dynamic: BTreeMap::new(),
        }
    }
}

impl GraphSchemaSources {
    pub fn with_dynamic(dynamic: BTreeMap<String, GraphSchemaSource>) -> Result<Self, String> {
        let value = Self {
            core: current_core_catalog().clone(),
            dynamic,
        };
        value.validate()?;
        Ok(value)
    }

    /// Replace every persisted core module with this binary's one coherent set.
    /// Dynamic bytes remain untouched. This is the atomic engine-upgrade rule:
    /// no graph can observe a mixture of core catalog versions.
    pub fn reconcile_current_core(&mut self) {
        self.core = current_core_catalog().clone();
    }

    pub fn reconciled_current_core(mut self) -> Result<Self, String> {
        self.reconcile_current_core();
        self.validate()?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_core_sources(&self.core)?;
        validate_dynamic_sources(&self.dynamic)
    }

    pub fn composed_digest(&self) -> Digest256 {
        let mut fields = Vec::<Vec<u8>>::new();
        for (scope, sources) in [
            (b"core".as_slice(), &self.core),
            (b"dynamic", &self.dynamic),
        ] {
            for (source_id, source) in sources {
                fields.push(scope.to_vec());
                fields.push(source_id.as_bytes().to_vec());
                fields.push(origin_bytes(&source.origin));
                fields.push(optional_digest_bytes(source.shapes_sha256));
                fields.push(optional_digest_bytes(source.ontology_sha256));
            }
        }
        let refs: Vec<&[u8]> = fields.iter().map(Vec::as_slice).collect();
        Digest256::framed(COMPOSED_DIGEST_DOMAIN, &refs)
            .expect("bounded schema fields always fit digest framing")
    }

    pub fn all(&self) -> impl Iterator<Item = (&str, &GraphSchemaSource)> {
        self.core
            .iter()
            .chain(self.dynamic.iter())
            .map(|(id, source)| (id.as_str(), source))
    }

    pub fn shapes(&self) -> impl Iterator<Item = (&str, &str)> {
        self.all()
            .filter_map(|(id, source)| source.shapes_ttl.as_deref().map(|document| (id, document)))
    }

    pub fn ontologies(&self) -> impl Iterator<Item = (&str, &str)> {
        self.all().filter_map(|(id, source)| {
            source
                .ontology_ttl
                .as_deref()
                .map(|document| (id, document))
        })
    }

    pub fn has_shapes(&self) -> bool {
        self.shapes().next().is_some()
    }

    pub fn attach_dynamic(
        &mut self,
        source_id: String,
        source: GraphSchemaSource,
    ) -> Result<bool, String> {
        validate_dynamic_identity(&source_id, &source.origin)?;
        source.validate()?;
        if self
            .dynamic
            .get(&source_id)
            .is_some_and(|current| current.documents_equal(&source))
        {
            return Ok(false);
        }
        if let Some(current) = self.dynamic.get(&source_id) {
            validate_replacement_progression(current, &source)?;
        }
        let previous = self.dynamic.insert(source_id.clone(), source);
        if let Err(error) = self.validate() {
            match previous {
                Some(previous) => {
                    self.dynamic.insert(source_id, previous);
                }
                None => {
                    self.dynamic.remove(&source_id);
                }
            }
            return Err(error);
        }
        Ok(true)
    }

    pub fn detach_dynamic(&mut self, source_id: &str) -> bool {
        self.dynamic.remove(source_id).is_some()
    }
}

fn validate_core_sources(core: &BTreeMap<String, GraphSchemaSource>) -> Result<(), String> {
    if core.len() > MAX_CORE_SCHEMA_SOURCES {
        return Err("core schema catalog exceeds its closed bound".to_string());
    }
    if core != current_core_catalog() {
        return Err("core schema catalog differs from this binary's immutable catalog".to_string());
    }

    let expected_set = current_core_set_digest();
    let mut modules = BTreeSet::new();
    for (source_id, source) in core {
        let (module, _) = validate_core_source(source_id, source, expected_set)?;
        if !modules.insert(module.to_string()) {
            return Err(format!(
                "multiple active core versions for module '{module}'"
            ));
        }
    }
    Ok(())
}

fn validate_core_source<'a>(
    source_id: &'a str,
    source: &GraphSchemaSource,
    expected_set: Digest256,
) -> Result<(&'a str, u32), String> {
    let Some((module, version)) = parse_core_source_id(source_id) else {
        return Err(format!("invalid core schema source id '{source_id}'"));
    };
    match &source.origin {
        SchemaSourceOrigin::Core {
            module: origin_module,
            version: origin_version,
            set_digest,
        } if origin_module == module
            && *origin_version == version
            && *set_digest == expected_set => {}
        _ => {
            return Err(format!(
                "core schema source '{source_id}' origin is invalid"
            ))
        }
    }
    source.validate()?;
    Ok((module, version))
}

fn validate_dynamic_sources(dynamic: &BTreeMap<String, GraphSchemaSource>) -> Result<(), String> {
    if dynamic.len() > eg_types::graph_schema::MAX_GRAPH_SCHEMA_SOURCES {
        return Err("dynamic schema source count exceeds 32".to_string());
    }
    let mut total = 0usize;
    for (source_id, source) in dynamic {
        validate_dynamic_identity(source_id, &source.origin)?;
        source.validate()?;
        total = total
            .checked_add(source.byte_len())
            .ok_or_else(|| "dynamic schema byte count overflowed".to_string())?;
    }
    if total > MAX_TOTAL_DYNAMIC_SCHEMA_BYTES {
        return Err(format!(
            "dynamic schema documents exceed {MAX_TOTAL_DYNAMIC_SCHEMA_BYTES} bytes"
        ));
    }
    Ok(())
}

fn validate_dynamic_identity(source_id: &str, origin: &SchemaSourceOrigin) -> Result<(), String> {
    if source_id.is_empty()
        || source_id.len() > eg_types::graph_schema::MAX_SCHEMA_SOURCE_ID_BYTES
        || source_id.chars().any(char::is_control)
    {
        return Err(format!(
            "schema source id must be printable and 1..={} bytes",
            eg_types::graph_schema::MAX_SCHEMA_SOURCE_ID_BYTES
        ));
    }
    let valid = match origin {
        SchemaSourceOrigin::Core { .. } => false,
        SchemaSourceOrigin::Operator => source_id == OPERATOR_SOURCE_ID,
        SchemaSourceOrigin::Admin { name } => source_id
            .strip_prefix("admin:")
            .is_some_and(|suffix| !suffix.is_empty() && suffix == name),
        SchemaSourceOrigin::Pack {
            connector,
            record_id,
        } => {
            source_id
                .strip_prefix("pack:")
                .is_some_and(|suffix| !suffix.is_empty() && suffix == connector)
                && !record_id.is_empty()
        }
        SchemaSourceOrigin::Ingestion { mapping, .. } => source_id
            .strip_prefix("ingest:")
            .is_some_and(|suffix| !suffix.is_empty() && suffix == mapping),
    };
    if valid {
        Ok(())
    } else {
        Err(format!(
            "schema source '{source_id}' does not match its authority origin"
        ))
    }
}

fn validate_replacement_progression(
    current: &GraphSchemaSource,
    replacement: &GraphSchemaSource,
) -> Result<(), String> {
    let advances = match (&current.origin, &replacement.origin) {
        (
            SchemaSourceOrigin::Pack {
                connector: current_connector,
                record_id: current_record,
            },
            SchemaSourceOrigin::Pack {
                connector: replacement_connector,
                record_id: replacement_record,
            },
        ) => {
            current_connector == replacement_connector
                && pack_record_advances(current_record, replacement_record)
        }
        (
            SchemaSourceOrigin::Ingestion {
                mapping: current_mapping,
                revision: current_revision,
            },
            SchemaSourceOrigin::Ingestion {
                mapping: replacement_mapping,
                revision: replacement_revision,
            },
        ) => current_mapping == replacement_mapping && replacement_revision > current_revision,
        (SchemaSourceOrigin::Pack { .. }, _) | (SchemaSourceOrigin::Ingestion { .. }, _) => false,
        _ => true,
    };
    if advances {
        Ok(())
    } else {
        Err("SCHEMA_SOURCE_REGRESSION: imported schema source did not advance".to_string())
    }
}

fn pack_record_advances(current: &str, replacement: &str) -> bool {
    match (
        pack_record_sequence(current),
        pack_record_sequence(replacement),
    ) {
        (
            Some((current_prefix, current_sequence)),
            Some((replacement_prefix, replacement_sequence)),
        ) if current_prefix == replacement_prefix => replacement_sequence > current_sequence,
        _ => replacement > current,
    }
}

fn pack_record_sequence(record_id: &str) -> Option<(&str, u64)> {
    let (prefix_and_sequence, _digest) = record_id.rsplit_once(':')?;
    let (prefix, sequence) = prefix_and_sequence.rsplit_once(':')?;
    Some((prefix, sequence.parse().ok()?))
}

pub fn lift_v2_integrity_policy(
    policy: Option<IntegrityPolicyV2>,
) -> Result<Arc<GraphSchemaSources>, String> {
    let mut dynamic = BTreeMap::new();
    if let Some(policy) = policy {
        let source = GraphSchemaSource::new(
            SchemaSourceOrigin::Operator,
            Some(Arc::from(policy.shapes_ttl)),
            None,
            0,
        )
        .expect("a v2 policy always carries a shapes document");
        dynamic.insert(OPERATOR_SOURCE_ID.to_string(), source);
    }
    GraphSchemaSources::with_dynamic(dynamic).map(Arc::new)
}

fn current_core_catalog() -> &'static BTreeMap<String, GraphSchemaSource> {
    static CATALOG: OnceLock<BTreeMap<String, GraphSchemaSource>> = OnceLock::new();
    CATALOG.get_or_init(build_core_catalog)
}

pub fn current_core_set_digest() -> Digest256 {
    static DIGEST: OnceLock<Digest256> = OnceLock::new();
    *DIGEST.get_or_init(|| {
        let specs = core_specs();
        let mut fields = Vec::<Vec<u8>>::new();
        for spec in specs {
            fields.push(spec.module.as_bytes().to_vec());
            fields.push(spec.version.to_be_bytes().to_vec());
            fields.push(optional_digest_bytes(
                spec.shapes.map(|v| Digest256::sha256(v.as_bytes())),
            ));
            fields.push(optional_digest_bytes(
                spec.ontology.map(|v| Digest256::sha256(v.as_bytes())),
            ));
        }
        let refs: Vec<&[u8]> = fields.iter().map(Vec::as_slice).collect();
        Digest256::framed(CORE_SET_DIGEST_DOMAIN, &refs)
            .expect("closed core catalog always fits digest framing")
    })
}

fn build_core_catalog() -> BTreeMap<String, GraphSchemaSource> {
    let set_digest = current_core_set_digest();
    core_specs()
        .iter()
        .map(|spec| {
            let source_id = format!("core:{}@{}", spec.module, spec.version);
            let source = GraphSchemaSource::new(
                SchemaSourceOrigin::Core {
                    module: spec.module.to_string(),
                    version: spec.version,
                    set_digest,
                },
                spec.shapes.map(Arc::from),
                spec.ontology.map(Arc::from),
                0,
            )
            .expect("closed core catalog entries always carry a document");
            (source_id, source)
        })
        .collect()
}

struct CoreSpec {
    module: &'static str,
    version: u32,
    shapes: Option<&'static str>,
    ontology: Option<&'static str>,
}

fn core_specs() -> &'static [CoreSpec] {
    const SPECS: &[CoreSpec] = &[
        CoreSpec {
            module: "catalog",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/catalog-v1.ttl")),
        },
        CoreSpec {
            module: "foundation",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/core-foundation-v1.ttl")),
        },
        CoreSpec {
            module: "capability",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/capability-v1.ttl")),
        },
        CoreSpec {
            module: "archimate",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/archimate-v1.ttl")),
        },
        CoreSpec {
            module: "enterprise",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/enterprise-v1.ttl")),
        },
        CoreSpec {
            module: "governance-shapes",
            version: 1,
            shapes: Some(include_str!("../../ontology/governance-core-v1.shapes.ttl")),
            ontology: None,
        },
        CoreSpec {
            module: "a2a",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/a2a-v1.ttl")),
        },
        CoreSpec {
            module: "action",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/action-v1.ttl")),
        },
        CoreSpec {
            module: "argumentation",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/argumentation-v1.ttl")),
        },
        CoreSpec {
            module: "calendar",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/calendar-v1.ttl")),
        },
        CoreSpec {
            module: "company",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/company-v1.ttl")),
        },
        CoreSpec {
            module: "company_infra",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/company_infra-v1.ttl")),
        },
        CoreSpec {
            module: "concepts",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/concepts-v1.ttl")),
        },
        CoreSpec {
            module: "documentation",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/documentation-v1.ttl")),
        },
        CoreSpec {
            module: "energy_geopolitics",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/energy_geopolitics-v1.ttl")),
        },
        CoreSpec {
            module: "government",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/government-v1.ttl")),
        },
        CoreSpec {
            module: "harness",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/harness-v1.ttl")),
        },
        CoreSpec {
            module: "hr",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/hr-v1.ttl")),
        },
        CoreSpec {
            module: "identity",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/identity-v1.ttl")),
        },
        CoreSpec {
            module: "infrastructure",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/infrastructure-v1.ttl")),
        },
        CoreSpec {
            module: "medical",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/medical-v1.ttl")),
        },
        CoreSpec {
            module: "native_source_connector",
            version: 1,
            shapes: None,
            ontology: Some(include_str!(
                "../../ontology/native_source_connector-v1.ttl"
            )),
        },
        CoreSpec {
            module: "orchestration",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/orchestration-v1.ttl")),
        },
        CoreSpec {
            module: "personal",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/personal-v1.ttl")),
        },
        CoreSpec {
            module: "process_intelligence",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/process_intelligence-v1.ttl")),
        },
        CoreSpec {
            module: "sdd",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/sdd-v1.ttl")),
        },
        CoreSpec {
            module: "sdlc_lifecycle",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/sdlc_lifecycle-v1.ttl")),
        },
        CoreSpec {
            module: "software",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/software-v1.ttl")),
        },
        CoreSpec {
            module: "system",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/system-v1.ttl")),
        },
        CoreSpec {
            module: "trm",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/trm-v1.ttl")),
        },
        CoreSpec {
            module: "worldview",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/worldview-v1.ttl")),
        },
    ];
    SPECS
}

fn parse_core_source_id(source_id: &str) -> Option<(&str, u32)> {
    let body = source_id.strip_prefix(CORE_SOURCE_PREFIX)?;
    let (module, version) = body.rsplit_once('@')?;
    if module.is_empty() || version.is_empty() {
        return None;
    }
    Some((module, version.parse().ok()?))
}

fn optional_digest_bytes(digest: Option<Digest256>) -> Vec<u8> {
    match digest {
        Some(value) => {
            let mut bytes = Vec::with_capacity(33);
            bytes.push(1);
            bytes.extend_from_slice(value.as_bytes());
            bytes
        }
        None => vec![0],
    }
}

fn origin_bytes(origin: &SchemaSourceOrigin) -> Vec<u8> {
    match origin {
        SchemaSourceOrigin::Core {
            module,
            version,
            set_digest,
        } => {
            let mut value = format!("core\0{module}\0{version}\0").into_bytes();
            value.extend_from_slice(set_digest.as_bytes());
            value
        }
        SchemaSourceOrigin::Operator => b"operator".to_vec(),
        SchemaSourceOrigin::Admin { name } => format!("admin\0{name}").into_bytes(),
        SchemaSourceOrigin::Pack {
            connector,
            record_id,
        } => format!("pack\0{connector}\0{record_id}").into_bytes(),
        SchemaSourceOrigin::Ingestion { mapping, revision } => {
            format!("ingestion\0{mapping}\0{revision}").into_bytes()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admin_source(name: &str, attached_at_ms: u64) -> GraphSchemaSource {
        GraphSchemaSource::new(
            SchemaSourceOrigin::Admin {
                name: name.to_string(),
            },
            Some(Arc::<str>::from(format!(
                "@prefix sh: <http://www.w3.org/ns/shacl#> . # {name}"
            ))),
            None,
            attached_at_ms,
        )
        .unwrap()
    }

    #[test]
    fn core_catalog_has_one_version_per_module_and_is_outside_dynamic_quota() {
        let sources = GraphSchemaSources::default();
        sources.validate().unwrap();
        assert_eq!(sources.core.len(), 31);
        assert!(sources.dynamic.is_empty());
        assert!(sources.core.keys().all(|id| id.starts_with("core:")));
    }

    #[test]
    fn core_reconciliation_is_atomic_and_preserves_dynamic_bytes() {
        let mut sources = GraphSchemaSources::default();
        let operator = GraphSchemaSource::new(
            SchemaSourceOrigin::Operator,
            Some(Arc::from("@prefix sh: <http://www.w3.org/ns/shacl#> .")),
            None,
            0,
        )
        .unwrap();
        sources.dynamic.insert("operator".into(), operator.clone());
        sources.core.clear();
        sources.reconcile_current_core();
        assert_eq!(sources.dynamic.get("operator"), Some(&operator));
        assert_eq!(sources.core, current_core_catalog().clone());
    }

    #[test]
    fn document_digest_corruption_is_refused() {
        let mut source =
            GraphSchemaSource::new(SchemaSourceOrigin::Operator, Some(Arc::from("a")), None, 0)
                .unwrap();
        source.shapes_sha256 = Some(Digest256::sha256(b"b"));
        assert!(source.validate().unwrap_err().contains("digest mismatch"));
    }

    #[test]
    fn low_level_source_seam_enforces_the_document_bound() {
        let oversized = "x".repeat(eg_types::graph_schema::MAX_SCHEMA_DOCUMENT_BYTES + 1);
        let source = GraphSchemaSource::new(
            SchemaSourceOrigin::Admin {
                name: "oversized".to_string(),
            },
            Some(Arc::from(oversized)),
            None,
            0,
        )
        .unwrap();
        let mut sources = GraphSchemaSources::default();
        assert!(sources
            .attach_dynamic("admin:oversized".to_string(), source)
            .unwrap_err()
            .contains("exceeds"));
        assert!(sources.dynamic.is_empty());
    }

    #[test]
    fn dynamic_quota_excludes_the_immutable_core_catalog() {
        let mut sources = GraphSchemaSources::default();
        for index in 0..eg_types::graph_schema::MAX_GRAPH_SCHEMA_SOURCES {
            sources
                .attach_dynamic(
                    format!("admin:admin-{index:02}"),
                    admin_source(&format!("admin-{index:02}"), 0),
                )
                .unwrap();
        }
        assert_eq!(
            sources.all().count(),
            sources.core.len() + eg_types::graph_schema::MAX_GRAPH_SCHEMA_SOURCES
        );
        let error = sources
            .attach_dynamic("admin:overflow".to_string(), admin_source("overflow", 0))
            .unwrap_err();
        assert!(error.contains("exceeds 32"));
        assert!(!sources.dynamic.contains_key("admin:overflow"));
    }

    #[test]
    fn composed_digest_is_ordered_and_excludes_provenance_time() {
        let mut left = GraphSchemaSources::default();
        left.attach_dynamic("admin:a".to_string(), admin_source("a", 1))
            .unwrap();
        left.attach_dynamic("admin:b".to_string(), admin_source("b", 2))
            .unwrap();

        let mut right = GraphSchemaSources::default();
        right
            .attach_dynamic("admin:b".to_string(), admin_source("b", 200))
            .unwrap();
        right
            .attach_dynamic("admin:a".to_string(), admin_source("a", 100))
            .unwrap();

        assert_eq!(left.composed_digest(), right.composed_digest());
    }

    #[test]
    fn dynamic_seam_rejects_forged_or_mismatched_authority() {
        let mut sources = GraphSchemaSources::default();
        let core = GraphSchemaSource::new(
            SchemaSourceOrigin::Core {
                module: "forged".to_string(),
                version: 1,
                set_digest: current_core_set_digest(),
            },
            None,
            Some(Arc::from("@prefix owl: <http://www.w3.org/2002/07/owl#> .")),
            0,
        )
        .unwrap();
        assert!(sources
            .attach_dynamic("core:forged@1".to_string(), core)
            .is_err());
        assert!(sources
            .attach_dynamic("pack:wrong".to_string(), admin_source("wrong", 0))
            .is_err());
        let mismatched_pack = GraphSchemaSource::new(
            SchemaSourceOrigin::Pack {
                connector: "actual".to_string(),
                record_id: "r1".to_string(),
            },
            None,
            Some(Arc::from("@prefix owl: <http://www.w3.org/2002/07/owl#> .")),
            0,
        )
        .unwrap();
        assert!(sources
            .attach_dynamic("pack:claimed".to_string(), mismatched_pack)
            .is_err());
        assert!(sources
            .attach_dynamic("admin:claimed".to_string(), admin_source("actual", 0))
            .is_err());
        assert!(sources.dynamic.is_empty());
    }

    #[test]
    fn imported_replacements_are_monotonic_or_byte_identical() {
        let pack = |record_id: &str, document: &'static str| {
            GraphSchemaSource::new(
                SchemaSourceOrigin::Pack {
                    connector: "github".to_string(),
                    record_id: record_id.to_string(),
                },
                None,
                Some(Arc::from(document)),
                0,
            )
            .unwrap()
        };
        let ingest = |revision: u64, document: &'static str| {
            GraphSchemaSource::new(
                SchemaSourceOrigin::Ingestion {
                    mapping: "cmdb".to_string(),
                    revision,
                },
                None,
                Some(Arc::from(document)),
                0,
            )
            .unwrap()
        };
        let mut sources = GraphSchemaSources::default();
        let ontology_a = "@prefix owl: <http://www.w3.org/2002/07/owl#> . # a";
        let ontology_b = "@prefix owl: <http://www.w3.org/2002/07/owl#> . # b";
        assert!(sources
            .attach_dynamic("pack:github".to_string(), pack("r2", ontology_a))
            .unwrap());
        assert!(!sources
            .attach_dynamic("pack:github".to_string(), pack("r2", ontology_a))
            .unwrap());
        assert!(sources
            .attach_dynamic("pack:github".to_string(), pack("r1", ontology_b))
            .unwrap_err()
            .contains("SCHEMA_SOURCE_REGRESSION"));
        let numeric_pack = |record_id: &str, document: &'static str| {
            GraphSchemaSource::new(
                SchemaSourceOrigin::Pack {
                    connector: "numeric".to_string(),
                    record_id: record_id.to_string(),
                },
                None,
                Some(Arc::from(document)),
                0,
            )
            .unwrap()
        };
        assert!(sources
            .attach_dynamic(
                "pack:numeric".to_string(),
                numeric_pack("pack:numeric:2:aaaa", ontology_a),
            )
            .unwrap());
        assert!(sources
            .attach_dynamic(
                "pack:numeric".to_string(),
                numeric_pack("pack:numeric:10:bbbb", ontology_b),
            )
            .unwrap());
        assert!(sources
            .attach_dynamic("ingest:cmdb".to_string(), ingest(2, ontology_a))
            .unwrap());
        assert!(sources
            .attach_dynamic("ingest:cmdb".to_string(), ingest(1, ontology_b))
            .unwrap_err()
            .contains("SCHEMA_SOURCE_REGRESSION"));
        assert!(sources
            .attach_dynamic("ingest:cmdb".to_string(), ingest(3, ontology_b))
            .unwrap());
    }

    #[test]
    fn empty_pack_head_is_a_monotonic_withdrawal_tombstone() {
        let pack = |record_id: &str, ontology: Option<&'static str>| {
            GraphSchemaSource::new(
                SchemaSourceOrigin::Pack {
                    connector: "graph-os".to_string(),
                    record_id: record_id.to_string(),
                },
                None,
                ontology.map(Arc::from),
                0,
            )
            .unwrap()
        };
        let mut sources = GraphSchemaSources::default();
        let ontology = "@prefix owl: <http://www.w3.org/2002/07/owl#> .";
        assert!(sources
            .attach_dynamic(
                "pack:graph-os".to_string(),
                pack("pack:graph-os:1:aaaa", Some(ontology)),
            )
            .unwrap());
        assert!(sources
            .attach_dynamic(
                "pack:graph-os".to_string(),
                pack("pack:graph-os:2:bbbb", None),
            )
            .unwrap());
        let tombstone = sources.dynamic.get("pack:graph-os").unwrap();
        assert!(tombstone.shapes_ttl.is_none() && tombstone.ontology_ttl.is_none());
        assert!(sources
            .attach_dynamic(
                "pack:graph-os".to_string(),
                pack("pack:graph-os:1:cccc", Some(ontology)),
            )
            .unwrap_err()
            .contains("SCHEMA_SOURCE_REGRESSION"));
    }

    #[test]
    fn core_catalog_cannot_be_partially_installed() {
        let mut sources = GraphSchemaSources::default();
        sources.core.pop_first();
        assert!(sources
            .validate()
            .unwrap_err()
            .contains("immutable catalog"));
    }

    #[test]
    fn core_artifact_bytes_and_legacy_iri_namespace_are_pinned() {
        let expected = [
            (
                "a2a",
                "4f40dd67ed1eb4e43c94ec0883a3ecde0fa0209518aa41c6de82aa12a8a953bd",
            ),
            (
                "action",
                "0caab1bd94bbb6400621e79203890307bca9b4b454d1126f0e37ad6bd51199e2",
            ),
            (
                "archimate",
                "64da730d94db932bb679ae124b2fa6c2e4d490252b04a29f592b4dfef8e857d7",
            ),
            (
                "capability",
                "5ba669042ea3f9298e79b9c47101f726f1592d0c72411c98ab134b575f634972",
            ),
            (
                "argumentation",
                "28ff3919c4980fadb4187e57298b48ed1b78d4302443113d4355ccfdf201dc63",
            ),
            (
                "calendar",
                "7d99dfdb0b533f560d5da7dddb53ccd3ee00c0004a0cf6253af8c76861a6b311",
            ),
            (
                "catalog",
                "6345043f80eb300e674c665c24633892625060b83edd1db86feff5d9b7bf8086",
            ),
            (
                "company",
                "8aa9fdb642b0592f61a5ad6e7d2aa1e09d5355caf774b1321f667c472872aeab",
            ),
            (
                "company_infra",
                "0657987bd3d6d09ab7895d32ed2cd30ede3861b54a51f184e65c880617ba655b",
            ),
            (
                "concepts",
                "ed407ca369e706d7f1cf7f61e6835439e5a9bcd8595168b7255a120b58c8379f",
            ),
            (
                "foundation",
                "3577b40611053d1df6c516e8d2a28b645b8f3a232a0038ead7e7836e0f7595ec",
            ),
            (
                "documentation",
                "a19f19a4a99e223650d90241e464dbdb3a0a0db6d21f70fc6f63ef7f16367280",
            ),
            (
                "energy_geopolitics",
                "61a8f42107a85f1cc7a6b49a81c13f9cf5b8c6db92dac191874ba39a15a6a576",
            ),
            (
                "enterprise",
                "760d754db49890573d005121cb8617eea23dc49e7bb1e85933dc34af8942222e",
            ),
            (
                "governance-shapes",
                "c8895377a350f58d78f73f329492a7ad65cb6d8d336fd5c62b6605234fb35791",
            ),
            (
                "government",
                "0bdbdb86ed45a08bc0fbceafdd054efcbe5553ce7b641682902aed6aa39c777c",
            ),
            (
                "harness",
                "a146018e130a8b0ad08f1acea9e1fedfd3a5aff27ae5bf165caf38793e7577a2",
            ),
            (
                "hr",
                "8cbc3513fb4b0110db79b77f7bfa6bafbaf18ae77ad189552a45657a8f636058",
            ),
            (
                "identity",
                "b8458dac3c76c18a8d841a0c48767ce964468b7b6b52654f780012cd63306946",
            ),
            (
                "infrastructure",
                "8994c97643e59e81aba6a4e694b988373d0ce8497461576e1db3057e3dd12e74",
            ),
            (
                "medical",
                "9a3b0b73aff6d4420cf13e4c06711a6331fdaba135f15a9871b6cccc883019f7",
            ),
            (
                "native_source_connector",
                "6059550cf741973318fe562a4fd0de47504a29143a936311a9217d83b5479ca4",
            ),
            (
                "orchestration",
                "725d1932fc61eaffe263458a402eb56e6c78d3f5831389447beba99b63214b93",
            ),
            (
                "personal",
                "91da060a39822336595a35bf8b493e3be9bd571f33ec531f862ff5d23ce2778b",
            ),
            (
                "process_intelligence",
                "90f5496af573556c58451e513e64b0aa16aea1d9aba15e2553c8e675f59e2743",
            ),
            (
                "sdd",
                "2779d726d43ac4158c5e2a0aaabb2008be158a1284b2fa9650e4093a486a8491",
            ),
            (
                "sdlc_lifecycle",
                "53ce870fe4ad732b6d3154467a09d1478c5f5fb1a425fd7cbb35c73b4d3156f6",
            ),
            (
                "software",
                "a50f4ea1082268c3c62089277a9f3ef653ed442a79020164c720f1c719f12a58",
            ),
            (
                "system",
                "570f3722ecedacdc91c8494724a0261e9a9b5bb282d9c269662178a4f763bd1e",
            ),
            (
                "trm",
                "60ce99874cdd37c8be1dfbf685e5db52f6b91cd834fd3f4e1d151b9dc4626f22",
            ),
            (
                "worldview",
                "36b6110bafbd55b19ca0dac28a2c0f8e0348f9be601982bd579456f0e1481416",
            ),
        ];
        let catalog = current_core_catalog();
        for (module, digest) in expected {
            let source = &catalog[&format!("core:{module}@1")];
            let actual = source.shapes_sha256.or(source.ontology_sha256).unwrap();
            assert_eq!(actual.to_hex(), digest, "artifact drift for {module}");
            let document = source
                .shapes_ttl
                .as_deref()
                .or(source.ontology_ttl.as_deref())
                .unwrap();
            assert!(document.contains("http://knuckles.team/kg"));
            assert!(!document.contains("@prefix eg:"));
        }
    }
}
