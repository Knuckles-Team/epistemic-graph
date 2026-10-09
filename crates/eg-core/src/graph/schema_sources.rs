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

use super::LegacyIntegrityPolicy;

/// Closed upper bound for one binary's immutable catalog.  The current catalog
/// contains the aggregate document, foundation, 31 domain TBoxes (the world model
/// as its life, environment and nutrition modules), the core governance-shape
/// slice, the world-model shapes (35 artifacts, EH-364) and the swarm-topology
/// vocabulary and shapes (37, ST-1) and the finance module and its shapes (39,
/// EH-411) and the temporal bi-temporal-fact shapes (40, AU-SEMANTIC-R004).
/// It is deliberately independent of the dynamic
/// 32-source tenant quota, and equal to the wire bound
/// `eg_types::graph_schema::MAX_CORE_GRAPH_SCHEMA_SOURCES`.
pub const MAX_CORE_SCHEMA_SOURCES: usize = 64;
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
    /// EH-403: a governed candidate attached under `approved:<name>` by
    /// `GraphSchema.AttachApproved`, which verified `approval_lease_id` first.
    Approved {
        name: String,
        approval_lease_id: String,
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
    if origin_owns_key(source_id, origin) {
        Ok(())
    } else {
        Err(format!(
            "schema source '{source_id}' does not match its authority origin"
        ))
    }
}

/// Whether `origin` is the authority its key prefix asserts (`admin:<name>`,
/// `pack:<connector>`, `ingest:<mapping>`, `approved:<name>`, `operator`).
fn origin_owns_key(source_id: &str, origin: &SchemaSourceOrigin) -> bool {
    match origin {
        SchemaSourceOrigin::Core { .. } => false,
        SchemaSourceOrigin::Operator => source_id == OPERATOR_SOURCE_ID,
        SchemaSourceOrigin::Admin { name } => keyed(source_id, "admin:", name),
        SchemaSourceOrigin::Pack {
            connector,
            record_id,
        } => keyed(source_id, "pack:", connector) && !record_id.is_empty(),
        SchemaSourceOrigin::Ingestion { mapping, .. } => keyed(source_id, "ingest:", mapping),
        SchemaSourceOrigin::Approved {
            name,
            approval_lease_id,
        } => keyed(source_id, "approved:", name) && !approval_lease_id.is_empty(),
    }
}

/// `source_id` is exactly `<prefix><name>` with a non-empty name.
fn keyed(source_id: &str, prefix: &str, name: &str) -> bool {
    source_id
        .strip_prefix(prefix)
        .is_some_and(|suffix| !suffix.is_empty() && suffix == name)
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
    policy: Option<LegacyIntegrityPolicy>,
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
            module: "life",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/life-v1.ttl")),
        },
        CoreSpec {
            module: "environment",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/environment-v1.ttl")),
        },
        CoreSpec {
            module: "nutrition",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/nutrition-v1.ttl")),
        },
        CoreSpec {
            module: "world-model-shapes",
            version: 1,
            shapes: Some(include_str!("../../ontology/world_model-v1.shapes.ttl")),
            ontology: None,
        },
        // SWARM-TOPOLOGY-DECIDE-DESIGN §4 (ST-1): topology classes, slot roles, stop
        // rules and task-shape admissibility, with the template-projection shapes.
        // Authored by agent-utilities, owned here (no AU .ttl, RF-ADR-009).
        CoreSpec {
            module: "swarm-topology",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/swarm_topology-v1.ttl")),
        },
        CoreSpec {
            module: "swarm-topology-shapes",
            version: 1,
            shapes: Some(include_str!("../../ontology/swarm_topology-v1.shapes.ttl")),
            ontology: None,
        },
        CoreSpec {
            module: "finance",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/finance-v1.ttl")),
        },
        CoreSpec {
            module: "finance-shapes",
            version: 1,
            shapes: Some(include_str!("../../ontology/finance-v1.shapes.ttl")),
            ontology: None,
        },
        CoreSpec {
            module: "worldview",
            version: 1,
            shapes: None,
            ontology: Some(include_str!("../../ontology/worldview-v1.ttl")),
        },
        // AU-SEMANTIC-R004: bi-temporal fact invariants (validity window,
        // contradiction/superseded-closure) over the engine's own
        // :TemporalFact vocabulary (core-foundation@1). Shapes-only, same
        // pattern as governance-shapes/world-model-shapes.
        CoreSpec {
            module: "temporal-shapes",
            version: 1,
            shapes: Some(include_str!("../../ontology/temporal-v1.shapes.ttl")),
            ontology: None,
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
        SchemaSourceOrigin::Approved {
            name,
            approval_lease_id,
        } => format!("approved\0{name}\0{approval_lease_id}").into_bytes(),
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
        assert_eq!(sources.core.len(), 40);
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
        // Pin the exact bytes compiled by `include_str!`, rather than a
        // pre-write renderer buffer (which may retain an extra terminal blank
        // line). Runtime source identities are derived from these bytes.
        let expected = [
            (
                "a2a",
                "cb29dbc911f8cdac78e4206fe914927342b9abc8ff9fe21d640498434f193898",
            ),
            (
                "action",
                "ecbbd3b188a10d231920d30a66dc9c69ebc659c4cfd857cfbd888d5bbc71f24c",
            ),
            (
                "archimate",
                "64da730d94db932bb679ae124b2fa6c2e4d490252b04a29f592b4dfef8e857d7",
            ),
            (
                "capability",
                "81c5518d2679e96848327df81c879ce08644cdcb5fbf806f2835c2a276b05011",
            ),
            (
                "argumentation",
                "72b9f83c590e65272e289224610e9d3b1231e19886943d66767b6749f3fb9ea8",
            ),
            (
                "calendar",
                "c1c17692f1cedd26fb4a4cdab4ee3db655c570d7448f5123f8ad4efafe36e6d0",
            ),
            (
                "catalog",
                "7b3a24987e3d63e3b5b21c78111cae7d5b4397c16f8b4e528ccff40559d1f5fb",
            ),
            (
                "company",
                "77b0c36d8818fbab307b201acfcceb339289090e44f420834960b346af48a1a9",
            ),
            (
                "company_infra",
                "416c839ba3cc5e53631cc7b601c8542cbe3d12fd78587c31cd62b8d2662ce1d3",
            ),
            (
                "concepts",
                "297fad75a6d0ef0135a6f8026fcc72ea4cddadcc79531c9e62fb57d35eb5fa6a",
            ),
            (
                "foundation",
                "cd3a0e736d9174d582e8b3c321966a4cd7ebf5c1264ac6640d78f4a54dc47378",
            ),
            (
                "documentation",
                "24e5e1224a9c46b6dd61b3acda6b2102e75a577bbcec8b116d2908cf23bd1c14",
            ),
            (
                "energy_geopolitics",
                "36f99061e8c71f8c4b3d0fdb4c26e1993fbbc31bf033028bab2dfd95d1d14d20",
            ),
            (
                "enterprise",
                "7ec8ca794b81774227d8a660c1cd2cafd25230921d3b0eb89ee2690b2fd67a27",
            ),
            (
                "governance-shapes",
                "8195ee0454e851d182b14c9d30e1bcfa2638b44719964cd51e08d75cdc13b12d",
            ),
            (
                "government",
                "7f3f909c1d726d5394a189115782d4eb6e9fa04f736eda8e6e1be10c8eb98e71",
            ),
            (
                "harness",
                "e762f8807d827551a37b6e3b6fd573fc66039b83310acdd63a3835be63168e91",
            ),
            (
                "hr",
                "6eb8eb2231b041b9bb58aa9c131c41349c4326f869725ac63cb6799899836b3f",
            ),
            (
                "identity",
                "b32a277c15f100d26175c2a1a24af63151a50b8f6fb54f953cbc66626c1c8828",
            ),
            (
                "infrastructure",
                "a14ccc9d0c004c92e50bb3ac10dff4fcd06f4c46131f8b4b2ee079994f1bdab1",
            ),
            (
                "medical",
                "e143acf6c6189f36c092a1fc1715e56b13a2e82d9ce6be36c54fb2475ef9b61d",
            ),
            (
                "native_source_connector",
                "aff5b66926ab41858fb8b549aae343f3286f317cf875534bb3e5e0cd89e9a35c",
            ),
            (
                "orchestration",
                "ea7640cb3dab3ec392f7d0e928b96043035fded71163f573cb1a7064aa9e338a",
            ),
            (
                "personal",
                "55ba0a0b4173d123d59e594fe8a8ca84716bd7a320407cbbed247c4ca30dfabe",
            ),
            (
                "process_intelligence",
                "3972c8d2ccb65009401a9b0f05d4e072cedfdebb4a11c534df6cdb924fb2697b",
            ),
            (
                "sdd",
                "1763e32c22024ad0a4716872955ac972853e4b79631bac4925bb65e260af40df",
            ),
            (
                "sdlc_lifecycle",
                "81ada4f509f18c302b3cca917d3c59862c6563abaa0961cb9de5f8df934738e3",
            ),
            (
                "software",
                "0f9945da89dce3c4ee657028edc67003e4a52a419807d50904323afd9c25aa66",
            ),
            (
                "system",
                "06cb25a6d2ef673cbfa25ceefb3d200f69ec6ab0d21fd7bb8bb049f0ed051824",
            ),
            (
                "trm",
                "2e5d9cffcfc8e8915470641e85766f3ce775919539f22c57746d5a5e29d6c55d",
            ),
            (
                "life",
                "c5f0d321f894e5852f868a1d85d7a2a11bfad5ea91921f2d8f6a6e0a89a03566",
            ),
            (
                "environment",
                "21af644bc9b2499efb9cde3e73a6b9b9eee05ccb118382beb7c1a2f8c07e56a8",
            ),
            (
                "nutrition",
                "06772fb766c86eda3c3acf70dda010a948f6917c496415a0df1a64ae2dac27ad",
            ),
            (
                "world-model-shapes",
                "f2be16c304237069bc97b91afc01ac90c846a3f466b7d7ace89ed3e210acdb8d",
            ),
            (
                "swarm-topology",
                "4aae8c4b8203a82179bcc1bd02623c4fae0f2b8c187ee429d854e80ae271a8a8",
            ),
            (
                "swarm-topology-shapes",
                "699371a30c9e2b770bc565df246fbdfeb7525614e3557b25fc22923b82ea9e9c",
            ),
            (
                "finance",
                "9cadbbd05049f96cdc4766dec4d1f70ca70ea3a3c5ada0bb2257610339930e01",
            ),
            (
                "finance-shapes",
                "c5c8e77e8d79283a6fe53dd97a22e16561012088f18d902793b3cb7aa197a239",
            ),
            (
                "worldview",
                "1c42096ed2fdbd70d2f65b1f7b0a40bba7f7c83f4929a8cc5f16090ba1903fb1",
            ),
            (
                "temporal-shapes",
                "9bb13dd655059b6757b33621446f960bd6992bcb5aea31d5a85f4483c86bba59",
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
