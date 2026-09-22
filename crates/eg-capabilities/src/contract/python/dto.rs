//! Render schema-driven nested Python DTO modules.

use std::fmt::Write as _;

use super::{mapping_type, optional, HEADER};

/// One generated nested-DTO surface. The renderer is schema-driven: roots name
/// JSON-Schema definitions and every transitive `$ref` is collected from the
/// request/result documents. Adding another surface is a registry row, not a
/// hand-written generated module.
pub(super) struct DtoSurface {
    pub(super) method: &'static str,
    pub(super) module: &'static str,
    pub(super) result_domain: &'static str,
    pub(super) roots: &'static [&'static str],
    pub(super) result_model: Option<&'static str>,
    pub(super) required: bool,
    /// Public wire-format identities owned by the Rust contract. Keeping them
    /// generated prevents a Python builder from copying a release number.
    pub(super) constants: &'static [(&'static str, u64)],
}

pub(super) const DTO_SURFACES: &[DtoSurface] = &[
    DtoSurface {
        method: "ListRegisteredServers",
        module: "server_registry",
        result_domain: "cluster",
        roots: &[
            "RegisteredServerListRequest",
            "RegisteredServerCursor",
            "RegisteredServerView",
            "RegisteredServerListPage",
        ],
        result_model: Some("RegisteredServerListPage"),
        required: true,
        constants: &[],
    },
    DtoSurface {
        method: "AgentComponent",
        module: "agent_component",
        result_domain: "storage",
        roots: &[
            "AgentComponentOp",
            "AgentComponentEntry",
            "AgentComponentContentRequest",
            "AgentComponentContentResult",
            "AgentComponentSearchRequest",
            "AgentComponentSearchPage",
        ],
        result_model: None,
        required: true,
        constants: &[],
    },
    DtoSurface {
        method: "WriteBack",
        module: "write_back",
        result_domain: "storage",
        roots: &["WriteBackOp", "WriteBackReceiptPage"],
        result_model: None,
        required: true,
        constants: &[],
    },
    DtoSurface {
        method: "ConnectorPack",
        module: "connector_pack",
        result_domain: "storage",
        roots: &[
            "ConnectorPackOp",
            "ConnectorPackImportRequest",
            "ConnectorPackStatusRequest",
            "ConnectorPackStatus",
            "PackImportResult",
            "PackWriteErrorCode",
        ],
        result_model: None,
        required: true,
        constants: &[(
            "CONNECTOR_PACK_SCHEMA_VERSION",
            eg_types::connector_pack::CONNECTOR_PACK_SCHEMA_VERSION as u64,
        )],
    },
    DtoSurface {
        method: "SourceIngest",
        module: "source_ingestion",
        result_domain: "ingestion",
        roots: &[
            "SourceIngestionRequest",
            "SourceIngestionReceipt",
            "SourceIngestStatus",
        ],
        result_model: Some("SourceIngestionReceipt"),
        required: true,
        constants: &[],
    },
    DtoSurface {
        method: "IndexRepository",
        module: "index_repository",
        result_domain: "ingestion",
        roots: &["IndexResult"],
        result_model: Some("IndexResult"),
        required: true,
        constants: &[],
    },
    DtoSurface {
        method: "GraphSchema",
        module: "graph_schema",
        result_domain: "reasoning",
        roots: &[
            "GraphSchemaOp",
            "GraphSchemaCommitted",
            "GraphSchemaSourceView",
            "GraphSchemaSourcesView",
            "SchemaSourceOriginView",
        ],
        result_model: Some("GraphSchemaCommitted"),
        required: true,
        constants: &[],
    },
    DtoSurface {
        method: "OwlReason",
        module: "rdf_report",
        result_domain: "reasoning",
        roots: &[
            "OwlReasonResult",
            "OwlPropertyFact",
            "OwlExplainResult",
            "ProofNodeWire",
            "DatalogReasoningResult",
            "ShaclValidationReport",
            "ShaclValidationResult",
            "ShaclSeverity",
        ],
        result_model: Some("OwlReasonResult"),
        required: true,
        constants: &[],
    },
];

/// Typed result models whose DTO is emitted by another method's shared module.
/// Keeping this separate avoids emitting the same generated module twice while
/// allowing a small read method to return a model from the SourceIngest family.
pub(super) const SHARED_DTO_RESULT_MODELS: &[(&str, &str)] = &[
    ("SourceIngestStatus", "SourceIngestStatus"),
    ("GraphSchemaList", "GraphSchemaSourcesView"),
    ("OwlReasonDistributed", "OwlReasonResult"),
    ("OwlExplain", "OwlExplainResult"),
    ("RunDatalogReasoning", "DatalogReasoningResult"),
    ("ShaclValidate", "ShaclValidationReport"),
];

/// Schema-specific digest projections rendered as model methods. Framing and
/// MessagePack encoding live in the shared generated `digest` module; this row
/// only states which model fields form the projection.
pub(super) struct CanonicalDigestSpec {
    pub(super) model: &'static str,
    domain: &'static str,
    pub(super) digest_field: Option<&'static str>,
    pub(super) projection_fields: &'static [&'static str],
    pub(super) canonical_json_paths: &'static [&'static str],
    pub(super) omit_none_paths: &'static [&'static str],
    named_struct_paths: &'static [(&'static str, &'static [&'static str])],
}

pub(super) const CANONICAL_DIGEST_SPECS: &[CanonicalDigestSpec] = &[
    CanonicalDigestSpec {
        model: "SourceChangeSet",
        domain: "eg/source-change-set/v1",
        digest_field: Some("change_set_digest"),
        projection_fields: &[
            "schema_version",
            "change_set_id",
            "change_set_digest",
            "tenant_id",
            "actor",
            "purpose",
            "connector_id",
            "source_instance_id",
            "entity_id",
            "field_scope",
            "base_source_version",
            "desired_patch",
            "source_of_truth_rule",
            "field_provenance",
            "required_capability",
            "policy_digest",
            "authorization",
            "idempotency_key",
            "expires_at_ms",
            "reconciliation_procedure",
        ],
        canonical_json_paths: &["desired_patch"],
        omit_none_paths: &[],
        named_struct_paths: &[(
            "authorization",
            &[
                "mode",
                "authorization_ref",
                "decision_digest",
                "input_digest",
                "output_digest",
                "authorized",
            ],
        )],
    },
    // Mirrors `eg_types::source_ingestion::{SOURCE_INGESTION_DIGEST_DOMAIN,
    // SOURCE_INGESTION_DIGEST_PROJECTION_FIELDS,
    // SOURCE_INGESTION_CANONICAL_JSON_PATHS,
    // SOURCE_INGESTION_OMIT_NONE_PATHS}` once that independently owned protocol
    // branch is composed. Nested named-struct paths preserve the corresponding
    // Rust declaration order, which JSON Schema property maps do not retain.
    CanonicalDigestSpec {
        model: "SourceIngestionRequest",
        domain: "eg/source-ingestion-batch/v2",
        digest_field: None,
        projection_fields: &[
            "connector",
            "mode",
            "strict_schema",
            "records",
            "relationships",
            "provider_checkpoint",
            "expected_previous_checkpoint",
            "authoritative_live_ids",
            "empty_authoritative_approval",
            "withdrawals",
        ],
        canonical_json_paths: &[
            "records[*].payload",
            "relationships[*].properties",
            "provider_checkpoint.position",
            "expected_previous_checkpoint.position",
        ],
        omit_none_paths: &[
            "records[*].updated_at",
            "relationships[*].properties",
            "provider_checkpoint.content_hash",
            "provider_checkpoint.watermark",
            "provider_checkpoint.pending_watermark",
            "expected_previous_checkpoint.content_hash",
            "expected_previous_checkpoint.watermark",
            "expected_previous_checkpoint.pending_watermark",
        ],
        named_struct_paths: &[
            (
                "records[*]",
                &[
                    "stream",
                    "record_id",
                    "mapping_reference",
                    "payload",
                    "updated_at",
                    "provenance",
                ],
            ),
            (
                "records[*].provenance",
                &[
                    "connector",
                    "adapter_kind",
                    "server",
                    "tool",
                    "tool_schema_sha256",
                    "source_uri",
                ],
            ),
            (
                "relationships[*]",
                &[
                    "source",
                    "target",
                    "relation_reference",
                    "properties",
                    "provenance",
                ],
            ),
            ("relationships[*].source", &["stream", "record_id"]),
            ("relationships[*].target", &["stream", "record_id"]),
            (
                "relationships[*].provenance",
                &[
                    "connector",
                    "adapter_kind",
                    "server",
                    "tool",
                    "tool_schema_sha256",
                    "source_uri",
                ],
            ),
            (
                "provider_checkpoint",
                &[
                    "stream",
                    "position",
                    "content_hash",
                    "watermark",
                    "pending_watermark",
                ],
            ),
            (
                "expected_previous_checkpoint",
                &[
                    "stream",
                    "position",
                    "content_hash",
                    "watermark",
                    "pending_watermark",
                ],
            ),
            ("authoritative_live_ids[*]", &["stream", "record_id"]),
            ("withdrawals[*]", &["entity", "reason"]),
            ("withdrawals[*].entity", &["stream", "record_id"]),
        ],
    },
];

/// Named schema types whose custom serde representation is an arbitrary JSON value,
/// rather than the object shell exposed solely to keep the JSON Schema bounded.
const JSON_VALUE_TYPES: &[&str] = &["SourceJson"];

struct ScopedPatchDigestSpec {
    model: &'static str,
    method: &'static str,
    domain: &'static str,
    scope_field: &'static str,
    patch_field: &'static str,
}

const SCOPED_PATCH_DIGEST_SPECS: &[ScopedPatchDigestSpec] = &[ScopedPatchDigestSpec {
    model: "SourceChangeSet",
    method: "patch_digest",
    domain: "eg/source-change-set-patch/v1",
    scope_field: "field_scope",
    patch_field: "desired_patch",
}];
fn ref_name(node: &serde_json::Value) -> Option<&str> {
    let reference = node.get("$ref").or_else(|| {
        node.get("allOf")
            .and_then(|value| value.as_array())
            .and_then(|values| values.first())
            .and_then(|value| value.get("$ref"))
    })?;
    reference.as_str()?.rsplit('/').next()
}

pub(super) fn dto_python_type(node: &serde_json::Value) -> String {
    dto_special_type(node).unwrap_or_else(|| constrained_annotation(node, dto_base_type(node)))
}

fn dto_special_type(node: &serde_json::Value) -> Option<String> {
    if let Some(name) = ref_name(node) {
        return Some(name.to_string());
    }
    if let Some(value) = node.get("const") {
        return Some(format!(
            "Literal[{}]",
            serde_json::to_string(value).expect("JSON literal")
        ));
    }
    if let Some(any_of) = dto_union_nodes(node) {
        return Some(dto_union_type(any_of));
    }
    if let Some(types) = node.get("type").and_then(|value| value.as_array()) {
        return Some(dto_type_array(node, types));
    }
    None
}

fn dto_union_nodes(node: &serde_json::Value) -> Option<&Vec<serde_json::Value>> {
    node.get("anyOf")
        .or_else(|| node.get("oneOf"))
        .and_then(serde_json::Value::as_array)
}

fn dto_type_array(node: &serde_json::Value, types: &[serde_json::Value]) -> String {
    let variants: Vec<_> = types
        .iter()
        .map(|wire_type| {
            if wire_type.as_str() == Some("null") {
                return serde_json::json!({"type": "null"});
            }
            let mut variant = node.clone();
            variant["type"] = wire_type.clone();
            variant
        })
        .collect();
    dto_union_type(&variants)
}

fn dto_union_type(nodes: &[serde_json::Value]) -> String {
    let mut parts = Vec::new();
    for node in nodes {
        let annotation = dto_python_type(node);
        if !parts.contains(&annotation) {
            parts.push(annotation);
        }
    }
    parts.join(" | ")
}

fn dto_base_type(node: &serde_json::Value) -> String {
    match node.get("type").and_then(|value| value.as_str()) {
        Some("string") => "str".to_string(),
        Some("integer") => "int".to_string(),
        Some("number") => "float".to_string(),
        Some("boolean") => "bool".to_string(),
        Some("null") => "None".to_string(),
        Some("array") => dto_array_type(node),
        Some("object") => mapping_type(node, dto_python_type),
        _ => "Any".to_string(),
    }
}

fn dto_array_type(node: &serde_json::Value) -> String {
    node.get("items")
        .map(|items| {
            // `serde_bytes::ByteBuf` is an integer array in JSON Schema but a
            // MessagePack binary value on the Python client's live transport.
            // Preserve that wire type instead of forcing callers through an
            // SDK-owned conversion DTO.
            if items.get("type").and_then(|value| value.as_str()) == Some("integer")
                && items.get("format").and_then(|value| value.as_str()) == Some("uint8")
                && items.get("minimum").and_then(|value| value.as_u64()) == Some(0)
                && items.get("maximum").and_then(|value| value.as_u64()) == Some(255)
            {
                "bytes".to_string()
            } else {
                format!("list[{}]", dto_python_type(items))
            }
        })
        .unwrap_or_else(|| "list[Any]".to_string())
}

fn constrained_annotation(node: &serde_json::Value, annotation: String) -> String {
    let mut constraints = Vec::new();
    if let Some(pattern) = node.get("pattern").and_then(|value| value.as_str()) {
        constraints.push(format!("pattern={pattern:?}"));
    }
    for (schema_name, python_name) in [
        ("minLength", "min_length"),
        ("maxLength", "max_length"),
        ("minItems", "min_length"),
        ("maxItems", "max_length"),
        ("minimum", "ge"),
        ("maximum", "le"),
    ] {
        if let Some(value) = node.get(schema_name).and_then(|value| value.as_i64()) {
            constraints.push(format!("{python_name}={value}"));
        } else if let Some(value) = node.get(schema_name).and_then(|value| value.as_u64()) {
            constraints.push(format!("{python_name}={value}"));
        }
    }
    if constraints.is_empty() {
        annotation
    } else {
        format!("Annotated[{annotation}, Field({})]", constraints.join(", "))
    }
}

fn collect_definition_refs(
    node: &serde_json::Value,
    definitions: &serde_json::Map<String, serde_json::Value>,
    names: &mut std::collections::BTreeSet<String>,
) {
    if let Some(name) = ref_name(node) {
        if names.insert(name.to_string()) {
            if let Some(definition) = definitions.get(name) {
                collect_definition_refs(definition, definitions, names);
            }
        }
    }
    match node {
        serde_json::Value::Array(values) => {
            for value in values {
                collect_definition_refs(value, definitions, names);
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values() {
                collect_definition_refs(value, definitions, names);
            }
        }
        _ => {}
    }
}

fn pascal_case(value: &str) -> String {
    value
        .split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            chars
                .next()
                .map(|first| first.to_ascii_uppercase().to_string() + chars.as_str())
                .unwrap_or_default()
        })
        .collect()
}

fn tagged_variants(node: &serde_json::Value) -> Option<(&str, Vec<&serde_json::Value>)> {
    let variants = node
        .get("oneOf")
        .or_else(|| node.get("anyOf"))?
        .as_array()?;
    let first = variants.first()?.get("properties")?.as_object()?;
    let tag = first.iter().find_map(|(name, value)| {
        (value.get("const").is_some()
            || value
                .get("enum")
                .and_then(|values| values.as_array())
                .is_some_and(|values| values.len() == 1))
        .then_some(name.as_str())
    })?;
    Some((tag, variants.iter().collect()))
}

fn variant_tag<'a>(variant: &'a serde_json::Value, tag: &str) -> Option<&'a str> {
    let node = variant.get("properties")?.get(tag)?;
    node.get("const")
        .and_then(|value| value.as_str())
        .or_else(|| {
            node.get("enum")
                .and_then(|values| values.as_array())
                .and_then(|values| values.first())
                .and_then(|value| value.as_str())
        })
}

fn string_literal_variants(node: &serde_json::Value) -> Option<Vec<&str>> {
    let variants = node
        .get("oneOf")
        .or_else(|| node.get("anyOf"))?
        .as_array()?;
    let values: Vec<&str> = variants
        .iter()
        .map(string_literal_value)
        .collect::<Option<_>>()?;
    (!values.is_empty()).then_some(values)
}

fn string_literal_value(variant: &serde_json::Value) -> Option<&str> {
    if let Some(value) = variant.get("const").and_then(|value| value.as_str()) {
        return Some(value);
    }
    let values = variant.get("enum")?.as_array()?;
    (values.len() == 1)
        .then(|| values.first().and_then(|value| value.as_str()))
        .flatten()
}

fn push_dto_fields(out: &mut String, node: &serde_json::Value) {
    let required: Vec<&str> = node
        .get("required")
        .and_then(|value| value.as_array())
        .map(|values| values.iter().filter_map(|value| value.as_str()).collect())
        .unwrap_or_default();
    let Some(properties) = node.get("properties").and_then(|value| value.as_object()) else {
        out.push_str("    pass\n");
        return;
    };
    for (name, schema) in properties {
        let annotation = dto_python_type(schema);
        if required.contains(&name.as_str()) {
            let _ = writeln!(out, "    {name}: {annotation}");
        } else {
            let _ = writeln!(out, "    {name}: {} = None", optional(&annotation));
        }
    }
}

fn push_digest_methods(out: &mut String, model: &str, leading_blank: bool) {
    if let Some(spec) = canonical_digest_spec(model) {
        if leading_blank {
            out.push('\n');
        }
        push_canonical_digest(out, spec);
    }
    for patch in SCOPED_PATCH_DIGEST_SPECS
        .iter()
        .filter(|patch| patch.model == model)
    {
        push_patch_digest(out, patch);
    }
}

fn canonical_digest_spec(model: &str) -> Option<&CanonicalDigestSpec> {
    CANONICAL_DIGEST_SPECS
        .iter()
        .find(|spec| spec.model == model)
}

fn push_canonical_digest(out: &mut String, spec: &CanonicalDigestSpec) {
    out.push_str("    def canonical_digest(self) -> str:\n");
    out.push_str("        value = self.model_dump(mode=\"json\")\n");
    if let Some(field) = spec.digest_field {
        let _ = writeln!(out, "        value[\"{field}\"] = \"0\" * 64");
    }
    out.push_str("        projection = {\n");
    for field in spec.projection_fields {
        let _ = writeln!(out, "            \"{field}\": value[\"{field}\"],");
    }
    out.push_str("        }\n");
    out.push_str("        return framed_named_msgpack_digest(\n");
    let _ = writeln!(out, "            b\"{}\",", spec.domain);
    out.push_str("            projection,\n            canonical_json_paths={\n");
    for path in spec.canonical_json_paths {
        let _ = writeln!(out, "                \"{path}\",");
    }
    out.push_str("            },\n");
    push_string_set(out, "omit_none_paths", spec.omit_none_paths);
    out.push_str("            named_struct_paths={\n");
    for (path, fields) in spec.named_struct_paths {
        let _ = writeln!(out, "                \"{path}\": (");
        for field in *fields {
            let _ = writeln!(out, "                    \"{field}\",");
        }
        out.push_str("                ),\n");
    }
    out.push_str("            },\n        )\n");
}

fn push_string_set(out: &mut String, name: &str, values: &[&str]) {
    if values.is_empty() {
        let _ = writeln!(out, "            {name}={{}},");
        return;
    }
    let _ = writeln!(out, "            {name}={{");
    for value in values {
        let _ = writeln!(out, "                \"{value}\",");
    }
    out.push_str("            },\n");
}

fn push_patch_digest(out: &mut String, patch: &ScopedPatchDigestSpec) {
    let _ = writeln!(out, "\n    def {}(self) -> str:", patch.method);
    let _ = writeln!(
        out,
        "        encoded = canonical_msgpack([sorted(self.{}), self.{}])",
        patch.scope_field, patch.patch_field
    );
    let _ = writeln!(
        out,
        "        return framed_sha256(b\"{}\", [encoded])",
        patch.domain
    );
}

fn push_dto_definition(out: &mut String, name: &str, node: &serde_json::Value) -> Vec<String> {
    if let Some(models) = push_digest_subclass(out, name, node) {
        return models;
    }
    if let Some(values) = node.get("enum").and_then(|value| value.as_array()) {
        push_string_enum(out, name, values.iter().filter_map(|value| value.as_str()));
        return Vec::new();
    }
    if let Some(values) = string_literal_variants(node) {
        push_string_enum(out, name, values);
        return Vec::new();
    }
    if let Some((tag, variants)) = tagged_variants(node) {
        return push_tagged_union(out, name, tag, variants);
    }
    if JSON_VALUE_TYPES.contains(&name) {
        let _ = writeln!(out, "\n\n{name} = Any");
        return Vec::new();
    }
    if node.get("type").and_then(|value| value.as_str()) == Some("object") {
        return push_object_model(out, name, node);
    }
    push_type_alias(out, name, &dto_python_type(node));
    Vec::new()
}

fn push_digest_subclass(
    out: &mut String,
    name: &str,
    node: &serde_json::Value,
) -> Option<Vec<String>> {
    let base = ref_name(node).filter(|_| canonical_digest_spec(name).is_some())?;
    let _ = writeln!(out, "\n\nclass {name}({base}):");
    push_digest_methods(out, name, false);
    Some(vec![name.to_string()])
}

fn push_string_enum<'a>(out: &mut String, name: &str, values: impl IntoIterator<Item = &'a str>) {
    let _ = writeln!(out, "\n\nclass {name}(str, Enum):");
    for value in values {
        let _ = writeln!(out, "    {} = {:?}", value.to_ascii_uppercase(), value);
    }
}

fn push_tagged_union(
    out: &mut String,
    name: &str,
    tag: &str,
    variants: Vec<&serde_json::Value>,
) -> Vec<String> {
    let mut names = Vec::new();
    for variant in variants {
        let value = variant_tag(variant, tag).expect("tagged variant has a tag");
        let variant_name = format!("{name}{}", pascal_case(value));
        let _ = writeln!(out, "\n\nclass {variant_name}(BaseModel):");
        out.push_str("    model_config = ConfigDict(extra=\"forbid\", frozen=True)\n\n");
        push_dto_fields(out, variant);
        names.push(variant_name);
    }
    push_union_alias(out, name, tag, &names);
    names
}

fn push_union_alias(out: &mut String, name: &str, tag: &str, variants: &[String]) {
    let joined = variants.join(" | ");
    let _ = writeln!(out, "\n\n{name} = Annotated[");
    if joined.len() + 5 <= 88 {
        let _ = writeln!(out, "    {joined},");
    } else {
        for (index, variant) in variants.iter().enumerate() {
            let prefix = if index == 0 { "    " } else { "    | " };
            let suffix = if index + 1 == variants.len() { "," } else { "" };
            let _ = writeln!(out, "{prefix}{variant}{suffix}");
        }
    }
    let _ = writeln!(out, "    Field(discriminator={tag:?}),\n]");
}

fn push_object_model(out: &mut String, name: &str, node: &serde_json::Value) -> Vec<String> {
    let _ = writeln!(out, "\n\nclass {name}(BaseModel):");
    out.push_str("    model_config = ConfigDict(extra=\"forbid\", frozen=True)\n\n");
    push_dto_fields(out, node);
    push_digest_methods(out, name, true);
    vec![name.to_string()]
}

fn push_type_alias(out: &mut String, name: &str, annotation: &str) {
    if annotation.len() + name.len() + 3 <= 88 {
        let _ = writeln!(out, "\n\n{name} = {annotation}");
    } else if let Some((base, field)) = annotation
        .strip_prefix("Annotated[")
        .and_then(|value| value.strip_suffix(']'))
        .and_then(|value| value.split_once(", Field("))
    {
        let _ = writeln!(out, "\n\n{name} = Annotated[");
        let _ = writeln!(out, "    {base},");
        out.push_str("    Field(\n");
        for constraint in field.trim_end_matches(')').split(", ") {
            let _ = writeln!(out, "        {constraint},");
        }
        out.push_str("    ),\n]\n");
    } else {
        let _ = writeln!(out, "\n\n{name} = (\n    {annotation}\n)");
    }
}

fn dto_emission_rank(name: &str, node: &serde_json::Value) -> u8 {
    if ref_name(node).is_some() && CANONICAL_DIGEST_SPECS.iter().any(|spec| spec.model == name) {
        // A transparent request wrapper is a real Python subclass, so its base
        // model must already exist when the class statement executes.
        return 1;
    }
    if JSON_VALUE_TYPES.contains(&name) {
        return 2;
    }
    if node.get("enum").is_some()
        || string_literal_variants(node).is_some()
        || tagged_variants(node).is_some()
        || node.get("type").and_then(|value| value.as_str()) == Some("object")
    {
        return 0;
    }
    // Type aliases are evaluated at module import time. Emit them only after
    // every class they may reference; postponed annotations protect class fields,
    // but do not protect `Alias = list[ClassDefinedLater]`.
    2
}

fn resolved_properties<'a>(
    model: &str,
    definitions: &'a serde_json::Map<String, serde_json::Value>,
) -> Option<&'a serde_json::Map<String, serde_json::Value>> {
    let mut name = model;
    let mut visited = std::collections::BTreeSet::new();
    loop {
        if !visited.insert(name) {
            return None;
        }
        let node = definitions.get(name)?;
        if let Some(properties) = node.get("properties").and_then(|value| value.as_object()) {
            return Some(properties);
        }
        name = ref_name(node)?;
    }
}

pub(super) fn dto_module(
    surface: &DtoSurface,
    definitions: &serde_json::Map<String, serde_json::Value>,
) -> String {
    let names = collect_surface_names(surface, definitions);
    validate_digest_specs(&names, definitions);
    let (body, models) = render_definitions(&names, definitions);
    let mut out = String::from(HEADER);
    let _ = writeln!(
        out,
        "\"\"\"Generated {} nested wire DTOs.\"\"\"",
        surface.method
    );
    out.push_str("\nfrom __future__ import annotations\n\n");
    push_dto_imports(&mut out, &body);
    if !surface.constants.is_empty() {
        out.push('\n');
    }
    for (name, value) in surface.constants {
        let _ = writeln!(out, "{name} = {value}");
    }
    out.push_str(&body);
    // Two blank lines after a trailing top-level class (E305 / the formatter);
    // a trailing alias assignment is already followed by exactly one.
    let ends_in_class = body
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty() && !line.starts_with(' '))
        .is_some_and(|line| line.starts_with("class "));
    if ends_in_class && !models.is_empty() {
        out.push('\n');
    }
    for model in models {
        let _ = writeln!(out, "\n{model}.model_rebuild()");
    }
    out
}

fn collect_surface_names(
    surface: &DtoSurface,
    definitions: &serde_json::Map<String, serde_json::Value>,
) -> std::collections::BTreeSet<String> {
    let mut names = std::collections::BTreeSet::new();
    for root in surface.roots {
        names.insert((*root).to_string());
        let node = definitions
            .get(*root)
            .unwrap_or_else(|| panic!("{} DTO root {root} is missing", surface.method));
        collect_definition_refs(node, definitions, &mut names);
    }
    names
}

fn validate_digest_specs(
    names: &std::collections::BTreeSet<String>,
    definitions: &serde_json::Map<String, serde_json::Value>,
) {
    for spec in CANONICAL_DIGEST_SPECS
        .iter()
        .filter(|spec| names.contains(spec.model))
    {
        validate_digest_spec(spec, definitions);
    }
}

fn validate_digest_spec(
    spec: &CanonicalDigestSpec,
    definitions: &serde_json::Map<String, serde_json::Value>,
) {
    let properties = resolved_properties(spec.model, definitions)
        .unwrap_or_else(|| panic!("{} digest model has no object schema", spec.model));
    let projected: std::collections::BTreeSet<_> = spec.projection_fields.iter().copied().collect();
    let declared: std::collections::BTreeSet<_> = properties.keys().map(String::as_str).collect();
    assert_eq!(
        projected, declared,
        "{} digest projection and model fields disagree",
        spec.model
    );
    assert!(
        spec.digest_field
            .is_none_or(|field| projected.contains(field)),
        "{} digest field is outside its projection",
        spec.model
    );
    for path in digest_paths(spec) {
        let root = path.split(['.', '[']).next().expect("nonempty DTO path");
        assert!(
            projected.contains(root),
            "{} digest path {path} is outside its projection",
            spec.model
        );
    }
}

fn digest_paths(spec: &CanonicalDigestSpec) -> impl Iterator<Item = &str> {
    spec.canonical_json_paths
        .iter()
        .chain(spec.omit_none_paths)
        .copied()
        .chain(spec.named_struct_paths.iter().map(|(path, _)| *path))
}

fn render_definitions(
    names: &std::collections::BTreeSet<String>,
    definitions: &serde_json::Map<String, serde_json::Value>,
) -> (String, Vec<String>) {
    let mut body = String::new();
    let mut models = Vec::new();
    let mut ordered_names: Vec<&String> = names.iter().collect();
    ordered_names.sort_by_key(|name| {
        let node = definitions
            .get(name.as_str())
            .expect("referenced DTO definition exists");
        dto_emission_rank(name, node)
    });
    for name in ordered_names {
        let node = definitions
            .get(name)
            .unwrap_or_else(|| panic!("referenced DTO definition {name} is missing"));
        models.extend(push_dto_definition(&mut body, name, node));
    }
    (body, models)
}

fn push_dto_imports(out: &mut String, body: &str) {
    if body.contains("(str, Enum)") {
        out.push_str("from enum import Enum\n");
    }
    let typing_names = ["Annotated", "Any", "Literal"]
        .into_iter()
        .filter(|name| body.contains(name))
        .collect::<Vec<_>>();
    if !typing_names.is_empty() {
        let _ = writeln!(out, "from typing import {}", typing_names.join(", "));
    }
    if body.contains("(str, Enum)") || !typing_names.is_empty() {
        out.push('\n');
    }
    out.push_str("from pydantic import BaseModel, ConfigDict, Field\n");
    let digest_names = [
        "canonical_msgpack",
        "framed_named_msgpack_digest",
        "framed_sha256",
    ]
    .into_iter()
    .filter(|name| body.contains(&format!("{name}(")))
    .collect::<Vec<_>>();
    if !digest_names.is_empty() {
        out.push_str("\nfrom .digest import (\n");
        for name in digest_names {
            let _ = writeln!(out, "    {name},");
        }
        out.push_str(")\n");
    }
}
