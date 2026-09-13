//! JSON Schema emission for the wire request and result bodies.
//!
//! The request side needs no separate DTO catalog: a `Method` variant's inline fields
//! ARE its request schema, so one `schema_for!(Method)` pass covers all of them. The
//! emitted document re-shapes schemars' variant list into a `methods` map keyed by the
//! variant's own serde tag, which is what a descriptor's request schema points at — an
//! index into a positional array would silently re-target on any reorder.
//!
//! The result side is one `contract/schemas/result.<domain>.json` per contract domain,
//! rendered from the [`Catalog`] of `eg_types::result_contract` markers: for each method,
//! its body (or one body per request op), the `ResultPayload` encoding, the schema of
//! the Rust type the handler encodes, and -- for a caller-shaped body -- the declared
//! reason. Every document is an `artifact_digests` entry, so a result-type change moves
//! a digest exactly like a request-shape change does.

use std::collections::{BTreeMap, BTreeSet};

use eg_types::protocol::{Method, ResultPayload};

use super::results::{Catalog, Declared};
use super::{normalize, pretty, Artifact};

fn to_value(schema: schemars::Schema) -> serde_json::Value {
    serde_json::to_value(schema).expect("a JSON Schema is serializable")
}

/// Definitions live under `$defs` (2020-12) or `definitions` (draft-07).
fn definitions(root: &serde_json::Value) -> serde_json::Value {
    root.get("$defs")
        .or_else(|| root.get("definitions"))
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}))
}

/// Variant subschemas sit under `oneOf` or `anyOf` depending on the derive's shape.
fn variant_subschemas(root: &serde_json::Value) -> Vec<serde_json::Value> {
    root.get("oneOf")
        .or_else(|| root.get("anyOf"))
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default()
}

/// The internal `field` tag a variant subschema pins, as either a `const` or a one-value
/// `enum`.
fn variant_tag(subschema: &serde_json::Value, field: &str) -> Option<String> {
    let tag = subschema.get("properties")?.get(field)?;
    if let Some(name) = tag.get("const").and_then(|c| c.as_str()) {
        return Some(name.to_string());
    }
    tag.get("enum")
        .and_then(|e| e.as_array())
        .and_then(|e| e.first())
        .and_then(|e| e.as_str())
        .map(str::to_string)
}

/// Split the `Method` schema into one subschema per variant, keyed by serde tag.
pub(super) fn method_request_document() -> serde_json::Value {
    let root = to_value(schemars::schema_for!(Method));
    let mut methods: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    for subschema in variant_subschemas(&root) {
        if let Some(tag) = variant_tag(&subschema, "method") {
            methods.insert(tag, subschema);
        }
    }
    assert_eq!(
        methods.len(),
        crate::method_descriptors().count(),
        "the derived `Method` schema does not expose one tagged subschema per variant"
    );
    serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "Method",
        "$comment": "Wire encoding is MessagePack, not JSON. A field typed here as an \
    array of uint8 is a `serde_bytes` Vec<u8> and travels as a MessagePack `bin`, not as an \
    array of integers; a validator applied to the raw wire frame must account for that. \
    Every other type maps directly.",
        "$defs": definitions(&root),
        "methods": methods,
    })
}

/// The `$defs` entry a `$ref` (possibly wrapped in `allOf`) names.
fn referenced<'a>(
    node: &serde_json::Value,
    defs: &'a serde_json::Value,
) -> Option<&'a serde_json::Value> {
    let reference = node.get("$ref").or_else(|| {
        node.get("allOf")
            .and_then(|all| all.as_array())
            .and_then(|all| all.first())
            .and_then(|first| first.get("$ref"))
    })?;
    let name = reference.as_str()?.strip_prefix("#/$defs/")?;
    defs.get(name)
}

/// Every tag one op-enum variant subschema can carry: an internal tag field's value, a
/// unit variant's string, or an externally tagged variant's single key.
fn op_variant_tags(variant: &serde_json::Value) -> Vec<String> {
    if let Some(values) = variant.get("enum").and_then(|e| e.as_array()) {
        return values
            .iter()
            .filter_map(|value| value.as_str().map(str::to_string))
            .collect();
    }
    if let Some(value) = variant.get("const").and_then(|c| c.as_str()) {
        return vec![value.to_string()];
    }
    let properties = variant.get("properties").and_then(|p| p.as_object());
    if let Some(tag) = properties.and_then(|properties| {
        properties
            .keys()
            .find_map(|field| variant_tag(variant, field))
    }) {
        return vec![tag];
    }
    variant
        .get("required")
        .and_then(|r| r.as_array())
        .filter(|required| required.len() == 1)
        .and_then(|required| required[0].as_str())
        .map(|key| vec![key.to_string()])
        .unwrap_or_default()
}

/// The op tags the request of `method` can select, when its params carry an `op` enum.
fn op_tags(request: &serde_json::Value, method: &str) -> Option<BTreeSet<String>> {
    let op = request
        .get("methods")?
        .get(method)?
        .get("properties")?
        .get("params")?
        .get("properties")?
        .get("op")?;
    let defs = request.get("$defs")?;
    let op_enum = referenced(op, defs)?;
    Some(
        variant_subschemas(op_enum)
            .iter()
            .flat_map(op_variant_tags)
            .collect(),
    )
}

/// `contract/schemas/result.<domain>.json`.
pub(super) fn result_document_path(domain: &str) -> String {
    format!("contract/schemas/result.{domain}.json")
}

fn method_result_json(
    id: &str,
    declared: &Declared,
    request: &serde_json::Value,
) -> serde_json::Value {
    let selected_by = declared.by_op.then(|| {
        let tags = op_tags(request, id).unwrap_or_else(|| {
            panic!("{id}: declares per-op results but its request has no op enum")
        });
        let ops: BTreeSet<String> = declared.bodies.keys().map(|op| op.to_string()).collect();
        assert_eq!(
            tags, ops,
            "{id}: the declared result ops and the request op enum disagree"
        );
        "op"
    });
    let bodies: BTreeMap<&str, serde_json::Value> = declared
        .bodies
        .iter()
        .map(|(key, body)| {
            let dynamic = body.dynamic.map(|reason| {
                serde_json::json!({"reason": reason.as_str(), "summary": reason.summary()})
            });
            (
                *key,
                serde_json::json!({
                    "encoding": body.encoding,
                    "dynamic": dynamic,
                    "schema": body.schema,
                }),
            )
        })
        .collect();
    serde_json::json!({"selected_by": selected_by, "bodies": bodies})
}

fn result_documents(catalog: &Catalog, request: &serde_json::Value) -> Vec<Artifact> {
    let mut by_domain: BTreeMap<&str, BTreeMap<&str, serde_json::Value>> = BTreeMap::new();
    for descriptor in crate::method_descriptors() {
        let id = descriptor.id.as_str();
        if let Some(declared) = catalog.methods.get(id) {
            by_domain
                .entry(descriptor.domain)
                .or_default()
                .insert(id, method_result_json(id, declared, request));
        }
    }
    by_domain
        .into_iter()
        .map(|(domain, methods)| {
            let defs = catalog.definitions.get(domain).cloned().unwrap_or_default();
            Artifact {
                path: result_document_path(domain),
                bytes: pretty(&serde_json::json!({
                    "$schema": "https://json-schema.org/draft/2020-12/schema",
                    "title": format!("{domain} method results"),
                    "$comment": "One entry per method: its body, or one body per request op \
    (`selected_by`). `encoding` is the ResultPayload variant; a `Raw` body is a MessagePack \
    `bin` holding the MessagePack encoding of `schema`, a `Json` body is that value as a JSON \
    tree. A body with a `dynamic` reason is chosen by the caller and has the unconstrained \
    schema `true`.",
                    "methods": methods,
                    "$defs": serde_json::Value::Object(defs),
                })),
            }
        })
        .collect()
}

/// `crates/eg-capabilities/generated/method_catalog.rs` -- the compile-time
/// `MethodId -> (SchemaId, request-schema digest)` table.
///
/// The admission path binds a method's schema identity into
/// `OperationReplayIdentity` and may not read a file to learn it, so the same
/// generator that writes `contract/schemas/method.request.json` writes the Rust
/// the engine compiles against. One source, two renderings; `--check` byte-diffs
/// both.
pub(super) fn method_catalog_source() -> Vec<u8> {
    let document = method_request_document();
    let methods = document
        .get("methods")
        .and_then(|value| value.as_object())
        .expect("the method request document exposes a methods map");
    let mut rows = String::new();
    for (id, subschema) in methods {
        let digest = super::sha256_hex(&pretty(subschema));
        rows.push_str(&format!(
            "    (\n        \"{id}\",\n        \"contract/schemas/method.request.json#/methods/{id}\",\n        {literal},\n    ),\n",
            literal = hex_literal(&digest)
        ));
    }
    let source = [
        "// @generated by `cargo run -p eg-capabilities --features contract --bin gen_contract`.\n",
        "// DO NOT EDIT: `gen_contract --check` byte-diffs this file.\n",
        "//\n",
        "// Each row is `(MethodId, SchemaId, sha256 of that method's request subschema)`,\n",
        "// sorted by method id. The digest is over the exact bytes\n",
        "// `contract/schemas/method.request.json` publishes for the method, so a schema\n",
        "// change moves the digest and therefore every replay identity minted under it.\n",
        "\n",
        "/// One `(method id, schema id, request-schema digest)` row per contract method.\n",
        "pub const METHOD_CATALOG: &[(&str, &str, [u8; 32])] = &[\n",
    ]
    .concat()
        + &rows
        + "];\n";
    normalize(source)
}

/// The 32-byte literal for one lowercase hex digest, written out rather than
/// parsed: the table is `const`, and a `const fn` hex parser would be a second
/// implementation of something the generator already knows exactly.
fn hex_literal(digest: &str) -> String {
    let bytes: Vec<String> = (0..digest.len())
        .step_by(2)
        .map(|index| format!("0x{}", &digest[index..index + 2]))
        .collect();
    format!("[{}]", bytes.join(", "))
}

/// Every generated schema file, in deterministic path order.
pub(super) fn artifacts(catalog: &Catalog) -> Vec<Artifact> {
    let request = method_request_document();
    let mut out = vec![
        Artifact {
            path: "contract/schemas/method.request.json".to_string(),
            bytes: pretty(&request),
        },
        Artifact {
            path: "contract/schemas/result.payload.json".to_string(),
            bytes: pretty(&to_value(schemars::schema_for!(ResultPayload))),
        },
    ];
    out.extend(result_documents(catalog, &request));
    out
}
