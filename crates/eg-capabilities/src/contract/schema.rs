//! JSON Schema emission for the wire request and result bodies.
//!
//! The request side needs no separate DTO catalog: a `Method` variant's inline fields
//! ARE its request schema, so one `schema_for!(Method)` pass covers all of them. The
//! emitted document re-shapes schemars' variant list into a `methods` map keyed by the
//! variant's own serde tag, which is what `SchemaRef::MethodVariant` points at — an
//! index into a positional array would silently re-target on any reorder.
//!
//! The result side has only the seven typed `ResultPayload` variants; `Json` and `Raw`
//! declare no shape and get no file (`SchemaRef::Opaque`).

use std::collections::BTreeMap;

use eg_types::protocol::{Method, ResultPayload};

use super::{pretty, Artifact};

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

/// The `method` tag a variant subschema pins, as either a `const` or a one-value `enum`.
fn variant_tag(subschema: &serde_json::Value) -> Option<String> {
    let tag = subschema.get("properties")?.get("method")?;
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
        if let Some(tag) = variant_tag(&subschema) {
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
        "$defs": definitions(&root),
        "methods": methods,
    })
}

fn result_artifact(shape: &str, schema: serde_json::Value) -> Artifact {
    Artifact {
        path: format!("contract/schemas/result.{shape}.json"),
        bytes: pretty(&schema),
    }
}

/// Every generated schema file, in deterministic path order.
pub(super) fn artifacts() -> Vec<Artifact> {
    vec![
        Artifact {
            path: "contract/schemas/method.request.json".to_string(),
            bytes: pretty(&method_request_document()),
        },
        Artifact {
            path: "contract/schemas/result.payload.json".to_string(),
            bytes: pretty(&to_value(schemars::schema_for!(ResultPayload))),
        },
        result_artifact("Bool", to_value(schemars::schema_for!(bool))),
        result_artifact("Count", to_value(schemars::schema_for!(u64))),
        result_artifact("Float", to_value(schemars::schema_for!(f64))),
        result_artifact("String", to_value(schemars::schema_for!(String))),
        result_artifact("Ids", to_value(schemars::schema_for!(Vec<String>))),
        result_artifact(
            "NodeList",
            to_value(schemars::schema_for!(Vec<(String, serde_json::Value)>)),
        ),
        result_artifact(
            "EdgeList",
            to_value(schemars::schema_for!(Vec<(String, String, Vec<u8>)>)),
        ),
    ]
}
