//! JSON Schema emission for the wire request and result bodies.
//!
//! The request side needs no separate DTO catalog: a `Method` variant's inline fields
//! ARE its request schema, so one `schema_for!(Method)` pass covers all of them. The
//! emitted document re-shapes schemars' variant list into a `methods` map keyed by the
//! variant's own serde tag, which is what `SchemaRef::MethodVariant` points at — an
//! index into a positional array would silently re-target on any reorder.
//!
//! The result side has the seven typed `ResultPayload` variants, one file each. `Json` and
//! `Raw` declare no `ResultPayload`-level shape (`SchemaRef::Opaque`), but a `Raw` result is
//! the MessagePack encoding of a typed eg-types DTO -- `AgentComponentEntry`,
//! `AgentComponentSearchPage`, `KgDelegateResult`, ... -- and those DTO shapes ARE the
//! response contract a client decodes. [`RESULT_BODIES`] names them per method, keyed by
//! the request op that selects each one, and each method gets a
//! `contract/schemas/result.body.<Method>.json`, so a result-DTO change moves an
//! `artifact_digests` entry exactly like a request-shape change does.

use std::collections::{BTreeMap, BTreeSet};

use eg_types::agent_component::{
    AgentComponentCommittedResult, AgentComponentEntry, AgentComponentOp, AgentComponentSearchPage,
};
use eg_types::agent_graph::{AgentGraphCommittedResult, AgentGraphEntry, AgentGraphOp};
use eg_types::agent_library::{
    AgentLibraryEntry, AgentLibraryEntryDraft, AgentLibraryOp, AgentLibraryWriteResult,
};
use eg_types::agent_template::{AgentTemplateCommittedResult, AgentTemplateEntry, AgentTemplateOp};
use eg_types::delegation::KgDelegateResult;
use eg_types::protocol::{Method, ResultPayload};
use schemars::{JsonSchema, Schema, SchemaGenerator};

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

/// Schema of one result body, registered into the method document's shared generator.
type BodySchema = fn(&mut SchemaGenerator) -> Schema;

fn body<T: JsonSchema>(generator: &mut SchemaGenerator) -> Schema {
    generator.subschema_for::<T>()
}

fn root_schema<T: JsonSchema>() -> Schema {
    schemars::schema_for!(T)
}

/// The request's op enum schema and the internal serde tag field that names its variant.
type OpSelector = (fn() -> Schema, &'static str);

/// One `ResultPayload::Raw` method whose bytes are the MessagePack encoding of a named
/// eg-types DTO.
struct ResultBody {
    method: &'static str,
    /// Present when the request's op selects the body; `None` when the method has
    /// exactly one body.
    selector: Option<OpSelector>,
    /// `(op tag, body schema)`; a single-body method uses the key `result`.
    bodies: &'static [(&'static str, BodySchema)],
}

/// The result DTO each op of a `Raw` method encodes.
///
/// Read off the dispatch arms, which live in the root package this crate cannot see:
/// `src/server/handlers/admin.rs` (`handle_agent_component`/`_template`/`_graph`/
/// `_library`, each `ResultPayload::raw(..)` of the persistence store's return) and
/// `src/server/handlers/delegation.rs` (`ResultPayload::raw(&KgDelegateResult)`). What IS
/// checked here is the key side: [`result_body_document`] asserts every op variant the
/// request enum declares has exactly one body, so a new op cannot ship undocumented.
/// `SemanticIndex`, `AnalyticsJob` and the other opaque results are not yet bound to a
/// DTO and stay `SchemaRef::Opaque`.
const RESULT_BODIES: &[ResultBody] = &[
    ResultBody {
        method: "AgentComponent",
        selector: Some((root_schema::<AgentComponentOp>, "op")),
        bodies: &[
            ("current", body::<Option<AgentComponentEntry>>),
            ("history", body::<Vec<AgentComponentEntry>>),
            ("publish", body::<AgentComponentCommittedResult>),
            ("retire", body::<AgentComponentCommittedResult>),
            ("search", body::<AgentComponentSearchPage>),
            ("status", body::<Option<AgentComponentCommittedResult>>),
        ],
    },
    ResultBody {
        method: "AgentGraph",
        selector: Some((root_schema::<AgentGraphOp>, "op")),
        bodies: &[
            ("current", body::<Option<AgentGraphEntry>>),
            ("history", body::<Vec<AgentGraphEntry>>),
            ("publish", body::<AgentGraphCommittedResult>),
            ("retire", body::<AgentGraphCommittedResult>),
            ("status", body::<Option<AgentGraphCommittedResult>>),
        ],
    },
    ResultBody {
        method: "AgentLibrary",
        selector: Some((root_schema::<AgentLibraryOp>, "operation")),
        bodies: &[
            ("current", body::<Option<AgentLibraryEntry>>),
            ("history", body::<Vec<AgentLibraryEntry>>),
            ("publish", body::<AgentLibraryWriteResult>),
            ("retire", body::<AgentLibraryWriteResult>),
            ("status", body::<Option<AgentLibraryWriteResult>>),
        ],
    },
    ResultBody {
        method: "AgentTemplate",
        selector: Some((root_schema::<AgentTemplateOp>, "op")),
        bodies: &[
            ("current", body::<Option<AgentTemplateEntry>>),
            ("history", body::<Vec<AgentTemplateEntry>>),
            ("instantiate", body::<AgentLibraryEntryDraft>),
            ("publish", body::<AgentTemplateCommittedResult>),
            ("retire", body::<AgentTemplateCommittedResult>),
            ("status", body::<Option<AgentTemplateCommittedResult>>),
        ],
    },
    ResultBody {
        method: "KgDelegate",
        selector: None,
        bodies: &[("result", body::<KgDelegateResult>)],
    },
];

fn result_body_file(method: &str) -> String {
    format!("contract/schemas/result.body.{method}.json")
}

/// The body-schema file for `method`, when [`RESULT_BODIES`] binds one.
pub(super) fn result_body_path(method: &str) -> Option<String> {
    RESULT_BODIES
        .iter()
        .any(|entry| entry.method == method)
        .then(|| result_body_file(method))
}

fn result_body_document(entry: &ResultBody) -> serde_json::Value {
    assert!(
        crate::method_descriptors().any(|d| d.id.as_str() == entry.method),
        "RESULT_BODIES names `{}`, which is not a contract method",
        entry.method
    );
    let declared: BTreeSet<&str> = entry.bodies.iter().map(|(op, _)| *op).collect();
    assert_eq!(
        declared.len(),
        entry.bodies.len(),
        "{}: an op is bound to more than one result body",
        entry.method
    );
    let selected_by = entry.selector.map(|(op_schema, field)| {
        let root = to_value(op_schema());
        let variants: BTreeSet<String> = variant_subschemas(&root)
            .iter()
            .filter_map(|subschema| variant_tag(subschema, field))
            .collect();
        let declared: BTreeSet<String> = declared.iter().map(|op| op.to_string()).collect();
        assert_eq!(
            variants, declared,
            "{}: the result-body catalog and the request op enum disagree",
            entry.method
        );
        field
    });
    let mut generator = SchemaGenerator::default();
    let bodies: BTreeMap<&str, serde_json::Value> = entry
        .bodies
        .iter()
        .map(|(op, schema)| (*op, to_value(schema(&mut generator))))
        .collect();
    serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": format!("{} result body", entry.method),
        "$comment": "Carried as ResultPayload::Raw: a MessagePack `bin` holding the \
    MessagePack encoding of exactly one of `bodies`, chosen by the request's `selected_by` \
    tag (`result` when the method has a single body). The same MessagePack-vs-JSON caveat \
    as method.request.json applies to byte fields.",
        "method": entry.method,
        "selected_by": selected_by,
        "bodies": bodies,
        "$defs": serde_json::Value::Object(generator.take_definitions(true)),
    })
}

fn result_artifact(shape: &str, schema: serde_json::Value) -> Artifact {
    Artifact {
        path: format!("contract/schemas/result.{shape}.json"),
        bytes: pretty(&schema),
    }
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
pub(super) fn artifacts() -> Vec<Artifact> {
    let bodies = RESULT_BODIES.iter().map(|entry| Artifact {
        path: result_body_file(entry.method),
        bytes: pretty(&result_body_document(entry)),
    });
    let mut out = vec![
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
    ];
    out.extend(bodies);
    out
}
