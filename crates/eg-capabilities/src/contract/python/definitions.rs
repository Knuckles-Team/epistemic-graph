//! The definition set a DTO surface renders from: the request document's
//! definitions merged with one result domain's.

use crate::contract::results::Catalog;

/// The first doc-comment line schemars copied into a definition, which names
/// the Rust type that emitted it well enough to find in a collision report.
pub(super) fn definition_origin(definition: &serde_json::Value) -> String {
    definition
        .get("description")
        .and_then(|value| value.as_str())
        .and_then(|text| text.lines().next())
        .map_or_else(|| "<undocumented type>".to_string(), str::to_string)
}

/// The request document's definitions merged with `result_domain`'s. A name
/// both documents define must be the same schema in both.
pub(super) fn merged_definitions(
    document: &serde_json::Value,
    catalog: &Catalog,
    result_domain: &str,
) -> serde_json::Map<String, serde_json::Value> {
    let mut definitions = document
        .get("$defs")
        .and_then(|value| value.as_object())
        .cloned()
        .unwrap_or_default();
    for definition in definitions.values_mut() {
        rebase_root_refs(definition);
    }
    if let Some(domain_definitions) = catalog.definitions.get(result_domain) {
        for (name, definition) in domain_definitions {
            if let Some(previous) = definitions.insert(name.clone(), definition.clone()) {
                assert_eq!(
                    &previous,
                    definition,
                    "two distinct Rust types share the schema definition name {name} \
                     (request side: {}; result domain {result_domain}: {}); give one a \
                     domain-specific name",
                    definition_origin(&previous),
                    definition_origin(definition),
                );
            }
        }
    }
    // Closed error codes travel on the error response, not inside a successful
    // result body, so they are intentionally absent from both method and result
    // documents.  They remain Rust-owned wire types and the typed Python seam
    // must generate them from that authority rather than copy string literals.
    let write_codes = serde_json::json!({
        "type": "string",
        "enum": eg_types::connector_pack::PackWriteErrorCode::ALL
            .iter()
            .map(|code| code.as_str())
            .collect::<Vec<_>>(),
    });
    definitions.insert("PackWriteErrorCode".to_string(), write_codes);
    definitions
}

/// The request document is rooted at `Method`, so its definitions name `Method`
/// as the root reference `#`. The merged set is not rooted at `Method`: spell
/// that reference `#/$defs/Method`, as every result document does, so a
/// definition shared by both documents compares equal.
fn rebase_root_refs(node: &mut serde_json::Value) {
    match node {
        serde_json::Value::Object(map) => {
            if map.get("$ref").and_then(|r| r.as_str()) == Some("#") {
                map.insert("$ref".to_string(), "#/$defs/Method".into());
            }
            map.values_mut().for_each(rebase_root_refs);
        }
        serde_json::Value::Array(items) => items.iter_mut().for_each(rebase_root_refs),
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => {}
    }
}
