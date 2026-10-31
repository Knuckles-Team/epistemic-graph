//! EH-403 / AUD-27: a schema-repair candidate as a TYPED record contract.
//!
//! EG owns every SHACL/RDF document (DECISIONS 2026-09-24). A drift repair
//! therefore reaches EG as data -- the field names, their JSON types and
//! whether each is required -- and EG renders the SHACL itself
//! ([`render_repair_shapes`]), deterministically, under the source's own
//! namespace. The approval binds the typed contract
//! ([`super::approval::approved_candidate_digest`]), so no RDF text ever
//! crosses the wire from a caller.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Most fields one record contract may declare.
pub const MAX_CONTRACT_FIELDS: usize = 1024;
/// Longest field name.
pub const MAX_CONTRACT_FIELD_BYTES: usize = 256;

/// A JSON value type, in canonical order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum JsonType {
    Null,
    Boolean,
    Integer,
    Number,
    String,
    Array,
    Object,
}

impl JsonType {
    /// The XSD datatype a single-typed scalar field is constrained to.
    fn xsd(self) -> Option<&'static str> {
        match self {
            Self::Boolean => Some("xsd:boolean"),
            Self::Integer => Some("xsd:integer"),
            Self::Number => Some("xsd:decimal"),
            Self::String => Some("xsd:string"),
            Self::Null | Self::Array | Self::Object => None,
        }
    }
}

/// One field of a record contract. Fields are declared alphabetically so the
/// canonical JSON is the same as a sorted-key encoder's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FieldContract {
    /// Present (and non-null unless `types` has `null`) in every record.
    pub required: bool,
    /// The types a value may have: non-empty, sorted, unique.
    pub types: Vec<JsonType>,
}

/// The typed record contract a source's repair candidate declares.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RecordContract {
    pub fields: BTreeMap<String, FieldContract>,
}

impl RecordContract {
    /// Bounds, printable names, non-empty sorted unique type sets.
    pub fn validate(&self) -> Result<(), String> {
        if self.fields.is_empty() || self.fields.len() > MAX_CONTRACT_FIELDS {
            return Err(format!(
                "a record contract declares 1..={MAX_CONTRACT_FIELDS} fields"
            ));
        }
        self.fields
            .iter()
            .try_for_each(|(name, field)| validate_field(name, field))
    }

    /// The canonical JSON the approval digest is taken over.
    pub fn canonical_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }
}

fn validate_field(name: &str, field: &FieldContract) -> Result<(), String> {
    let printable = !name.is_empty() && !name.chars().any(char::is_control);
    if !printable || name.len() > MAX_CONTRACT_FIELD_BYTES {
        return Err(format!(
            "record contract field names are printable and 1..={MAX_CONTRACT_FIELD_BYTES} bytes"
        ));
    }
    let sorted_unique = field.types.windows(2).all(|pair| pair[0] < pair[1]);
    if field.types.is_empty() || !sorted_unique {
        return Err(format!(
            "record contract field '{name}' needs a non-empty, sorted, unique type set"
        ));
    }
    Ok(())
}

/// Percent-encode everything outside `[A-Za-z0-9_-]` for an IRI local name.
fn local_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            out.push(ch);
        } else {
            let mut buf = [0u8; 4];
            for byte in ch.encode_utf8(&mut buf).bytes() {
                out.push_str(&format!("%{byte:02X}"));
            }
        }
    }
    out
}

fn property_shape(namespace: &str, name: &str, field: &FieldContract) -> String {
    let mut shape = format!("  sh:property [ sh:path <{namespace}{}>", local_name(name));
    if field.required && !field.types.contains(&JsonType::Null) {
        shape.push_str("\n    ; sh:minCount 1");
    }
    let scalars: Vec<JsonType> = field
        .types
        .iter()
        .copied()
        .filter(|kind| *kind != JsonType::Null)
        .collect();
    if let [single] = scalars.as_slice() {
        if let Some(xsd) = single.xsd() {
            shape.push_str(&format!("\n    ; sh:datatype {xsd}"));
        }
    }
    shape.push_str(" ]");
    shape
}

/// The SHACL shapes document of `contract` for the source `name`.
pub fn render_repair_shapes(name: &str, contract: &RecordContract) -> String {
    let namespace = format!("urn:eg:source:{}#", local_name(name));
    let mut parts = vec![format!(
        "@prefix sh: <http://www.w3.org/ns/shacl#> .\n\
         @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n\n\
         <{namespace}RecordShape> a sh:NodeShape ;\n  sh:targetClass <{namespace}Record>"
    )];
    parts.extend(
        contract
            .fields
            .iter()
            .map(|(field, spec)| property_shape(&namespace, field, spec)),
    );
    format!("{} .\n", parts.join(" ;\n"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contract() -> RecordContract {
        let mut fields = BTreeMap::new();
        fields.insert(
            "name".to_string(),
            FieldContract {
                required: true,
                types: vec![JsonType::String],
            },
        );
        fields.insert(
            "note field".to_string(),
            FieldContract {
                required: false,
                types: vec![JsonType::Null, JsonType::Integer],
            },
        );
        RecordContract { fields }
    }

    #[test]
    fn the_canonical_json_is_the_sorted_key_encoding() {
        assert_eq!(
            contract().canonical_json(),
            r#"{"fields":{"name":{"required":true,"types":["string"]},"note field":{"required":false,"types":["null","integer"]}}}"#
        );
    }

    #[test]
    fn shapes_are_rendered_deterministically_with_datatype_and_presence() {
        let shapes = render_repair_shapes("container-manager-mcp", &contract());
        assert_eq!(
            shapes,
            render_repair_shapes("container-manager-mcp", &contract())
        );
        assert!(shapes.contains("<urn:eg:source:container-manager-mcp#name>\n    ; sh:minCount 1\n    ; sh:datatype xsd:string ]"));
        assert!(shapes.contains(
            "<urn:eg:source:container-manager-mcp#note%20field>\n    ; sh:datatype xsd:integer ]"
        ));
        assert!(shapes.ends_with(" .\n"));
    }

    #[test]
    fn empty_unsorted_or_unprintable_contracts_are_refused() {
        contract().validate().unwrap();
        assert!(RecordContract {
            fields: BTreeMap::new()
        }
        .validate()
        .is_err());
        let mut bad = contract();
        bad.fields.get_mut("name").unwrap().types = vec![JsonType::String, JsonType::Null];
        assert!(bad.validate().is_err());
        let mut control = contract();
        control.fields.insert(
            "a\u{7}".to_string(),
            FieldContract {
                required: false,
                types: vec![JsonType::String],
            },
        );
        assert!(control.validate().is_err());
    }
}
