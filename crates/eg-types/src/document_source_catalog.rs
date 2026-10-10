//! Typed catalog model for an attached MongoDB/DocumentDB source
//! (EG-UNIFIED-DATA-PLANE-R019): the inferred document JSON shape EG uses as
//! the source's catalog. This is the typed-model slice (`.1`): the shape
//! model, its field-type vocabulary, and the refusal for a malformed field
//! path. The native-driver connection, change-stream capture, and the
//! conformance entry against real MongoDB/DocumentDB are later children.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The inferred BSON/JSON type of one field across a sampled document set.
/// `Mixed` records that sampling observed more than one type at this path —
/// never silently collapsed to the first type seen.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DocumentFieldType {
    String,
    Int32,
    Int64,
    Double,
    Bool,
    Date,
    ObjectId,
    Array,
    Document,
    Null,
    Mixed,
}

/// One discovered field at a dotted document path (`"address.city"`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentField {
    pub path: String,
    pub field_type: DocumentFieldType,
    /// Fraction of sampled documents (0-100) that carried this path at all.
    pub presence_pct: u8,
}

/// The inferred shape of one MongoDB/DocumentDB collection: its discovered
/// fields keyed by dotted path, built from a bounded document sample.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentCollectionShape {
    pub collection: String,
    pub fields: BTreeMap<String, DocumentField>,
}

/// The catalog for one attached MongoDB/DocumentDB source: every collection's
/// inferred shape, keyed by collection name. Pure data — connecting through
/// the native driver and capturing change streams are later children.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentSourceCatalog {
    pub database: String,
    pub collections: BTreeMap<String, DocumentCollectionShape>,
}

/// A document field path failed validation. Carries the offending path so a
/// caller can report it without re-deriving it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidFieldPath(pub String);

impl std::fmt::Display for InvalidFieldPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid document field path: {:?}", self.0)
    }
}

impl std::error::Error for InvalidFieldPath {}

/// Confirm a dotted field path is well-formed: non-empty, no leading/trailing
/// dot, no empty segment (`"a..b"`), and no `$`-prefixed segment (MongoDB
/// operator syntax, never a real field name). Refuses rather than silently
/// dropping the offending segment.
pub fn validate_field_path(path: &str) -> Result<(), InvalidFieldPath> {
    if path.is_empty() || path.starts_with('.') || path.ends_with('.') {
        return Err(InvalidFieldPath(path.to_string()));
    }
    for segment in path.split('.') {
        if segment.is_empty() || segment.starts_with('$') {
            return Err(InvalidFieldPath(path.to_string()));
        }
    }
    Ok(())
}

impl DocumentCollectionShape {
    /// Insert a discovered field, refusing a malformed path rather than
    /// admitting it into the shape.
    pub fn insert_field(&mut self, field: DocumentField) -> Result<(), InvalidFieldPath> {
        validate_field_path(&field.path)?;
        self.fields.insert(field.path.clone(), field);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(path: &str) -> DocumentField {
        DocumentField {
            path: path.to_string(),
            field_type: DocumentFieldType::String,
            presence_pct: 100,
        }
    }

    // spec: EG-UNIFIED-DATA-PLANE-R019.1
    #[test]
    fn valid_path_round_trips_through_insert() {
        let mut shape = DocumentCollectionShape {
            collection: "people".to_string(),
            fields: BTreeMap::new(),
        };
        shape.insert_field(field("address.city")).unwrap();
        assert!(shape.fields.contains_key("address.city"));
    }

    // spec: EG-UNIFIED-DATA-PLANE-R019.1
    #[test]
    fn empty_path_is_refused() {
        assert_eq!(
            validate_field_path(""),
            Err(InvalidFieldPath(String::new()))
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R019.1
    #[test]
    fn leading_or_trailing_dot_is_refused() {
        assert!(validate_field_path(".a").is_err());
        assert!(validate_field_path("a.").is_err());
    }

    // spec: EG-UNIFIED-DATA-PLANE-R019.1
    #[test]
    fn empty_segment_is_refused() {
        assert!(validate_field_path("a..b").is_err());
    }

    // spec: EG-UNIFIED-DATA-PLANE-R019.1
    #[test]
    fn operator_prefixed_segment_is_refused() {
        assert!(validate_field_path("a.$where").is_err());
    }

    // spec: EG-UNIFIED-DATA-PLANE-R019.1
    #[test]
    fn malformed_path_is_refused_not_silently_inserted() {
        let mut shape = DocumentCollectionShape::default();
        let before = shape.fields.len();
        let result = shape.insert_field(field("$bad"));
        assert!(result.is_err());
        assert_eq!(shape.fields.len(), before);
    }

    // spec: EG-UNIFIED-DATA-PLANE-R019.1
    #[test]
    fn mixed_type_is_a_distinct_variant_not_collapsed() {
        let f = DocumentField {
            path: "value".to_string(),
            field_type: DocumentFieldType::Mixed,
            presence_pct: 100,
        };
        assert_eq!(f.field_type, DocumentFieldType::Mixed);
        assert_ne!(f.field_type, DocumentFieldType::String);
    }

    // spec: EG-UNIFIED-DATA-PLANE-R019.1
    #[test]
    fn catalog_serializes_round_trip() {
        let mut catalog = DocumentSourceCatalog {
            database: "homeDb".to_string(),
            collections: BTreeMap::new(),
        };
        let mut shape = DocumentCollectionShape {
            collection: "people".to_string(),
            fields: BTreeMap::new(),
        };
        shape.insert_field(field("name")).unwrap();
        catalog.collections.insert("people".to_string(), shape);

        let encoded = serde_json::to_string(&catalog).unwrap();
        let decoded: DocumentSourceCatalog = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, catalog);
    }
}
