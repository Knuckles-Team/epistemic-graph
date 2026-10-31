//! Mappings a connector declares at `connector://resources`.
//!
//! A connector's MCP server serves one JSON declaration of its data sources and
//! the record-type mappings onto its ontology. The pack captures it as the
//! `connector://resources` resource entry. EG persists only the closed mapping
//! projection ingestion consumes. Relations come from the pack ontology's
//! object properties (domain and range), never from the declaration.

use std::collections::{BTreeMap, BTreeSet};

use medulla_types::connector_pack::{ConnectorRelationshipMapping, ConnectorSchemaMapping};

/// The URI of the declaration entry in a connector pack.
pub const CONNECTOR_DECLARATION_URI: &str = "connector://resources";

/// One declared record-type mapping. Closed: an unknown key is refused.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct DeclaredMapping {
    #[serde(default)]
    ontology_class: Option<String>,
    #[serde(default)]
    fields: BTreeMap<String, String>,
    #[serde(default)]
    datatypes: BTreeMap<String, String>,
}

/// Decode `schema_mappings` from the declaration JSON, keyed by record type.
/// A declaration with no `schema_mappings` declares none.
pub fn decode_schema_mappings(
    body: &[u8],
) -> Result<BTreeMap<String, ConnectorSchemaMapping>, String> {
    let document: serde_json::Value = serde_json::from_slice(body)
        .map_err(|_| "connector declaration is not valid JSON".to_string())?;
    let object = document
        .as_object()
        .ok_or_else(|| "connector declaration must be a JSON object".to_string())?;
    let Some(declared) = object.get("schema_mappings") else {
        return Ok(BTreeMap::new());
    };
    let declared: BTreeMap<String, DeclaredMapping> = serde_json::from_value(declared.clone())
        .map_err(|_| "connector declaration mappings are invalid".to_string())?;
    let mut mappings = BTreeMap::new();
    for (key, mapping) in declared {
        if key.is_empty()
            || mapping
                .fields
                .iter()
                .any(|(k, v)| k.is_empty() || v.is_empty())
        {
            return Err("schema mappings contain an empty key or field".to_string());
        }
        validate_mapping_datatypes(&mapping)?;
        mappings.insert(
            key,
            ConnectorSchemaMapping {
                ontology_class: mapping.ontology_class.unwrap_or_default(),
                fields: mapping.fields,
                datatypes: mapping.datatypes,
            },
        );
    }
    Ok(mappings)
}

fn validate_mapping_datatypes(mapping: &DeclaredMapping) -> Result<(), String> {
    for (field, datatype) in &mapping.datatypes {
        if !medulla_types::connector_pack::source_datatype_supported(datatype) {
            return Err("schema mapping contains an unsupported datatype".into());
        }
        if mapping
            .fields
            .get(field)
            .is_none_or(|target| target.starts_with("xsd:"))
        {
            return Err("schema datatype requires an explicit mapped field target".into());
        }
    }
    Ok(())
}

/// Relations declared by the pack ontologies' object properties, keyed
/// `<domain>/<property>` so each maps to one exact reference path. The relation
/// type defaults to the property name.
#[cfg(feature = "rdf")]
pub fn decode_relationship_mappings(
    ontologies: &[&str],
) -> BTreeMap<String, ConnectorRelationshipMapping> {
    let mut relations = BTreeMap::new();
    let mut ambiguous = BTreeSet::new();
    for property in medulla_rdf::pack::declared_object_properties(ontologies) {
        let key = format!("{}/{}", property.domain, property.name);
        let mapping = ConnectorRelationshipMapping {
            source_resource: property.domain,
            relationship: property.name,
            target_resource: property.range,
            lpg_rel_type: String::new(),
        };
        if relations.get(&key).is_some_and(|prior| prior != &mapping) {
            ambiguous.insert(key.clone());
        }
        relations.insert(key, mapping);
    }
    for key in ambiguous {
        relations.remove(&key);
    }
    relations
}

#[cfg(not(feature = "rdf"))]
pub fn decode_relationship_mappings(
    _ontologies: &[&str],
) -> BTreeMap<String, ConnectorRelationshipMapping> {
    BTreeMap::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: MEDULLA-TYPED-PACKS-R101.10
    #[test]
    fn extracts_declared_schema_mappings() {
        let mappings = decode_schema_mappings(
            br#"{"connector":"demo","sources":[],"schema_mappings":{"DemoItem":{"ontology_class":"Document","fields":{"id":"identifier"}}}}"#,
        )
        .unwrap();
        assert_eq!(mappings["DemoItem"].ontology_class, "Document");
        assert_eq!(mappings["DemoItem"].fields["id"], "identifier");
    }

    // spec: MEDULLA-TYPED-PACKS-R101.10
    #[test]
    fn a_declaration_without_mappings_declares_none() {
        assert!(decode_schema_mappings(br#"{"connector":"demo"}"#)
            .unwrap()
            .is_empty());
    }

    // spec: MEDULLA-TYPED-PACKS-R101.10
    #[test]
    fn refuses_a_malformed_or_open_mapping() {
        for body in [
            &br"connector: demo"[..],
            br#"[]"#,
            br#"{"schema_mappings":{"A":{"ontology_class":"Document","extra":1}}}"#,
            br#"{"schema_mappings":{"A":{"fields":{"id":""}}}}"#,
            br#"{"schema_mappings":{"":{}}}"#,
            br#"{"schema_mappings":[]}"#,
        ] {
            assert!(decode_schema_mappings(body).is_err(), "{body:?}");
        }
    }

    #[test]
    fn datatype_must_be_supported_and_bound_to_a_named_target() {
        let valid = br#"{"schema_mappings":{"A":{"fields":{"time":"observationTime"},"datatypes":{"time":"xsd:dateTime"}}}}"#;
        let mapping = decode_schema_mappings(valid).unwrap();
        assert_eq!(mapping["A"].datatypes["time"], "xsd:dateTime");
        for body in [
            br#"{"schema_mappings":{"A":{"datatypes":{"time":"xsd:dateTime"}}}}"#.as_slice(),
            br#"{"schema_mappings":{"A":{"fields":{"time":"observationTime"},"datatypes":{"time":"xsd:invented"}}}}"#,
            br#"{"schema_mappings":{"A":{"fields":{"time":"xsd:string"},"datatypes":{"time":"xsd:dateTime"}}}}"#,
        ] {
            assert!(decode_schema_mappings(body).is_err());
        }
    }

    // spec: MEDULLA-TYPED-PACKS-R101.10
    #[cfg(feature = "rdf")]
    #[test]
    fn relations_come_from_object_properties() {
        let ttl = "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n\
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\
            @prefix ex: <https://example.org/demo#> .\n\
            ex:contains a owl:ObjectProperty ; rdfs:domain ex:Document ; rdfs:range ex:Section .\n";
        let relations = decode_relationship_mappings(&[ttl]);
        assert_eq!(relations["Document/contains"].source_resource, "Document");
        assert_eq!(relations["Document/contains"].target_resource, "Section");
        assert_eq!(relations["Document/contains"].lpg_rel_type, "");
    }
}
