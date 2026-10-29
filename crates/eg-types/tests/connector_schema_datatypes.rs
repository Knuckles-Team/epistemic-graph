use std::collections::BTreeMap;

use eg_types::connector_pack::ConnectorSchemaMapping;
use serde::Serialize;

#[derive(Serialize)]
struct LegacyMapping {
    ontology_class: String,
    fields: BTreeMap<String, String>,
}

#[test]
fn empty_datatype_map_preserves_legacy_messagepack_identity() {
    let fields = BTreeMap::from([("name".to_string(), "label".to_string())]);
    let legacy = LegacyMapping {
        ontology_class: "Document".into(),
        fields: fields.clone(),
    };
    let current = ConnectorSchemaMapping {
        ontology_class: "Document".into(),
        fields,
        datatypes: BTreeMap::new(),
    };
    assert_eq!(
        rmp_serde::to_vec_named(&legacy).unwrap(),
        rmp_serde::to_vec_named(&current).unwrap()
    );
    let decoded: ConnectorSchemaMapping =
        rmp_serde::from_slice(&rmp_serde::to_vec_named(&legacy).unwrap()).unwrap();
    assert!(decoded.datatypes.is_empty());
}
