use super::*;
use crate::tables::property_graph::{
    EdgeEndpoint, EdgeTableDefinition, ElementKeyResolution, EndpointResolution, LabelDefinition,
    PropertyDefinition, PropertySet, VertexTableDefinition,
};
use crate::tables::schema::{Column, RefAction, TableConstraint};

const TENANT: &str = "tenant/acme";

fn id(name: &str) -> SqlIdentifier {
    SqlIdentifier::unquoted(name).unwrap()
}

fn name(name: &str) -> SqlName {
    SqlName::new(vec![id(name)]).unwrap()
}

fn column(name: &str, ty: ColumnType, primary_key: bool) -> Column {
    Column::new(name, ty, !primary_key, primary_key)
}

fn relation(
    object_id: &str,
    kind: RelationKind,
    revision: u64,
    schema: TableSchema,
) -> RelationCatalogSnapshot {
    let table_name = name(&schema.name);
    RelationCatalogSnapshot::new(
        RelationObjectId::new(object_id).unwrap(),
        kind,
        TENANT,
        &table_name,
        revision,
        schema,
    )
    .unwrap()
}

fn fixtures() -> Vec<RelationCatalogSnapshot> {
    let people = TableSchema::new(
        "people",
        vec![
            column("id", ColumnType::Int, true),
            column("display_name", ColumnType::Text, false),
        ],
    );
    let cities = TableSchema::new(
        "cities",
        vec![
            column("id", ColumnType::Int, true),
            column("city_name", ColumnType::Text, false),
        ],
    );
    let lives_in = TableSchema::new(
        "lives_in",
        vec![
            column("id", ColumnType::Int, true),
            column("person_id", ColumnType::Int, false),
            column("city_id", ColumnType::Int, false),
            column("since", ColumnType::Timestamp, false),
        ],
    )
    .with_constraints(vec![
        TableConstraint::ForeignKey {
            name: Some("lives_in_person".into()),
            columns: vec!["person_id".into()],
            ref_table: "people".into(),
            ref_columns: vec!["id".into()],
            on_delete: RefAction::Restrict,
            on_update: RefAction::Restrict,
        },
        TableConstraint::ForeignKey {
            name: Some("lives_in_city".into()),
            columns: vec!["city_id".into()],
            ref_table: "cities".into(),
            ref_columns: vec!["id".into()],
            on_delete: RefAction::Restrict,
            on_update: RefAction::Restrict,
        },
    ]);
    vec![
        relation("relation/people/1", RelationKind::Table, 11, people),
        relation("relation/cities/1", RelationKind::Table, 12, cities),
        relation("relation/lives-in/1", RelationKind::Table, 13, lives_in),
    ]
}

fn draft(graph_name: &str, temporary: bool) -> PropertyGraphDefinition {
    PropertyGraphDefinition::new(
        TENANT,
        name(graph_name),
        temporary,
        vec![
            VertexTableDefinition {
                relation: name("people"),
                alias: id("person"),
                key_columns: Vec::new(),
                key_resolution: ElementKeyResolution::PrimaryKey,
                labels: vec![LabelDefinition {
                    name: id("person"),
                    properties: PropertySet::AllColumns,
                }],
            },
            VertexTableDefinition {
                relation: name("cities"),
                alias: id("city"),
                key_columns: Vec::new(),
                key_resolution: ElementKeyResolution::PrimaryKey,
                labels: vec![LabelDefinition {
                    name: id("city"),
                    properties: PropertySet::None,
                }],
            },
        ],
        vec![EdgeTableDefinition {
            relation: name("lives_in"),
            alias: id("lives"),
            key_columns: Vec::new(),
            key_resolution: ElementKeyResolution::PrimaryKey,
            source: EdgeEndpoint {
                edge_key_columns: Vec::new(),
                vertex_alias: id("person"),
                vertex_key_columns: Vec::new(),
                resolution: EndpointResolution::ForeignKey,
            },
            destination: EdgeEndpoint {
                edge_key_columns: Vec::new(),
                vertex_alias: id("city"),
                vertex_key_columns: Vec::new(),
                resolution: EndpointResolution::ForeignKey,
            },
            labels: vec![LabelDefinition {
                name: id("lives"),
                properties: PropertySet::Explicit(vec![PropertyDefinition {
                    source_column: id("since"),
                    property_name: id("since"),
                }]),
            }],
        }],
    )
    .unwrap()
}

fn admit(graph_name: &str, relations: &[RelationCatalogSnapshot]) -> PropertyGraphCatalogRecord {
    PropertyGraphCatalogRecord::admit(
        PropertyGraphObjectId::new(format!("graph/{graph_name}/1")).unwrap(),
        PropertyGraphOwner::new("role/analytics").unwrap(),
        21,
        1,
        &draft(graph_name, false),
        relations,
    )
    .unwrap()
}

#[test]
fn admission_resolves_exact_keys_foreign_keys_properties_and_types() {
    let tables = fixtures();
    let record = admit("social", &tables);
    assert_eq!(record.name.tenant_scope, TENANT);
    assert_eq!(record.name.schema.value(), "public");
    assert_eq!(
        record.accepted_definition.name.quoted_sql(),
        "\"public\".\"social\""
    );
    assert!(record
        .accepted_definition
        .vertex_tables
        .iter()
        .all(|vertex| {
            vertex.key_resolution == ElementKeyResolution::Explicit
                && vertex.key_columns == vec![id("id")]
                && vertex.relation.0[0].value() == "public"
        }));
    let edge = &record.accepted_definition.edge_tables[0];
    assert_eq!(edge.source.edge_key_columns, vec![id("person_id")]);
    assert_eq!(edge.destination.edge_key_columns, vec![id("city_id")]);
    assert_eq!(edge.source.vertex_key_columns, vec![id("id")]);
    assert_eq!(edge.source.resolution, EndpointResolution::Explicit);
    assert_eq!(record.dependencies.len(), 3);
    let people = record
        .dependencies
        .iter()
        .find(|dependency| dependency.name.object.value() == "people")
        .unwrap();
    assert!(people.columns.contains(&ResolvedColumnDependency {
        name: id("display_name"),
        column_type: ColumnType::Text,
    }));
    assert_eq!(record.definition_digest, record.accepted_definition.digest);
    assert!(record.validate().is_ok());
}

#[test]
fn admission_is_deterministic_for_equivalent_catalog_snapshots() {
    let first = admit("social", &fixtures());
    let second = admit("social", &fixtures());
    assert_eq!(first, second);
    assert_eq!(first.record_digest, second.record_digest);
    assert_eq!(first.dependency_digest, second.dependency_digest);
    let bytes = first.canonical_bytes().unwrap();
    assert_eq!(decode_property_graph_catalog_record(&bytes).unwrap(), first);

    let mut with_unknown = serde_json::to_value(&first).unwrap();
    with_unknown
        .as_object_mut()
        .unwrap()
        .insert("unknown".into(), serde_json::Value::Bool(true));
    assert!(
        decode_property_graph_catalog_record(&serde_json::to_vec(&with_unknown).unwrap())
            .unwrap_err()
            .contains("unknown field")
    );

    let mut nested = serde_json::to_value(&first.accepted_definition).unwrap();
    nested["vertex_tables"][0]
        .as_object_mut()
        .unwrap()
        .insert("unknown".into(), serde_json::Value::Bool(true));
    assert!(serde_json::from_value::<PropertyGraphDefinition>(nested)
        .unwrap_err()
        .to_string()
        .contains("unknown field"));

    let mut nested_record = serde_json::to_value(&first).unwrap();
    nested_record["accepted_definition"]["edge_tables"][0]
        .as_object_mut()
        .unwrap()
        .insert("unknown".into(), serde_json::Value::Bool(true));
    assert!(
        serde_json::from_value::<PropertyGraphCatalogRecord>(nested_record)
            .unwrap_err()
            .to_string()
            .contains("unknown field")
    );
}

#[test]
fn temporary_graphs_and_relation_namespace_collisions_fail_closed() {
    let tables = fixtures();
    let error = PropertyGraphCatalogRecord::admit(
        PropertyGraphObjectId::new("graph/temp/1").unwrap(),
        PropertyGraphOwner::new("role/analytics").unwrap(),
        1,
        1,
        &draft("session_graph", true),
        &tables,
    )
    .unwrap_err();
    assert!(error.contains("connection-scoped"));

    let error = PropertyGraphCatalogRecord::admit(
        PropertyGraphObjectId::new("graph/collision/1").unwrap(),
        PropertyGraphOwner::new("role/analytics").unwrap(),
        1,
        1,
        &draft("people", false),
        &tables,
    )
    .unwrap_err();
    assert!(error.contains("shared namespace"));
}

#[test]
fn views_occupy_the_same_relation_namespace_as_property_graphs() {
    let mut relations = fixtures();
    relations.push(relation(
        "view/social/1",
        RelationKind::View,
        14,
        TableSchema::new("social", vec![column("id", ColumnType::Int, true)]),
    ));
    let error = PropertyGraphCatalogRecord::admit(
        PropertyGraphObjectId::new("graph/social/1").unwrap(),
        PropertyGraphOwner::new("role/analytics").unwrap(),
        1,
        1,
        &draft("social", false),
        &relations,
    )
    .unwrap_err();
    assert!(error.contains("shared namespace"));
}

#[test]
fn type_mismatch_and_ambiguous_foreign_keys_are_rejected() {
    let mut mismatched = fixtures();
    let edge_schema = TableSchema::new(
        "lives_in",
        vec![
            column("id", ColumnType::Int, true),
            column("person_id", ColumnType::Text, false),
            column("city_id", ColumnType::Int, false),
            column("since", ColumnType::Timestamp, false),
        ],
    )
    .with_constraints(mismatched[2].schema.constraints().to_vec());
    mismatched[2] = relation("relation/lives-in/1", RelationKind::Table, 14, edge_schema);
    assert!(PropertyGraphCatalogRecord::admit(
        PropertyGraphObjectId::new("graph/social/1").unwrap(),
        PropertyGraphOwner::new("role/analytics").unwrap(),
        1,
        1,
        &draft("social", false),
        &mismatched,
    )
    .unwrap_err()
    .contains("different types"));

    let mut ambiguous = fixtures();
    let duplicate = TableConstraint::ForeignKey {
        name: Some("lives_in_person_duplicate".into()),
        columns: vec!["person_id".into()],
        ref_table: "people".into(),
        ref_columns: vec!["id".into()],
        on_delete: RefAction::Restrict,
        on_update: RefAction::Restrict,
    };
    ambiguous[2].schema.push_constraint(duplicate);
    ambiguous[2].schema_digest = ambiguous[2].schema.schema_digest().unwrap();
    assert!(PropertyGraphCatalogRecord::admit(
        PropertyGraphObjectId::new("graph/social/1").unwrap(),
        PropertyGraphOwner::new("role/analytics").unwrap(),
        1,
        1,
        &draft("social", false),
        &ambiguous,
    )
    .unwrap_err()
    .contains("multiple foreign keys"));
}

#[test]
fn shared_label_properties_require_exact_column_type_equality() {
    let mut tables = fixtures();
    tables[1] = relation(
        "relation/cities/1",
        RelationKind::Table,
        14,
        TableSchema::new(
            "cities",
            vec![
                column("id", ColumnType::Int, true),
                column("city_name", ColumnType::Int, false),
            ],
        ),
    );
    let mut definition = draft("social", false);
    // `PropertyGraphDefinition::new` canonicalizes vertex tables by alias, so the
    // element is selected by alias rather than by position: attaching a `cities`
    // column to the `people` relation would fail earlier, on column resolution.
    let city = definition
        .vertex_tables
        .iter_mut()
        .find(|vertex| vertex.alias == id("city"))
        .expect("city vertex table");
    city.labels = vec![LabelDefinition {
        name: id("person"),
        properties: PropertySet::Explicit(vec![PropertyDefinition {
            source_column: id("city_name"),
            property_name: id("display_name"),
        }]),
    }];
    definition = PropertyGraphDefinition::new(
        definition.tenant_scope,
        definition.name,
        false,
        definition.vertex_tables,
        definition.edge_tables,
    )
    .unwrap();
    assert!(PropertyGraphCatalogRecord::admit(
        PropertyGraphObjectId::new("graph/social/1").unwrap(),
        PropertyGraphOwner::new("role/analytics").unwrap(),
        1,
        1,
        &definition,
        &tables,
    )
    .unwrap_err()
    .contains("inconsistent column types"));
}

#[test]
fn tampered_definition_dependency_and_record_digests_are_rejected() {
    let mut definition = admit("social", &fixtures());
    definition.definition_revision += 1;
    assert!(definition.validate().unwrap_err().contains("record digest"));

    let mut dependency = admit("social", &fixtures());
    dependency.dependencies[0].catalog_revision += 1;
    assert!(dependency
        .validate()
        .unwrap_err()
        .contains("dependency digest"));

    let mut record = admit("social", &fixtures());
    record.record_digest.replace_range(..1, "f");
    assert!(record.validate().unwrap_err().contains("record digest"));
}

#[test]
fn reordered_dependency_columns_are_not_a_second_canonical_record() {
    let mut record = admit("social", &fixtures());
    record
        .dependencies
        .iter_mut()
        .find(|dependency| dependency.name.object.value() == "people")
        .unwrap()
        .columns
        .reverse();
    record.dependency_digest = digest_value(
        b"epistemic-graph/sql-pgq/dependencies/v1\0",
        &record.dependencies,
        "property graph dependencies",
    )
    .unwrap();
    record.record_digest = record.compute_record_digest().unwrap();
    assert!(record
        .validate()
        .unwrap_err()
        .contains("columns are not canonically ordered"));
}

#[test]
fn self_consistent_forged_dependencies_fail_authoritative_reresolution() {
    let tables = fixtures();
    let mut record = admit("social", &tables);
    let dependency = record
        .dependencies
        .iter_mut()
        .find(|dependency| dependency.name.object.value() == "people")
        .unwrap();
    dependency
        .columns
        .iter_mut()
        .find(|column| column.name.value() == "id")
        .unwrap()
        .column_type = ColumnType::Text;
    record.dependency_digest = digest_value(
        b"epistemic-graph/sql-pgq/dependencies/v1\0",
        &record.dependencies,
        "property graph dependencies",
    )
    .unwrap();
    record.record_digest = record.compute_record_digest().unwrap();
    assert!(record.validate().is_ok());

    let mut catalog = PropertyGraphCatalog::new(TENANT, &tables).unwrap();
    assert!(catalog
        .insert(record)
        .unwrap_err()
        .contains("authoritative catalog resolution"));
}

#[test]
fn rename_preserves_object_id_and_reverse_dependencies_remain_exact() {
    let tables = fixtures();
    let record = admit("social", &tables);
    let graph_id = record.object_id.clone();
    let people_id = tables[0].object_id.clone();
    let mut catalog = PropertyGraphCatalog::new(TENANT, &tables).unwrap();
    catalog.insert(record).unwrap();
    assert_eq!(catalog.dependents_of(&people_id), vec![graph_id.clone()]);
    assert!(catalog.fence_base_ddl(&people_id, false).is_err());
    assert_eq!(
        catalog.fence_base_ddl(&people_id, true).unwrap(),
        vec![graph_id.clone()]
    );

    catalog
        .rename(&graph_id, &name("community"), 22, 2)
        .unwrap();
    let renamed = catalog.get(&graph_id).unwrap();
    assert_eq!(renamed.object_id, graph_id);
    assert_eq!(renamed.name.object.value(), "community");
    let renamed_id = renamed.object_id.clone();
    assert_eq!(catalog.dependents_of(&people_id), vec![renamed_id.clone()]);
    assert!(catalog.rename(&renamed_id, &name("people"), 23, 3).is_err());
}

#[test]
fn duplicate_authoritative_table_identity_or_name_is_rejected() {
    let tables = fixtures();
    let mut duplicate_name = tables.clone();
    let mut replacement = tables[1].clone();
    replacement.name = tables[0].name.clone();
    replacement.schema = tables[0].schema.clone();
    replacement.schema_digest = replacement.schema.schema_digest().unwrap();
    duplicate_name[1] = replacement;
    assert!(PropertyGraphCatalog::new(TENANT, &duplicate_name)
        .unwrap_err()
        .contains("duplicate relation name"));

    let mut duplicate_id = tables.clone();
    duplicate_id[1].object_id = duplicate_id[0].object_id.clone();
    assert!(PropertyGraphCatalog::new(TENANT, &duplicate_id)
        .unwrap_err()
        .contains("duplicate relation object id"));
}
