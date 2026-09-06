#![cfg(feature = "sql")]

use eg_core::graph::GraphCore;
use eg_query::sql::{
    classify, exec_graph_table_typed_with_tables, lower_graph_table,
    lower_graph_table_to_datafusion, parse_graph_table, parse_graph_table_sql,
    parse_property_graph_ddl, PropertyGraphDdlOperation, StatementKind,
};
use eg_query::tables::{
    decode_property_graph, AlterPropertyGraphAction, Column, ColumnType, DropBehavior,
    PropertyGraphDefinition, PropertyGraphStatement, PropertySet, TableSchema, TableStore,
};
use serde_json::json;

const TENANT: &str = "tenant/acme";

fn shop_definition() -> PropertyGraphDefinition {
    let sql = r#"
        CREATE PROPERTY GRAPH shop
        VERTEX TABLES (
            customers KEY (customer_id)
                LABEL customer PROPERTIES (name),
            orders KEY (order_id)
                LABEL "order" PROPERTIES (ordered_when)
        )
        EDGE TABLES (
            customer_orders KEY (row_id)
                SOURCE KEY (customer_id) REFERENCES customers (customer_id)
                DESTINATION KEY (order_id) REFERENCES orders (order_id)
                LABEL has_placed PROPERTIES (since)
        )
    "#;
    match parse_property_graph_ddl(sql, TENANT).expect("valid SQL/PGQ definition") {
        PropertyGraphStatement::Create(definition) => definition,
        _ => panic!("expected CREATE definition"),
    }
}

#[test]
fn postgres_19_create_definition_is_canonical_and_digest_bound() {
    let definition = shop_definition();
    let bytes = definition.canonical_bytes().unwrap();
    assert_eq!(decode_property_graph(&bytes).unwrap(), definition);
    assert_eq!(definition.digest.len(), 64);

    let mut tampered = definition.clone();
    tampered.tenant_scope = "tenant/other".into();
    assert!(tampered.validate().unwrap_err().contains("digest mismatch"));
}

#[test]
fn postgres_19_alter_and_drop_forms_are_typed() {
    let alter = parse_property_graph_ddl("ALTER PROPERTY GRAPH shop RENAME TO storefront;", TENANT)
        .unwrap();
    assert!(matches!(
        alter,
        PropertyGraphStatement::Alter {
            if_exists: false,
            action: AlterPropertyGraphAction::RenameTo(_),
            ..
        }
    ));
    assert!(matches!(
        parse_property_graph_ddl(
            "ALTER PROPERTY GRAPH IF EXISTS shop SET SCHEMA archive",
            TENANT
        )
        .unwrap(),
        PropertyGraphStatement::Alter {
            if_exists: true,
            action: AlterPropertyGraphAction::SetSchema(_),
            ..
        }
    ));

    let drop = parse_property_graph_ddl(
        "DROP PROPERTY GRAPH IF EXISTS shop, archive.shop CASCADE",
        TENANT,
    )
    .unwrap();
    assert!(matches!(
        drop,
        PropertyGraphStatement::Drop {
            if_exists: true,
            behavior: DropBehavior::Cascade,
            ..
        }
    ));
}

#[test]
fn graph_table_fixed_path_lowers_to_the_existing_relational_surface() {
    let definition = shop_definition();
    let query = parse_graph_table(
        r#"GRAPH_TABLE (
            shop
            MATCH (c IS customer)-[h IS has_placed]->(
                o IS "order" WHERE o.ordered_when = CURRENT_DATE
            )
            COLUMNS (
                c.name AS customer_name,
                h.since AS relationship_since,
                o.ordered_when AS order_date
            )
        )"#,
    )
    .unwrap();
    let plan = lower_graph_table(&query, &definition, TENANT).unwrap();
    assert_eq!(plan.branch_count(), 1);
    let sql = plan.to_sql();
    assert!(sql.contains(r#"FROM "customers" AS "_pgq_v0""#));
    assert!(sql.contains(r#"JOIN "customer_orders" AS "_pgq_e0""#));
    assert!(sql.contains(r#""_pgq_e0"."customer_id" = "_pgq_v0"."customer_id""#));
    assert!(sql.contains(r#""_pgq_e0"."order_id" = "_pgq_v1"."order_id""#));
    assert!(sql.contains(r#""_pgq_v1"."ordered_when" = CURRENT_DATE"#));
    lower_graph_table_to_datafusion(&query, &definition, TENANT).unwrap();
}

#[test]
fn tenant_and_structure_bounds_fail_closed() {
    let definition = shop_definition();
    let query =
        parse_graph_table("GRAPH_TABLE (shop MATCH (c IS customer) COLUMNS (c.name))").unwrap();
    assert!(lower_graph_table(&query, &definition, "tenant/other")
        .unwrap_err()
        .contains("tenant scope"));
    assert!(parse_graph_table(&"x".repeat(eg_query::sql::MAX_PGQ_SQL_BYTES + 1)).is_err());
    assert!(parse_graph_table(
        "GRAPH_TABLE (shop MATCH (c IS customer) COLUMNS (c.name)); DROP TABLE users"
    )
    .is_err());
    for malformed in [
        "GRAPH_TABLE (shop MATCH (a)<-[e](b) COLUMNS (a.name))",
        "GRAPH_TABLE (shop MATCH (a)-[e]>(b) COLUMNS (a.name))",
        "GRAPH_TABLE (shop MATCH (a)-[e](b) COLUMNS (a.name))",
        "GRAPH_TABLE (shop MATCH (a)-->(b) COLUMNS (a.name))",
    ] {
        assert!(
            parse_graph_table(malformed).is_err(),
            "accepted {malformed}"
        );
    }
    assert!(
        parse_graph_table("GRAPH_TABLE (shop MATCH (c IS customer|typo) COLUMNS (c.name))")
            .and_then(|query| lower_graph_table(&query, &definition, TENANT))
            .unwrap_err()
            .contains("unknown vertex label")
    );
    assert!(
        parse_graph_table("GRAPH_TABLE (shop MATCH (order IS customer) COLUMNS (order.name))")
            .unwrap_err()
            .contains("requires SQL quoting")
    );
}

#[test]
fn equivalent_identifier_spellings_have_one_catalog_identity() {
    let bare = parse_property_graph_ddl(
        "CREATE PROPERTY GRAPH sample VERTEX TABLES (people KEY (id))",
        TENANT,
    )
    .unwrap();
    let quoted = parse_property_graph_ddl(
        "CREATE PROPERTY GRAPH \"sample\" VERTEX TABLES (\"people\" KEY (\"id\"))",
        TENANT,
    )
    .unwrap();
    assert_eq!(bare, quoted);
    assert_eq!(bare.digest().unwrap(), quoted.digest().unwrap());
    assert!(eg_query::sql::SqlNumber::parse("0); DROP TABLE users; --").is_err());
}

#[test]
fn unresolved_catalog_defaults_and_inconsistent_properties_fail_closed() {
    let draft = match parse_property_graph_ddl(
        "CREATE PROPERTY GRAPH g VERTEX TABLES (v) EDGE TABLES (e SOURCE v DESTINATION v)",
        TENANT,
    )
    .unwrap()
    {
        PropertyGraphStatement::Create(value) => value,
        _ => unreachable!(),
    };
    let query = parse_graph_table("GRAPH_TABLE (g MATCH (a)-[r]->(b) COLUMNS (a.id))").unwrap();
    assert!(lower_graph_table(&query, &draft, TENANT)
        .unwrap_err()
        .contains("unresolved key/property"));
    assert!(parse_property_graph_ddl(
        "CREATE PROPERTY GRAPH g VERTEX TABLES (a LABEL x NO PROPERTIES, b LABEL x PROPERTIES (p))",
        TENANT,
    )
    .unwrap_err()
    .contains("inconsistent property names"));
    assert!(parse_property_graph_ddl(
        "CREATE PROPERTY GRAPH g VERTEX TABLES (a LABEL x PROPERTIES (left_col AS p) LABEL y PROPERTIES (right_col AS p))",
        TENANT,
    )
    .unwrap_err()
    .contains("inconsistent source columns"));
    assert!(parse_property_graph_ddl(
        "CREATE PROPERTY GRAPH g VERTEX TABLES (a KEY (id) LABEL x, b KEY (id) LABEL x PROPERTIES (p), c KEY (id) LABEL x PROPERTIES (q))",
        TENANT,
    )
    .unwrap_err()
    .contains("inconsistent property names"));
    assert!(parse_property_graph_ddl(
        "ALTER PROPERTY GRAPH g ALTER VERTEX TABLE v DROP LABEL x ADD LABEL y",
        TENANT,
    )
    .unwrap_err()
    .contains("only ADD LABEL may repeat"));
    assert!(parse_property_graph_ddl(
        "ALTER PROPERTY GRAPH g ADD VERTEX TABLES (a AS same, b AS same)",
        TENANT,
    )
    .unwrap_err()
    .contains("duplicate ALTER ADD alias"));
    assert!(parse_property_graph_ddl(
        "CREATE PROPERTY GRAPH authorization VERTEX TABLES (v)",
        TENANT,
    )
    .unwrap_err()
    .contains("requires SQL quoting"));
    assert!(parse_property_graph_ddl(
        "CREATE PROPERTY GRAPH g EDGE TABLES (e SOURCE v DESTINATION v) VERTEX TABLES (v)",
        TENANT,
    )
    .unwrap_err()
    .contains("must occur once and before"));
    assert!(parse_property_graph_ddl(
        "CREATE PROPERTY GRAPH g VERTEX TABLES (a) NODE TABLES (b)",
        TENANT,
    )
    .unwrap_err()
    .contains("must occur once and before"));

    let mut invalid = shop_definition();
    invalid.vertex_tables[0].labels[0].properties = PropertySet::Explicit(Vec::new());
    assert!(invalid
        .validate()
        .unwrap_err()
        .contains("explicit property set"));
}

#[test]
fn undirected_self_loop_is_counted_once_but_directed_incoming_is_retained() {
    let definition = match parse_property_graph_ddl(
        "CREATE PROPERTY GRAPH g VERTEX TABLES (v KEY (id)) EDGE TABLES (e KEY (eid) SOURCE KEY (src) REFERENCES v (id) DESTINATION KEY (dst) REFERENCES v (id))",
        TENANT,
    )
    .unwrap()
    {
        PropertyGraphStatement::Create(value) => value,
        _ => unreachable!(),
    };
    let incoming = parse_graph_table("GRAPH_TABLE (g MATCH (a)<-[r]-(b) COLUMNS (a.id))").unwrap();
    let incoming_sql = lower_graph_table(&incoming, &definition, TENANT)
        .unwrap()
        .to_sql();
    assert!(!incoming_sql.contains("NOT ("));

    let either = parse_graph_table("GRAPH_TABLE (g MATCH (a)-[r]-(b) COLUMNS (a.id))").unwrap();
    let either_plan = lower_graph_table(&either, &definition, TENANT).unwrap();
    assert_eq!(either_plan.branch_count(), 2);
    assert_eq!(either_plan.to_sql().matches("NOT (").count(), 1);
}

#[test]
fn quoted_identifiers_cannot_escape_the_relational_renderer() {
    let ddl = r#"
        CREATE PROPERTY GRAPH "g"";drop"
        VERTEX TABLES (
            "people"";delete" KEY (id) LABEL person PROPERTIES (name)
        )
    "#;
    let definition = match parse_property_graph_ddl(ddl, TENANT).unwrap() {
        PropertyGraphStatement::Create(definition) => definition,
        _ => unreachable!(),
    };
    let query =
        parse_graph_table(r#"GRAPH_TABLE ("g"";drop" MATCH (p IS person) COLUMNS (p.name))"#)
            .unwrap();
    let sql = lower_graph_table(&query, &definition, TENANT)
        .unwrap()
        .to_sql();
    assert!(sql.contains(r#""people"";delete""#));
    assert!(!sql.contains("DELETE "));
}

#[test]
fn classifier_routes_pgq_to_explicit_catalog_admission() {
    let create =
        classify("CREATE PROPERTY GRAPH shop VERTEX TABLES (customers KEY (customer_id))").unwrap();
    let StatementKind::PropertyGraphDdlRequiresCatalogAdmission(admission) = create else {
        panic!("expected property-graph catalog admission");
    };
    assert_eq!(admission.operation, PropertyGraphDdlOperation::Create);
    assert_eq!(admission.names.len(), 1);
    assert_eq!(admission.names[0].leaf().value(), "shop");

    let drop = classify("DROP PROPERTY GRAPH IF EXISTS shop, archive.shop CASCADE").unwrap();
    let StatementKind::PropertyGraphDdlRequiresCatalogAdmission(admission) = drop else {
        panic!("expected property-graph catalog admission");
    };
    assert_eq!(admission.operation, PropertyGraphDdlOperation::Drop);
    assert_eq!(admission.names.len(), 2);

    assert!(classify("CREATE PROPERTY GRAPH").is_err());
    assert!(matches!(
        classify("SELECT * FROM GRAPH_TABLE (shop MATCH (c IS customer) COLUMNS (c.name))")
            .unwrap(),
        StatementKind::GraphTableReadRequiresCatalogAdmission(_)
    ));
}

#[test]
fn graph_table_exec_adapter_uses_existing_relational_tables() {
    let definition = shop_definition();
    let query = parse_graph_table_sql(
        r#"SELECT * FROM GRAPH_TABLE (
            shop
            MATCH (c IS customer)-[h IS has_placed]->(o IS "order")
            COLUMNS (
                c.name AS customer_name,
                h.since AS relationship_since,
                o.ordered_when AS order_date
            )
        )"#,
    )
    .unwrap();
    let (store, _path) = TableStore::open_temp().unwrap();
    store
        .create_table(
            &TableSchema::new(
                "customers",
                vec![
                    Column::new("customer_id", ColumnType::Text, false, true),
                    Column::new("name", ColumnType::Text, false, false),
                ],
            ),
            false,
        )
        .unwrap();
    store
        .create_table(
            &TableSchema::new(
                "orders",
                vec![
                    Column::new("order_id", ColumnType::Text, false, true),
                    Column::new("ordered_when", ColumnType::Text, false, false),
                ],
            ),
            false,
        )
        .unwrap();
    store
        .create_table(
            &TableSchema::new(
                "customer_orders",
                vec![
                    Column::new("row_id", ColumnType::Text, false, true),
                    Column::new("customer_id", ColumnType::Text, false, false),
                    Column::new("order_id", ColumnType::Text, false, false),
                    Column::new("since", ColumnType::Text, false, false),
                ],
            ),
            false,
        )
        .unwrap();
    store
        .insert_rows(
            "customers",
            &["customer_id".into(), "name".into()],
            &[vec![json!("c1"), json!("Ada")]],
        )
        .unwrap();
    store
        .insert_rows(
            "orders",
            &["order_id".into(), "ordered_when".into()],
            &[vec![json!("o1"), json!("2026-09-04")]],
        )
        .unwrap();
    store
        .insert_rows(
            "customer_orders",
            &[
                "row_id".into(),
                "customer_id".into(),
                "order_id".into(),
                "since".into(),
            ],
            &[vec![
                json!("r1"),
                json!("c1"),
                json!("o1"),
                json!("2026-01-01"),
            ]],
        )
        .unwrap();

    let view = GraphCore::new().analysis_snapshot();
    let result =
        exec_graph_table_typed_with_tables(&view, &store, &query, &definition, TENANT).unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0],
        vec![json!("Ada"), json!("2026-01-01"), json!("2026-09-04")]
    );
}
