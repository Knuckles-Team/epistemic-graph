#![cfg(feature = "sql")]
//! Durable SQL/PGQ property-graph catalog behaviour, exercised through the
//! public `TableStore` API — the same catalog transaction ordinary table and
//! view DDL commits through.

use eg_core::graph::GraphCore;
use eg_query::sql::{
    classify, exec_graph_table_typed_with_tables, parse_property_graph_ddl, StatementKind,
};
use eg_query::tables::schema::TableConstraint;
use eg_query::tables::{
    AlterPropertyGraphAction, Column, ColumnType, DropBehavior, ElementKind, GraphOwner,
    LabelDefinition, PropertyGraphDefinition, PropertyGraphStatement, PropertyGraphTxnOp,
    PropertySet, SqlIdentifier, SqlName, TableSchema, TableStore, TableTxn, TxnOp,
};
use serde_json::json;

// The store's owner scope is now the mutation TENANT of its bootstrap scope
// (RF-RULING-004), and `TenantId` rejects path semantics as a persistence-privacy
// rule, so a `/`-bearing scope is no longer expressible. The dot keeps the
// structured, non-bare-word shape this fixture is testing against.
const TENANT: &str = "tenant.acme";
const OWNER: &str = "role/analytics";

const SHOP_DDL: &str = r#"
    CREATE PROPERTY GRAPH shop
    VERTEX TABLES (
        customers KEY (customer_id) LABEL customer PROPERTIES (name),
        orders KEY (order_id) LABEL "order" PROPERTIES (ordered_when)
    )
    EDGE TABLES (
        customer_orders KEY (row_id)
            SOURCE KEY (customer_id) REFERENCES customers (customer_id)
            DESTINATION KEY (order_id) REFERENCES orders (order_id)
            LABEL has_placed PROPERTIES (since)
    )
"#;

fn id(value: &str) -> SqlIdentifier {
    SqlIdentifier::unquoted(value).expect("identifier")
}

fn name(value: &str) -> SqlName {
    SqlName::new(vec![id(value)]).expect("name")
}

fn definition(sql: &str) -> PropertyGraphDefinition {
    match parse_property_graph_ddl(sql, TENANT).expect("valid definition") {
        PropertyGraphStatement::Create(value) => value,
        _ => panic!("expected CREATE PROPERTY GRAPH"),
    }
}

fn text(column: &str, primary_key: bool) -> Column {
    Column::new(column, ColumnType::Text, !primary_key, primary_key)
}

fn text_col(column: &str) -> Column {
    text(column, false)
}

fn open_store() -> (TableStore, std::path::PathBuf) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "eg_pgq_catalog_{}_{}.redb",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let store = TableStore::open_scoped(
        &path,
        TENANT,
        eg_query::tables::store::dev_scope_grant::dev_verifier(),
        eg_query::tables::store::dev_scope_grant::DEV_PRINCIPAL,
        eg_query::tables::store::dev_scope_grant::DEV_PROOF,
    )
    .expect("open scoped store");
    (store, path)
}

fn base_tables(store: &TableStore) {
    for schema in [
        TableSchema::new(
            "customers",
            vec![text("customer_id", true), text("name", false)],
        ),
        TableSchema::new(
            "orders",
            vec![text("order_id", true), text("ordered_when", false)],
        ),
        TableSchema::new(
            "customer_orders",
            vec![
                text("row_id", true),
                text("customer_id", false),
                text("order_id", false),
                text("since", false),
            ],
        ),
    ] {
        store.create_table(&schema, false).expect("create table");
    }
}

fn shop_store() -> (TableStore, std::path::PathBuf) {
    let (store, path) = open_store();
    base_tables(&store);
    store
        .create_property_graph(TENANT, &definition(SHOP_DDL), OWNER)
        .expect("create property graph");
    (store, path)
}

#[test]
fn a_created_graph_is_durable_with_tenant_identity_owner_and_dependencies() {
    let (store, _path) = shop_store();
    let record = store
        .property_graph(TENANT, &name("shop"))
        .unwrap()
        .unwrap();
    assert_eq!(record.name.tenant_scope, TENANT);
    assert_eq!(record.name.schema.value(), "public");
    assert_eq!(record.name.object.value(), "shop");
    assert_eq!(record.owner.value(), OWNER);
    assert!(record.object_id.value().starts_with("propertygraph/"));
    assert_eq!(record.definition_revision, 1);
    assert!(record.catalog_revision > 0);
    assert!(record.validate().is_ok());

    // Every base relation is pinned by name, revision and schema digest.
    let mut dependencies: Vec<_> = record
        .dependencies
        .iter()
        .map(|dependency| dependency.name.object.value().to_string())
        .collect();
    dependencies.sort();
    assert_eq!(dependencies, ["customer_orders", "customers", "orders"]);
    assert!(record
        .dependencies
        .iter()
        .all(|dependency| dependency.catalog_revision > 0 && dependency.schema_digest.len() == 64));
    assert_eq!(store.list_property_graphs().unwrap(), vec!["shop"]);

    // The record survives a reopen of the same durable catalog file.
    drop(store);
    let reopened = TableStore::open_scoped(
        &_path,
        TENANT,
        eg_query::tables::store::dev_scope_grant::dev_verifier(),
        eg_query::tables::store::dev_scope_grant::DEV_PRINCIPAL,
        eg_query::tables::store::dev_scope_grant::DEV_PROOF,
    )
    .unwrap();
    assert_eq!(
        reopened.property_graph(TENANT, &name("shop")).unwrap(),
        Some(record)
    );
}

#[test]
fn the_graph_name_shares_one_relation_namespace_with_tables_and_views() {
    let (store, _path) = shop_store();
    assert!(store
        .create_table(&TableSchema::new("shop", vec![text("id", true)]), false)
        .unwrap_err()
        .contains("is a property graph"));
    assert!(store
        .create_view("shop", "SELECT 1", false)
        .unwrap_err()
        .contains("is a property graph"));

    let collide = definition(
        "CREATE PROPERTY GRAPH customers VERTEX TABLES (orders KEY (order_id) LABEL o PROPERTIES (ordered_when))",
    );
    assert!(store
        .create_property_graph(TENANT, &collide, OWNER)
        .unwrap_err()
        .contains("collides with a table or view"));
    assert!(store
        .create_property_graph(TENANT, &definition(SHOP_DDL), OWNER)
        .unwrap_err()
        .contains("already exists"));
}

#[test]
fn temporary_and_schema_qualified_graphs_are_rejected() {
    let (store, _path) = open_store();
    base_tables(&store);
    let temporary = definition(
        "CREATE TEMP PROPERTY GRAPH session_shop VERTEX TABLES (customers KEY (customer_id) LABEL customer PROPERTIES (name))",
    );
    assert!(store
        .create_property_graph(TENANT, &temporary, OWNER)
        .unwrap_err()
        .contains("connection-scoped"));

    let qualified = definition(
        "CREATE PROPERTY GRAPH archive.shop VERTEX TABLES (customers KEY (customer_id) LABEL customer PROPERTIES (name))",
    );
    assert!(store
        .create_property_graph(TENANT, &qualified, OWNER)
        .unwrap_err()
        .contains("public"));
}

#[test]
fn an_admitted_graph_fences_ddl_on_every_base_relation_it_pins() {
    let (store, _path) = shop_store();
    for error in [
        store.drop_table("customers", false).unwrap_err(),
        store
            .add_column("customers", text("nickname", false))
            .unwrap_err(),
        store.rename_table("customers", "people").unwrap_err(),
        store
            .alter_column_type("orders", "ordered_when", ColumnType::Int)
            .unwrap_err(),
    ] {
        assert!(error.contains("property graph"), "unfenced: {error}");
        assert!(error.contains("shop"), "no dependent named: {error}");
    }
    // An unrelated relation is untouched by the fence.
    store
        .create_table(&TableSchema::new("audit", vec![text("id", true)]), false)
        .unwrap();
    store.drop_table("audit", false).unwrap();

    assert_eq!(
        store
            .drop_property_graph(TENANT, &[name("shop")], false, DropBehavior::Restrict)
            .unwrap(),
        1
    );
    store.drop_table("customers", false).unwrap();
}

#[test]
fn drop_is_exact_and_if_exists_is_the_only_tolerated_absence() {
    let (store, _path) = shop_store();
    assert!(store
        .drop_property_graph(TENANT, &[name("missing")], false, DropBehavior::Restrict)
        .unwrap_err()
        .contains("does not exist"));
    assert_eq!(
        store
            .drop_property_graph(TENANT, &[name("missing")], true, DropBehavior::Restrict)
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .drop_property_graph(TENANT, &[name("shop")], true, DropBehavior::Cascade)
            .unwrap(),
        1
    );
    assert_eq!(store.property_graph(TENANT, &name("shop")).unwrap(), None);
    assert!(store.list_property_graphs().unwrap().is_empty());
}

#[test]
fn rename_keeps_the_object_id_while_revisions_advance_and_the_old_name_frees() {
    let (store, _path) = shop_store();
    let before = store
        .property_graph(TENANT, &name("shop"))
        .unwrap()
        .unwrap();
    let renamed = store
        .alter_property_graph(
            TENANT,
            &name("shop"),
            false,
            &AlterPropertyGraphAction::RenameTo(id("storefront")),
            "role/ops",
        )
        .unwrap()
        .unwrap();
    assert_eq!(renamed.object_id, before.object_id);
    assert_eq!(renamed.owner, before.owner);
    assert_eq!(renamed.name.object.value(), "storefront");
    assert!(renamed.definition_revision > before.definition_revision);
    assert!(renamed.catalog_revision > before.catalog_revision);
    assert_eq!(store.property_graph(TENANT, &name("shop")).unwrap(), None);

    // The freed name is available to an ordinary relation again.
    store
        .create_table(&TableSchema::new("shop", vec![text("id", true)]), false)
        .unwrap();
    assert!(store
        .alter_property_graph(
            TENANT,
            &name("storefront"),
            false,
            &AlterPropertyGraphAction::RenameTo(id("shop")),
            "role/ops",
        )
        .unwrap_err()
        .contains("collides"));
}

#[test]
fn alter_resolves_owner_element_and_label_changes_and_rejects_set_schema() {
    let (store, _path) = shop_store();
    let owned = store
        .alter_property_graph(
            TENANT,
            &name("shop"),
            false,
            &AlterPropertyGraphAction::OwnerTo(GraphOwner::CurrentUser),
            "role/ops",
        )
        .unwrap()
        .unwrap();
    assert_eq!(owned.owner.value(), "role/ops");

    assert!(store
        .alter_property_graph(
            TENANT,
            &name("shop"),
            false,
            &AlterPropertyGraphAction::SetSchema(id("archive")),
            "role/ops",
        )
        .unwrap_err()
        .contains("SET SCHEMA"));

    // A vertex table still referenced by an edge table needs CASCADE.
    let drop_orders = |behavior| AlterPropertyGraphAction::DropTables {
        kind: ElementKind::Vertex,
        aliases: vec![id("orders")],
        behavior,
    };
    assert!(store
        .alter_property_graph(
            TENANT,
            &name("shop"),
            false,
            &drop_orders(DropBehavior::Restrict),
            "role/ops"
        )
        .unwrap_err()
        .contains("CASCADE"));
    let cascaded = store
        .alter_property_graph(
            TENANT,
            &name("shop"),
            false,
            &drop_orders(DropBehavior::Cascade),
            "role/ops",
        )
        .unwrap()
        .unwrap();
    assert!(cascaded.accepted_definition.edge_tables.is_empty());
    assert_eq!(cascaded.accepted_definition.vertex_tables.len(), 1);
    assert_eq!(
        cascaded
            .dependencies
            .iter()
            .map(|dependency| dependency.name.object.value())
            .collect::<Vec<_>>(),
        vec!["customers"]
    );
    // The relations the graph no longer pins are free to be dropped again.
    store.drop_table("customer_orders", false).unwrap();

    let labelled = store
        .alter_property_graph(
            TENANT,
            &name("shop"),
            false,
            &AlterPropertyGraphAction::AlterElement {
                kind: ElementKind::Vertex,
                alias: id("customers"),
                actions: vec![eg_query::tables::AlterElementAction::AddLabel(
                    LabelDefinition {
                        name: id("account"),
                        properties: PropertySet::Explicit(vec![
                            eg_query::tables::PropertyDefinition {
                                source_column: id("name"),
                                property_name: id("name"),
                            },
                        ]),
                    },
                )],
            },
            "role/ops",
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        labelled.accepted_definition.vertex_tables[0].labels.len(),
        2
    );

    assert!(store
        .alter_property_graph(
            TENANT,
            &name("missing"),
            false,
            &AlterPropertyGraphAction::OwnerTo(GraphOwner::SessionUser),
            "role/ops",
        )
        .unwrap_err()
        .contains("does not exist"));
}

#[test]
fn the_graph_record_commits_in_the_same_catalog_transaction_as_its_base_tables() {
    let (store, _path) = open_store();
    let mut txn = TableTxn::new();
    txn.push(TxnOp::CreateTable {
        schema: TableSchema::new("people", vec![text("person_id", true), text("name", false)]),
        if_not_exists: false,
    });
    txn.push(TxnOp::PropertyGraphDdl(PropertyGraphTxnOp::Create {
        tenant_scope: TENANT.to_string(),
        definition: definition(
            "CREATE PROPERTY GRAPH social VERTEX TABLES (people KEY (person_id) LABEL person PROPERTIES (name))",
        ),
        owner: OWNER.to_string(),
    }));
    store.commit_txn(&txn).unwrap();
    assert!(store
        .property_graph(TENANT, &name("social"))
        .unwrap()
        .is_some());

    // A graph that cannot be admitted rolls the whole catalog transaction back,
    // including the table staged before it.
    let mut failing = TableTxn::new();
    failing.push(TxnOp::CreateTable {
        schema: TableSchema::new("places", vec![text("place_id", true)]),
        if_not_exists: false,
    });
    failing.push(TxnOp::PropertyGraphDdl(PropertyGraphTxnOp::Create {
        tenant_scope: TENANT.to_string(),
        definition: definition(
            "CREATE PROPERTY GRAPH cities VERTEX TABLES (places KEY (absent_column) LABEL place PROPERTIES (place_id))",
        ),
        owner: OWNER.to_string(),
    }));
    assert!(store.commit_txn(&failing).is_err());
    assert!(!store.list_tables().unwrap().contains(&"places".to_string()));
    assert_eq!(store.property_graph(TENANT, &name("cities")).unwrap(), None);

    let mut removal = TableTxn::new();
    removal.push(TxnOp::PropertyGraphDdl(PropertyGraphTxnOp::Drop {
        tenant_scope: TENANT.to_string(),
        names: vec![name("social")],
        if_exists: false,
        behavior: DropBehavior::Restrict,
    }));
    removal.push(TxnOp::DropTable {
        name: "people".to_string(),
        if_exists: false,
    });
    store.commit_txn(&removal).unwrap();
    assert_eq!(store.property_graph(TENANT, &name("social")).unwrap(), None);
}

#[test]
fn create_then_select_from_graph_table_round_trips_through_the_store() {
    let (store, _path) = shop_store();
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

    // Exactly the sequence both server routes perform: classify the statement,
    // resolve its definition from the DURABLE catalog, lower, and execute on the
    // existing relational executor.
    let statement = classify(
        r#"SELECT * FROM GRAPH_TABLE (
            shop
            MATCH (c:customer)-[h:has_placed]->(o:"order")
            COLUMNS (c.name AS customer_name, o.ordered_when AS order_date)
        )"#,
    )
    .unwrap();
    let StatementKind::GraphTableReadRequiresCatalogAdmission(query) = statement else {
        panic!("expected a GRAPH_TABLE read");
    };
    let record = store.property_graph(TENANT, &query.graph).unwrap().unwrap();
    let view = GraphCore::new().analysis_snapshot();
    let result = exec_graph_table_typed_with_tables(
        &view,
        &store,
        &query,
        &record.accepted_definition,
        TENANT,
    )
    .unwrap();
    assert_eq!(result.rows, vec![vec![json!("Ada"), json!("2026-09-04")]]);

    // The same read fails closed once the graph is gone from the catalog.
    store
        .drop_property_graph(TENANT, &[name("shop")], false, DropBehavior::Restrict)
        .unwrap();
    assert_eq!(store.property_graph(TENANT, &query.graph).unwrap(), None);
}

#[test]
fn element_id_stays_unique_across_a_label_disjunction_union() {
    let (store, _path) = open_store();
    for schema in [
        TableSchema::new("people", vec![text("person_id", true)]),
        TableSchema::new("places", vec![text("place_id", true)]),
    ] {
        store.create_table(&schema, false).unwrap();
    }
    // The SAME key value in two element tables with independent key spaces.
    store
        .insert_rows("people", &["person_id".into()], &[vec![json!("k1")]])
        .unwrap();
    store
        .insert_rows("places", &["place_id".into()], &[vec![json!("k1")]])
        .unwrap();
    store
        .create_property_graph(
            TENANT,
            &definition(
                "CREATE PROPERTY GRAPH labelled VERTEX TABLES (\
                 people KEY (person_id) LABEL person PROPERTIES (person_id), \
                 places KEY (place_id) LABEL place PROPERTIES (place_id))",
            ),
            OWNER,
        )
        .unwrap();

    let StatementKind::GraphTableReadRequiresCatalogAdmission(query) = classify(
        "SELECT * FROM GRAPH_TABLE (labelled MATCH (v:person | place) COLUMNS (ELEMENT_ID(v) AS element_id))",
    )
    .unwrap() else {
        panic!("expected a GRAPH_TABLE read");
    };
    let record = store.property_graph(TENANT, &query.graph).unwrap().unwrap();
    let view = GraphCore::new().analysis_snapshot();
    let result = exec_graph_table_typed_with_tables(
        &view,
        &store,
        &query,
        &record.accepted_definition,
        TENANT,
    )
    .unwrap();
    let mut ids: Vec<_> = result.rows.iter().map(|row| row[0].clone()).collect();
    ids.sort_by_key(|value| value.to_string());
    assert_eq!(ids, vec![json!("6:people:k1"), json!("6:places:k1")]);
}

#[test]
fn a_graph_admitted_under_one_tenant_is_not_readable_under_another() {
    let (store, _path) = shop_store();
    let other = "tenant/other";
    // The record carries its admitting tenant, so a read under a different
    // verified scope sees nothing at all -- not a different error, nothing.
    assert_eq!(store.property_graph(other, &name("shop")).unwrap(), None);
    assert!(store
        .drop_property_graph(other, &[name("shop")], false, DropBehavior::Restrict)
        .unwrap_err()
        .contains("does not exist"));
    assert!(store
        .alter_property_graph(
            other,
            &name("shop"),
            false,
            &AlterPropertyGraphAction::RenameTo(id("stolen")),
            "role/ops",
        )
        .unwrap_err()
        .contains("does not exist"));
    // …and it is untouched for its own tenant.
    assert!(store
        .property_graph(TENANT, &name("shop"))
        .unwrap()
        .is_some());

    // Lowering refuses a definition whose scope is not the verified one.
    let record = store
        .property_graph(TENANT, &name("shop"))
        .unwrap()
        .unwrap();
    let StatementKind::GraphTableReadRequiresCatalogAdmission(query) =
        classify("SELECT * FROM GRAPH_TABLE (shop MATCH (c:customer) COLUMNS (c.name))").unwrap()
    else {
        panic!("expected a GRAPH_TABLE read");
    };
    let view = GraphCore::new().analysis_snapshot();
    assert!(exec_graph_table_typed_with_tables(
        &view,
        &store,
        &query,
        &record.accepted_definition,
        other,
    )
    .unwrap_err()
    .contains("tenant scope"));
}

#[test]
fn quoted_identifiers_in_a_persisted_graph_cannot_escape_the_emitted_sql() {
    let (store, _path) = open_store();
    // A relation, a column, an element alias and a graph name each carrying the
    // exact bytes that would break out of a quoted identifier or a literal.
    store
        .create_table(
            &TableSchema::new(
                r#"tab";drop"#,
                vec![text(r#"id";--"#, true), text(r#"na'me"#, false)],
            ),
            false,
        )
        .unwrap();
    let ddl = concat!(
        r#"CREATE PROPERTY GRAPH "g"";drop" VERTEX TABLES ("#,
        r#""tab"";drop" AS "al'ias" KEY ("id"";--") LABEL "lab'el" PROPERTIES ("na'me"))"#
    );
    store
        .create_property_graph(TENANT, &definition(ddl), OWNER)
        .unwrap();

    let graph = SqlName::new(vec![SqlIdentifier::quoted(r#"g";drop"#).unwrap()]).unwrap();
    let record = store.property_graph(TENANT, &graph).unwrap().unwrap();
    let StatementKind::GraphTableReadRequiresCatalogAdmission(query) = classify(concat!(
        r#"SELECT * FROM GRAPH_TABLE ("g"";drop" MATCH (v:"lab'el") "#,
        r#"COLUMNS (ELEMENT_ID(v) AS element_id, v."na'me" AS n))"#
    ))
    .unwrap() else {
        panic!("expected a GRAPH_TABLE read");
    };
    let sql = eg_query::sql::lower_graph_table(&query, &record.accepted_definition, TENANT)
        .unwrap()
        .to_sql();

    // Identifiers are `"`-doubled; the ELEMENT_ID tag is a literal, so its `'`
    // is `'`-doubled. Both dangerous bytes exist ONLY inside those quotings.
    assert!(sql.contains(r#""tab"";drop""#), "{sql}");
    assert!(sql.contains(r#""id"";--""#), "{sql}");
    // The prefix is the RAW alias length (`al'ias` is 6 bytes); the literal
    // shows it `'`-doubled, and evaluates back to exactly those 6 bytes.
    assert!(sql.contains(r#"'6:al''ias:'"#), "{sql}");
    assert_eq!(sql.matches('"').count() % 2, 0, "unbalanced quoting: {sql}");

    // The proof that nothing escaped: the SQL parser reads this as EXACTLY ONE
    // statement (the lowerer rejects any other count), and re-renders the same
    // doubled identifier -- so `";drop` was one identifier, never a boundary.
    let statement =
        eg_query::sql::lower_graph_table_to_datafusion(&query, &record.accepted_definition, TENANT)
            .unwrap();
    let rendered = statement.to_string();
    assert!(rendered.contains(r#""tab"";drop""#), "{rendered}");
    assert!(
        !rendered.to_ascii_uppercase().contains("DROP TABLE"),
        "{rendered}"
    );
}

#[test]
fn a_table_cannot_be_renamed_onto_an_admitted_graph_name() {
    let (store, _path) = shop_store();
    store
        .create_table(&TableSchema::new("staging", vec![text("id", true)]), false)
        .unwrap();
    // Renaming a table ONTO the graph's name is the reverse of the fence, and
    // it is refused before the rename does any work.
    assert!(store
        .rename_table("staging", "shop")
        .unwrap_err()
        .contains("is a property graph"));
    assert!(store
        .list_tables()
        .unwrap()
        .contains(&"staging".to_string()));
    assert!(store
        .property_graph(TENANT, &name("shop"))
        .unwrap()
        .is_some());
    // A free name still works, proving the guard is not blanket.
    store.rename_table("staging", "staged").unwrap();
}

#[test]
fn drop_cascade_and_restrict_are_equivalent_for_a_leaf_graph() {
    for behavior in [DropBehavior::Restrict, DropBehavior::Cascade] {
        let (store, _path) = shop_store();
        assert_eq!(
            store
                .drop_property_graph(TENANT, &[name("shop")], false, behavior)
                .unwrap(),
            1
        );
        assert_eq!(store.property_graph(TENANT, &name("shop")).unwrap(), None);
        // Nothing depends on a property graph, so neither behaviour removed
        // anything else either.
        let mut tables = store.list_tables().unwrap();
        tables.sort();
        assert_eq!(tables, ["customer_orders", "customers", "orders"]);
    }
}

#[test]
fn a_composite_key_element_id_fails_closed_through_the_persisted_catalog() {
    let (store, _path) = open_store();
    store
        .create_table(
            &TableSchema::new("pairs", vec![text_col("left_id"), text_col("right_id")])
                .with_constraints(vec![TableConstraint::PrimaryKey {
                    name: Some("pairs_pk".into()),
                    columns: vec!["left_id".into(), "right_id".into()],
                }]),
            false,
        )
        .unwrap();
    store
        .create_property_graph(
            TENANT,
            &definition(
                "CREATE PROPERTY GRAPH pairs_graph VERTEX TABLES (\
                 pairs KEY (left_id, right_id) LABEL pair PROPERTIES (left_id))",
            ),
            OWNER,
        )
        .unwrap();
    let StatementKind::GraphTableReadRequiresCatalogAdmission(query) = classify(
        "SELECT * FROM GRAPH_TABLE (pairs_graph MATCH (p:pair) COLUMNS (ELEMENT_ID(p) AS eid))",
    )
    .unwrap() else {
        panic!("expected a GRAPH_TABLE read");
    };
    let record = store.property_graph(TENANT, &query.graph).unwrap().unwrap();
    let view = GraphCore::new().analysis_snapshot();
    assert!(exec_graph_table_typed_with_tables(
        &view,
        &store,
        &query,
        &record.accepted_definition,
        TENANT,
    )
    .unwrap_err()
    .contains("single-column element key"));
}

#[test]
fn negation_and_conjunction_select_the_right_element_tables_from_the_catalog() {
    let (store, _path) = open_store();
    for schema in [
        TableSchema::new("people", vec![text("person_id", true)]),
        TableSchema::new("places", vec![text("place_id", true)]),
    ] {
        store.create_table(&schema, false).unwrap();
    }
    store
        .insert_rows("people", &["person_id".into()], &[vec![json!("p1")]])
        .unwrap();
    store
        .insert_rows("places", &["place_id".into()], &[vec![json!("q1")]])
        .unwrap();
    store
        .create_property_graph(
            TENANT,
            &definition(
                "CREATE PROPERTY GRAPH labelled VERTEX TABLES (\
                 people KEY (person_id) LABEL person PROPERTIES (person_id) \
                 LABEL staff PROPERTIES (person_id), \
                 places KEY (place_id) LABEL place PROPERTIES (place_id))",
            ),
            OWNER,
        )
        .unwrap();

    // Which element tables each label expression actually selects, proven by the
    // rows returned -- not by the AST shape.
    for (pattern, expected) in [
        ("(v:person & staff)", vec![json!("6:people:p1")]),
        ("(v:!person)", vec![json!("6:places:q1")]),
        ("(v:!person & !place)", vec![]),
        (
            "(v:person | place)",
            vec![json!("6:people:p1"), json!("6:places:q1")],
        ),
    ] {
        let sql = format!(
            "SELECT * FROM GRAPH_TABLE (labelled MATCH {pattern} COLUMNS (ELEMENT_ID(v) AS eid))"
        );
        let StatementKind::GraphTableReadRequiresCatalogAdmission(query) = classify(&sql).unwrap()
        else {
            panic!("expected a GRAPH_TABLE read");
        };
        let record = store.property_graph(TENANT, &query.graph).unwrap().unwrap();
        let view = GraphCore::new().analysis_snapshot();
        let outcome = exec_graph_table_typed_with_tables(
            &view,
            &store,
            &query,
            &record.accepted_definition,
            TENANT,
        );
        if expected.is_empty() {
            assert!(
                outcome.unwrap_err().contains("matches no catalog element"),
                "{pattern} should select nothing"
            );
            continue;
        }
        let mut ids: Vec<_> = outcome
            .unwrap()
            .rows
            .iter()
            .map(|row| row[0].clone())
            .collect();
        ids.sort_by_key(|value| value.to_string());
        assert_eq!(ids, expected, "{pattern}");
    }
}

#[test]
fn element_id_stays_injective_when_an_alias_and_a_key_both_contain_the_separator() {
    let (store, _path) = open_store();
    // The adversarial pair for a bare `alias:key` join: alias `a` with key
    // `b:c`, and alias `a:b` with key `c`. Both render `a:b:c` unprefixed.
    for schema in [
        TableSchema::new("t_one", vec![text("k", true)]),
        TableSchema::new("t_two", vec![text("k", true)]),
    ] {
        store.create_table(&schema, false).unwrap();
    }
    store
        .insert_rows("t_one", &["k".into()], &[vec![json!("b:c")]])
        .unwrap();
    store
        .insert_rows("t_two", &["k".into()], &[vec![json!("c")]])
        .unwrap();
    store
        .create_property_graph(
            TENANT,
            &definition(concat!(
                r#"CREATE PROPERTY GRAPH sep VERTEX TABLES ("#,
                r#"t_one AS "a" KEY (k) LABEL one PROPERTIES (k), "#,
                r#"t_two AS "a:b" KEY (k) LABEL two PROPERTIES (k))"#
            )),
            OWNER,
        )
        .unwrap();

    let StatementKind::GraphTableReadRequiresCatalogAdmission(query) = classify(
        "SELECT * FROM GRAPH_TABLE (sep MATCH (v:one | two) COLUMNS (ELEMENT_ID(v) AS eid))",
    )
    .unwrap() else {
        panic!("expected a GRAPH_TABLE read");
    };
    let record = store.property_graph(TENANT, &query.graph).unwrap().unwrap();
    let view = GraphCore::new().analysis_snapshot();
    let result = exec_graph_table_typed_with_tables(
        &view,
        &store,
        &query,
        &record.accepted_definition,
        TENANT,
    )
    .unwrap();
    let mut ids: Vec<_> = result.rows.iter().map(|row| row[0].clone()).collect();
    ids.sort_by_key(|value| value.to_string());
    // Length-prefixed, so the split point is determined and the two differ.
    assert_eq!(ids, vec![json!("1:a:b:c"), json!("3:a:b:c")]);
    assert_ne!(ids[0], ids[1]);
}

#[test]
fn a_property_graph_op_whose_declared_scope_disagrees_with_its_definition_is_refused() {
    let (store, _path) = open_store();
    store
        .create_table(
            &TableSchema::new("people", vec![text("person_id", true)]),
            false,
        )
        .unwrap();
    let definition = definition(
        "CREATE PROPERTY GRAPH social VERTEX TABLES (\
         people KEY (person_id) LABEL person PROPERTIES (person_id))",
    );

    // Through the store API…
    assert!(store
        .create_property_graph("tenant/other", &definition, OWNER)
        .unwrap_err()
        .contains("does not match its definition"));

    // …and through the catalog transaction, where the op carries its own copy
    // of the verified scope: the two must agree or nothing is admitted.
    let mut txn = TableTxn::new();
    txn.push(TxnOp::PropertyGraphDdl(PropertyGraphTxnOp::Create {
        tenant_scope: "tenant/other".to_string(),
        definition: definition.clone(),
        owner: OWNER.to_string(),
    }));
    assert!(store
        .commit_txn(&txn)
        .unwrap_err()
        .contains("does not match its definition"));
    assert_eq!(store.property_graph(TENANT, &name("social")).unwrap(), None);
    assert_eq!(
        store
            .property_graph("tenant/other", &name("social"))
            .unwrap(),
        None
    );

    // The agreeing form still commits.
    let mut txn = TableTxn::new();
    txn.push(TxnOp::PropertyGraphDdl(PropertyGraphTxnOp::Create {
        tenant_scope: TENANT.to_string(),
        definition,
        owner: OWNER.to_string(),
    }));
    store.commit_txn(&txn).unwrap();
    assert!(store
        .property_graph(TENANT, &name("social"))
        .unwrap()
        .is_some());
}
