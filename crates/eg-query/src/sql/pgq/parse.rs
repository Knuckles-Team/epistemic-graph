//! SQL/PGQ definition and graph-pattern parsing over the bounded lexer.

use super::lex::{parse_name, Cursor};
use crate::tables::property_graph::{
    AlterElementAction, AlterPropertyGraphAction, DropBehavior, EdgeEndpoint, EdgeTableDefinition,
    ElementKeyResolution, ElementKind, EndpointResolution, GraphOwner, LabelDefinition,
    PropertyDefinition, PropertyGraphDefinition, PropertyGraphPrivilegeOperation,
    PropertyGraphPrivilegeStatement, PropertyGraphStatement, PropertySet, SqlIdentifier,
    VertexTableDefinition, MAX_PROPERTY_GRAPH_ELEMENTS, MAX_PROPERTY_GRAPH_KEY_COLUMNS,
    MAX_PROPERTY_GRAPH_LABELS_PER_ELEMENT, MAX_PROPERTY_GRAPH_PROPERTIES_PER_LABEL,
};

pub fn parse_property_graph_privilege(
    sql: &str,
) -> Result<PropertyGraphPrivilegeStatement, String> {
    let mut parser = Cursor::new(sql)?;
    let operation = if parser.keyword("GRANT") {
        PropertyGraphPrivilegeOperation::Grant
    } else if parser.keyword("REVOKE") {
        PropertyGraphPrivilegeOperation::Revoke
    } else {
        return Err("expected GRANT or REVOKE SELECT ON PROPERTY GRAPH".into());
    };
    parser.expect_keyword("SELECT")?;
    parser.expect_keyword("ON")?;
    parser.expect_keyword("PROPERTY")?;
    parser.expect_keyword("GRAPH")?;
    let name = parse_name(&mut parser)?;
    parser.expect_keyword(match operation {
        PropertyGraphPrivilegeOperation::Grant => "TO",
        PropertyGraphPrivilegeOperation::Revoke => "FROM",
    })?;
    let principal = parser.principal_identity()?;
    parser.finish()?;
    Ok(PropertyGraphPrivilegeStatement {
        operation,
        name,
        principal,
    })
}

pub fn parse_property_graph_ddl(
    sql: &str,
    tenant_scope: &str,
) -> Result<PropertyGraphStatement, String> {
    let mut parser = Cursor::new(sql)?;
    let statement = if parser.keyword("CREATE") {
        parse_create(&mut parser, tenant_scope)?
    } else if parser.keyword("ALTER") {
        parse_alter(&mut parser, tenant_scope)?
    } else if parser.keyword("DROP") {
        parse_drop_graph(&mut parser, tenant_scope)?
    } else {
        return Err("expected CREATE, ALTER, or DROP PROPERTY GRAPH".into());
    };
    parser.finish()?;
    statement.validate()?;
    Ok(statement)
}

#[derive(Clone, Copy)]
enum TableSectionOrder {
    None,
    Vertex,
    Edge,
}

fn parse_create(cursor: &mut Cursor, tenant: &str) -> Result<PropertyGraphStatement, String> {
    let temporary = cursor.keyword("TEMP") || cursor.keyword("TEMPORARY");
    cursor.expect_keyword("PROPERTY")?;
    cursor.expect_keyword("GRAPH")?;
    let name = parse_name(cursor)?;
    let (vertices, edges) = parse_table_sections(cursor)?;
    Ok(PropertyGraphStatement::Create(
        PropertyGraphDefinition::new(tenant, name, temporary, vertices, edges)?,
    ))
}

fn parse_alter(cursor: &mut Cursor, tenant: &str) -> Result<PropertyGraphStatement, String> {
    cursor.expect_keyword("PROPERTY")?;
    cursor.expect_keyword("GRAPH")?;
    let if_exists = if cursor.keyword("IF") {
        cursor.expect_keyword("EXISTS")?;
        true
    } else {
        false
    };
    let name = parse_name(cursor)?;
    let action = parse_alter_action(cursor)?;
    if if_exists && !matches!(&action, AlterPropertyGraphAction::SetSchema(_)) {
        return Err("IF EXISTS is only valid with ALTER PROPERTY GRAPH SET SCHEMA".into());
    }
    Ok(PropertyGraphStatement::Alter {
        tenant_scope: tenant.into(),
        name,
        if_exists,
        action,
    })
}

fn parse_alter_action(cursor: &mut Cursor) -> Result<AlterPropertyGraphAction, String> {
    if cursor.keyword("ADD") {
        let (vertex_tables, edge_tables) = parse_table_sections(cursor)?;
        return Ok(AlterPropertyGraphAction::Add {
            vertex_tables,
            edge_tables,
        });
    }
    if cursor.keyword("DROP") {
        let kind = parse_element_kind(cursor)?;
        cursor.expect_keyword("TABLES")?;
        return Ok(AlterPropertyGraphAction::DropTables {
            kind,
            aliases: parse_identifier_list(cursor, MAX_PROPERTY_GRAPH_ELEMENTS)?,
            behavior: parse_drop_behavior(cursor),
        });
    }
    if cursor.keyword("ALTER") {
        let kind = parse_element_kind(cursor)?;
        cursor.expect_keyword("TABLE")?;
        return Ok(AlterPropertyGraphAction::AlterElement {
            kind,
            alias: cursor.identifier()?,
            actions: parse_alter_element_actions(cursor)?,
        });
    }
    if cursor.keyword("OWNER") {
        cursor.expect_keyword("TO")?;
        return parse_alter_owner(cursor).map(AlterPropertyGraphAction::OwnerTo);
    }
    if cursor.keyword("RENAME") {
        cursor.expect_keyword("TO")?;
        return cursor.identifier().map(AlterPropertyGraphAction::RenameTo);
    }
    if cursor.keyword("SET") {
        cursor.expect_keyword("SCHEMA")?;
        cursor.identifier().map(AlterPropertyGraphAction::SetSchema)
    } else {
        Err("unsupported ALTER PROPERTY GRAPH action".into())
    }
}

fn parse_alter_owner(cursor: &mut Cursor) -> Result<GraphOwner, String> {
    if cursor.keyword("CURRENT_USER") {
        return Ok(GraphOwner::CurrentUser);
    }
    if cursor.keyword("SESSION_USER") {
        return Ok(GraphOwner::SessionUser);
    }
    cursor.identifier().map(GraphOwner::Role)
}

fn parse_drop_graph(cursor: &mut Cursor, tenant: &str) -> Result<PropertyGraphStatement, String> {
    cursor.expect_keyword("PROPERTY")?;
    cursor.expect_keyword("GRAPH")?;
    let if_exists = if cursor.keyword("IF") {
        cursor.expect_keyword("EXISTS")?;
        true
    } else {
        false
    };
    let mut names = Vec::new();
    loop {
        if names.len() == MAX_PROPERTY_GRAPH_ELEMENTS {
            return Err("too many graph names".into());
        }
        names.push(parse_name(cursor)?);
        if !cursor.symbol(',') {
            break;
        }
    }
    Ok(PropertyGraphStatement::Drop {
        tenant_scope: tenant.into(),
        names,
        if_exists,
        behavior: parse_drop_behavior(cursor),
    })
}

fn parse_table_sections(
    cursor: &mut Cursor,
) -> Result<(Vec<VertexTableDefinition>, Vec<EdgeTableDefinition>), String> {
    let (mut vertices, mut edges) = (Vec::new(), Vec::new());
    let mut order = TableSectionOrder::None;
    while cursor.any_keyword(&["VERTEX", "NODE", "EDGE", "RELATIONSHIP"]) {
        let kind = parse_element_kind(cursor)?;
        order = advance_table_section(order, kind)?;
        cursor.expect_keyword("TABLES")?;
        match kind {
            ElementKind::Vertex => vertices = parse_table_list(cursor, parse_vertex_table)?,
            ElementKind::Edge => edges = parse_table_list(cursor, parse_edge_table)?,
        }
    }
    if matches!(order, TableSectionOrder::None) {
        Err("property graph requires TABLES".into())
    } else {
        Ok((vertices, edges))
    }
}

fn parse_table_list<T>(
    cursor: &mut Cursor,
    parse: fn(&mut Cursor) -> Result<T, String>,
) -> Result<Vec<T>, String> {
    cursor.expect('(')?;
    let mut values = Vec::new();
    loop {
        values.push(parse(cursor)?);
        if !cursor.symbol(',') {
            break;
        }
    }
    cursor.expect(')')?;
    Ok(values)
}

fn parse_vertex_table(cursor: &mut Cursor) -> Result<VertexTableDefinition, String> {
    let relation = parse_name(cursor)?;
    let alias = if cursor.keyword("AS") {
        cursor.identifier()?
    } else {
        relation.leaf().clone()
    };
    let key_columns = if cursor.keyword("KEY") {
        parse_identifier_list(cursor, MAX_PROPERTY_GRAPH_KEY_COLUMNS)?
    } else {
        vec![]
    };
    let key_resolution = if key_columns.is_empty() {
        ElementKeyResolution::PrimaryKey
    } else {
        ElementKeyResolution::Explicit
    };
    let labels = parse_labels(cursor, &alias)?;
    Ok(VertexTableDefinition {
        relation,
        alias,
        key_columns,
        key_resolution,
        labels,
    })
}

fn parse_edge_table(cursor: &mut Cursor) -> Result<EdgeTableDefinition, String> {
    let relation = parse_name(cursor)?;
    let alias = if cursor.keyword("AS") {
        cursor.identifier()?
    } else {
        relation.leaf().clone()
    };
    let key_columns = if cursor.keyword("KEY") {
        parse_identifier_list(cursor, MAX_PROPERTY_GRAPH_KEY_COLUMNS)?
    } else {
        vec![]
    };
    let key_resolution = if key_columns.is_empty() {
        ElementKeyResolution::PrimaryKey
    } else {
        ElementKeyResolution::Explicit
    };
    cursor.expect_keyword("SOURCE")?;
    let source = parse_endpoint(cursor)?;
    cursor.expect_keyword("DESTINATION")?;
    let destination = parse_endpoint(cursor)?;
    let labels = parse_labels(cursor, &alias)?;
    Ok(EdgeTableDefinition {
        relation,
        alias,
        key_columns,
        key_resolution,
        source,
        destination,
        labels,
    })
}

fn parse_endpoint(cursor: &mut Cursor) -> Result<EdgeEndpoint, String> {
    let edge_key_columns = if cursor.keyword("KEY") {
        let value = parse_identifier_list(cursor, MAX_PROPERTY_GRAPH_KEY_COLUMNS)?;
        cursor.expect_keyword("REFERENCES")?;
        value
    } else {
        vec![]
    };
    let vertex_alias = cursor.identifier()?;
    let vertex_key_columns = if cursor.peek('(') {
        parse_identifier_list(cursor, MAX_PROPERTY_GRAPH_KEY_COLUMNS)?
    } else {
        vec![]
    };
    let resolution = if edge_key_columns.is_empty() {
        EndpointResolution::ForeignKey
    } else if vertex_key_columns.is_empty() {
        EndpointResolution::ExplicitEdgeCatalogVertexKey
    } else {
        EndpointResolution::Explicit
    };
    Ok(EdgeEndpoint {
        edge_key_columns,
        vertex_alias,
        vertex_key_columns,
        resolution,
    })
}

fn parse_labels(
    cursor: &mut Cursor,
    alias: &SqlIdentifier,
) -> Result<Vec<LabelDefinition>, String> {
    if cursor.any_keyword(&["NO", "PROPERTIES"]) {
        return Ok(vec![LabelDefinition {
            name: alias.clone(),
            properties: parse_property_set(cursor)?,
        }]);
    }
    let mut values = Vec::new();
    while cursor.any_keyword(&["LABEL", "DEFAULT"]) {
        if values.len() == MAX_PROPERTY_GRAPH_LABELS_PER_ELEMENT {
            return Err("too many labels".into());
        }
        let name = if cursor.keyword("DEFAULT") {
            cursor.expect_keyword("LABEL")?;
            alias.clone()
        } else {
            cursor.expect_keyword("LABEL")?;
            cursor.identifier()?
        };
        let properties = if cursor.any_keyword(&["NO", "PROPERTIES"]) {
            parse_property_set(cursor)?
        } else {
            PropertySet::AllColumns
        };
        values.push(LabelDefinition { name, properties });
    }
    if values.is_empty() {
        values.push(LabelDefinition {
            name: alias.clone(),
            properties: PropertySet::AllColumns,
        });
    }
    Ok(values)
}

fn parse_property_set(cursor: &mut Cursor) -> Result<PropertySet, String> {
    if cursor.keyword("NO") {
        cursor.expect_keyword("PROPERTIES")?;
        return Ok(PropertySet::None);
    }
    cursor.expect_keyword("PROPERTIES")?;
    if cursor.keyword("ALL") {
        cursor.expect_keyword("COLUMNS")?;
        return Ok(PropertySet::AllColumns);
    }
    cursor.expect('(')?;
    let mut values = Vec::new();
    loop {
        if values.len() == MAX_PROPERTY_GRAPH_PROPERTIES_PER_LABEL {
            return Err("too many properties".into());
        }
        let source_column = cursor.identifier()?;
        let property_name = if cursor.keyword("AS") {
            cursor.identifier()?
        } else {
            source_column.clone()
        };
        values.push(PropertyDefinition {
            source_column,
            property_name,
        });
        if !cursor.symbol(',') {
            break;
        }
    }
    cursor.expect(')')?;
    Ok(PropertySet::Explicit(values))
}

fn parse_alter_element_actions(cursor: &mut Cursor) -> Result<Vec<AlterElementAction>, String> {
    let mut actions = Vec::new();
    while let Some(action) = parse_alter_element_action(cursor)? {
        actions.push(action);
    }
    validate_parsed_alter_actions(&actions)?;
    Ok(actions)
}

fn parse_alter_element_action(cursor: &mut Cursor) -> Result<Option<AlterElementAction>, String> {
    if cursor.keyword("ADD") {
        return parse_add_label(cursor).map(Some);
    }
    if cursor.keyword("DROP") {
        return parse_drop_label(cursor).map(Some);
    }
    if cursor.keyword("ALTER") {
        return parse_alter_label(cursor).map(Some);
    }
    Ok(None)
}

fn parse_add_label(cursor: &mut Cursor) -> Result<AlterElementAction, String> {
    cursor.expect_keyword("LABEL")?;
    let name = cursor.identifier()?;
    let properties = if cursor.any_keyword(&["NO", "PROPERTIES"]) {
        parse_property_set(cursor)?
    } else {
        PropertySet::AllColumns
    };
    Ok(AlterElementAction::AddLabel(LabelDefinition {
        name,
        properties,
    }))
}

fn parse_drop_label(cursor: &mut Cursor) -> Result<AlterElementAction, String> {
    cursor.expect_keyword("LABEL")?;
    Ok(AlterElementAction::DropLabel {
        label: cursor.identifier()?,
        behavior: parse_drop_behavior(cursor),
    })
}

fn parse_alter_label(cursor: &mut Cursor) -> Result<AlterElementAction, String> {
    cursor.expect_keyword("LABEL")?;
    let label = cursor.identifier()?;
    if cursor.keyword("ADD") {
        cursor.expect_keyword("PROPERTIES")?;
        return Ok(AlterElementAction::AddProperties {
            label,
            properties: parse_property_body(cursor)?,
        });
    }
    if cursor.keyword("DROP") {
        cursor.expect_keyword("PROPERTIES")?;
        return Ok(AlterElementAction::DropProperties {
            label,
            properties: parse_identifier_list(cursor, MAX_PROPERTY_GRAPH_PROPERTIES_PER_LABEL)?,
            behavior: parse_drop_behavior(cursor),
        });
    }
    Err("expected ADD or DROP after ALTER LABEL".into())
}

fn parse_property_body(cursor: &mut Cursor) -> Result<Vec<PropertyDefinition>, String> {
    cursor.expect('(')?;
    let mut values = Vec::new();
    loop {
        if values.len() == MAX_PROPERTY_GRAPH_PROPERTIES_PER_LABEL {
            return Err("too many properties".into());
        }
        let source_column = cursor.identifier()?;
        let property_name = if cursor.keyword("AS") {
            cursor.identifier()?
        } else {
            source_column.clone()
        };
        values.push(PropertyDefinition {
            source_column,
            property_name,
        });
        if !cursor.symbol(',') {
            break;
        }
    }
    cursor.expect(')')?;
    Ok(values)
}

fn parse_element_kind(cursor: &mut Cursor) -> Result<ElementKind, String> {
    if cursor.keyword("VERTEX") || cursor.keyword("NODE") {
        Ok(ElementKind::Vertex)
    } else if cursor.keyword("EDGE") || cursor.keyword("RELATIONSHIP") {
        Ok(ElementKind::Edge)
    } else {
        Err("expected VERTEX/NODE or EDGE/RELATIONSHIP".into())
    }
}

fn parse_drop_behavior(cursor: &mut Cursor) -> DropBehavior {
    if cursor.keyword("CASCADE") {
        DropBehavior::Cascade
    } else {
        cursor.keyword("RESTRICT");
        DropBehavior::Restrict
    }
}

fn parse_identifier_list(cursor: &mut Cursor, max: usize) -> Result<Vec<SqlIdentifier>, String> {
    cursor.expect('(')?;
    let mut values = Vec::new();
    loop {
        if values.len() == max {
            return Err("identifier list exceeds bound".into());
        }
        values.push(cursor.identifier()?);
        if !cursor.symbol(',') {
            break;
        }
    }
    cursor.expect(')')?;
    Ok(values)
}

fn advance_table_section(
    order: TableSectionOrder,
    kind: ElementKind,
) -> Result<TableSectionOrder, String> {
    match (order, kind) {
        (TableSectionOrder::None, ElementKind::Vertex) => Ok(TableSectionOrder::Vertex),
        (TableSectionOrder::None | TableSectionOrder::Vertex, ElementKind::Edge) => {
            Ok(TableSectionOrder::Edge)
        }
        (TableSectionOrder::Vertex | TableSectionOrder::Edge, ElementKind::Vertex) => {
            Err("VERTEX/NODE TABLES must occur once and before EDGE TABLES".into())
        }
        (TableSectionOrder::Edge, ElementKind::Edge) => {
            Err("EDGE/RELATIONSHIP TABLES may occur only once".into())
        }
    }
}

fn validate_parsed_alter_actions(actions: &[AlterElementAction]) -> Result<(), String> {
    if actions.is_empty() {
        return Err("ALTER element requires a label action".into());
    }
    if actions.len() > 1
        && !actions
            .iter()
            .all(|action| matches!(action, AlterElementAction::AddLabel(_)))
    {
        return Err("only ADD LABEL may repeat in one ALTER element statement".into());
    }
    Ok(())
}
