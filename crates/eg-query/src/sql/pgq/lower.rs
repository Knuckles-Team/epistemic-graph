//! Relational lowering for bounded fixed SQL/PGQ graph patterns.

use std::collections::{BTreeMap, BTreeSet};

use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser as SqlParser;

use super::ast::*;
use super::lex::{SqlNumber, MAX_GRAPH_EXPR_DEPTH, MAX_GRAPH_TABLE_BRANCHES};
use crate::tables::property_graph::{
    EdgeEndpoint, EdgeTableDefinition, ElementKeyResolution, EndpointResolution,
    PropertyGraphDefinition, PropertySet, SqlIdentifier, SqlName, VertexTableDefinition,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalGraphPlan {
    branches: Vec<RelationalSelect>,
    output_columns: Vec<SqlIdentifier>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RelationalSelect {
    from: RelationRef,
    joins: Vec<RelationalJoin>,
    predicates: Vec<RelationalExpr>,
    projections: Vec<(RelationalExpr, SqlIdentifier)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RelationRef {
    relation: SqlName,
    alias: SqlIdentifier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RelationalJoin {
    relation: RelationRef,
    conditions: Vec<(ColumnRef, ColumnRef)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ColumnRef {
    relation_alias: SqlIdentifier,
    column: SqlIdentifier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RelationalExpr {
    Column(ColumnRef),
    String(String),
    Number(SqlNumber),
    Boolean(bool),
    Null,
    CurrentDate,
    Not(Box<RelationalExpr>),
    Binary(Box<RelationalExpr>, BinaryOp, Box<RelationalExpr>),
}

#[derive(Clone, Copy)]
enum Selected<'a> {
    Vertex(&'a VertexTableDefinition),
    Edge(&'a EdgeTableDefinition),
}

#[derive(Clone)]
struct Selection<'a> {
    vertices: Vec<&'a VertexTableDefinition>,
    edges: Vec<(&'a EdgeTableDefinition, EdgeDirection, bool)>,
}

type VariableBindings<'a> = BTreeMap<SqlIdentifier, (Selected<'a>, SqlIdentifier)>;

pub fn lower_graph_table(
    query: &GraphTableQuery,
    definition: &PropertyGraphDefinition,
    verified_tenant_scope: &str,
) -> Result<RelationalGraphPlan, String> {
    validate_lowering_authority(query, definition, verified_tenant_scope)?;
    let selections = select_catalog_paths(query, definition)?;
    let branches = selections
        .into_iter()
        .filter(connected)
        .map(|selection| lower_branch(query, &selection))
        .collect::<Result<Vec<_>, _>>()?;
    if branches.is_empty() {
        return Err("GRAPH_TABLE pattern has no catalog-consistent path".into());
    }
    Ok(RelationalGraphPlan {
        branches,
        output_columns: query
            .columns
            .iter()
            .map(|column| column.alias.clone())
            .collect(),
    })
}

fn validate_lowering_authority(
    query: &GraphTableQuery,
    definition: &PropertyGraphDefinition,
    verified_tenant_scope: &str,
) -> Result<(), String> {
    definition.validate()?;
    if definition.tenant_scope != verified_tenant_scope {
        return Err("property graph tenant scope does not match verified authority".into());
    }
    if definition.name != query.graph {
        return Err("GRAPH_TABLE name does not match the catalog definition".into());
    }
    if has_unresolved_catalog_requirements(definition) {
        return Err("property graph has unresolved key/property catalog requirements".into());
    }
    Ok(())
}

fn has_unresolved_catalog_requirements(definition: &PropertyGraphDefinition) -> bool {
    definition
        .vertex_tables
        .iter()
        .any(|table| table.key_resolution != ElementKeyResolution::Explicit)
        || definition.edge_tables.iter().any(|table| {
            table.key_resolution != ElementKeyResolution::Explicit
                || table.source.resolution != EndpointResolution::Explicit
                || table.destination.resolution != EndpointResolution::Explicit
        })
        || definition
            .vertex_tables
            .iter()
            .flat_map(|table| &table.labels)
            .chain(
                definition
                    .edge_tables
                    .iter()
                    .flat_map(|table| &table.labels),
            )
            .any(|label| matches!(label.properties, PropertySet::AllColumns))
}

fn select_catalog_paths<'a>(
    query: &GraphTableQuery,
    definition: &'a PropertyGraphDefinition,
) -> Result<Vec<Selection<'a>>, String> {
    let vertices = std::iter::once(&query.path.first)
        .chain(query.path.steps.iter().map(|step| &step.1))
        .map(|pattern| vertex_candidates(pattern, definition))
        .collect::<Result<Vec<_>, _>>()?;
    let edges = query
        .path
        .steps
        .iter()
        .map(|step| edge_candidates(&step.0.element, definition))
        .collect::<Result<Vec<_>, _>>()?;
    let mut selections = vec![Selection {
        vertices: vec![],
        edges: vec![],
    }];
    for (index, candidates) in vertices.iter().enumerate() {
        selections = expand_vertices(selections, candidates)?;
        if index < edges.len() {
            selections = expand_edges(
                selections,
                &edges[index],
                query.path.steps[index].0.direction,
            )?;
        }
    }
    Ok(selections)
}

pub fn lower_graph_table_to_datafusion(
    query: &GraphTableQuery,
    definition: &PropertyGraphDefinition,
    verified_tenant_scope: &str,
) -> Result<Statement, String> {
    let sql = lower_graph_table(query, definition, verified_tenant_scope)?.to_sql();
    let mut statements = SqlParser::parse_sql(&PostgreSqlDialect {}, &sql)
        .map_err(|error| format!("DataFusion rejected lowered SQL/PGQ: {error}"))?;
    if statements.len() != 1 {
        return Err("lowered SQL/PGQ did not produce exactly one statement".into());
    }
    Ok(statements.remove(0))
}

impl RelationalGraphPlan {
    pub fn branch_count(&self) -> usize {
        self.branches.len()
    }

    pub fn output_columns(&self) -> &[SqlIdentifier] {
        &self.output_columns
    }

    pub fn to_sql(&self) -> String {
        self.branches
            .iter()
            .map(RelationalSelect::to_sql)
            .collect::<Vec<_>>()
            .join(" UNION ALL ")
    }
}

impl RelationalSelect {
    fn to_sql(&self) -> String {
        let projection = self
            .projections
            .iter()
            .map(|(expr, alias)| format!("{} AS {}", expr.to_sql(), alias.quoted_sql()))
            .collect::<Vec<_>>()
            .join(", ");
        let mut sql = format!(
            "SELECT {projection} FROM {} AS {}",
            self.from.relation.quoted_sql(),
            self.from.alias.quoted_sql()
        );
        for join in &self.joins {
            let on = join
                .conditions
                .iter()
                .map(|(a, b)| format!("{} = {}", a.to_sql(), b.to_sql()))
                .collect::<Vec<_>>()
                .join(" AND ");
            sql.push_str(&format!(
                " JOIN {} AS {} ON {on}",
                join.relation.relation.quoted_sql(),
                join.relation.alias.quoted_sql()
            ));
        }
        if !self.predicates.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(
                &self
                    .predicates
                    .iter()
                    .map(RelationalExpr::to_sql)
                    .collect::<Vec<_>>()
                    .join(" AND "),
            );
        }
        sql
    }
}

impl ColumnRef {
    fn to_sql(&self) -> String {
        format!(
            "{}.{}",
            self.relation_alias.quoted_sql(),
            self.column.quoted_sql()
        )
    }
}

impl RelationalExpr {
    fn to_sql(&self) -> String {
        match self {
            Self::Column(value) => value.to_sql(),
            Self::String(value) => format!("'{}'", value.replace('\'', "''")),
            Self::Number(value) => value.sql().into(),
            Self::Boolean(true) => "TRUE".into(),
            Self::Boolean(false) => "FALSE".into(),
            Self::Null => "NULL".into(),
            Self::CurrentDate => "CURRENT_DATE".into(),
            Self::Not(value) => format!("NOT ({})", value.to_sql()),
            Self::Binary(a, op, b) => format!("({} {} {})", a.to_sql(), op.sql(), b.to_sql()),
        }
    }
}

impl BinaryOp {
    fn sql(self) -> &'static str {
        const SPELLINGS: [&str; 8] = ["=", "<>", "<", "<=", ">", ">=", "AND", "OR"];
        SPELLINGS[self as usize]
    }
}

fn expand_vertices<'a>(
    prior: Vec<Selection<'a>>,
    candidates: &[&'a VertexTableDefinition],
) -> Result<Vec<Selection<'a>>, String> {
    let mut out = Vec::new();
    for selection in prior {
        for candidate in candidates {
            let mut next = selection.clone();
            next.vertices.push(*candidate);
            out.push(next);
        }
    }
    bound_branches(out)
}

fn expand_edges<'a>(
    prior: Vec<Selection<'a>>,
    candidates: &[&'a EdgeTableDefinition],
    direction: EdgeDirection,
) -> Result<Vec<Selection<'a>>, String> {
    let directions: &[EdgeDirection] = match direction {
        EdgeDirection::Either => &[EdgeDirection::Outgoing, EdgeDirection::Incoming],
        EdgeDirection::Outgoing => &[EdgeDirection::Outgoing],
        EdgeDirection::Incoming => &[EdgeDirection::Incoming],
    };
    let mut out = Vec::new();
    for selection in prior {
        for candidate in candidates {
            for direction in directions {
                let mut next = selection.clone();
                next.edges.push((
                    *candidate,
                    *direction,
                    direction == &EdgeDirection::Incoming && directions.len() == 2,
                ));
                out.push(next);
            }
        }
    }
    bound_branches(out)
}

fn bound_branches<T>(values: Vec<T>) -> Result<Vec<T>, String> {
    if values.len() > MAX_GRAPH_TABLE_BRANCHES {
        Err(format!(
            "GRAPH_TABLE expansion exceeds {MAX_GRAPH_TABLE_BRANCHES} branches"
        ))
    } else {
        Ok(values)
    }
}

fn connected(selection: &Selection<'_>) -> bool {
    selection
        .edges
        .iter()
        .enumerate()
        .all(|(index, (edge, direction, _))| {
            let left = selection.vertices[index];
            let right = selection.vertices[index + 1];
            match direction {
                EdgeDirection::Outgoing => {
                    edge.source.vertex_alias == left.alias
                        && edge.destination.vertex_alias == right.alias
                }
                EdgeDirection::Incoming => {
                    edge.destination.vertex_alias == left.alias
                        && edge.source.vertex_alias == right.alias
                }
                EdgeDirection::Either => false,
            }
        })
}

fn lower_branch(
    query: &GraphTableQuery,
    selected: &Selection<'_>,
) -> Result<RelationalSelect, String> {
    let va = generated_aliases("v", selected.vertices.len())?;
    let ea = generated_aliases("e", selected.edges.len())?;
    let variables = bind_variables(query, selected, &va, &ea)?;
    let joins = lower_joins(selected, &va, &ea)?;
    let predicates = lower_predicates(query, selected, &variables, &ea)?;
    let projections = lower_projections(query, &variables)?;
    Ok(RelationalSelect {
        from: RelationRef {
            relation: selected.vertices[0].relation.clone(),
            alias: va[0].clone(),
        },
        joins,
        predicates,
        projections,
    })
}

fn generated_aliases(kind: &str, count: usize) -> Result<Vec<SqlIdentifier>, String> {
    (0..count)
        .map(|index| generated_alias(kind, index))
        .collect()
}

fn bind_variables<'a>(
    query: &GraphTableQuery,
    selected: &Selection<'a>,
    vertex_aliases: &[SqlIdentifier],
    edge_aliases: &[SqlIdentifier],
) -> Result<VariableBindings<'a>, String> {
    let mut variables = BTreeMap::new();
    bind(
        &mut variables,
        query.path.first.variable.as_ref(),
        Selected::Vertex(selected.vertices[0]),
        &vertex_aliases[0],
    )?;
    for (i, (edge_pattern, vertex_pattern)) in query.path.steps.iter().enumerate() {
        bind(
            &mut variables,
            edge_pattern.element.variable.as_ref(),
            Selected::Edge(selected.edges[i].0),
            &edge_aliases[i],
        )?;
        bind(
            &mut variables,
            vertex_pattern.variable.as_ref(),
            Selected::Vertex(selected.vertices[i + 1]),
            &vertex_aliases[i + 1],
        )?;
    }
    Ok(variables)
}

fn lower_joins(
    selected: &Selection<'_>,
    vertex_aliases: &[SqlIdentifier],
    edge_aliases: &[SqlIdentifier],
) -> Result<Vec<RelationalJoin>, String> {
    let mut joins = Vec::new();
    for (i, (edge, direction, _)) in selected.edges.iter().enumerate() {
        let (left, right) = match direction {
            EdgeDirection::Outgoing => (&edge.source, &edge.destination),
            EdgeDirection::Incoming => (&edge.destination, &edge.source),
            EdgeDirection::Either => unreachable!(),
        };
        joins.push(RelationalJoin {
            relation: RelationRef {
                relation: edge.relation.clone(),
                alias: edge_aliases[i].clone(),
            },
            conditions: endpoint_conditions(
                edge,
                left,
                &edge_aliases[i],
                selected.vertices[i],
                &vertex_aliases[i],
            )?,
        });
        joins.push(RelationalJoin {
            relation: RelationRef {
                relation: selected.vertices[i + 1].relation.clone(),
                alias: vertex_aliases[i + 1].clone(),
            },
            conditions: endpoint_conditions(
                edge,
                right,
                &edge_aliases[i],
                selected.vertices[i + 1],
                &vertex_aliases[i + 1],
            )?,
        });
    }
    Ok(joins)
}

fn lower_predicates(
    query: &GraphTableQuery,
    selected: &Selection<'_>,
    variables: &VariableBindings<'_>,
    edge_aliases: &[SqlIdentifier],
) -> Result<Vec<RelationalExpr>, String> {
    let elements = std::iter::once(&query.path.first).chain(
        query
            .path
            .steps
            .iter()
            .flat_map(|(edge, vertex)| [&edge.element, vertex]),
    );
    let mut predicates = elements
        .filter_map(|element| element.predicate.as_ref())
        .map(|expr| lower_expr(expr, &variables, 0))
        .collect::<Result<Vec<_>, _>>()?;
    for (index, (edge, _, exclude_self_loop)) in selected.edges.iter().enumerate() {
        if *exclude_self_loop && edge.source.vertex_alias == edge.destination.vertex_alias {
            predicates.push(non_self_loop_predicate(edge, &edge_aliases[index])?);
        }
    }
    Ok(predicates)
}

fn lower_projections(
    query: &GraphTableQuery,
    variables: &VariableBindings<'_>,
) -> Result<Vec<(RelationalExpr, SqlIdentifier)>, String> {
    query
        .columns
        .iter()
        .map(|column| {
            Ok((
                lower_expr(&column.expression, &variables, 0)?,
                column.alias.clone(),
            ))
        })
        .collect()
}

fn non_self_loop_predicate(
    edge: &EdgeTableDefinition,
    edge_alias: &SqlIdentifier,
) -> Result<RelationalExpr, String> {
    if edge.source.edge_key_columns.is_empty()
        || edge.source.edge_key_columns.len() != edge.destination.edge_key_columns.len()
    {
        return Err(format!(
            "undirected edge '{}' requires equal-width explicit endpoint keys",
            edge.alias.value()
        ));
    }
    let equalities = edge
        .source
        .edge_key_columns
        .iter()
        .zip(&edge.destination.edge_key_columns)
        .map(|(source, destination)| {
            RelationalExpr::Binary(
                Box::new(RelationalExpr::Column(ColumnRef {
                    relation_alias: edge_alias.clone(),
                    column: source.clone(),
                })),
                BinaryOp::Eq,
                Box::new(RelationalExpr::Column(ColumnRef {
                    relation_alias: edge_alias.clone(),
                    column: destination.clone(),
                })),
            )
        })
        .reduce(|left, right| {
            RelationalExpr::Binary(Box::new(left), BinaryOp::And, Box::new(right))
        })
        .expect("non-empty endpoint key");
    Ok(RelationalExpr::Not(Box::new(equalities)))
}

fn bind<'a>(
    vars: &mut VariableBindings<'a>,
    variable: Option<&SqlIdentifier>,
    element: Selected<'a>,
    alias: &SqlIdentifier,
) -> Result<(), String> {
    if let Some(variable) = variable {
        if vars
            .insert(variable.clone(), (element, alias.clone()))
            .is_some()
        {
            return Err(format!("duplicate graph variable '{}'", variable.value()));
        }
    }
    Ok(())
}

fn endpoint_conditions(
    edge: &EdgeTableDefinition,
    endpoint: &EdgeEndpoint,
    edge_alias: &SqlIdentifier,
    vertex: &VertexTableDefinition,
    vertex_alias: &SqlIdentifier,
) -> Result<Vec<(ColumnRef, ColumnRef)>, String> {
    if endpoint.edge_key_columns.is_empty() {
        return Err(format!(
            "edge '{}' needs explicit endpoint keys",
            edge.alias.value()
        ));
    }
    let vertex_columns = if endpoint.vertex_key_columns.is_empty() {
        &vertex.key_columns
    } else {
        &endpoint.vertex_key_columns
    };
    if vertex_columns.is_empty() || vertex_columns.len() != endpoint.edge_key_columns.len() {
        return Err(format!(
            "edge '{}' has unresolved endpoint key width",
            edge.alias.value()
        ));
    }
    Ok(endpoint
        .edge_key_columns
        .iter()
        .zip(vertex_columns)
        .map(|(a, b)| {
            (
                ColumnRef {
                    relation_alias: edge_alias.clone(),
                    column: a.clone(),
                },
                ColumnRef {
                    relation_alias: vertex_alias.clone(),
                    column: b.clone(),
                },
            )
        })
        .collect())
}

fn lower_expr(
    expr: &GraphExpr,
    vars: &VariableBindings<'_>,
    depth: usize,
) -> Result<RelationalExpr, String> {
    if depth > MAX_GRAPH_EXPR_DEPTH {
        return Err("graph expression depth limit exceeded".into());
    }
    Ok(match expr {
        GraphExpr::Property(variable, property) => {
            let (element, alias) = vars
                .get(variable)
                .ok_or_else(|| format!("unknown graph variable '{}'", variable.value()))?;
            RelationalExpr::Column(ColumnRef {
                relation_alias: alias.clone(),
                column: resolve_property(*element, property)?,
            })
        }
        GraphExpr::String(v) => RelationalExpr::String(v.clone()),
        GraphExpr::Number(v) => RelationalExpr::Number(v.clone()),
        GraphExpr::Boolean(v) => RelationalExpr::Boolean(*v),
        GraphExpr::Null => RelationalExpr::Null,
        GraphExpr::CurrentDate => RelationalExpr::CurrentDate,
        GraphExpr::Not(v) => RelationalExpr::Not(Box::new(lower_expr(v, vars, depth + 1)?)),
        GraphExpr::Binary(a, op, b) => RelationalExpr::Binary(
            Box::new(lower_expr(a, vars, depth + 1)?),
            *op,
            Box::new(lower_expr(b, vars, depth + 1)?),
        ),
    })
}

fn resolve_property(
    element: Selected<'_>,
    property: &SqlIdentifier,
) -> Result<SqlIdentifier, String> {
    let (alias, labels) = match element {
        Selected::Vertex(table) => (&table.alias, &table.labels),
        Selected::Edge(table) => (&table.alias, &table.labels),
    };
    let mut matches = BTreeSet::new();
    for label in labels {
        match &label.properties {
            PropertySet::None => {}
            PropertySet::AllColumns => {
                matches.insert(property.clone());
            }
            PropertySet::Explicit(properties) => {
                for item in properties {
                    if &item.property_name == property {
                        matches.insert(item.source_column.clone());
                    }
                }
            }
        }
    }
    if matches.len() != 1 {
        return Err(format!(
            "property '{}' on '{}' resolves to {} columns",
            property.value(),
            alias.value(),
            matches.len()
        ));
    }
    Ok(matches.into_iter().next().expect("one property"))
}

fn vertex_candidates<'a>(
    pattern: &ElementPattern,
    definition: &'a PropertyGraphDefinition,
) -> Result<Vec<&'a VertexTableDefinition>, String> {
    for label in &pattern.labels {
        if !definition
            .vertex_tables
            .iter()
            .any(|table| vertex_has_label(table, label))
        {
            return Err(format!("unknown vertex label: {}", label.value()));
        }
    }
    let values = definition
        .vertex_tables
        .iter()
        .filter(|table| vertex_matches_labels(table, &pattern.labels))
        .collect::<Vec<_>>();
    if values.is_empty() {
        Err("vertex pattern matches no catalog element".into())
    } else {
        Ok(values)
    }
}

fn vertex_has_label(table: &VertexTableDefinition, label: &SqlIdentifier) -> bool {
    table.labels.iter().any(|item| &item.name == label)
}

fn vertex_matches_labels(table: &VertexTableDefinition, labels: &[SqlIdentifier]) -> bool {
    labels.is_empty() || labels.iter().any(|label| vertex_has_label(table, label))
}

fn edge_candidates<'a>(
    pattern: &ElementPattern,
    definition: &'a PropertyGraphDefinition,
) -> Result<Vec<&'a EdgeTableDefinition>, String> {
    for label in &pattern.labels {
        if !definition
            .edge_tables
            .iter()
            .any(|table| edge_has_label(table, label))
        {
            return Err(format!("unknown edge label: {}", label.value()));
        }
    }
    let values = definition
        .edge_tables
        .iter()
        .filter(|table| edge_matches_labels(table, &pattern.labels))
        .collect::<Vec<_>>();
    if values.is_empty() {
        Err("edge pattern matches no catalog element".into())
    } else {
        Ok(values)
    }
}

fn edge_has_label(table: &EdgeTableDefinition, label: &SqlIdentifier) -> bool {
    table.labels.iter().any(|item| &item.name == label)
}

fn edge_matches_labels(table: &EdgeTableDefinition, labels: &[SqlIdentifier]) -> bool {
    labels.is_empty() || labels.iter().any(|label| edge_has_label(table, label))
}

fn generated_alias(kind: &str, index: usize) -> Result<SqlIdentifier, String> {
    SqlIdentifier::unquoted(format!("_pgq_{kind}{index}"))
}
