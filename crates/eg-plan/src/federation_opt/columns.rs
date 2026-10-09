//! Column-carrying foreign results and conservative column pushdown (EH-572).
//!
//! A source may report an exact, inexact, or unsupported filter. All filters are
//! evaluated again over the returned rows: an inexact remote predicate may return
//! a superset. Connector pushdown must never omit a matching row; the residual
//! restores the exact local comparison semantics. Projection retains every column
//! the residual needs and only removes
//! those columns after filtering.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use serde_json::Value;

use crate::rowset::RowSet;

/// A foreign row retains only columns exposed by the source's registered mapping.
#[derive(Clone, Debug, PartialEq)]
pub struct ForeignRow {
    pub id: String,
    pub score: Option<f32>,
    pub columns: BTreeMap<String, Value>,
}

/// Ordered foreign rows. Like `RowSet`, the first row with a given id wins.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ForeignRows {
    rows: Vec<ForeignRow>,
}

impl ForeignRows {
    /// Bridge a legacy id/score-only source into the column currency. Its column
    /// mapping is empty, so a column plan cannot accidentally read hidden data.
    pub fn from_rowset(rows: RowSet) -> Self {
        Self::from_rows(rows.rows().iter().map(|row| ForeignRow {
            id: row.id.clone(),
            score: row.score,
            columns: BTreeMap::new(),
        }))
    }

    pub fn from_rows(rows: impl IntoIterator<Item = ForeignRow>) -> Self {
        let mut seen = HashSet::new();
        Self {
            rows: rows
                .into_iter()
                .filter(|row| seen.insert(row.id.clone()))
                .collect(),
        }
    }

    pub fn rows(&self) -> &[ForeignRow] {
        &self.rows
    }

    /// Cross the existing id/score algebra boundary after column residuals run.
    pub fn into_rowset(self) -> RowSet {
        RowSet::from_rows(self.rows.into_iter().map(|row| (row.id, row.score)))
    }

    /// Re-evaluate all predicates locally, including those the source called exact.
    /// An absent column is not equal to JSON null and never satisfies a comparison.
    pub fn filter(mut self, predicates: &[ColumnPredicate]) -> Self {
        self.rows
            .retain(|row| predicates.iter().all(|pred| pred.matches(&row.columns)));
        self
    }

    /// Remove all columns except those requested by the final caller. This must run
    /// after `filter`, since residual predicates may need unreturned columns.
    pub fn project(mut self, columns: &[String]) -> Self {
        let keep: BTreeSet<&str> = columns.iter().map(String::as_str).collect();
        for row in &mut self.rows {
            row.columns.retain(|name, _| keep.contains(name.as_str()));
        }
        self
    }
}

/// The source's claim about a pushed predicate. `Inexact` may return extra rows;
/// `Unsupported` must not be sent to the source at all.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushdownSupport {
    Exact,
    Inexact,
    Unsupported,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Comparison {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// A comparison over a column exposed by a registered source mapping. Values use
/// JSON scalar semantics: numbers compare numerically and strings lexically;
/// different types and non-scalars do not satisfy ordered comparisons.
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnPredicate {
    pub column: String,
    pub comparison: Comparison,
    pub value: Value,
}

impl ColumnPredicate {
    pub fn matches(&self, columns: &BTreeMap<String, Value>) -> bool {
        let Some(value) = columns.get(&self.column) else {
            return false;
        };
        let order = match (value, &self.value) {
            (Value::Number(left), Value::Number(right)) => left
                .as_f64()
                .zip(right.as_f64())
                .and_then(|(l, r)| l.partial_cmp(&r)),
            (Value::String(left), Value::String(right)) => Some(left.cmp(right)),
            _ => None,
        };
        match self.comparison {
            Comparison::Eq => value == &self.value,
            Comparison::Ne => value != &self.value,
            Comparison::Lt => order == Some(std::cmp::Ordering::Less),
            Comparison::Le => matches!(
                order,
                Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
            ),
            Comparison::Gt => order == Some(std::cmp::Ordering::Greater),
            Comparison::Ge => matches!(
                order,
                Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
            ),
        }
    }
}

/// The request sent to a column-capable connector and the exact local work left
/// after it. A connector's advertised support cannot expose an unregistered column.
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnPlan {
    /// Columns the connector must return so the local residual can run.
    pub remote_projection: Vec<String>,
    /// Only filters certified as exact/inexact by the connector.
    pub remote_filters: Vec<ColumnPredicate>,
    /// All filters remain here, regardless of the connector's claim.
    pub residual_filters: Vec<ColumnPredicate>,
    /// Columns to expose after the residual runs.
    pub output_projection: Vec<String>,
}

impl ColumnPlan {
    /// Plan a narrowing request. `exposed` is the mapping-approved column set;
    /// an unknown projected or filtered column fails closed before any fetch.
    pub fn new(
        exposed: &BTreeSet<String>,
        output: &[String],
        predicates: &[ColumnPredicate],
        projection_supported: bool,
        support: impl Fn(&ColumnPredicate) -> PushdownSupport,
    ) -> Result<Self, String> {
        let required: BTreeSet<String> = output
            .iter()
            .cloned()
            .chain(predicates.iter().map(|pred| pred.column.clone()))
            .collect();
        if let Some(name) = required.iter().find(|name| !exposed.contains(*name)) {
            return Err(format!(
                "federation: column '{name}' is not exposed by this source"
            ));
        }
        let remote_projection = if projection_supported {
            required.into_iter().collect()
        } else {
            exposed.iter().cloned().collect()
        };
        let remote_filters = predicates
            .iter()
            .filter(|pred| support(pred) != PushdownSupport::Unsupported)
            .cloned()
            .collect();
        Ok(Self {
            remote_projection,
            remote_filters,
            residual_filters: predicates.to_vec(),
            output_projection: output.to_vec(),
        })
    }

    /// Complete a connector result. `rows` may include extra rows for any inexact
    /// remote filter; the local residual restores exact result equivalence.
    pub fn finish(&self, rows: ForeignRows) -> ForeignRows {
        rows.filter(&self.residual_filters)
            .project(&self.output_projection)
    }
}

/// SQL's column capability is deliberately narrow: a string equality over a
/// selected column may be evaluated remotely as a superset (collation can make
/// it broader), and the local lexical residual still decides exact membership.
/// Numeric and ordered comparisons need typed SQL column decoding first.
pub fn sql_filter_support(predicate: &ColumnPredicate) -> PushdownSupport {
    if predicate.comparison == Comparison::Eq && predicate.value.is_string() {
        PushdownSupport::Inexact
    } else {
        PushdownSupport::Unsupported
    }
}

/// Wrap an already registered read-only SQL statement in a column projection and
/// conservative string-equality predicates. The SQL source re-validates the
/// entire composed statement and DSN before opening a connection. Fields are
/// CAST to lexical text because the existing bounded column reader decodes text.
#[cfg(feature = "federation-sql")]
pub(crate) fn render_sql_columns(
    base: &str,
    id_field: &str,
    score_field: Option<&str>,
    plan: &ColumnPlan,
    dialect: crate::sql_text::SqlDialect,
) -> Result<String, String> {
    use crate::sql_text::{quote_identifier, render_literal, SqlDialect};

    let mut fields: BTreeSet<String> = plan.remote_projection.iter().cloned().collect();
    fields.insert(id_field.to_string());
    if let Some(score) = score_field {
        fields.insert(score.to_string());
    }
    let cast_type = match dialect {
        SqlDialect::Postgres => "TEXT",
        SqlDialect::MySql => "CHAR",
    };
    let projection = fields
        .iter()
        .map(|field| {
            let quoted = quote_identifier(field, dialect)?;
            Ok(format!("CAST(eg_fed.{quoted} AS {cast_type}) AS {quoted}"))
        })
        .collect::<Result<Vec<_>, String>>()?
        .join(", ");
    let predicates = plan
        .remote_filters
        .iter()
        .filter(|pred| sql_filter_support(pred) != PushdownSupport::Unsupported)
        .map(|pred| {
            let quoted = quote_identifier(&pred.column, dialect)?;
            let value = render_literal(pred.value.as_str().unwrap_or_default(), false, dialect)?;
            Ok(format!("CAST(eg_fed.{quoted} AS {cast_type}) = {value}"))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let body = base.trim_end().trim_end_matches(';').trim_end();
    let mut statement = format!("SELECT {projection} FROM ({body}) AS eg_fed");
    if !predicates.is_empty() {
        statement.push_str(" WHERE ");
        statement.push_str(&predicates.join(" AND "));
    }
    Ok(statement)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rows() -> ForeignRows {
        ForeignRows::from_rows([
            ForeignRow {
                id: "a".into(),
                score: Some(0.5),
                columns: BTreeMap::from([("age".into(), json!(19)), ("name".into(), json!("Ada"))]),
            },
            ForeignRow {
                id: "b".into(),
                score: None,
                columns: BTreeMap::from([("age".into(), json!(42)), ("name".into(), json!("Bea"))]),
            },
            ForeignRow {
                id: "c".into(),
                score: None,
                columns: BTreeMap::from([("age".into(), json!(55)), ("name".into(), json!("Cam"))]),
            },
        ])
    }

    #[test]
    fn inexact_remote_superset_matches_naive_residual() {
        let exposed = BTreeSet::from(["age".into(), "name".into()]);
        let pred = ColumnPredicate {
            column: "age".into(),
            comparison: Comparison::Ge,
            value: json!(40),
        };
        let plan = ColumnPlan::new(&exposed, &["name".into()], &[pred.clone()], true, |_| {
            PushdownSupport::Inexact
        })
        .unwrap();
        assert_eq!(plan.remote_projection, vec!["age", "name"]);
        assert_eq!(plan.remote_filters, vec![pred]);
        let naive = plan.finish(rows());
        let remote_superset =
            ForeignRows::from_rows(rows().rows().iter().filter(|r| r.id != "a").cloned());
        assert_eq!(plan.finish(remote_superset), naive);
        assert_eq!(naive.into_rowset().ids(), vec!["b", "c"]);
    }

    #[test]
    fn unsupported_filter_stays_local_and_unexposed_column_fails() {
        let exposed = BTreeSet::from(["age".into(), "name".into()]);
        let pred = ColumnPredicate {
            column: "age".into(),
            comparison: Comparison::Lt,
            value: json!(40),
        };
        let plan = ColumnPlan::new(&exposed, &["name".into()], &[pred], true, |_| {
            PushdownSupport::Unsupported
        })
        .unwrap();
        assert!(plan.remote_filters.is_empty());
        assert_eq!(plan.finish(rows()).into_rowset().ids(), vec!["a"]);
        assert!(
            ColumnPlan::new(&exposed, &["secret".into()], &[], true, |_| {
                PushdownSupport::Exact
            })
            .is_err()
        );
    }

    #[test]
    fn missing_column_does_not_match_null_or_ordered_predicate() {
        let absent = BTreeMap::new();
        for comparison in [Comparison::Eq, Comparison::Lt, Comparison::Ge] {
            let pred = ColumnPredicate {
                column: "missing".into(),
                comparison,
                value: Value::Null,
            };
            assert!(!pred.matches(&absent));
        }
    }

    #[test]
    fn named_id_only_source_has_no_column_access() {
        let mut registry = crate::federation::ForeignSourceRegistry::new();
        registry.register_table("legacy", [("a".to_string(), Some(0.5))]);
        let rows = registry.resolve_columns("legacy").unwrap();
        assert_eq!(rows.rows().len(), 1);
        assert!(rows.rows()[0].columns.is_empty());
        assert_eq!(rows.into_rowset().ids(), vec!["a"]);
        assert!(registry.resolve_columns("unbound").is_err());
    }

    #[test]
    fn sql_column_mapping_is_explicit_and_legacy_serde_is_empty() {
        let json = json!({"Sql": {
            "dsn": "postgres://db/records",
            "query": "SELECT id, name FROM records",
            "id_field": "id",
            "score_field": null
        }});
        let legacy: eg_types::wire::ForeignSourceSpec = serde_json::from_value(json).unwrap();
        let eg_types::wire::ForeignSourceSpec::Sql { columns, .. } = &legacy else {
            panic!("expected SQL spec");
        };
        assert!(columns.is_empty());
        crate::federation::validate_column_mapping(&legacy).unwrap();

        let mapped = eg_types::wire::ForeignSourceSpec::Sql {
            dsn: "postgres://db/records".into(),
            query: "SELECT id, name FROM records".into(),
            id_field: "id".into(),
            score_field: None,
            columns: vec!["name".into()],
        };
        crate::federation::validate_column_mapping(&mapped).unwrap();
        let mut registry = crate::federation::ForeignSourceRegistry::new();
        registry.register_spec("records", mapped.clone());
        let session = crate::federation_opt::FederationSession::from_env();
        let refused = registry
            .query_columns("records", &["secret".into()], &[], &session)
            .unwrap_err();
        assert!(refused.contains("not exposed"));
        let numeric = registry
            .query_columns(
                "records",
                &["name".into()],
                &[ColumnPredicate {
                    column: "name".into(),
                    comparison: Comparison::Eq,
                    value: json!(42),
                }],
                &session,
            )
            .unwrap_err();
        assert!(numeric.contains("require string values"));
        let with_columns = |columns: Vec<String>| {
            let eg_types::wire::ForeignSourceSpec::Sql {
                dsn,
                query,
                id_field,
                score_field,
                ..
            } = mapped.clone()
            else {
                panic!("the mapped fixture is a Sql spec");
            };
            eg_types::wire::ForeignSourceSpec::Sql {
                dsn,
                query,
                id_field,
                score_field,
                columns,
            }
        };
        let bad = with_columns(vec!["name; DROP".into()]);
        assert!(crate::federation::validate_column_mapping(&bad).is_err());
        let dup = with_columns(vec!["ID".into()]);
        assert!(crate::federation::validate_column_mapping(&dup).is_err());
    }

    #[cfg(feature = "federation-sql")]
    #[test]
    fn sql_projection_and_filter_quote_identifiers_and_literals() {
        use crate::sql_text::SqlDialect;
        let exposed = BTreeSet::from(["id".into(), "name".into(), "age".into()]);
        let pred = ColumnPredicate {
            column: "name".into(),
            comparison: Comparison::Eq,
            value: json!("O'Brien"),
        };
        let plan =
            ColumnPlan::new(&exposed, &["age".into()], &[pred], true, sql_filter_support).unwrap();
        let rendered = render_sql_columns(
            "SELECT id, name, age FROM people",
            "id",
            None,
            &plan,
            SqlDialect::Postgres,
        )
        .unwrap();
        assert!(rendered.contains("CAST(eg_fed.\"age\" AS TEXT) AS \"age\""));
        assert!(rendered.contains("CAST(eg_fed.\"name\" AS TEXT) = 'O''Brien'"));
        assert!(
            render_sql_columns("SELECT 1", "id; DROP", None, &plan, SqlDialect::Postgres).is_err()
        );
    }

    #[test]
    fn mapped_sql_source_residual_recovers_inexact_remote_superset() {
        use std::sync::Arc;

        struct SqlSuperset;
        impl crate::federation::ForeignSource for SqlSuperset {
            fn fetch(&self) -> Result<crate::rowset::RowSet, String> {
                unreachable!("column query must use fetch_projected")
            }

            fn fetch_projected(&self, plan: &ColumnPlan) -> Result<ForeignRows, String> {
                assert_eq!(plan.remote_projection, vec!["name"]);
                assert_eq!(plan.remote_filters.len(), 1);
                Ok(ForeignRows::from_rows([
                    ForeignRow {
                        id: "a".into(),
                        score: None,
                        columns: BTreeMap::from([("name".into(), json!("Ada"))]),
                    },
                    ForeignRow {
                        id: "b".into(),
                        score: None,
                        columns: BTreeMap::from([("name".into(), json!("Bea"))]),
                    },
                ]))
            }
        }

        let spec = eg_types::wire::ForeignSourceSpec::Sql {
            dsn: "postgres://db.invalid/records".into(),
            query: "SELECT id, name FROM records".into(),
            id_field: "id".into(),
            score_field: None,
            columns: vec!["name".into()],
        };
        let mut registry = crate::federation::ForeignSourceRegistry::new();
        registry.register_mock_spec("records", spec, Arc::new(SqlSuperset));
        let rows = registry
            .query_columns(
                "records",
                &["name".into()],
                &[ColumnPredicate {
                    column: "name".into(),
                    comparison: Comparison::Eq,
                    value: json!("Bea"),
                }],
                &crate::federation_opt::FederationSession::from_env(),
            )
            .unwrap();
        assert_eq!(rows.rows().len(), 1);
        assert_eq!(rows.rows()[0].id, "b");
        assert_eq!(
            rows.rows()[0].columns,
            BTreeMap::from([("name".into(), json!("Bea"))])
        );
    }
}
