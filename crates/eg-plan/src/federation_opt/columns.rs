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
}
