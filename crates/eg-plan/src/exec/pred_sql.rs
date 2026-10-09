//! The relational FILTER leg's SQL compiler: `Pred` → a DataFusion `WHERE` fragment.
//!
//! Numeric literals are emitted bare, strings single-quote-escaped, identifiers
//! double-quote-escaped; every identifier/literal is bounded. The predicate algebra
//! (UQL-01: typed `Cmp`, `In`, `Between`, `IsNull`, `And`/`Or`/`Not`) compiles to the
//! matching SQL connectives, so SQL's three-valued logic is the semantics: a comparison
//! with a missing property is UNKNOWN, and `NOT UNKNOWN` does not keep the row.
//! `eg_plan::pred_eval` implements the SAME logic per row for the paths that cannot use
//! SQL (incremental maintenance, edge predicates).
//!
//! JSONPath and spatial predicates are evaluated per row outside SQL. At the top level
//! of a `Filter` they are split out before this compiler runs (the `1=1` arms below are
//! placeholders, never the decision); nested under a connective they are refused, since
//! no SQL fragment can stand in for them there.

use crate::algebra::Pred;
use crate::pred_eval::spatial_predicate;
use eg_types::wire::{CmpOp, PredLiteral};

const MAX_FILTER_PREDICATES: usize = 256;
const MAX_FILTER_IDENTIFIER_BYTES: usize = 256;
const MAX_FILTER_LITERAL_BYTES: usize = 1024 * 1024;
/// How deep a predicate tree may nest (connectives) before it is refused.
const MAX_FILTER_DEPTH: usize = 64;

/// Where a predicate sits: directly in the `Filter` list, or under a connective.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Position {
    TopLevel,
    Nested(usize),
}

impl Position {
    fn inner(self) -> Result<Position, String> {
        let depth = match self {
            Position::TopLevel => 1,
            Position::Nested(d) => d + 1,
        };
        if depth > MAX_FILTER_DEPTH {
            return Err("filter predicate nesting exceeds its safety bound".into());
        }
        Ok(Position::Nested(depth))
    }
}

fn sql_identifier(value: &str) -> Result<String, String> {
    if value.is_empty()
        || value.len() > MAX_FILTER_IDENTIFIER_BYTES
        || value.chars().any(char::is_control)
    {
        return Err("filter property name is invalid or exceeds its safety bound".into());
    }
    Ok(format!("\"{}\"", value.replace('"', "\"\"")))
}

/// A bounded, escaped SQL string literal.
pub(crate) fn sql_literal(value: &str) -> Result<String, String> {
    if value.len() > MAX_FILTER_LITERAL_BYTES || value.contains('\0') {
        return Err("filter literal is invalid or exceeds its safety bound".into());
    }
    Ok(format!("'{}'", value.replace('\'', "''")))
}

fn sql_number(n: f64) -> Result<String, String> {
    if !n.is_finite() {
        return Err("filter numeric literal must be finite".into());
    }
    Ok(format!("{n}"))
}

fn sql_scalar(value: &PredLiteral) -> Result<String, String> {
    match value {
        PredLiteral::Str(s) => sql_literal(s),
        PredLiteral::Num(n) => sql_number(*n),
        PredLiteral::Bool(true) => Ok("TRUE".into()),
        PredLiteral::Bool(false) => Ok("FALSE".into()),
    }
}

fn sql_cmp(op: CmpOp) -> &'static str {
    match op {
        CmpOp::Eq => "=",
        CmpOp::Ne => "<>",
        CmpOp::Gt => ">",
        CmpOp::Ge => ">=",
        CmpOp::Lt => "<",
        CmpOp::Le => "<=",
    }
}

/// Compile `preds` (an implicit conjunction) to a SQL `WHERE` fragment.
pub(crate) fn where_clause(preds: &[Pred]) -> Result<String, String> {
    if preds.len() > MAX_FILTER_PREDICATES {
        return Err("filter predicate count exceeds its safety bound".into());
    }
    if preds.is_empty() {
        return Ok("1=1".into());
    }
    let clauses = preds
        .iter()
        .map(|p| pred_sql(p, Position::TopLevel))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(clauses.join(" AND "))
}

fn connective(preds: &[Pred], word: &str, empty: &str, at: Position) -> Result<String, String> {
    if preds.len() > MAX_FILTER_PREDICATES {
        return Err("filter predicate count exceeds its safety bound".into());
    }
    if preds.is_empty() {
        return Ok(empty.into());
    }
    let inner = at.inner()?;
    let parts = preds
        .iter()
        .map(|p| pred_sql(p, inner))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(format!("({})", parts.join(&format!(" {word} "))))
}

fn per_row_only(at: Position) -> Result<String, String> {
    match at {
        Position::TopLevel => Ok("1=1".into()),
        Position::Nested(_) => Err("JSONPath and spatial predicates cannot be nested under \
             AND/OR/NOT; list them as top-level WHERE conjuncts"
            .into()),
    }
}

fn in_list(prop: &str, values: &[PredLiteral]) -> Result<String, String> {
    if values.is_empty() {
        return Ok("FALSE".into());
    }
    if values.len() > MAX_FILTER_PREDICATES {
        return Err("filter IN list exceeds its safety bound".into());
    }
    let items = values
        .iter()
        .map(sql_scalar)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(format!(
        "{} IN ({})",
        sql_identifier(prop)?,
        items.join(", ")
    ))
}

fn pred_sql(p: &Pred, at: Position) -> Result<String, String> {
    Ok(match p {
        Pred::Eq { prop, value } => format!("{} = {}", sql_identifier(prop)?, sql_literal(value)?),
        Pred::GtNum { prop, n } => format!("{} > {}", sql_identifier(prop)?, sql_number(*n)?),
        Pred::LtNum { prop, n } => format!("{} < {}", sql_identifier(prop)?, sql_number(*n)?),
        Pred::Cmp { prop, op, value } => format!(
            "{} {} {}",
            sql_identifier(prop)?,
            sql_cmp(*op),
            sql_scalar(value)?
        ),
        Pred::In { prop, values } => in_list(prop, values)?,
        Pred::Between { prop, lo, hi } => format!(
            "{} BETWEEN {} AND {}",
            sql_identifier(prop)?,
            sql_scalar(lo)?,
            sql_scalar(hi)?
        ),
        Pred::IsNull { prop } => format!("{} IS NULL", sql_identifier(prop)?),
        Pred::And { preds } => connective(preds, "AND", "TRUE", at)?,
        Pred::Or { preds } => connective(preds, "OR", "FALSE", at)?,
        Pred::Not { pred } => format!("NOT ({})", pred_sql(pred, at.inner()?)?),
        Pred::JsonPath { .. } => per_row_only(at)?,
        // The shared exhaustive pattern remains unconditional without `geo`.
        spatial_predicate!() => per_row_only(at)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: EG-FEDERATED-QUERY-R021, EG-FEDERATED-QUERY-R024, EG-FEDERATED-QUERY-R070
    #[test]
    fn wire_supplied_identifiers_and_literals_cannot_escape_sql() {
        let clause = where_clause(&[Pred::Eq {
            prop: "name\" OR 1=1 --".into(),
            value: "x' OR '1'='1".into(),
        }])
        .unwrap();
        assert_eq!(clause, "\"name\"\" OR 1=1 --\" = 'x'' OR ''1''=''1'");
    }

    #[test]
    fn invalid_or_unbounded_wire_predicates_fail_closed() {
        assert!(where_clause(&[Pred::GtNum {
            prop: "score".into(),
            n: f64::NAN,
        }])
        .is_err());
        assert!(where_clause(&[Pred::Eq {
            prop: "bad\nname".into(),
            value: String::new(),
        }])
        .is_err());
        let too_many = (0..=MAX_FILTER_PREDICATES)
            .map(|_| Pred::Eq {
                prop: "type".into(),
                value: "Document".into(),
            })
            .collect::<Vec<_>>();
        assert!(where_clause(&too_many).is_err());
    }

    #[test]
    fn predicate_algebra_compiles_to_sql_connectives() {
        let tree = Pred::Or {
            preds: vec![
                Pred::Cmp {
                    prop: "year".into(),
                    op: CmpOp::Ge,
                    value: PredLiteral::Num(2020.0),
                },
                Pred::Not {
                    pred: Box::new(Pred::In {
                        prop: "lang".into(),
                        values: vec![PredLiteral::Str("en".into()), PredLiteral::Bool(true)],
                    }),
                },
                Pred::And {
                    preds: vec![
                        Pred::IsNull { prop: "x".into() },
                        Pred::Between {
                            prop: "n".into(),
                            lo: PredLiteral::Num(-1.0),
                            hi: PredLiteral::Num(2.5),
                        },
                    ],
                },
            ],
        };
        assert_eq!(
            where_clause(&[tree]).unwrap(),
            "(\"year\" >= 2020 OR NOT (\"lang\" IN ('en', TRUE)) OR \
             (\"x\" IS NULL AND \"n\" BETWEEN -1 AND 2.5))"
        );
    }

    #[test]
    fn per_row_predicates_are_refused_under_a_connective() {
        let nested = Pred::Not {
            pred: Box::new(Pred::JsonPath {
                path: "$.a".into(),
                op: eg_types::wire::JsonPathOp::Exists,
            }),
        };
        assert!(where_clause(&[nested]).is_err());
    }

    #[test]
    fn nesting_is_bounded() {
        let mut p = Pred::IsNull { prop: "x".into() };
        for _ in 0..=MAX_FILTER_DEPTH {
            p = Pred::Not { pred: Box::new(p) };
        }
        assert!(where_clause(&[p]).is_err());
    }
}
