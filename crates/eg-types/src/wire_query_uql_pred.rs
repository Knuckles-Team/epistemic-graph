//! UQL printer, predicates (UQL-06): the `WHERE` algebra, JSONPath and spatial
//! predicates. Split from `wire_query_uql.rs` (KISS functions-per-file); the contract is
//! that module's.

use super::*;

// ── predicates ──────────────────────────────────────────────────────────────────

/// A top-level `AND` list (a `Filter`'s `preds`): each member is an operand, so a
/// nested connective is parenthesised and re-parses as the same single node.
pub fn conjunction(preds: &[Pred]) -> Printed {
    let parts = preds.iter().map(operand).collect::<Result<Vec<_>, _>>()?;
    Ok(parts.join(" AND "))
}

/// A predicate in operand position: connectives are parenthesised, atoms bare.
fn operand(pred: &Pred) -> Printed {
    let text = uql_pred(pred)?;
    Ok(if matches!(pred, Pred::And { .. } | Pred::Or { .. }) {
        format!("({text})")
    } else {
        text
    })
}

fn connective(preds: &[Pred], word: &str) -> Printed {
    if preds.len() < 2 {
        return Err(refuse(
            UqlPrintCode::DegenerateShape,
            format!("{word} with fewer than two members"),
        ));
    }
    let parts = preds.iter().map(operand).collect::<Result<Vec<_>, _>>()?;
    Ok(parts.join(&format!(" {word} ")))
}

/// One predicate's canonical UQL.
pub fn uql_pred(pred: &Pred) -> Printed {
    match pred {
        Pred::Eq { prop, value } => Ok(format!("{} = {}", uql_ident(prop), uql_quote(value))),
        Pred::GtNum { prop, n } => Ok(format!("{} > {}", uql_ident(prop), uql_num(*n)?)),
        Pred::LtNum { prop, n } => Ok(format!("{} < {}", uql_ident(prop), uql_num(*n)?)),
        Pred::Cmp { prop, op, value } => Ok(format!(
            "{} {} {}",
            uql_ident(prop),
            cmp_op(*op),
            uql_scalar(value)?
        )),
        Pred::In { prop, values } => in_list(prop, values),
        Pred::Between { prop, lo, hi } => Ok(format!(
            "{} BETWEEN {} AND {}",
            uql_ident(prop),
            uql_scalar(lo)?,
            uql_scalar(hi)?
        )),
        Pred::IsNull { prop } => Ok(format!("{} IS NULL", uql_ident(prop))),
        Pred::And { preds } => connective(preds, "AND"),
        Pred::Or { preds } => connective(preds, "OR"),
        Pred::Not { pred } => Ok(format!("NOT {}", operand(pred)?)),
        Pred::JsonPath { path, op } => json_path(path, op),
        #[cfg(feature = "geo")]
        Pred::SpatialWithin { column, wkt } => spatial_pred("WITHIN", column, wkt, None),
        #[cfg(feature = "geo")]
        Pred::SpatialDWithin {
            column,
            wkt,
            distance,
        } => spatial_pred("DWITHIN", column, wkt, Some(*distance)),
        #[cfg(feature = "geo")]
        Pred::SpatialContains { column, wkt } => spatial_pred("CONTAINS", column, wkt, None),
        #[cfg(feature = "geo")]
        Pred::SpatialCovers { column, wkt } => spatial_pred("COVERS", column, wkt, None),
        #[cfg(feature = "geo")]
        Pred::SpatialTouches { column, wkt } => spatial_pred("TOUCHES", column, wkt, None),
        #[cfg(feature = "geo")]
        Pred::SpatialCrosses { column, wkt } => spatial_pred("CROSSES", column, wkt, None),
        #[cfg(feature = "geo")]
        Pred::SpatialOverlaps { column, wkt } => spatial_pred("OVERLAPS", column, wkt, None),
        #[cfg(feature = "geo")]
        Pred::SpatialEquals { column, wkt } => spatial_pred("EQUALS", column, wkt, None),
        #[cfg(feature = "geo")]
        Pred::SpatialDisjoint { column, wkt } => spatial_pred("DISJOINT", column, wkt, None),
    }
}

/// The UQL spelling of a comparison operator.
pub fn cmp_op(op: CmpOp) -> &'static str {
    match op {
        CmpOp::Eq => "=",
        CmpOp::Ne => "!=",
        CmpOp::Gt => ">",
        CmpOp::Ge => ">=",
        CmpOp::Lt => "<",
        CmpOp::Le => "<=",
    }
}

/// A typed scalar literal.
pub fn uql_scalar(value: &Scalar) -> Printed {
    match value {
        Scalar::Str(s) => Ok(uql_quote(s)),
        Scalar::Num(n) => uql_num(*n),
        Scalar::Bool(b) => Ok(uql_bool(*b).into()),
    }
}

fn in_list(prop: &str, values: &[Scalar]) -> Printed {
    if values.is_empty() {
        return Err(refuse(
            UqlPrintCode::DegenerateShape,
            "IN with an empty list",
        ));
    }
    let parts = values
        .iter()
        .map(uql_scalar)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(format!("{} IN ({})", uql_ident(prop), parts.join(", ")))
}

fn json_path(path: &str, op: &JsonPathOp) -> Printed {
    let path = if is_path_token(path) {
        path.to_string()
    } else {
        format!("JSONPATH {}", uql_quote(path))
    };
    Ok(match op {
        JsonPathOp::Exists => format!("{path} EXISTS"),
        JsonPathOp::Eq { value } => format!("{path} = {}", uql_json(value)?),
        JsonPathOp::Contains { value } => format!("{path} @> {}", uql_json(value)?),
    })
}

/// Would the UQL lexer read `s` back as ONE bare JSONPath token (`$.a.b[0]`)?
pub fn is_path_token(s: &str) -> bool {
    let mut chars = s.chars();
    chars.next() == Some('$')
        && matches!(chars.next(), Some('.') | Some('['))
        && s.chars()
            .all(|c| c.is_alphanumeric() || "$._[]*".contains(c))
}

#[cfg(feature = "geo")]
fn spatial_pred(rel: &str, column: &str, wkt: &str, distance: Option<f64>) -> Printed {
    let tail = match distance {
        Some(d) => format!(", {}", uql_num(d)?),
        None => String::new(),
    };
    Ok(format!(
        "SPATIAL {rel}({}, {}{tail})",
        uql_ident(column),
        uql_quote(wkt)
    ))
}
