//! The predicate algebra (EH-365): `OR` < `AND` < `NOT` precedence, parentheses, and
//! every atom — typed comparisons, `IN`, `BETWEEN`, `IS [NOT] NULL`, JSONPath and
//! spatial predicates.
//!
//! Canonical lowering (so `print ∘ parse` is the identity on the wire):
//!  * a top-level `a AND b` is the flat `Filter.preds` list; a PARENTHESISED conjunction
//!    is one `Pred::And` node; `a OR b` is one `Pred::Or`;
//!  * `x > <num>` → `GtNum`, `x < <num>` → `LtNum`, `x = '<str>'`/`x = name` → `Eq`
//!    (the legacy forms); every other comparison → `Cmp` with a TYPED literal — so
//!    `x = 3` compares numbers and `x = TRUE` booleans (the old stringified `Eq` split
//!    is gone);
//!  * `x NOT IN …`, `x NOT BETWEEN …`, `x IS NOT NULL` → `Not { … }`.

use eg_types::wire::{CmpOp, JsonPathOp, Pred, Scalar};

use super::Parser;
use crate::uql::diag::UqlError;
use crate::uql::lexer::Tok;

/// The comparison-operator tokens.
const CMP_OPS: [(Tok, CmpOp); 6] = [
    (Tok::Eq, CmpOp::Eq),
    (Tok::Ne, CmpOp::Ne),
    (Tok::Gt, CmpOp::Gt),
    (Tok::Ge, CmpOp::Ge),
    (Tok::Lt, CmpOp::Lt),
    (Tok::Le, CmpOp::Le),
];

impl<'a> Parser<'a> {
    /// A `WHERE` body: the top-level conjunct list.
    pub(super) fn pred_list(&mut self) -> Result<Vec<Pred>, UqlError> {
        let first = self.conjuncts()?;
        if !self.peek_kw("OR") {
            return Ok(first);
        }
        let mut alts = vec![and_of(first)];
        while self.eat_kw("OR") {
            alts.push(and_of(self.conjuncts()?));
        }
        Ok(vec![Pred::Or { preds: alts }])
    }

    /// `pred = conj { OR conj }` as ONE node (inside parentheses).
    fn pred_node(&mut self) -> Result<Pred, UqlError> {
        let mut list = self.pred_list()?;
        Ok(if list.len() == 1 {
            list.remove(0)
        } else {
            Pred::And { preds: list }
        })
    }

    /// `conj = neg { AND neg }`.
    fn conjuncts(&mut self) -> Result<Vec<Pred>, UqlError> {
        let mut out = vec![self.negation()?];
        while self.eat_kw("AND") {
            out.push(self.negation()?);
        }
        Ok(out)
    }

    /// `neg = NOT neg | ( pred ) | atom`.
    fn negation(&mut self) -> Result<Pred, UqlError> {
        if self.eat_kw("NOT") {
            self.enter()?;
            let inner = self.negation()?;
            self.leave();
            return Ok(Pred::Not {
                pred: Box::new(inner),
            });
        }
        if self.eat(&Tok::LParen) {
            self.enter()?;
            let inner = self.pred_node()?;
            self.leave();
            self.expect(&Tok::RParen, "`)` to close the group")?;
            return Ok(inner);
        }
        self.atom()
    }

    fn atom(&mut self) -> Result<Pred, UqlError> {
        if self.peek_kw("SPATIAL") {
            self.bump();
            return self.spatial_pred();
        }
        if matches!(self.peek_kind(), Some(Tok::Path(_))) || self.peek_kw("JSONPATH") {
            return self.json_path_pred();
        }
        let prop = self.name("a property name")?;
        if let Some(op) = self.cmp_op() {
            return self.comparison(prop, op);
        }
        self.keyword_atom(prop)
    }

    fn cmp_op(&mut self) -> Option<CmpOp> {
        let (_, op) = CMP_OPS.iter().find(|(t, _)| self.peek(t))?;
        self.bump();
        Some(*op)
    }

    fn comparison(&mut self, prop: String, op: CmpOp) -> Result<Pred, UqlError> {
        Ok(match (op, self.scalar()?) {
            (CmpOp::Gt, Scalar::Num(n)) => Pred::GtNum { prop, n },
            (CmpOp::Lt, Scalar::Num(n)) => Pred::LtNum { prop, n },
            (CmpOp::Eq, Scalar::Str(value)) => Pred::Eq { prop, value },
            (op, value) => Pred::Cmp { prop, op, value },
        })
    }

    /// `[NOT] IN …`, `[NOT] BETWEEN …`, `IS [NOT] NULL` after a property name.
    fn keyword_atom(&mut self, prop: String) -> Result<Pred, UqlError> {
        if self.eat_kw("IS") {
            let negated = self.eat_kw("NOT");
            self.expect_kw("NULL")?;
            return Ok(negate(Pred::IsNull { prop }, negated));
        }
        let negated = self.eat_kw("NOT");
        if self.eat_kw("IN") {
            return Ok(negate(self.in_list(prop)?, negated));
        }
        if self.eat_kw("BETWEEN") {
            let lo = self.scalar()?;
            self.expect_kw("AND")?;
            let hi = self.scalar()?;
            return Ok(negate(Pred::Between { prop, lo, hi }, negated));
        }
        Err(self
            .err_here(
                "expected a comparison (`=`, `!=`, `>`, `>=`, `<`, `<=`), `IN`, `BETWEEN` or `IS`",
            )
            .expecting(
                ["=", "!=", ">", ">=", "<", "<=", "IN", "BETWEEN", "IS"]
                    .iter()
                    .map(|s| format!("`{s}`"))
                    .collect(),
            ))
    }

    fn in_list(&mut self, prop: String) -> Result<Pred, UqlError> {
        if !self.eat(&Tok::LParen) {
            let values = self.list_param()?;
            return Ok(Pred::In { prop, values });
        }
        let mut values = vec![self.scalar()?];
        while self.eat(&Tok::Comma) {
            values.push(self.scalar()?);
        }
        self.expect(&Tok::RParen, "`)` to close the IN list")?;
        Ok(Pred::In { prop, values })
    }

    /// `path ( EXISTS | = json | @> json )`.
    fn json_path_pred(&mut self) -> Result<Pred, UqlError> {
        let path = if self.eat_kw("JSONPATH") {
            self.string("a JSONPath")?
        } else {
            let Some(Tok::Path(p)) = self.peek_kind() else {
                return Err(self.err_here("expected a JSONPath"));
            };
            let p = p.clone();
            self.bump();
            p
        };
        let op = if self.eat_kw("EXISTS") {
            JsonPathOp::Exists
        } else if self.eat(&Tok::Eq) {
            JsonPathOp::Eq {
                value: self.json_value()?,
            }
        } else if self.eat(&Tok::AtGt) {
            JsonPathOp::Contains {
                value: self.json_value()?,
            }
        } else {
            return Err(self.err_here("expected `EXISTS`, `=` or `@>` after a JSONPath"));
        };
        Ok(Pred::JsonPath { path, op })
    }

    crate::uql::parser::gated! { "geo",
        /// `SPATIAL rel ( column , 'wkt' [, distance] )` — `SPATIAL` consumed.
        fn spatial_pred(&mut self) -> Result<Pred, UqlError> {
            let rel = self.name("a spatial relation (`WITHIN`, `DWITHIN`, `CONTAINS`, …)")?;
            self.expect(&Tok::LParen, "`(` after the spatial relation")?;
            let column = self.name("the geometry property")?;
            self.expect(&Tok::Comma, "`,` before the WKT geometry")?;
            let wkt = self.string("a WKT geometry")?;
            let distance = if self.eat(&Tok::Comma) {
                Some(self.number("a distance")?)
            } else {
                None
            };
            self.expect(&Tok::RParen, "`)` to close the spatial predicate")?;
            spatial(&rel, column, wkt, distance)
                .ok_or_else(|| self.err_at(self.prev_start(), "unknown spatial relation or arity"))
        }
    }
}

fn and_of(mut preds: Vec<Pred>) -> Pred {
    if preds.len() == 1 {
        preds.remove(0)
    } else {
        Pred::And { preds }
    }
}

fn negate(pred: Pred, negated: bool) -> Pred {
    if negated {
        Pred::Not {
            pred: Box::new(pred),
        }
    } else {
        pred
    }
}

/// The spatial predicate named `rel` (case-insensitive); `DWITHIN` alone takes a distance.
#[cfg(feature = "geo")]
fn spatial(rel: &str, column: String, wkt: String, distance: Option<f64>) -> Option<Pred> {
    let rel = rel.to_ascii_uppercase();
    Some(match (rel.as_str(), distance) {
        ("DWITHIN", Some(distance)) => Pred::SpatialDWithin {
            column,
            wkt,
            distance,
        },
        ("WITHIN", None) => Pred::SpatialWithin { column, wkt },
        ("CONTAINS", None) => Pred::SpatialContains { column, wkt },
        ("COVERS", None) => Pred::SpatialCovers { column, wkt },
        ("TOUCHES", None) => Pred::SpatialTouches { column, wkt },
        ("CROSSES", None) => Pred::SpatialCrosses { column, wkt },
        ("OVERLAPS", None) => Pred::SpatialOverlaps { column, wkt },
        ("EQUALS", None) => Pred::SpatialEquals { column, wkt },
        ("DISJOINT", None) => Pred::SpatialDisjoint { column, wkt },
        _ => return None,
    })
}
