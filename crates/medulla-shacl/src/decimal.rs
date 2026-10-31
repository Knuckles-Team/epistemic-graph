//! Exact lexical validation and ordering for XSD integer and decimal literals.

use medulla_rdf::oxrdf::{Literal, Term};
use std::cmp::Ordering;

fn decimal_kind(literal: &Literal) -> Option<bool> {
    match literal.datatype().as_str() {
        "http://www.w3.org/2001/XMLSchema#integer" => Some(true),
        "http://www.w3.org/2001/XMLSchema#decimal" => Some(false),
        _ => None,
    }
}

struct Decimal<'a> {
    negative: bool,
    whole: &'a str,
    fraction: &'a str,
}

fn parse(value: &str, integer: bool) -> Option<Decimal<'_>> {
    let value = value.trim_matches([' ', '\t', '\r', '\n']);
    let negative = value.starts_with('-');
    let unsigned = value.strip_prefix(['+', '-']).unwrap_or(value);
    let (whole, fraction) = match unsigned.split_once('.') {
        Some(_) if integer => return None,
        Some(parts) => parts,
        None => (unsigned, ""),
    };
    if whole.is_empty() && fraction.is_empty()
        || !whole
            .bytes()
            .chain(fraction.bytes())
            .all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let whole = whole.trim_start_matches('0');
    let fraction = fraction.trim_end_matches('0');
    Some(Decimal {
        negative: negative && !(whole.is_empty() && fraction.is_empty()),
        whole,
        fraction,
    })
}

pub(crate) fn valid_literal(literal: &Literal) -> bool {
    decimal_kind(literal).is_none_or(|integer| parse(literal.value(), integer).is_some())
}

fn compare(left: Decimal<'_>, right: Decimal<'_>) -> Ordering {
    if left.negative != right.negative {
        return right.negative.cmp(&left.negative);
    }
    let width = left.fraction.len().max(right.fraction.len());
    let order = left
        .whole
        .len()
        .cmp(&right.whole.len())
        .then_with(|| left.whole.cmp(right.whole))
        .then_with(|| {
            left.fraction
                .bytes()
                .chain(std::iter::repeat(b'0'))
                .take(width)
                .cmp(
                    right
                        .fraction
                        .bytes()
                        .chain(std::iter::repeat(b'0'))
                        .take(width),
                )
        });
    if left.negative {
        order.reverse()
    } else {
        order
    }
}

/// Outer None means other datatypes; inner None means an invalid numeric literal.
pub(crate) fn comparison(left: &Term, right: &Term) -> Option<Option<Ordering>> {
    let (Term::Literal(left), Term::Literal(right)) = (left, right) else {
        return None;
    };
    let (left_kind, right_kind) = (decimal_kind(left)?, decimal_kind(right)?);
    Some(
        parse(left.value(), left_kind)
            .zip(parse(right.value(), right_kind))
            .map(|(left, right)| compare(left, right)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use medulla_rdf::oxrdf::NamedNode;

    fn literal(value: &str, kind: &str) -> Term {
        Literal::new_typed_literal(
            value,
            NamedNode::new(format!("http://www.w3.org/2001/XMLSchema#{kind}")).unwrap(),
        )
        .into()
    }

    #[test]
    fn exact_order_handles_sign_scale_and_unbounded_digits() {
        for (left, right, expected) in [
            ("9007199254740993", "9007199254740992", Ordering::Greater),
            (
                "9999999999999999999999999999999999999999",
                "10000000000000000000000000000000000000000",
                Ordering::Less,
            ),
            ("-2", "-1.9", Ordering::Less),
            ("-0.00", "+0", Ordering::Equal),
            ("0001.2300", "1.23", Ordering::Equal),
            (".001", "0.0009", Ordering::Greater),
            (" 1.\t", "1.0", Ordering::Equal),
        ] {
            assert_eq!(
                comparison(&literal(left, "decimal"), &literal(right, "decimal")),
                Some(Some(expected)),
                "{left} vs {right}"
            );
        }
        assert_eq!(
            comparison(&literal("1", "integer"), &literal("1.0", "decimal")),
            Some(Some(Ordering::Equal))
        );
    }

    #[test]
    fn malformed_numeric_lexical_forms_are_not_values() {
        for kind in ["integer", "decimal"] {
            for value in ["", "+", ".", "NaN", "INF", "1e2", "1.2.3", "1 2", "１２"] {
                let Term::Literal(value) = literal(value, kind) else {
                    unreachable!()
                };
                assert!(!valid_literal(&value), "{value:?}");
            }
        }
        assert_eq!(
            comparison(&literal("0.5", "integer"), &literal("0", "integer")),
            Some(None)
        );
        assert_eq!(
            comparison(&literal("2", "string"), &literal("1", "string")),
            None
        );
    }
}
