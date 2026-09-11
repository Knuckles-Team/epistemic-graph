use eg_rdf::oxrdf::{Literal, NamedNode, Term};
use regex::RegexBuilder;

/// An `xsd:integer` result literal (`STRLEN` uses this numeric result).
pub(super) fn int_term(value: usize) -> Term {
    Term::Literal(Literal::new_typed_literal(
        value.to_string(),
        NamedNode::new_unchecked(crate::vocab::XSD_INTEGER),
    ))
}

pub(super) fn bool_term(value: bool) -> Term {
    Term::Literal(Literal::new_typed_literal(
        if value { "true" } else { "false" },
        NamedNode::new_unchecked(crate::vocab::XSD_BOOLEAN),
    ))
}

/// Effective boolean value for the evaluator's supported literal types.
pub(super) fn ebv(term: Option<&Term>) -> bool {
    match term {
        Some(Term::Literal(literal)) => {
            let datatype = literal.datatype().as_str();
            if datatype == crate::vocab::XSD_BOOLEAN {
                literal.value() == "true" || literal.value() == "1"
            } else if datatype == crate::vocab::XSD_STRING
                || datatype == crate::vocab::RDF_LANG_STRING
            {
                !literal.value().is_empty()
            } else if let Some(number) = numeric_of(&Term::Literal(literal.clone())) {
                number != 0.0 && !number.is_nan()
            } else {
                false
            }
        }
        _ => false,
    }
}

/// A literal's lexical form as `f64`, when it parses as a number.
pub(super) fn numeric_of(term: &Term) -> Option<f64> {
    match term {
        Term::Literal(literal) => literal.value().trim().parse::<f64>().ok(),
        _ => None,
    }
}

/// `STR()`-style lexical rendering.
pub(super) fn lexical_of(term: Option<&Term>) -> String {
    match term {
        Some(Term::Literal(literal)) => literal.value().to_string(),
        Some(Term::NamedNode(node)) => node.as_str().to_string(),
        _ => String::new(),
    }
}

/// SPARQL equality with the evaluator's numeric literal fallback.
pub(super) fn term_eq(left: &Option<Term>, right: &Option<Term>) -> bool {
    let (Some(left), Some(right)) = (left, right) else {
        return false;
    };
    if left == right {
        return true;
    }
    matches!(
        (literal_lexical(left), literal_lexical(right)),
        (Some(left), Some(right))
            if matches!(
                (left.trim().parse::<f64>(), right.trim().parse::<f64>()),
                (Ok(left), Ok(right)) if left == right
            )
    )
}

pub(super) fn literal_lexical(term: &Term) -> Option<String> {
    match term {
        Term::Literal(literal) => Some(literal.value().to_string()),
        _ => None,
    }
}

/// Basic language-range matching (RFC 4647 §3.3.1).
pub(super) fn lang_range_matches(range: &str, tag: &str) -> bool {
    if range == "*" {
        return !tag.is_empty();
    }
    let range = range.to_ascii_lowercase();
    let tag = tag.to_ascii_lowercase();
    tag == range
        || tag
            .strip_prefix(&range)
            .is_some_and(|rest| rest.starts_with('-'))
}

pub(super) fn pattern_ok(value: &str, pattern: &str, flags: Option<&str>) -> bool {
    let mut builder = RegexBuilder::new(pattern);
    if let Some(flags) = flags {
        builder.case_insensitive(flags.contains('i'));
        builder.multi_line(flags.contains('m'));
        builder.dot_matches_new_line(flags.contains('s'));
        builder.ignore_whitespace(flags.contains('x'));
    }
    match builder.build() {
        Ok(regex) => regex.is_match(value),
        Err(_) => false,
    }
}
