//! Shared shapes of the boolean SPARQL built-ins evaluated by
//! [`super::eval_bool_function`] (CONCEPT:EG-KG.ontology.rich-filter): the unary
//! term-type tests and the two-string relations.

use spargebra::algebra::{Expression, Function};

use super::{expr_str, term_lexical, Binding, Ctx, Solution};

/// A predicate over an evaluated term.
type TermTest = fn(&Binding) -> bool;

/// The unary term-type tests (`isNumeric`/`isIRI`/`isBlank`/`isLiteral`): the built-in
/// and the predicate its evaluated argument must satisfy.
static TERM_TYPE_TESTS: [(Function, TermTest); 4] = [
    (Function::IsNumeric, binding_is_numeric),
    (Function::IsIri, binding_is_iri),
    (Function::IsBlank, binding_is_blank),
    (Function::IsLiteral, binding_is_literal),
];

/// The term-type predicate for `f`, if `f` is one of [`TERM_TYPE_TESTS`].
pub(super) fn term_type_test(f: &Function) -> Option<TermTest> {
    TERM_TYPE_TESTS
        .iter()
        .find(|(builtin, _)| builtin == f)
        .map(|(_, test)| *test)
}

fn binding_is_numeric(b: &Binding) -> bool {
    term_lexical(b).parse::<f64>().is_ok()
}

fn binding_is_iri(b: &Binding) -> bool {
    matches!(b, Binding::Node(s) if s.starts_with('<'))
}

fn binding_is_blank(b: &Binding) -> bool {
    matches!(b, Binding::Node(s) if s.starts_with("_:"))
}

fn binding_is_literal(b: &Binding) -> bool {
    matches!(b, Binding::Literal(_))
}

/// `CONTAINS`/`STRSTARTS`/`STRENDS`: evaluate both operands to strings and apply
/// `relation(first, second)`.
pub(super) fn eval_bool_str_relation(
    ctx: &Ctx,
    args: &[Expression],
    sol: &Solution,
    relation: impl FnOnce(&str, &str) -> bool,
) -> Option<bool> {
    let text = expr_str(ctx, args.first()?, sol)?;
    let part = expr_str(ctx, args.get(1)?, sol)?;
    Some(relation(&text, &part))
}
