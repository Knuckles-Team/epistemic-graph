use super::public_ast::GqlValue;

/// A `@name(arg: value, …)` directive on a field, spread, or inline fragment.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Directive {
    pub name: String,
    pub args: Vec<(String, GqlValue)>,
}

/// An operation variable definition `$name: Type = default` (the `Type` is parsed but
/// ignored — this surface is untyped; only the name + optional default matter).
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct VarDef {
    pub name: String,
    pub default: Option<GqlValue>,
}

/// A field in a RAW selection set — may still carry directives and contain nested raw
/// selections (which may themselves be spreads / inline fragments).
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RawField {
    pub alias: String,
    pub name: String,
    pub args: Vec<(String, GqlValue)>,
    pub directives: Vec<Directive>,
    pub selections: Vec<RawSelection>,
}

/// One member of a raw selection set: a field, a fragment spread (`...Name`), or an
/// inline fragment (`... on Type { … }` / `... { … }`).
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RawSelection {
    Field(RawField),
    Spread {
        name: String,
        directives: Vec<Directive>,
    },
    Inline {
        type_cond: Option<String>,
        directives: Vec<Directive>,
        selections: Vec<RawSelection>,
    },
}

/// A named fragment definition `fragment Name on Type { … }`.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Fragment {
    pub name: String,
    #[allow(dead_code)]
    pub type_cond: String,
    pub selections: Vec<RawSelection>,
}

/// A fully parsed RAW document: the single operation (kind + variable defs + selection)
/// plus any named fragment definitions it references.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RawDocument {
    pub op_kind: &'static str,
    pub var_defs: Vec<VarDef>,
    pub selections: Vec<RawSelection>,
    pub fragments: Vec<Fragment>,
}
