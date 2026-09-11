//! A minimal, dependency-free GraphQL parser (CONCEPT:EG-KG.query.sparql-completeness, writes CONCEPT:EG-KG.query.mutation).
//!
//! Covers the read subset the resolver needs — the same subset a relational/graph DB
//! GraphQL surface exposes: an anonymous (or named) `query` operation whose selection
//! set is one-or-more ROOT fields (each a node TYPE), each carrying optional ARGUMENTS
//! (`first`/`limit` ints + property-equality filters) and a nested SELECTION SET of
//! scalar fields (node properties) and object fields (edge relationships, recursed).
//!
//! It ALSO parses `mutation` and `subscription` operations (CONCEPT:EG-KG.query.mutation). A mutation
//! is a selection set of write root fields (`createNode`/`updateNode`/`deleteNode`/
//! `addEdge`/`removeEdge`) whose arguments may carry OBJECT / LIST values (the `props`
//! map). A subscription mirrors a query's selection set (the resolver serves it as a
//! poll over the current matches — see `crate::subscription`).
//!
//! NOT async-graphql: a hand-written tokenizer + recursive-descent parser, pure Rust,
//! so the surface stays Pi-excludable (the facade gates the whole crate behind
//! `graphql`).
//!
//! ## Fragments / variables / directives (CONCEPT:EG-KG.query.fragments-variables-directives)
//! The lexer also emits `$` (variable refs), `@` (directives), `...` (spreads) and `=`
//! (variable defaults). [`parse_raw`] yields a [`RawDocument`] that retains named
//! fragment definitions, fragment spreads / inline fragments, operation variable
//! definitions, and field/spread directives. The resolver
//! ([`crate::resolver::flatten_document`]) inlines the spreads, applies `@skip`/
//! `@include`, and substitutes variable references — so the [`Query`]/[`Operation`]
//! the rest of the crate sees is a plain, already-desugared selection of [`Field`]s.

mod cursor;
mod document;
mod error;
mod lexer;
mod public_ast;
mod raw_ast;
mod values;

use serde_json::Value;

use document::P;
use lexer::lex;

pub use error::GqlError;
pub use public_ast::{Field, GqlValue, Mutation, Operation, Query, Subscription};
pub(crate) use raw_ast::{Directive, Fragment, RawDocument, RawField, RawSelection, VarDef};

/// Convert a parsed [`GqlValue`] to a `serde_json::Value` (CONCEPT:EG-KG.query.mutation). Shared by
/// the resolver (filter literals) and the mutation executor (write payloads), so the
/// two surfaces coerce argument values identically. An unsubstituted [`GqlValue::Var`]
/// (CONCEPT:EG-KG.query.fragments-variables-directives) coerces to `null` — the resolver substitutes vars BEFORE this runs,
/// so a `Var` reaching here means it was unbound.
pub(crate) fn gql_to_json(v: &GqlValue) -> Value {
    match v {
        GqlValue::Int(n) => Value::Number((*n).into()),
        GqlValue::Float(f) => serde_json::Number::from_f64(*f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        GqlValue::Str(s) => Value::String(s.clone()),
        GqlValue::Bool(b) => Value::Bool(*b),
        GqlValue::Null => Value::Null,
        GqlValue::Var(_) => Value::Null,
        GqlValue::List(items) => Value::Array(items.iter().map(gql_to_json).collect()),
        GqlValue::Object(fields) => Value::Object(
            fields
                .iter()
                .map(|(k, val)| (k.clone(), gql_to_json(val)))
                .collect(),
        ),
    }
}

/// Parse a GraphQL document into a [`Query`] (the READ path). Accepts a bare selection
/// set (`{ … }`), `query { … }`, or `query Name { … }`. Fragments / variables /
/// directives (CONCEPT:EG-KG.query.fragments-variables-directives) are desugared here with NO execution variables (the
/// variable-aware entry point is [`crate::resolver::execute_with_variables`]). A
/// `mutation` / `subscription` document is reported as an error, since this entry point
/// only yields the query case.
pub fn parse(src: &str) -> Result<Query, GqlError> {
    let doc = parse_raw(src)?;
    if doc.op_kind != "query" {
        return Err(GqlError {
            msg: format!(
                "expected a query operation, found a {0} (use the {0} execution path)",
                doc.op_kind
            ),
            at: 0,
        });
    }
    let roots = crate::resolver::flatten_document(&doc, &crate::resolver::Variables::new())
        .map_err(|msg| GqlError { msg, at: 0 })?;
    Ok(Query { roots })
}

/// Parse a GraphQL document into an [`Operation`] (CONCEPT:EG-KG.query.mutation): a `query`,
/// `mutation`, or `subscription`. A bare selection set (`{ … }`) is a query. Fragments,
/// variables, and directives (CONCEPT:EG-KG.query.fragments-variables-directives) are desugared with no execution variables.
pub fn parse_operation(src: &str) -> Result<Operation, GqlError> {
    let doc = parse_raw(src)?;
    let roots = crate::resolver::flatten_document(&doc, &crate::resolver::Variables::new())
        .map_err(|msg| GqlError { msg, at: 0 })?;
    Ok(match doc.op_kind {
        "mutation" => Operation::Mutation(Mutation { roots }),
        "subscription" => Operation::Subscription(Subscription { roots }),
        _ => Operation::Query(Query { roots }),
    })
}

/// Parse a GraphQL document into the RAW (pre-desugar) [`RawDocument`] (CONCEPT:EG-KG.query.fragments-variables-directives),
/// retaining fragments, variable definitions, directives, and `$var` references. The
/// resolver lowers it to a plain [`Field`] tree once execution variables are known.
pub(crate) fn parse_raw(src: &str) -> Result<RawDocument, GqlError> {
    let toks = lex(src)?;
    let mut p = P::new(&toks, src.len());
    let doc = p.parse_raw_document()?;
    p.expect_eof()?;
    Ok(doc)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_nested_query_with_args() {
        let q = parse(
            r#"{
                Person(first: 2, name: "Alice") {
                    name
                    knows { name }
                }
            }"#,
        )
        .unwrap();
        assert_eq!(q.roots.len(), 1);
        let person = &q.roots[0];
        assert_eq!(person.name, "Person");
        assert_eq!(person.args.len(), 2);
        assert_eq!(person.args[0], ("first".into(), GqlValue::Int(2)));
        assert_eq!(
            person.args[1],
            ("name".into(), GqlValue::Str("Alice".into()))
        );
        assert_eq!(person.selection.len(), 2);
        assert_eq!(person.selection[0].name, "name");
        let knows = &person.selection[1];
        assert_eq!(knows.name, "knows");
        assert_eq!(knows.selection[0].name, "name");
    }

    #[test]
    fn accepts_query_keyword_and_name() {
        let q = parse("query Q { Doc { title } }").unwrap();
        assert_eq!(q.roots[0].name, "Doc");
    }

    #[test]
    fn read_parse_rejects_mutation() {
        // The READ entry point yields only the query case (the write path uses
        // `parse_operation`), so a mutation document is reported as not-a-query.
        let e = parse("mutation { createNode(label: \"Doc\") { id } }").unwrap_err();
        assert!(e.msg.contains("mutation"), "got {}", e.msg);
    }

    #[test]
    fn parses_mutation_with_object_and_list_args() {
        let op = parse_operation(
            r#"mutation {
                createNode(label: "Person", props: {name: "Alice", tags: ["a", "b"], age: 30}) {
                    id
                    name
                }
            }"#,
        )
        .unwrap();
        let Operation::Mutation(m) = op else {
            panic!("expected a mutation");
        };
        assert_eq!(m.roots.len(), 1);
        let create = &m.roots[0];
        assert_eq!(create.name, "createNode");
        assert_eq!(
            create.args[0],
            ("label".into(), GqlValue::Str("Person".into()))
        );
        let GqlValue::Object(props) = &create.args[1].1 else {
            panic!("props must be an object");
        };
        assert_eq!(props[0], ("name".into(), GqlValue::Str("Alice".into())));
        assert_eq!(
            props[1],
            (
                "tags".into(),
                GqlValue::List(vec![GqlValue::Str("a".into()), GqlValue::Str("b".into()),])
            )
        );
        assert_eq!(props[2], ("age".into(), GqlValue::Int(30)));
        // the selection set shapes the returned object.
        assert_eq!(create.selection[0].name, "id");
        assert_eq!(create.selection[1].name, "name");
    }

    #[test]
    fn parses_subscription() {
        let op = parse_operation("subscription { Person { name } }").unwrap();
        let Operation::Subscription(s) = op else {
            panic!("expected a subscription");
        };
        assert_eq!(s.roots[0].name, "Person");
    }

    #[test]
    fn empty_selection_is_error() {
        let e = parse("{ }").unwrap_err();
        assert!(e.msg.contains("at least one field"), "got {}", e.msg);
    }

    // ── CONCEPT:EG-KG.query.fragments-variables-directives — fragments / variables / directives ──────────────────────

    #[test]
    fn raw_doc_retains_fragments_and_var_defs() {
        let doc = parse_raw(
            r#"query Q($x: Int, $active: Boolean = true) {
                Person { ...frag ... on Person { extra } }
            }
            fragment frag on Person { name @skip(if: $x) }"#,
        )
        .unwrap();
        assert_eq!(doc.op_kind, "query");
        assert_eq!(doc.var_defs.len(), 2);
        assert_eq!(doc.var_defs[0].name, "x");
        assert_eq!(doc.var_defs[1].name, "active");
        assert_eq!(doc.var_defs[1].default, Some(GqlValue::Bool(true)));
        assert_eq!(doc.fragments.len(), 1);
        assert_eq!(doc.fragments[0].name, "frag");
        // the root Person selection holds a spread + an inline fragment.
        let RawSelection::Field(person) = &doc.selections[0] else {
            panic!("expected a field");
        };
        assert!(matches!(person.selections[0], RawSelection::Spread { .. }));
        assert!(matches!(person.selections[1], RawSelection::Inline { .. }));
    }

    #[test]
    fn parses_variable_reference_in_args() {
        let doc = parse_raw("query Q($n: Int) { Person(first: $n) { name } }").unwrap();
        let RawSelection::Field(person) = &doc.selections[0] else {
            panic!("expected a field");
        };
        assert_eq!(person.args[0], ("first".into(), GqlValue::Var("n".into())));
    }
}
