//! Lexer (UQL-02): comments, numbers, unicode, quoted identifiers, JSONPath/params.

use crate::uql::lexer::{lex, Tok};
use crate::uql::parse;
use eg_types::wire::{Op, Pred};

fn kinds(src: &str) -> Vec<Tok> {
    lex(src).unwrap().into_iter().map(|t| t.kind).collect()
}

#[test]
fn hash_and_double_dash_comments_run_to_end_of_line() {
    let src = "MATCH (:Doc)   # the source\n  |> LIMIT 3 -- keep three\n";
    assert_eq!(
        parse(src).unwrap().ops,
        vec![
            Op::Scan {
                label: "Doc".into()
            },
            Op::Limit { k: 3 }
        ]
    );
}

#[test]
fn scientific_notation_and_exact_text() {
    assert_eq!(
        kinds("1e-9 6.02E+23 .5 3"),
        vec![
            Tok::Num(1e-9),
            Tok::Num(6.02e23),
            Tok::Num(0.5),
            Tok::Num(3.0)
        ]
    );
    // `3 e` is a number then a word, and `1..3` is a range, not a decimal.
    assert_eq!(kinds("3 e"), vec![Tok::Num(3.0), Tok::Ident("e".into())]);
    assert_eq!(
        kinds("1..3"),
        vec![Tok::Num(1.0), Tok::DotDot, Tok::Num(3.0)]
    );
}

#[test]
fn unicode_strings_survive_intact() {
    let plan = parse("MATCH (:Doc) WHERE city = 'Zürich — 東京'").unwrap();
    assert_eq!(
        plan.ops[1],
        Op::Filter {
            preds: vec![Pred::Eq {
                prop: "city".into(),
                value: "Zürich — 東京".into()
            }]
        }
    );
}

#[test]
fn unicode_and_quoted_identifiers() {
    assert_eq!(
        parse("MATCH (:Größe)").unwrap().ops[0],
        Op::Scan {
            label: "Größe".into()
        }
    );
    assert_eq!(
        parse("MATCH (:`research paper`) WHERE `LIMIT` > 1")
            .unwrap()
            .ops,
        vec![
            Op::Scan {
                label: "research paper".into()
            },
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "LIMIT".into(),
                    n: 1.0
                }]
            }
        ]
    );
    assert_eq!(kinds("`a``b`"), vec![Tok::QIdent("a`b".into())]);
}

#[test]
fn string_escapes() {
    assert_eq!(
        kinds(r#"'it''s' "say \"hi\" \\""#),
        vec![Tok::Str("it's".into()), Tok::Str("say \"hi\" \\".into())]
    );
}

#[test]
fn dollar_is_a_parameter_or_a_json_path() {
    assert_eq!(
        kinds("$min $.a.b[0] $[1]"),
        vec![
            Tok::Param("min".into()),
            Tok::Path("$.a.b[0]".into()),
            Tok::Path("$[1]".into())
        ]
    );
}

#[test]
fn angle_forms_are_disambiguated() {
    assert_eq!(
        kinds("<http://ex/T>"),
        vec![Tok::Iri("<http://ex/T>".into())]
    );
    assert_eq!(
        kinds("a<-3"),
        vec![Tok::Ident("a".into()), Tok::Lt, Tok::Dash, Tok::Num(3.0)]
    );
    assert_eq!(kinds("<-["), vec![Tok::LArrow, Tok::LBracket]);
    assert_eq!(
        kinds("<> <= >= != @>"),
        vec![Tok::Ne, Tok::Le, Tok::Ge, Tok::Ne, Tok::AtGt]
    );
}

#[test]
fn signed_timestamps_parse() {
    assert_eq!(
        parse("MATCH (:E) |> AS OF @-5").unwrap().ops[1],
        Op::AsOf {
            ts: -5.0,
            axis: eg_types::wire::TimeAxis::Valid
        }
    );
}
