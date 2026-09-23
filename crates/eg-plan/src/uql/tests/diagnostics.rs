//! Structured diagnostics (UQL-02): codes, spans, expected sets, multi-line carets.

use crate::uql::{parse, UqlCode, ALL_CODES};

#[test]
fn an_unknown_stage_names_every_alternative() {
    let e = parse("MATCH (:Doc) |> FROBNICATE").unwrap_err();
    assert_eq!(e.code, UqlCode::UnknownStage);
    assert!(e.expected.contains(&"`TRAVERSE`".to_string()));
    assert!(e.expected.contains(&"`MATCH`".to_string()));
    assert_eq!(&"MATCH (:Doc) |> FROBNICATE"[e.at..e.end], "FROBNICATE");
}

#[test]
fn the_caret_lands_on_the_right_line_and_column() {
    let src = "MATCH (:Café)\n  |> LIMIT )";
    let e = parse(src).unwrap_err();
    let rendered = e.render(src);
    assert!(rendered.contains("at 2:12"), "{rendered}");
    assert!(rendered.contains("  2 |   |> LIMIT )"), "{rendered}");
    assert!(
        rendered.lines().nth(2).unwrap().ends_with("           ^"),
        "{rendered}"
    );
}

#[test]
fn the_caret_spans_the_whole_token() {
    let src = "MATCH () |> FROBNICATE";
    let rendered = parse(src).unwrap_err().render(src);
    assert!(rendered.contains("^^^^^^^^^^"), "{rendered}");
}

#[test]
fn lexical_errors_carry_codes() {
    assert_eq!(
        parse("MATCH () WHERE a = 'x").unwrap_err().code,
        UqlCode::UnterminatedString
    );
    assert_eq!(
        parse("MATCH (:`x)").unwrap_err().code,
        UqlCode::UnterminatedIdentifier
    );
    assert_eq!(
        parse("MATCH () |> LIMIT %").unwrap_err().code,
        UqlCode::UnexpectedCharacter
    );
    assert_eq!(
        parse("MATCH () |> LIMIT $ 5").unwrap_err().code,
        UqlCode::EmptyParameterName
    );
}

#[test]
fn integers_are_exact() {
    assert_eq!(
        parse("MATCH () |> LIMIT 1.5").unwrap_err().code,
        UqlCode::ExpectedInteger
    );
    assert_eq!(
        parse("MATCH () |> LIMIT 1e3").unwrap_err().code,
        UqlCode::ExpectedInteger
    );
    assert_eq!(
        parse("MATCH () |> LIMIT 99999999999999999999999")
            .unwrap_err()
            .code,
        UqlCode::ExpectedInteger
    );
    assert_eq!(
        parse("MATCH () |> TRAVERSE R{3,1}").unwrap_err().code,
        UqlCode::InvalidRange
    );
}

#[test]
fn trailing_tokens_and_credentials_are_named() {
    assert_eq!(
        parse("MATCH () LIMIT 1").unwrap_err().code,
        UqlCode::TrailingTokens
    );
    let e = parse("FOREIGN ENGINE 'tcp://x'").unwrap_err();
    assert_eq!(e.code, UqlCode::CredentialBearingSpec);
}

#[test]
fn every_code_has_a_distinct_stable_name() {
    let mut names: Vec<&str> = ALL_CODES.iter().map(|c| c.as_str()).collect();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), ALL_CODES.len());
}
