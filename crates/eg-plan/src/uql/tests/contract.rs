//! The executable contracts (UQL-06 / UQL-10):
//!  * the parser's dispatch table and the grammar table name the same keywords;
//!  * every grammar example parses — or, in a build without its feature, fails with
//!    `UQL_FEATURE_NOT_IN_BUILD` naming exactly that feature;
//!  * `docs/uql.md` embeds the generated EBNF verbatim, and every ```uql block in the
//!    UQL docs parses;
//!  * the printer's reserved words cover every grammar keyword;
//!  * EVERY `wire::Op` / `wire::Pred` variant compiled into this build prints and re-parses
//!    to its canonical form, or sits on the (empty) builder-only allowlist.

use std::collections::BTreeSet;

use crate::uql::grammar::{self, Feature, PRODUCTIONS};
use crate::uql::parser::Parser;
use crate::uql::{canonicalize, parse, parse_statement, Params, UqlCode, UqlError};
use eg_types::wire::{
    uql_sample_op, uql_sample_pred, Op, OpKind, Plan, PredKind, UQL_RESERVED_WORDS,
};

/// Variants with NO text spelling, each with the diagnostic a caller gets. EMPTY: every
/// wire variant is expressible in UQL. (Credential-bearing `ForeignScan` SPECS are refused
/// by value, not by variant — see `credential_bearing_foreign_specs_are_refused`.)
const BUILDER_ONLY: &[(OpKind, &str)] = &[];

/// Is `e` the refusal of a clause whose feature this build lacks?
fn not_in_build(e: &UqlError) -> bool {
    let all = [
        Feature::Text,
        Feature::Owl,
        Feature::WasmUdf,
        Feature::Federation,
        Feature::Geo,
        Feature::Tensor,
        Feature::Stream,
        Feature::Timeseries,
        Feature::Probabilistic,
        Feature::Epistemic,
        Feature::Numeric,
    ];
    e.code == UqlCode::FeatureNotInBuild
        && all
            .iter()
            .any(|f| !f.enabled() && e.msg.contains(&format!("`{}`", f.name())))
}

// spec: EG-FEDERATED-QUERY-R061
#[test]
fn dispatch_table_and_grammar_name_the_same_keywords() {
    let table: BTreeSet<&str> = Parser::stage_table()
        .iter()
        .map(|(kw, _)| *kw)
        .chain(["MATCH", "VALIDATE"])
        .collect();
    let grammar: BTreeSet<&str> = grammar::stage_keywords().into_iter().collect();
    assert_eq!(table, grammar);
}

// spec: EG-FEDERATED-QUERY-R061
#[test]
fn every_grammar_example_parses_or_names_its_missing_feature() {
    for prod in PRODUCTIONS.iter().filter(|p| !p.example.is_empty()) {
        match parse_statement(prod.example, &Params::new()) {
            Ok(_) => {}
            Err(e) => assert!(
                prod.feature.is_some_and(|f| !f.enabled()) && not_in_build(&e),
                "grammar example `{}` ({}) failed: {}",
                prod.example,
                prod.name,
                e.render(prod.example)
            ),
        }
    }
}

#[test]
fn printer_reserves_every_grammar_keyword() {
    let reserved: BTreeSet<&str> = UQL_RESERVED_WORDS.iter().copied().collect();
    let missing: Vec<String> = grammar::keywords()
        .into_iter()
        .filter(|k| !reserved.contains(k.as_str()))
        .collect();
    assert!(missing.is_empty(), "add to UQL_RESERVED_WORDS: {missing:?}");
}

fn docs(name: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs")
        .join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// The body of `docs/uql.md`'s `name` generated block.
fn generated_block(doc: &str, name: &str) -> String {
    let begin = format!("<!-- BEGIN GENERATED: {name} -->\n```text\n");
    let end = format!("```\n<!-- END GENERATED: {name} -->");
    let start = doc
        .find(&begin)
        .unwrap_or_else(|| panic!("docs/uql.md lacks the generated `{name}` block"));
    let body = &doc[start + begin.len()..];
    let stop = body
        .find(&end)
        .unwrap_or_else(|| panic!("unterminated generated `{name}` block"));
    body[..stop].to_string()
}

#[test]
fn docs_embed_the_generated_grammars() {
    let doc = docs("uql.md");
    let regen = "run `cargo run -q -p eg-plan --example uql_grammar -- write-docs`";
    assert_eq!(
        generated_block(&doc, "uql-grammar"),
        grammar::ebnf(),
        "docs/uql.md grammar drifted; {regen}"
    );
    assert_eq!(
        generated_block(&doc, "decide-text-grammar"),
        crate::decide_text::grammar::ebnf(),
        "docs/uql.md DecideText grammar drifted; {regen}"
    );
}

/// Every ```uql fenced block in the UQL docs.
fn uql_blocks(doc: &str) -> Vec<String> {
    doc.split("```uql\n")
        .skip(1)
        .map(|rest| rest[..rest.find("```").unwrap_or(rest.len())].to_string())
        .collect()
}

#[test]
fn every_uql_block_in_the_docs_parses() {
    for name in ["uql.md", "analytics_in_uql.md"] {
        let doc = docs(name);
        for block in uql_blocks(&doc) {
            if let Err(e) = parse_statement(&block, &Params::new()) {
                assert!(not_in_build(&e), "{name}: {}", e.render(&block));
            }
        }
    }
    assert!(!uql_blocks(&docs("uql.md")).is_empty());
}

/// Print one plan and re-parse it; `Ok(())` when it round-trips (or the build lacks
/// the op's feature), the reason otherwise.
fn round_trip(plan: &Plan) -> Result<(), String> {
    let text = plan.to_uql().map_err(|e| e.to_string())?;
    match parse(&text) {
        Ok(back) if back == canonicalize(plan) => Ok(()),
        Ok(back) => Err(format!("`{text}` re-parsed to {back:?}")),
        Err(e) if not_in_build(&e) => Ok(()),
        Err(e) => Err(e.render(&text)),
    }
}

#[test]
fn every_op_variant_prints_and_reparses() {
    assert!(
        BUILDER_ONLY.is_empty(),
        "the builder-only allowlist must stay empty"
    );
    for kind in OpKind::all() {
        let plan = Plan::new(vec![uql_sample_op(kind)]);
        if let Err(why) = round_trip(&plan) {
            panic!("Op::{} has no faithful UQL spelling: {why}", kind.name());
        }
    }
}

#[test]
fn every_pred_variant_prints_and_reparses() {
    for kind in PredKind::all() {
        let plan = Plan::new(vec![
            Op::ScanAll {},
            Op::Filter {
                preds: vec![uql_sample_pred(kind)],
            },
        ]);
        if let Err(why) = round_trip(&plan) {
            panic!("Pred::{} has no faithful UQL spelling: {why}", kind.name());
        }
    }
}

#[cfg(feature = "federation")]
#[test]
fn credential_bearing_foreign_specs_are_refused() {
    use eg_types::wire::{ForeignSourceSpec, UqlPrintCode};
    let sql = Plan::new(vec![Op::ForeignScan {
        source: Box::new(ForeignSourceSpec::Sql {
            dsn: ["postgres://", "u:", "secret", "@h/db"].concat(),
            query: "select 1".into(),
            id_field: "id".into(),
            score_field: None,
            columns: Vec::new(),
        }),
        join: false,
    }]);
    let e = sql.to_uql().unwrap_err();
    assert_eq!(e.code, UqlPrintCode::CredentialBearingSpec);
    assert!(!e.detail.contains("secret"));
}
