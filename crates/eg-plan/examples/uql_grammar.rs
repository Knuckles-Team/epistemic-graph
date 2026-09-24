//! Print or install the generated UQL artifacts (UQL-10) — the reference EBNF blocks that
//! `docs/uql.md` embeds (UQL's and DecideText's, EH-452), or the NL planner's system
//! prompt — straight from `eg_plan::uql::grammar` / `eg_plan::decide_text::grammar`.
//!
//! ```text
//! cargo run -q -p eg-plan --example uql_grammar -- write-docs  # rewrite both docs/uql.md blocks
//! cargo run -q -p eg-plan --example uql_grammar                # print the UQL EBNF block
//! cargo run -q -p eg-plan --example uql_grammar -- decide-text # print the DecideText EBNF block
//! cargo run -q -p eg-plan --example uql_grammar -- prompt      # print the NL system prompt
//! ```

/// `(marker name, generated text)` for every generated block of `docs/uql.md`.
fn blocks() -> [(&'static str, String); 2] {
    [
        ("uql-grammar", eg_plan::uql::grammar::ebnf()),
        ("decide-text-grammar", eg_plan::decide_text::grammar::ebnf()),
    ]
}

/// Replace the body of the `name` generated block of `doc` with `body`.
fn splice(doc: &str, name: &str, body: &str) -> Result<String, String> {
    let begin = format!("<!-- BEGIN GENERATED: {name} -->\n```text\n");
    let end_marker = format!("```\n<!-- END GENERATED: {name} -->");
    let start = doc.find(&begin).ok_or(format!(
        "docs/uql.md lacks the BEGIN GENERATED: {name} marker"
    ))? + begin.len();
    let end = start
        + doc[start..].find(&end_marker).ok_or(format!(
            "docs/uql.md lacks the END GENERATED: {name} marker"
        ))?;
    Ok(format!("{}{body}{}", &doc[..start], &doc[end..]))
}

fn write_docs() -> Result<(), String> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/uql.md");
    let mut doc = std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    for (name, body) in blocks() {
        doc = splice(&doc, name, &body)?;
    }
    std::fs::write(&path, doc).map_err(|e| format!("{}: {e}", path.display()))
}

fn main() {
    match std::env::args().nth(1).as_deref() {
        Some("prompt") => println!("{}", eg_plan::uql::grammar::nl_system_prompt()),
        Some("decide-text") => print!("{}", eg_plan::decide_text::grammar::ebnf()),
        Some("write-docs") => {
            if let Err(e) = write_docs() {
                eprintln!("uql_grammar: {e}");
                std::process::exit(1);
            }
        }
        _ => print!("{}", eg_plan::uql::grammar::ebnf()),
    }
}
