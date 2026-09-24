//! Print or install the generated UQL artifacts (UQL-10) — the reference EBNF that
//! `docs/uql.md` embeds, or the NL planner's system prompt — straight from
//! `eg_plan::uql::grammar`.
//!
//! ```text
//! cargo run -q -p eg-plan --example uql_grammar -- write-docs  # rewrite the docs/uql.md block in place
//! cargo run -q -p eg-plan --example uql_grammar                # print the EBNF block
//! cargo run -q -p eg-plan --example uql_grammar -- prompt      # print the NL system prompt
//! ```

const BEGIN: &str = "<!-- BEGIN GENERATED: uql-grammar -->\n```text\n";
const END: &str = "```\n<!-- END GENERATED: uql-grammar -->";

fn write_docs() -> Result<(), String> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/uql.md");
    let doc = std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    let start = doc
        .find(BEGIN)
        .ok_or("docs/uql.md lacks the BEGIN GENERATED marker")?
        + BEGIN.len();
    let end = start
        + doc[start..]
            .find(END)
            .ok_or("docs/uql.md lacks the END GENERATED marker")?;
    let updated = format!(
        "{}{}{}",
        &doc[..start],
        eg_plan::uql::grammar::ebnf(),
        &doc[end..]
    );
    std::fs::write(&path, updated).map_err(|e| format!("{}: {e}", path.display()))
}

fn main() {
    match std::env::args().nth(1).as_deref() {
        Some("prompt") => println!("{}", eg_plan::uql::grammar::nl_system_prompt()),
        Some("write-docs") => {
            if let Err(e) = write_docs() {
                eprintln!("uql_grammar: {e}");
                std::process::exit(1);
            }
        }
        _ => print!("{}", eg_plan::uql::grammar::ebnf()),
    }
}
