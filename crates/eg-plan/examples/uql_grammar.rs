//! Print the generated UQL artifacts (UQL-10) — the reference EBNF that `docs/uql.md`
//! embeds, or the NL planner's system prompt — straight from `eg_plan::uql::grammar`.
//!
//! ```text
//! cargo run -p eg-plan --example uql_grammar            # the EBNF block
//! cargo run -p eg-plan --example uql_grammar -- prompt  # the NL system prompt
//! ```

fn main() {
    let what = std::env::args().nth(1).unwrap_or_default();
    if what == "prompt" {
        println!("{}", eg_plan::uql::grammar::nl_system_prompt());
    } else {
        print!("{}", eg_plan::uql::grammar::ebnf());
    }
}
