//! libFuzzer target for the UQL parser (CONCEPT:EG-KG.query.uql-libfuzzer-parse-target).
//!
//! Feeds ARBITRARY BYTES to `eg_plan::uql::parse` and asserts the parser never panics
//! or hangs — it must always return `Ok(Plan)` or `Err(UqlError)` for ANY input. The
//! proptest harness (`crates/eg-plan/tests/fuzz_pipelines.rs`,
//! CONCEPT:EG-KG.query.pipeline-fuzz) already fuzzes RANDOM but VALID pipelines on
//! stable; this covers the complementary UNSTRUCTURED byte-level surface that only a
//! coverage-guided fuzzer reaches, hardening the hand-written lexer/parser against
//! malformed input.
//!
//! It also checks the PRINTER contract on every input that parses (UQL-06): a parsed
//! plan prints, and the printed text re-parses to the identical plan — so the fuzzer
//! searches for inputs whose canonical spelling does not round-trip. Deeply nested
//! input must fail with `UQL_NESTING_TOO_DEEP`, never overflow the stack.
//!
//! Run (needs nightly + `cargo install cargo-fuzz`):
//!   cargo +nightly fuzz run uql_parse -- -runs=10000   # short smoke
//!   cargo +nightly fuzz run uql_parse                  # open-ended soak

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Only well-formed UTF-8 reaches the parser (UQL is text); non-UTF-8 inputs are
    // out of the parser's contract and are skipped, keeping the corpus focused on the
    // lexer/parser rather than the UTF-8 boundary.
    if let Ok(src) = std::str::from_utf8(data) {
        // The parser must total: Ok(Plan) or Err(UqlError), never a panic/hang. The
        // result is intentionally dropped — we assert only that `parse` returns.
        let params = eg_plan::uql::Params::new();
        let Ok(stmt) = eg_plan::uql::parse_statement(src, &params) else {
            return;
        };
        if let eg_plan::uql::Body::Pipeline(plan) = &stmt.body {
            let text = plan.to_uql().expect("a parsed plan always prints");
            let back = eg_plan::uql::parse(&text).expect("printed UQL re-parses");
            assert_eq!(&back, plan, "print/parse round trip diverged for `{text}`");
        }
    }
});
