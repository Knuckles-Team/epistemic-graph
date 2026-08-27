//! CX-EG-02 characterization-test entry point.
//!
//! Cargo only auto-discovers `.rs` files directly under `tests/` as
//! integration-test binaries, not files nested in a subdirectory. This file
//! is the thin, mechanical loader that gives each
//! `tests/characterization/<fn>.rs` file a `mod` so `cargo test` finds it;
//! it carries no test logic of its own. One line is added here per
//! characterization file -- everything else in the two-commit discipline
//! (the actual `#[test]` bodies) lives under `tests/characterization/`.

#[path = "characterization/knn_similarity.rs"]
mod knn_similarity;
