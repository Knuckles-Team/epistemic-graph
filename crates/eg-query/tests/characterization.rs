//! CX-EG-01 characterization-test entry point.
//!
//! Cargo only auto-discovers `.rs` files directly under `tests/` as
//! integration-test binaries, not files nested in a subdirectory. This file
//! is the thin, mechanical loader that gives each
//! `tests/characterization/<fn>.rs` file a `mod` so `cargo test` finds it;
//! it carries no test logic of its own. One line is added here per
//! characterization file -- everything else in the two-commit discipline
//! (the actual `#[test]` bodies) lives under `tests/characterization/`.

#[path = "characterization/verify_schema_migrations.rs"]
mod verify_schema_migrations;
#[path = "characterization/insert_on_conflict_in.rs"]
mod insert_on_conflict_in;
#[path = "characterization/apply_txn_op.rs"]
mod apply_txn_op;
