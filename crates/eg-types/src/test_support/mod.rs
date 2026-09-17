//! Shared test fixtures for eg-types and its consumers (feature `test-support`).
//!
//! `cfg(test)` does not cross a crate boundary, so a consumer's tests reach these
//! through the non-default `test-support` feature declared as a dev-dependency
//! (the same precedent as eg-query's `dev-scope-grant`). Production builds never
//! enable it.

pub mod sql_source;
