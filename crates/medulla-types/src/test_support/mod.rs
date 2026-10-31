//! Shared test fixtures for medulla-types and its consumers (feature `test-support`).
//!
//! `cfg(test)` does not cross a crate boundary, so a consumer's tests reach these
//! through the non-default `test-support` feature declared as a dev-dependency
//! (the same precedent as medulla-query's `dev-scope-grant`). Production builds never
//! enable it.

pub mod change_envelope;
pub mod contract_wave;
pub mod decision;
pub mod host_start;
pub mod identity;
pub mod method_bodies;
pub mod repository_index;
pub mod source_ingestion;
pub mod sql_source;
pub mod topology;
