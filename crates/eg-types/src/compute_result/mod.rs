//! Result bodies of the `compute` contract domain.
//!
//! Each type here is the Rust body a `result_contract::compute` marker declares, so
//! the published result schema and the bytes a handler encodes are the same type.
//! They live at the bottom of the DAG for the same reason the request DTOs in
//! [`crate::wire`] do; the domain crates that compute them re-export them
//! (`pub use eg_types::compute_result::finance::Quote;`) so their algorithm code is
//! unchanged. Each family is gated by the feature that gates its `Method` variants,
//! so a build without a compute domain drops its result types too.

pub mod algorithms;
#[cfg(feature = "datascience")]
pub mod datascience;
#[cfg(feature = "compute-dist")]
pub mod distributed;
#[cfg(feature = "finance")]
pub mod finance;
#[cfg(feature = "graphlearn")]
pub mod graphlearn;
#[cfg(feature = "mining")]
pub mod mining;
#[cfg(feature = "ml-pipeline")]
pub mod pipeline;
