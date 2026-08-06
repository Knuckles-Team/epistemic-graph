//! `eg-viz-core` — native visualization contracts (D-VZ-1, lane V0).
//!
//! Neither `agent-utilities` nor `epistemic-graph` has a visualization library,
//! and `matplotlib` does not scale: it draws every point regardless of how many
//! pixels exist. `open-source-libraries/xy` (Apache-2.0) solves exactly that with
//! a Rust LOD core + columnar store + binary view protocol + GPU client — see
//! `plans/au-eg-program/PROGRAM.md` ("Native visualization — assimilating xy into
//! epistemic-graph"). **Decision: reimplement the architecture in-tree; do not
//! depend on the `xy` PyPI package**, which is alpha and Python-first.
//!
//! This crate is lane **V0**: contracts only. It defines the surface every later
//! visualization lane (V1-V8) implements against, and ships no ColumnStore, no LOD
//! kernel, and no renderer itself:
//!
//! - [`spec`] — [`spec::ViewSpec`], the declarative chart IR (marks, encodings,
//!   scales, facets, theme tokens, interaction flags). Serde-serializable,
//!   versioned, round-trip tested.
//! - [`result`] — [`result::ViewResult`], the binary tile/payload metadata a
//!   render produces: row count, LOD tier actually used, whether it is exact or
//!   density-approximated, query hash, seed, wall time. Carries the SAME
//!   exact-vs-proposal trust distinction the quantum lane's `QuantumResult` does
//!   (`PROGRAM.md` "quantum-native" section) — see that module's doc for the
//!   parallel.
//! - [`capability`] — [`capability::CapabilityMatrix`], which marks are supported
//!   for static export vs. interactive vs. server-side, as data the planner and
//!   the UI both read.
//! - [`tier`] — [`tier::select_tier`], the pure, testable tier-selection function.
//!   ★ Budgets are expressed in **primitives per frame and bytes**, never in "rows
//!   in the table" — see that module's doc for why.
//! - [`job`] — the `viz` job family binding into the EXISTING `eg-jobs` analytics-
//!   job plane (CONCEPT:INT-P2-1), not a parallel job system.
//! - [`traits`] — the trait boundaries V1 (`ColumnStoreIngest`), V2 (`LodKernel`),
//!   V3a (`StaticExportBackend`), and V3b (`InteractiveRenderBackend`) implement.
//!
//! ## Crate shape
//!
//! A true leaf crate: no dependency on any other workspace crate (V0's charter row
//! reads "Depends on: —"), so the facade's `viz`/`viz-interactive`/
//! `viz-gpu-export` features (all default-off, see the root `Cargo.toml`) add
//! nothing to a default `cargo build` — this crate is not even compiled unless one
//! of those features is enabled. The `eg-jobs` interop proof in [`job`] lives
//! behind `dev-dependencies` for exactly this reason (see `Cargo.toml`).

pub mod capability;
pub mod job;
pub mod result;
pub mod spec;
pub mod tier;
pub mod traits;

pub use capability::{CapabilityEntry, CapabilityMatrix, Status, SupportLevel, Surface};
pub use job::{query_hash, spec_digest, viz_algorithm_name, ViewJobRequest, VIZ_JOB_FAMILY};
pub use result::{PayloadKind, PayloadRef, ReductionKind, ViewResult, ViewResultError};
pub use spec::{
    Aggregate, EncodingSpec, Encodings, FacetSpec, InteractionFlags, MarkKind, MarkSpec, MarkStyle,
    ScaleKind, ScaleSpec, ThemeTokens, ViewSpec,
};
pub use tier::{select_tier, FrameBudget, LodTier, TierDecision, TierInput};
pub use traits::{
    ColumnDescriptor, ColumnStoreIngest, ColumnType, InteractiveRenderBackend, LodKernel,
    StaticExportBackend, StaticExportFormat, Viewport,
};
