//! `datascience` domain — pyo3 stub (Wave 0, `EG-PYENGINE-PLAN.md` §4.1). This
//! file exists ONLY so `crates/eg-pyengine/src/lib.rs`'s module tree compiles
//! and `PyEngine::datascience()` has a stable, already-registered return type
//! before the `datascience` Wave-1 lane starts — that lane's own first commit is
//! "flesh out my already-declared file," never "add a new `mod` line" or a
//! new `lib.rs` accessor (`EG-PYENGINE-PLAN.md` §5.0: no Wave-1 lane edits
//! `lib.rs`/`Cargo.toml` after Wave 0 lands).
//!
//! Deliberately near-empty: a plain struct plus an empty `#[pymethods]`
//! block, nothing more (`EG-PYENGINE-PLAN.md` §12.1 — no trait hierarchy, no
//! macro DSL, no generic `DomainOps<T>` to produce this file; the `datascience`
//! lane should find nothing here to unpick).

use pyo3::prelude::*;

use crate::authority::EmbeddedAuthority;
use crate::SharedRegistry;

/// One in-process handle to the `datascience` surface, sharing the SAME
/// registry + authority the `Engine` it was accessed from carries
/// (`PyEngine::datascience()`, `lib.rs`) — an `Arc`-backed clone, never a copy of
/// registry or policy state.
#[pyclass(module = "epistemic_graph.engine", name = "Datascience")]
pub(crate) struct PyDatascienceOps {
    /// Unused until the `datascience` lane adds its first `#[pymethods]` that
    /// reads it — kept `pub(crate)` (not private) so `lib.rs`'s
    /// `PyEngine::datascience()` accessor can construct this struct directly, the
    /// same shape every other domain accessor uses.
    #[allow(dead_code)]
    pub(crate) registry: SharedRegistry,
    #[allow(dead_code)]
    pub(crate) authority: EmbeddedAuthority,
}

#[pymethods]
impl PyDatascienceOps {}
