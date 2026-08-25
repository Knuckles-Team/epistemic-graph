//! Error mapping: turn an `eg-core`/durable-apply string error into the SAME
//! Python exception a caller of the out-of-process socket transport already
//! gets from `epistemic_graph.client`, so `epistemic_graph.embedded`'s
//! differential-parity requirement (`EG-PYENGINE-PLAN.md` §3.2, "same
//! exception class") holds for the embedded path too.
//!
//! ## What `client.py`'s `_send` actually does (grepped this session)
//!
//! `client.py`'s `_send` (`client.py:13260-13392`) raises a bare built-in
//! `RuntimeError(err_msg)` (`client.py:13387`) for every engine error
//! **except one**: a message starting with `RESULT_TOO_LARGE` gets the
//! dedicated `ResultTooLargeError` (`client.py:2529`,
//! `client.py:13383-13386`). `grep -n "^class .*Error" epistemic_graph/client.py`
//! finds exactly four typed exception classes total —
//! `ResultTooLargeError:2529`, `StaleRouteError:2542`,
//! `LedgerNotPopulatedError:2561`, `CdcGapError:2578` — and only the first is
//! raised from a generic string-prefix match. `StaleRouteError` is raised
//! from a *structured* `{"status": "redirected", "redirect": {...}}` result
//! body (`client.py:13350-13369`), not a string prefix; `LedgerNotPopulatedError`/
//! `CdcGapError` are raised by specific sub-client call sites after
//! inspecting a typed result field (a `populated`/gap marker), not by
//! `_send`'s generic error path at all. So `INVALID_ARGUMENT:`/
//! `ACCESS_DENIED:`/`NOT_FOUND:` (the same `"PREFIX: message"` convention
//! `src/server/dispatch.rs:33` already uses) have **no dedicated class** in
//! `client.py` today — `map_engine_error` below falls through to the SAME
//! bare `RuntimeError` `_send` does for them, rather than inventing a
//! parallel embedded-only taxonomy.
//!
//! `resolve_client_error_class` is exposed separately for a domain lane that
//! needs to raise `StaleRouteError`/`LedgerNotPopulatedError`/`CdcGapError`
//! directly with their own structured constructor arguments (a route dict, a
//! populated flag, a gap cursor) — that is a per-domain judgment call about
//! *when* one of those applies, not a string-prefix convention this shared
//! function can decide for every caller.
//!
//! ## Cheap by construction (`EG-PYENGINE-PLAN.md` §12.1)
//!
//! `epistemic_graph.client` is imported at most ONCE per process, cached in a
//! `std::sync::OnceLock` via pyo3 0.29's `OnceLockExt::get_or_init_py_attached`
//! (pyo3's own `GILOnceCell` is `pub(crate)`-private as of 0.29.2 — this is
//! its documented replacement: an ordinary `OnceLock` whose init closure runs
//! with the GIL safely re-attached if it has to block, so it composes with
//! this crate's own `Python::attach` re-entry the same way). Every subsequent
//! error maps via a plain cached-module `getattr`, not a fresh
//! `sys.modules` import — this function only runs on the error path, but
//! there is no reason to pay an import lookup twice just because the first
//! call already resolved it.

use std::sync::OnceLock;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::sync::OnceLockExt;
use pyo3::types::{PyModule, PyType};

/// The `epistemic_graph.client` module, imported once and cached. `None`
/// means the import failed — e.g. this wheel does not ship
/// `epistemic_graph.client` alongside the compiled engine kernel (see
/// BUG-PE-002, `EG-PYENGINE-PLAN.md` §12.1's bug register: the packaging lane
/// owns fixing that; this module's job is to degrade to a plain
/// `RuntimeError` rather than panic or raise an unrelated `ImportError` in
/// its place when it happens).
static CLIENT_MODULE: OnceLock<Option<Py<PyModule>>> = OnceLock::new();

fn client_module(py: Python<'_>) -> Option<Bound<'_, PyModule>> {
    let cached: &Option<Py<PyModule>> = CLIENT_MODULE.get_or_init_py_attached(py, || {
        py.import("epistemic_graph.client").ok().map(Bound::unbind)
    });
    cached.as_ref().map(|module| module.bind(py).clone())
}

/// Look up one of `epistemic_graph.client`'s existing typed exception classes
/// by name (`"ResultTooLargeError"`, `"StaleRouteError"`,
/// `"LedgerNotPopulatedError"`, `"CdcGapError"`, ...) for a domain method that
/// needs to raise one directly with its own constructor arguments. `None`
/// when the cached module import failed or the name is not defined there —
/// the caller decides the fallback; this function never invents a substitute
/// exception.
pub(crate) fn resolve_client_error_class<'py>(
    py: Python<'py>,
    name: &str,
) -> Option<Bound<'py, PyType>> {
    client_module(py)?
        .getattr(name)
        .ok()?
        .cast_into::<PyType>()
        .ok()
}

/// Map an `eg-core`/durable-apply string error (the `"PREFIX: message"`
/// convention `src/server/dispatch.rs:33` already uses) onto the SAME
/// exception `client.py`'s `_send` raises for the same failure — see the
/// module doc for exactly which prefix has a dedicated class today. Every
/// domain module calls this ONE function rather than constructing its own
/// `PyValueError`/`PyKeyError` ad hoc (the pattern this replaces — the
/// prototype's `add_node`/`create_graph`/`get_node_properties` did exactly
/// that at `lib.rs:159,177,196` before this Wave).
pub(crate) fn map_engine_error<E: std::fmt::Display>(err: E) -> PyErr {
    let message = err.to_string();
    if message.starts_with("RESULT_TOO_LARGE") {
        let mapped = Python::attach(|py| {
            resolve_client_error_class(py, "ResultTooLargeError")
                .map(|class| PyErr::from_type(class, (message.clone(),)))
        });
        if let Some(mapped) = mapped {
            return mapped;
        }
    }
    // No dedicated class for INVALID_ARGUMENT/ACCESS_DENIED/NOT_FOUND/
    // anything else — matches `_send`'s own fallback (`client.py:13387`)
    // exactly.
    PyRuntimeError::new_err(message)
}

// No `#[cfg(test)]` module here: this file only compiles under the `python`
// feature (`extension-module` mode, dlopen'd BY Python), which — per
// `lib.rs`'s own test-module doc — cannot attach a Python interpreter inside
// a `cargo test` binary. The pyo3-layer proof for this crate is the
// maturin-built wheel + `tests/test_engine_smoke.py`, not `cargo test
// --features python`; see `lib.rs`'s `mod tests` doc comment for the full
// reasoning (unchanged by this Wave).
