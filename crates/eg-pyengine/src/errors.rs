//! Error mapping: turn an `eg-core`/durable-apply string error into the SAME
//! Python exception a caller of the out-of-process socket transport already
//! gets from `epistemic_graph.client`, so `epistemic_graph.embedded`'s
//! differential-parity requirement (`EG-PYENGINE-PLAN.md` §3.2, "same
//! exception class") holds for the embedded path too.
//!
//! ## What `client.py`'s `_send` does
//!
//! `_send` raises `EngineResponseError(code, detail)` for an engine refusal:
//! `code` is the stable wire code, `detail` the optional diagnostic text.
//! `ResultTooLargeError` and `StaleRouteError` are subclasses of it.
//! `StaleRouteError` is raised from a *structured*
//! `{"status": "redirected", "redirect": {...}}` result body, not from the
//! code alone; `LedgerNotPopulatedError`/`CdcGapError` are raised by specific
//! sub-client call sites after inspecting a typed result field. The embedded
//! path receives the same refusals as `"CODE: detail"` strings (the
//! convention the server dispatch uses), so `map_engine_error` below splits
//! that string and raises the same class with the same arguments. A message
//! with no leading code stays a plain `RuntimeError`, as in `_send`.
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
    let Some((code, detail)) = split_wire_code(&message) else {
        return PyRuntimeError::new_err(message);
    };
    // `_send` raises `EngineResponseError(code, detail)` for every coded
    // refusal and its `ResultTooLargeError` subclass for that one code; the
    // embedded path raises the same class with the same two arguments.
    let class_name = if code == "RESULT_TOO_LARGE" {
        "ResultTooLargeError"
    } else {
        "EngineResponseError"
    };
    let mapped = Python::attach(|py| {
        resolve_client_error_class(py, class_name)
            .map(|class| PyErr::from_type(class, (code.to_string(), detail.map(str::to_string))))
    });
    mapped.unwrap_or_else(|| PyRuntimeError::new_err(message.clone()))
}

/// Split the engine's `"CODE: detail"` error convention into its stable wire
/// code and optional diagnostic detail. `None` when the text does not start
/// with an upper-snake-case code, so an uncoded message stays a plain
/// `RuntimeError` exactly as `_send` treats a response without a code.
fn split_wire_code(message: &str) -> Option<(&str, Option<&str>)> {
    const TOO_LARGE: &str = "RESULT_TOO_LARGE";
    if message.starts_with(TOO_LARGE) && !message.starts_with("RESULT_TOO_LARGE: ") {
        let rest = message[TOO_LARGE.len()..].trim_start();
        return Some((TOO_LARGE, Some(rest).filter(|text| !text.is_empty())));
    }
    let (code, detail) = match message.split_once(": ") {
        Some((code, detail)) => (code, Some(detail).filter(|text| !text.is_empty())),
        None => (message, None),
    };
    let mut chars = code.chars();
    let starts_upper = chars.next().is_some_and(|c| c.is_ascii_uppercase());
    let rest_is_code = chars.all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_');
    (starts_upper && rest_is_code).then_some((code, detail))
}

// No `#[cfg(test)]` module here: this file only compiles under the `python`
// feature (`extension-module` mode, dlopen'd BY Python), which — per
// `lib.rs`'s own test-module doc — cannot attach a Python interpreter inside
// a `cargo test` binary. The pyo3-layer proof for this crate is the
// maturin-built wheel + `tests/test_engine_smoke.py`, not `cargo test
// --features python`; see `lib.rs`'s `mod tests` doc comment for the full
// reasoning (unchanged by this Wave).
