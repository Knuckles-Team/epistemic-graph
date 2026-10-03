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
//! convention the server dispatch uses), so `map_engine_error` below
//! normalizes that string the same way the server does before the socket
//! transport ever sees it (see "Classifying a refusal" below) and raises the
//! same class with the same two arguments — never a plain `RuntimeError` for
//! a message that merely lacks a leading code, because `_send` never sees
//! one of those either.
//!
//! `resolve_client_error_class` is exposed separately for a domain lane that
//! needs to raise `StaleRouteError`/`LedgerNotPopulatedError`/`CdcGapError`
//! directly with their own structured constructor arguments (a route dict, a
//! populated flag, a gap cursor) — that is a per-domain judgment call about
//! *when* one of those applies, not a string-prefix convention this shared
//! function can decide for every caller.
//!
//! ## Classifying a refusal the SAME way the socket transport does
//!
//! The server never puts raw, unclassified text on the wire: every response
//! passes through `eg_core::protocol::Response::err`, which keeps a
//! `"CODE: detail"` prefix (or a bare code) only when `CODE` is one of the
//! engine's *declared* refusal codes, and otherwise folds the whole thing
//! into `INTERNAL` with a fixed `"unclassified engine refusal"` detail —
//! discarding the original text rather than exposing an unregistered token,
//! or something that merely looks like `RESULT_TOO_LARGE`, as if it were a
//! real declared code. `_raise_send_error` in `client.py` then reads that
//! already-normalized `error`/`error_detail` pair straight off the wire, so
//! an uncoded refusal from the server is never a plain `RuntimeError` there
//! — it is `EngineResponseError("INTERNAL", "unclassified engine refusal")`.
//! `map_engine_error` below calls that SAME `Response::err` (via
//! `classify_engine_refusal`) rather than re-implementing its decision, so
//! the embedded path cannot drift from it: an uncoded message, an
//! unregistered uppercase-looking prefix, and a `RESULT_TOO_LARGE`-lookalike
//! all classify to `INTERNAL` on both transports, and a real declared code
//! keeps its own detail on both.
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

use eg_core::protocol::Response;
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
    let (code, detail) = classify_engine_refusal(err.to_string());
    // `_raise_send_error` in `client.py` raises `ResultTooLargeError` for
    // exactly the one declared `RESULT_TOO_LARGE` code and
    // `EngineResponseError` for every other declared code (including the
    // `INTERNAL` fallback `Response::err` assigns to anything undeclared) —
    // the same two-way split, driven by the same normalized code, here.
    let class_name = if code == "RESULT_TOO_LARGE" {
        "ResultTooLargeError"
    } else {
        "EngineResponseError"
    };
    let mapped = Python::attach(|py| {
        resolve_client_error_class(py, class_name)
            .map(|class| PyErr::from_type(class, (code.clone(), detail.clone())))
    });
    mapped.unwrap_or_else(|| {
        PyRuntimeError::new_err(
            detail.map_or_else(|| code.clone(), |detail| format!("{code}: {detail}")),
        )
    })
}

/// The exact `(code, detail)` `eg_core::protocol::Response::err` would place
/// on the wire for this refusal text. The single source of truth
/// `map_engine_error` and its own tests both derive from — calling the
/// authoritative shared normalization directly rather than re-implementing
/// its "is this a declared code" decision as a local prefix heuristic (the
/// bug this replaces: a previous `split_wire_code` here treated ANY
/// capitalized-looking prefix as a code and special-cased only an exact
/// `"RESULT_TOO_LARGE: "` prefix, so an unregistered uppercase token and a
/// `RESULT_TOO_LARGE`-lookalike both classified differently here than on the
/// socket transport).
fn classify_engine_refusal(message: String) -> (String, Option<String>) {
    let normalized = Response::err(0, message);
    let code = normalized.error.unwrap_or_else(|| "INTERNAL".to_string());
    (code, normalized.error_detail)
}

#[cfg(test)]
mod tests {
    //! `classify_engine_refusal` calls straight into
    //! `eg_core::protocol::Response::err` and touches no pyo3/GIL state, so
    //! — unlike `map_engine_error` itself, which needs `Python::attach` and
    //! cannot run inside a `cargo test` binary built in `extension-module`
    //! mode (see `lib.rs`'s `mod tests` doc comment for the full reasoning,
    //! unchanged by this Wave) — it is safe to exercise directly here. This
    //! is the Rust half of the transport-parity proof for `EG-PYENGINE-
    //! PLAN.md` §3.2's "same exception class" requirement; the full
    //! exception type/code/detail parity across both transports is
    //! `tests/parity/test_parity_errors.py`.
    use super::classify_engine_refusal;

    #[test]
    fn an_uncoded_message_normalizes_to_internal_unclassified() {
        assert_eq!(
            classify_engine_refusal("graph 'missing' not found".to_string()),
            (
                "INTERNAL".to_string(),
                Some("unclassified engine refusal".to_string())
            ),
        );
    }

    #[test]
    fn an_unregistered_uppercase_token_is_not_treated_as_a_declared_code() {
        assert_eq!(
            classify_engine_refusal("NOT_A_REAL_CODE: boom".to_string()),
            (
                "INTERNAL".to_string(),
                Some("unclassified engine refusal".to_string())
            ),
        );
    }

    #[test]
    fn a_result_too_large_lookalike_is_not_the_real_refusal() {
        assert_eq!(
            classify_engine_refusal("RESULT_TOO_LARGEISH: not real".to_string()),
            (
                "INTERNAL".to_string(),
                Some("unclassified engine refusal".to_string())
            ),
        );
    }

    #[test]
    fn the_real_result_too_large_code_keeps_its_detail() {
        assert_eq!(
            classify_engine_refusal(
                "RESULT_TOO_LARGE: 50000 nodes exceeds the configured cap".to_string()
            ),
            (
                "RESULT_TOO_LARGE".to_string(),
                Some("50000 nodes exceeds the configured cap".to_string())
            ),
        );
    }

    #[test]
    fn a_real_registered_code_keeps_its_own_detail() {
        assert_eq!(
            classify_engine_refusal("GRAPH_NOT_FOUND: kg".to_string()),
            ("GRAPH_NOT_FOUND".to_string(), Some("kg".to_string())),
        );
    }
}
