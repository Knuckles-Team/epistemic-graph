//! Private wheel binding: the `eg2.` canonical body is eg-types' own bytes.
//!
//! The server MACs `Method::canonical_body_bytes()` of the request it decoded.
//! Handing the client's own request frame to the same decoder and encoder makes
//! the signed body identical by construction for every method -- field order,
//! defaults, sorted maps and float/byte widths are never restated in Python.

use eg_types::protocol::Method;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

const CODEC: &str = "eg/method-body/v1";

#[pyfunction]
fn _canonical_request_body(py: Python<'_>, frame: &Bound<'_, PyBytes>) -> PyResult<Py<PyBytes>> {
    let frame = frame.as_bytes();
    let body = py
        .detach(|| Method::canonical_body_of_request_frame(frame))
        .map_err(PyValueError::new_err)?;
    Ok(PyBytes::new(py, &body).unbind())
}

pub(super) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__method_body_codec__", CODEC)?;
    m.add_function(wrap_pyfunction!(_canonical_request_body, m)?)?;
    Ok(())
}
