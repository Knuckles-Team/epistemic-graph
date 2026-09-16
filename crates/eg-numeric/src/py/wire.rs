//! Private wheel binding: eg-types remains the sole SQL source codec authority.

use eg_types::msgpack::{self, MsgpackLimits};
use eg_types::storage_wire::source_batch::MAX_SQL_SOURCE_BATCH_BYTES;
use eg_types::storage_wire::{SqlSourceBatchRequest, SqlSourceJson};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

const CODEC: &str = "eg/sql-source/v1";

fn checked_input(input: &[u8]) -> PyResult<()> {
    if input.len() > MAX_SQL_SOURCE_BATCH_BYTES {
        return Err(PyValueError::new_err(
            "SQL source input exceeds the byte limit",
        ));
    }
    Ok(())
}

fn prepare(input: &[u8]) -> Result<(Vec<u8>, String, String, String, Vec<u8>), String> {
    let request: SqlSourceBatchRequest = msgpack::decode_bounded(
        input,
        MsgpackLimits::new(
            MAX_SQL_SOURCE_BATCH_BYTES,
            msgpack::MAX_PROPERTY_ITEMS,
            msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "SQL source MessagePack input is invalid")?;
    let bytes = request.canonical_bytes()?;
    let digests = request.canonical_digests()?;
    let body = eg_types::protocol::Method::SqlSourceBatch { batch: request }.canonical_body_bytes();
    if body.is_empty() {
        return Err("SQL source method serialization failed".into());
    }
    Ok((
        bytes,
        digests.source_digest.to_hex(),
        digests.mapping_digest.to_hex(),
        digests.batch_digest.to_hex(),
        body,
    ))
}

#[pyfunction]
fn _prepare_sql_source_batch(
    py: Python<'_>,
    input: &Bound<'_, PyBytes>,
) -> PyResult<(Py<PyBytes>, String, String, String, Py<PyBytes>)> {
    let input = input.as_bytes();
    checked_input(input)?;
    // PyBytes is immutable; borrow it while detached, rather than copying an
    // unchecked Python buffer or retaining a live caller-owned mutable object.
    let (batch, source_digest, mapping_digest, batch_digest, body) =
        py.detach(|| prepare(input))
            .map_err(PyValueError::new_err)?;
    Ok((
        PyBytes::new(py, &batch).unbind(),
        source_digest,
        mapping_digest,
        batch_digest,
        PyBytes::new(py, &body).unbind(),
    ))
}

#[pyfunction]
#[pyo3(signature = (input, *, raw_json = false))]
fn _canonical_sql_source_json(
    py: Python<'_>,
    input: &Bound<'_, PyBytes>,
    raw_json: bool,
) -> PyResult<Py<PyBytes>> {
    let input = input.as_bytes();
    checked_input(input)?;
    let bytes = py
        .detach(|| {
            let json = if raw_json {
                SqlSourceJson::from_bytes(eg_types::contract::RecordBytes::new(input.to_vec())?)?
            } else {
                SqlSourceJson::new(
                    msgpack::decode_property_value(input)
                        .map_err(|_| "SQL source JSON MessagePack input is invalid")?,
                )?
            };
            Ok::<_, String>(json.canonical_bytes().to_vec())
        })
        .map_err(PyValueError::new_err)?;
    Ok(PyBytes::new(py, &bytes).unbind())
}

pub(super) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__sql_source_codec__", CODEC)?;
    m.add(
        "__sql_source_limits__",
        (
            MAX_SQL_SOURCE_BATCH_BYTES,
            msgpack::MAX_PROPERTY_ITEMS,
            msgpack::DEFAULT_MAX_DEPTH,
        ),
    )?;
    m.add_function(wrap_pyfunction!(_prepare_sql_source_batch, m)?)?;
    m.add_function(wrap_pyfunction!(_canonical_sql_source_json, m)?)?;
    Ok(())
}
