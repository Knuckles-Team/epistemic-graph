//! `medulla.numeric.TenantPool`: the Python client's per-tenant pool is
//! the `medulla-types` placement-directory `TenantPool` (MEDULLA-PLACEMENT-R611.1), so one
//! implementation exists. The Python side builds and closes clients (they are
//! async); this type decides hits, recency and eviction.

use medulla_types::placement_directory::{PlacementModelError, TenantPool};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

fn value_err(error: PlacementModelError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

/// A bounded LRU of per-tenant clients.
#[pyclass(name = "TenantPool", module = "medulla.numeric")]
pub(super) struct PyTenantPool {
    inner: TenantPool<Py<PyAny>>,
}

#[pymethods]
impl PyTenantPool {
    #[new]
    fn new(capacity: usize) -> PyResult<Self> {
        Ok(Self {
            inner: TenantPool::new(capacity).map_err(value_err)?,
        })
    }

    /// The pooled client for `tenant` (now most recently used), or `None`.
    fn get(&mut self, py: Python<'_>, tenant: &str) -> PyResult<Option<Py<PyAny>>> {
        Ok(self
            .inner
            .get(tenant)
            .map_err(value_err)?
            .map(|client| client.clone_ref(py)))
    }

    /// Pool `client` for `tenant`; returns `(tenant, client)` the pool evicted
    /// or replaced, for the caller to close, else `None`.
    fn insert(&mut self, tenant: &str, client: Py<PyAny>) -> PyResult<Option<(String, Py<PyAny>)>> {
        self.inner.insert(tenant, client).map_err(value_err)
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    #[getter]
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    #[getter]
    fn evictions(&self) -> u64 {
        self.inner.evictions()
    }

    /// Pooled tenants, least recently used first.
    fn tenants(&self) -> Vec<String> {
        self.inner
            .tenants()
            .into_iter()
            .map(str::to_string)
            .collect()
    }
}

pub(super) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyTenantPool>()
}
