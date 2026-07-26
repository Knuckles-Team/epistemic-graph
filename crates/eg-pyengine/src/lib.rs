//! eg-pyengine — the in-process PyO3 embedding of the epistemic-graph engine core
//! (unified-binary program, workstream W-A; see
//! `docs/architecture/unified-inprocess-engine.md` for the full design).
//!
//! ## What this is
//!
//! The out-of-process shape (the scale-out default) reaches `eg-core`'s
//! `GraphCore`/`GraphRegistry` through the Tokio UDS/TCP server: every call pays
//! *serialize -> socket round-trip -> deserialize* against a SEPARATE process
//! (see the facade `AGENTS.md`, "why this drives architecture decisions"). This
//! crate is the OTHER shape: it embeds the SAME `eg-core` types directly in the
//! Python process as a pyo3 extension module (`epistemic_graph.engine`), so a
//! call is a plain Rust function call — no socket, no MessagePack framing, no
//! separate process to round-trip against.
//!
//! ## Why a separate crate (not a module in the facade)
//!
//! The main `epistemic-graph` wheel is a maturin `bindings = "bin"` build (it
//! ships the `epistemic-graph-server` binary; see `scripts/check_no_pyo3.sh`,
//! which asserts there is no pyo3 anywhere in `src/`/`epistemic_graph/`/the
//! top-level `Cargo.toml`/`pyproject.toml` — the SCALE-OUT build's contract). A
//! crate cannot be both a `bindings = "bin"` target and a pyo3 `cdylib` in the
//! same maturin invocation, so — mirroring `crates/eg-numeric`'s proven "one
//! kernel, two surfaces" split — this binding lives in its OWN crate with its
//! OWN `pyproject.toml`, built with a SEPARATE `maturin build -m
//! crates/eg-pyengine/Cargo.toml --features python` invocation. The resulting
//! `epistemic_graph.engine` extension is meant to be injected into the main
//! server wheel exactly the way `scripts/inject_numeric_kernel.py` already
//! injects `epistemic_graph.numeric` (a `scripts/inject_pyengine.py` sibling is
//! follow-up packaging work — see the design doc's "remaining work" section).
//!
//! It also cannot depend on the `epistemic-graph` facade crate itself: the
//! workspace DAG is `eg-types -> eg-ann -> eg-core -> eg-compute ->
//! epistemic-graph` (facade at the TOP), and the facade optionally depends on
//! THIS crate (feature `pyo3-engine`) so `cargo build --features pyo3-engine`
//! at the repo root actually compiles it — a dependency in the other direction
//! would cycle. So this crate talks to `eg-core` directly: the same layer the
//! facade's own `src/embedded.rs` (`EmbeddedEngine`, the SQLite-style in-process
//! Rust API) talks to. This crate is that SAME "engine core, in-process, no
//! socket" idea, reachable from `crates/`, where the facade cannot be a
//! dependency. The two are NOT the same code — `EmbeddedEngine` additionally
//! wires the redb durable store + the `query`/`cypher` surfaces, none of which
//! this prototype needs yet — but they are the SAME pattern against the SAME
//! `GraphCore`/`GraphRegistry`, no engine logic duplicated. See the design
//! doc's "remaining work" for the option of hoisting the shared glue (registry
//! + resolve-core-by-name) down into `eg-core` so both callers share one
//! implementation instead of two thin copies of it.
//!
//! ## Batching discipline (non-negotiable, `AGENTS.md`)
//!
//! Every exposed method here is a batch primitive over graph-resident data —
//! ONE `#[pymethods]` call does ONE engine mutation/read, matching the wire
//! protocol's own `Method::AddNode` / `Method::GetNodeProperties` shape 1:1. A
//! caller driving N independent nodes calls this N times because it has N
//! independent ops to do — but bulk work must get a real batch method here
//! (mirroring the wire's `Method::BatchUpdate` / `GetNodePropertiesBatch`)
//! rather than a per-element Python loop; see the design doc.
//!
//! ## GIL
//!
//! Every `python`-feature method releases the GIL for the actual engine call
//! via `Python::detach` (pyo3 0.29's name for what earlier pyo3 releases called
//! `Python::allow_threads` — same contract: run a closure with the GIL
//! released) — the Rust-side mutation never holds the Python interpreter lock.
//! This prototype's calls are synchronous, bounded, in-memory (no I/O), so
//! `detach` here is a correctness/consistency practice more than a latency
//! necessity today; the design doc covers the production case (a durable
//! commit that awaits an off-reactor redb fsync) where releasing the GIL
//! around that wait matters for real.
//!
//! ## What this prototype does NOT do (see the design doc)
//!
//! In-memory only — no redb durability, no `eg2.` identity/RLS, no batch ops
//! beyond one node/edge per call. It proves the embedding pattern end to end
//! (create a graph, add a node, read it back, in-process, through a real pyo3
//! boundary) without duplicating the engine's storage/durability logic.

use std::sync::Arc;

use eg_core::graph::GraphCore;
use eg_core::protocol::GraphType;
use eg_core::registry::GraphRegistry;
use parking_lot::RwLock;

/// A shared, cheaply-cloneable handle to one in-process engine's graph
/// registry. Plain `eg-core` types only — no pyo3 dependency — so this half is
/// unit-testable with a bare `cargo test -p eg-pyengine` (no `python` feature
/// needed), exactly like `crates/eg-numeric`'s pure `linalg`/`reductions`
/// modules are testable without its `python` feature.
pub type SharedRegistry = Arc<RwLock<GraphRegistry>>;

/// Open a fresh in-memory registry (`__commons__` pre-created, matching
/// `GraphRegistry::new()`'s own invariant).
pub fn new_registry() -> SharedRegistry {
    Arc::new(RwLock::new(GraphRegistry::new()))
}

/// Create a named graph. Errs if the name already exists (matching
/// `GraphRegistry::create_graph`'s own contract) — `__commons__` always exists
/// already, so creating it again is one such error.
pub fn create_graph(registry: &SharedRegistry, name: &str) -> Result<(), String> {
    registry.write().create_graph(name, GraphType::Global, None)
}

/// Resolve `graph`'s core, or a descriptive error for an unrecognized graph
/// name (matching the wire dispatch's own "graph not found" behavior).
pub fn resolve_core(registry: &SharedRegistry, graph: &str) -> Result<Arc<GraphCore>, String> {
    registry
        .read()
        .get(graph)
        .map(|entry| Arc::clone(&entry.core))
        .ok_or_else(|| format!("graph '{graph}' not found"))
}

// ---------------------------------------------------------------------------
// Python surface — pyo3 extension module `epistemic_graph.engine`.
// Gated behind the `python` feature so a plain rlib consumer of this crate
// (and a bare `cargo build -p eg-pyengine`) pulls no pyo3 at all — mirrors
// crates/eg-numeric's `py` module split exactly.
// ---------------------------------------------------------------------------
#[cfg(feature = "python")]
// pyo3 macro-generated code triggers a couple of unavoidable lints (the same
// ones crates/eg-numeric's own `py` module allows, for the same reason): a
// PyErr `.into()` round-trip, and cfgs the pyo3 macros emit that this Rust
// toolchain doesn't otherwise declare. Scoped to this optional extension
// module only.
#[allow(clippy::useless_conversion, unexpected_cfgs)]
mod py {
    use super::{create_graph, new_registry, resolve_core, SharedRegistry};
    use pyo3::exceptions::PyKeyError;
    use pyo3::prelude::*;
    use pyo3::types::PyBytes;

    /// One in-process handle to the engine core — the pyo3-visible analogue of
    /// `epistemic_graph::embedded::EmbeddedEngine`, minus durability (see the
    /// crate-level doc's "what this prototype does NOT do").
    ///
    /// Cheaply cloneable internally (`Arc`-backed registry): several Python
    /// call sites sharing one `Engine()` instance drive ONE in-process engine.
    #[pyclass(module = "epistemic_graph.engine", name = "Engine")]
    struct PyEngine {
        registry: SharedRegistry,
    }

    #[pymethods]
    impl PyEngine {
        /// Open a fresh in-memory engine.
        #[new]
        fn new() -> Self {
            PyEngine {
                registry: new_registry(),
            }
        }

        /// Create a named graph — ONE call, matching the wire `Method`
        /// (`CreateGraph`-shaped) this mirrors.
        fn create_graph(&self, py: Python<'_>, name: String) -> PyResult<()> {
            let registry = self.registry.clone();
            py.detach(move || create_graph(&registry, &name).map_err(PyKeyError::new_err))
        }

        /// AddNode — ONE batched call, no return payload (mirrors the wire
        /// `Method::AddNode`). `properties_msgpack` is the SAME MessagePack
        /// encoding `NodeClient.add` already produces client-side
        /// (`msgpack.packb`) for the socket transport — this boundary never
        /// decodes it, it is stored (and later returned) exactly as given,
        /// byte-for-byte identical to what the out-of-process path stores.
        fn add_node(
            &self,
            py: Python<'_>,
            graph: String,
            node_id: String,
            properties_msgpack: Vec<u8>,
        ) -> PyResult<()> {
            let registry = self.registry.clone();
            py.detach(move || {
                let core = resolve_core(&registry, &graph).map_err(PyKeyError::new_err)?;
                core.add_node(node_id, properties_msgpack);
                Ok(())
            })
        }

        /// GetNodeProperties — the raw MessagePack blob, or `None` if the node
        /// (or the graph) is absent. Same shape as the wire response;
        /// Python-side unpacks it with `msgpack.unpackb` exactly as it does
        /// today over the socket transport (see the design doc's
        /// transport-swap shim, `epistemic_graph/embedded.py`).
        fn get_node_properties(
            &self,
            py: Python<'_>,
            graph: String,
            node_id: String,
        ) -> PyResult<Option<Py<PyBytes>>> {
            let registry = self.registry.clone();
            let raw = py.detach(move || -> PyResult<Option<Vec<u8>>> {
                let core = resolve_core(&registry, &graph).map_err(PyKeyError::new_err)?;
                Ok(core.get_node_properties(&node_id))
            })?;
            Ok(raw.map(|bytes| PyBytes::new(py, &bytes).unbind()))
        }

        /// `True` if `node_id` exists in `graph` (an unrecognized graph name
        /// reads as `False` rather than raising, matching a cold/never-created
        /// tenant reading as empty).
        fn has_node(&self, py: Python<'_>, graph: String, node_id: String) -> bool {
            let registry = self.registry.clone();
            py.detach(move || {
                registry
                    .read()
                    .get(&graph)
                    .map(|entry| entry.core.has_node(&node_id))
                    .unwrap_or(false)
            })
        }

        /// Node count of `graph` (`0` for a graph that doesn't exist yet).
        fn node_count(&self, py: Python<'_>, graph: String) -> usize {
            let registry = self.registry.clone();
            py.detach(move || {
                registry
                    .read()
                    .get(&graph)
                    .map(|entry| entry.core.node_count())
                    .unwrap_or(0)
            })
        }
    }

    #[pymodule]
    fn engine(m: &Bound<'_, PyModule>) -> PyResult<()> {
        m.add_class::<PyEngine>()?;
        // Discovery marker, mirroring crates/eg-numeric's `__kernel__` — lets a
        // caller (and `tests/test_engine_smoke.py`) confirm it imported the
        // real compiled binding rather than some other `engine` module.
        m.add("__engine__", "eg-pyengine")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // Pure-Rust tests against the eg-core layer this binding wraps — NOT
    // through pyo3. `extension-module` mode configures pyo3 to be dlopen'd BY
    // Python rather than embedding libpython, so a `cfg(test)` binary built
    // with the `python` feature cannot `Python::with_gil` here (the same
    // reason `crates/eg-numeric` tests only its pure `linalg`/`reductions`
    // layer this way). The pyo3-layer proof is the maturin-built wheel +
    // `tests/test_engine_smoke.py`, mirroring
    // `crates/eg-numeric/tests/test_kernel_parity.py`.
    use super::*;

    #[test]
    fn add_and_get_round_trip() {
        let registry = new_registry();
        create_graph(&registry, "kg").unwrap();
        let core = resolve_core(&registry, "kg").unwrap();
        let payload = vec![0x81, 0xa1, b'k', 0x01]; // msgpack {"k":1}; opaque to this layer
        core.add_node("n1".to_string(), payload.clone());
        assert_eq!(core.get_node_properties("n1"), Some(payload));
        assert!(core.has_node("n1"));
        assert_eq!(core.node_count(), 1);
    }

    #[test]
    fn missing_graph_errors() {
        let registry = new_registry();
        assert!(resolve_core(&registry, "nope").is_err());
    }

    #[test]
    fn duplicate_graph_name_errors() {
        let registry = new_registry();
        create_graph(&registry, "kg").unwrap();
        assert!(create_graph(&registry, "kg").is_err());
        // __commons__ is pre-created by GraphRegistry::new(); creating it
        // again is the same "already exists" error the wire dispatch surfaces.
        assert!(create_graph(&registry, "__commons__").is_err());
    }
}
