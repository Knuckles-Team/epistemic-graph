//! eg-pyengine — the in-process PyO3 embedding of the epistemic-graph engine core
//! (unified-binary program, workstream W-A; see
//! `docs/architecture/unified-inprocess-engine.md` for the full design, and
//! `plans/pyengine/EG-PYENGINE-PLAN.md` for the Wave 0/Wave 1 execution plan
//! this file's shape follows).
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
//! follow-up packaging work — BUG-PE-002 in the plan's bug register: as of
//! Wave 0, nothing built here is reachable from a shipped wheel yet).
//!
//! It also cannot depend on the `epistemic-graph` facade crate itself: the
//! workspace DAG is `eg-types -> eg-ann -> eg-core -> eg-compute ->
//! epistemic-graph` (facade at the TOP), and the facade optionally depends on
//! THIS crate (feature `pyo3-engine`) so `cargo build --features pyo3-engine`
//! at the repo root actually compiles it — a dependency in the other direction
//! would cycle. So this crate talks to `eg-core` (and, behind per-domain
//! features added in Wave 0 §4.5, the leaf compute/query crates that also sit
//! below the facade — `eg-compute`, `eg-query`, `eg-plan`, `eg-tsdb`, `eg-rdf`,
//! `eg-jobs`, `eg-statechart`, `eg-wasm`) directly: the same layer the facade's
//! own `src/embedded.rs` (`EmbeddedEngine`, the SQLite-style in-process Rust
//! API) talks to. This crate is that SAME "engine core, in-process, no
//! socket" idea, reachable from `crates/`, where the facade cannot be a
//! dependency. The two are NOT the same code — `EmbeddedEngine` additionally
//! wires the redb durable store + the `query`/`cypher` surfaces — but they are
//! the SAME pattern against the SAME `GraphCore`/`GraphRegistry`, no engine
//! logic duplicated.
//!
//! ## Wave 0 — the foundation every domain lane builds on
//!
//! This file pre-declares one module + one stub pyclass + one `PyEngine`
//! accessor per Wave-1 domain lane (`EG-PYENGINE-PLAN.md` §4.1) — sub-objects,
//! ONE `#[pyclass]` per domain per file, mirroring how `epistemic_graph/client.py`'s
//! `EpistemicGraphClient` exposes `.nodes`/`.edges`/`.finance`/... as
//! sub-clients. Sub-objects (not multiple `#[pymethods]` blocks split across
//! files for one `#[pyclass]`) because pyo3 0.29's support for the latter is
//! unverified; the sub-object design sidesteps the question entirely and this
//! session did not have to test the alternative, since every domain got its
//! own real `#[pyclass]`/`impl`/file from the start.
//!
//! Two shared contracts every domain module threads through, also built here:
//! - [`authority::EmbeddedAuthority`] — per-agent Row-Level Security (§4.3).
//! - [`errors::map_engine_error`] — the ONE error-mapping function onto
//!   `epistemic_graph.client`'s existing typed exceptions (§4.4), replacing
//!   the prototype's ad hoc `map_err(PyKeyError::new_err)` calls.
//!
//! ## Durability — an explicit, documented seam, not a silent no-op
//!
//! `EG-PYENGINE-PLAN.md` §4.2 asks Wave 0 to hoist `src/mutation_apply.rs`'s
//! transport-agnostic "resolve graph, apply Method, commit durably" glue down
//! into `eg-core`, shared between `src/embedded.rs`'s `EmbeddedEngine` and this
//! crate, BEFORE any lane needs durability. That hoist is genuinely the
//! largest, highest-risk item the plan describes (moving/refactoring 788 lines
//! that today live at the facade layer) — too large to land safely inside this
//! Wave-0 session alongside the other four deliverables without an
//! unacceptable risk of leaving it half-migrated. Per the plan's own
//! documented fallback (§4.2, "flag this decision explicitly ... do not
//! silently choose it"): Wave 0 ships WITHOUT the hoist. `Engine.new()` still
//! gains the `persist_dir` parameter the plan requires, with NO silent
//! default in either direction (BUG-PE-003): `persist_dir=None` is refused
//! outright, `persist_dir=":memory:"` is the one explicit ephemeral opt-in
//! (matching `epistemic_graph/embedded.py`'s own shape — that module reads
//! `GRAPH_SERVICE_PERSIST_DIR` and raises if unset rather than falling back),
//! and any other path is ALSO refused (a loud `PyResult` error, never a
//! silently-discarded parameter — see BUG-PE-003 in the plan's bug register,
//! and `py::PyEngine::new`'s own doc comment below for the exact reasoning:
//! silently accepting a `persist_dir` this crate cannot yet honor is
//! precisely the class of bug that has already destroyed a production graph
//! once, via a DIFFERENT silent fallback in `agent-utilities`). The hoist
//! remains open follow-up work that every write-touching Wave-1 lane
//! (`graph_ops`, `txn`, `rdf`, `streaming`, `jobs`) blocks on before merging
//! its own final PR.
//!
//! **Single-writer-per-persist-dir, intra-process (answered this Wave, not
//! yet exercised — no lane has wired `persist_dir` to it):**
//! `src/persist_lock.rs`'s existing `PersistDirLock::acquire` (the pattern
//! the durability lane should reuse per §4.2, not reinvent) takes an
//! exclusive advisory `flock` via the `fs4` crate. Verified empirically this
//! session (a throwaway two-open-same-process probe, `fs4::try_lock_exclusive`):
//! BSD-style `flock` is scoped to the OPEN FILE DESCRIPTION, not the
//! process — a SECOND independent `File::open` + `try_lock_exclusive` on the
//! SAME path, even from the SAME process, returns `Ok(false)` (lock denied),
//! identically to a second OS process. **Consequence: if the durability lane
//! reuses `persist_lock.rs` as-is, two `Engine()` instances in one process
//! CANNOT share one `persist_dir`** — the second construction fails the lock
//! and refuses to start, exactly like two separate processes today. A lane
//! that wants intra-process multi-instance sharing of one `persist_dir` needs
//! a DELIBERATE addition (e.g. a process-local `Arc`-refcounted lock cache
//! keyed by canonical path, so a second in-process `Engine()` on the same
//! dir reuses the first's held lock instead of opening its own file
//! description) — not something this existing lock pattern gives for free.
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
//! Error mapping ([`errors::map_engine_error`]) re-attaches the GIL only on
//! the error path (a cached, GIL-synchronized module lookup, not a per-call
//! cost on the success path — see `errors.rs`'s own doc for why this matters:
//! the entire premise of this crate is beating the socket transport's
//! round-trip floor, so nothing here reintroduces per-call overhead on the
//! path that actually has to be fast).
//!
//! ## What this prototype's original 5 methods still do NOT do
//!
//! Real batch ops beyond one node/edge per call, and `eg2.`-style identity
//! (deliberately out of scope in-process — design doc §7). What Wave 0 adds on
//! top: per-agent RLS threading (proven end-to-end for the two read methods,
//! `get_node_properties`/`has_node`) and typed error mapping (proven for all
//! five). Durable persistence remains the explicit seam described above.

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
// The shared RLS contract (§4.3). Pure `eg-core` types only — no pyo3 — so it
// compiles (and is unit-tested) in the pure half too, same reasoning as
// `SharedRegistry` above.
// ---------------------------------------------------------------------------
mod authority;

// ---------------------------------------------------------------------------
// Python surface — pyo3 extension module `epistemic_graph.engine`.
// Gated behind the `python` feature so a plain rlib consumer of this crate
// (and a bare `cargo build -p eg-pyengine`) pulls no pyo3 at all — mirrors
// crates/eg-numeric's `py` module split exactly.
// ---------------------------------------------------------------------------

// The shared error-mapping contract (§4.4) — pyo3 types only, so gated with
// every domain module below.
#[cfg(feature = "python")]
mod errors;

// One module per Wave-1 domain lane (§4.1), declared ONCE, here, so no future
// lane ever adds a `mod` line to this file. Each points at an already-created
// near-empty stub file (`crates/eg-pyengine/src/<domain>.rs`) — see those
// files' own doc comments.
#[cfg(feature = "python")]
mod admin_ctl;
#[cfg(feature = "python")]
mod blob;
#[cfg(feature = "python")]
mod broker;
#[cfg(feature = "python")]
mod channels;
#[cfg(feature = "python")]
mod cluster_ctl;
#[cfg(feature = "python")]
mod datascience;
#[cfg(feature = "python")]
mod finance;
#[cfg(feature = "python")]
mod graph_ops;
#[cfg(feature = "python")]
mod graphlearn;
#[cfg(feature = "python")]
mod identity;
#[cfg(feature = "python")]
mod jobs;
#[cfg(feature = "python")]
mod kv;
#[cfg(feature = "python")]
mod longtail;
#[cfg(feature = "python")]
mod mining;
#[cfg(feature = "python")]
mod modality;
#[cfg(feature = "python")]
mod pipeline;
#[cfg(feature = "python")]
mod query;
#[cfg(feature = "python")]
mod rbac;
#[cfg(feature = "python")]
mod rdf;
#[cfg(feature = "python")]
mod sqlite_file;
#[cfg(feature = "python")]
mod statechart;
#[cfg(feature = "python")]
mod streaming;
#[cfg(feature = "python")]
mod timeseries;
#[cfg(feature = "python")]
mod txn;
#[cfg(feature = "python")]
mod wasm_udf;

#[cfg(feature = "python")]
// pyo3 macro-generated code triggers a couple of unavoidable lints (the same
// ones crates/eg-numeric's own `py` module allows, for the same reason): a
// PyErr `.into()` round-trip, and cfgs the pyo3 macros emit that this Rust
// toolchain doesn't otherwise declare. Scoped to this optional extension
// module only.
#[allow(clippy::useless_conversion, unexpected_cfgs)]
mod py {
    use super::authority::EmbeddedAuthority;
    use super::errors::map_engine_error;
    use super::{create_graph, new_registry, resolve_core, SharedRegistry};
    use pyo3::prelude::*;
    use pyo3::types::PyBytes;

    /// Sentinel `persist_dir` value meaning "explicit, deliberate ephemeral
    /// in-memory — never a real path." Matches `epistemic_graph/embedded.py`'s
    /// own shape (Python lane, commit `c18180f2`): that module reads
    /// `GRAPH_SERVICE_PERSIST_DIR` and RAISES if it is unset rather than
    /// silently falling back, with `persist_dir=":memory:"` as the one
    /// explicit opt-out a caller can pass instead. Threading the SAME
    /// sentinel through `PyEngine::new` (rather than treating a bare
    /// `persist_dir=None` as "ephemeral") means neither side of the
    /// transport has a silent default — see BUG-PE-003.
    const MEMORY_SENTINEL: &str = ":memory:";

    /// One in-process handle to the engine core — the pyo3-visible analogue of
    /// `epistemic_graph::embedded::EmbeddedEngine`, minus durability (see this
    /// crate's top-level doc, "Durability" section).
    ///
    /// Cheaply cloneable internally (`Arc`-backed registry, `Arc`-backed
    /// authority): several Python call sites sharing one `Engine()` instance
    /// drive ONE in-process engine with ONE asserted caller identity.
    #[pyclass(module = "epistemic_graph.engine", name = "Engine")]
    struct PyEngine {
        registry: SharedRegistry,
        authority: EmbeddedAuthority,
    }

    #[pymethods]
    impl PyEngine {
        /// Open a fresh in-memory engine, optionally binding a caller identity
        /// (`agent_id`/`tenant` — design doc §7: a deployment-identity
        /// assertion, bound once, at construction time, not re-verified per
        /// call) and a `persist_dir`.
        ///
        /// `persist_dir` has NO silent default (BUG-PE-003): `None` is
        /// REFUSED outright (a caller must decide), `Some(":memory:")` is the
        /// one explicit ephemeral opt-in (logged once, loudly, below), and
        /// any OTHER `Some(path)` is ALSO refused for now — this crate has no
        /// durable storage wiring yet (Wave 0 defers the `eg-core`
        /// durability hoist, `EG-PYENGINE-PLAN.md` §4.2 — see this file's
        /// top-level "Durability" doc). Accepting a real path and quietly
        /// running in-memory-only would repeat, at this new call site, the
        /// EXACT bug class already recorded against the wire path
        /// (`agent_utilities/knowledge_graph/core/graph_compute.py:1914-1921`).
        #[new]
        #[pyo3(signature = (persist_dir=None, agent_id=None, tenant=None))]
        fn new(
            persist_dir: Option<String>,
            agent_id: Option<String>,
            tenant: Option<String>,
        ) -> PyResult<Self> {
            match persist_dir.as_deref() {
                None => {
                    return Err(map_engine_error(
                        "INVALID_ARGUMENT: persist_dir is required (no silent default, \
                         BUG-PE-003) \u{2014} pass persist_dir=\":memory:\" to explicitly opt \
                         into ephemeral in-memory storage, or a real directory once the \
                         durability lane (EG-PYENGINE-PLAN.md \u{a7}4.2) wires it."
                            .to_string(),
                    ));
                }
                Some(MEMORY_SENTINEL) => {
                    eprintln!(
                        "eg-pyengine: Engine(persist_dir=\":memory:\") \u{2014} explicit \
                         ephemeral choice, running IN-MEMORY ONLY. Nothing durable; all graph \
                         state is lost on process exit."
                    );
                }
                Some(dir) => {
                    return Err(map_engine_error(format!(
                        "INVALID_ARGUMENT: persist_dir={dir:?} was given but eg-pyengine has no \
                         durable storage wiring yet (Wave 0 defers this to the durability hoist, \
                         EG-PYENGINE-PLAN.md \u{a7}4.2). Refusing to start rather than silently \
                         discarding the path and running in-memory-only (BUG-PE-003) \u{2014} \
                         pass persist_dir=\":memory:\" until the durability lane lands, or this \
                         Engine is not what you want."
                    )));
                }
            }
            Ok(PyEngine {
                registry: new_registry(),
                authority: EmbeddedAuthority::new(agent_id, tenant),
            })
        }

        /// Create a named graph — ONE call, matching the wire `Method`
        /// (`CreateGraph`-shaped) this mirrors. Not RLS-scoped: creating a
        /// graph is not a row read, so [`EmbeddedAuthority`] is not consulted
        /// here (matches the wire dispatch: graph *creation* is a
        /// capability/admin-scoped operation, not a row-visibility one).
        fn create_graph(&self, py: Python<'_>, name: String) -> PyResult<()> {
            let registry = self.registry.clone();
            py.detach(move || create_graph(&registry, &name).map_err(map_engine_error))
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
                let core = resolve_core(&registry, &graph).map_err(map_engine_error)?;
                core.add_node(node_id, properties_msgpack);
                Ok(())
            })
        }

        /// GetNodeProperties — the raw MessagePack blob, or `None` if the node
        /// is absent OR the resolved identity may not see it
        /// ([`EmbeddedAuthority::can_see_properties`] — the SAME
        /// `IsolationLayer::can_see_row` decision the wire dispatch's
        /// `GraphReadAuthority::filter_view` reaches for this row, closing
        /// the existence side channel exactly as the wire path does: an
        /// invisible row reads identically to an absent one, never a
        /// distinguishable "found but denied"). Python-side unpacks it with
        /// `msgpack.unpackb` exactly as it does today over the socket
        /// transport.
        ///
        /// `agent_id`, when given, OVERRIDES the identity this `Engine` was
        /// constructed with for this ONE call — the mechanism two principals
        /// sharing one embedded engine need for a genuine RLS differential
        /// test (`EG-PYENGINE-PLAN.md` §3's correctness bar, point 2; see
        /// `authority::EmbeddedAuthority::can_see_properties`'s own doc for
        /// why construction-time-only identity can't represent that case).
        /// `None` (the default) falls back to the construction-time identity,
        /// unchanged from before this parameter existed.
        #[pyo3(signature = (graph, node_id, agent_id=None))]
        fn get_node_properties(
            &self,
            py: Python<'_>,
            graph: String,
            node_id: String,
            agent_id: Option<String>,
        ) -> PyResult<Option<Py<PyBytes>>> {
            let registry = self.registry.clone();
            let authority = self.authority.clone();
            let raw = py.detach(move || -> PyResult<Option<Vec<u8>>> {
                let core = resolve_core(&registry, &graph).map_err(map_engine_error)?;
                let props = core.get_node_properties(&node_id);
                if !authority.can_see_properties(agent_id.as_deref(), props.as_deref()) {
                    return Ok(None);
                }
                Ok(props)
            })?;
            Ok(raw.map(|bytes| PyBytes::new(py, &bytes).unbind()))
        }

        /// `True` if `node_id` exists in `graph` AND the resolved identity may
        /// see it — an unrecognized graph name, an absent node, and a
        /// present-but-invisible node all read as `False`, the same
        /// existence-side-channel closure `get_node_properties` applies
        /// above. `agent_id` is the SAME per-call override as
        /// `get_node_properties` (see its doc comment).
        #[pyo3(signature = (graph, node_id, agent_id=None))]
        fn has_node(
            &self,
            py: Python<'_>,
            graph: String,
            node_id: String,
            agent_id: Option<String>,
        ) -> bool {
            let registry = self.registry.clone();
            let authority = self.authority.clone();
            py.detach(move || {
                let Some(core) = registry.read().get(&graph).map(|entry| entry.core.clone()) else {
                    return false;
                };
                if !core.has_node(&node_id) {
                    return false;
                }
                authority.can_see_properties(
                    agent_id.as_deref(),
                    core.get_node_properties(&node_id).as_deref(),
                )
            })
        }

        /// Node count of `graph` (`0` for a graph that doesn't exist yet).
        /// NOT RLS-filtered — an accurate per-agent-visible count would need
        /// to evaluate every node's visibility (the `filter_view` shape, not
        /// the point-check `can_see_properties` these other methods use);
        /// left as the prototype's original unfiltered behavior. Flagged
        /// explicitly (`EG-PYENGINE-PLAN.md` bug register) rather than
        /// silently declared "done": the `graph_ops` Wave-1 lane should
        /// revisit this once it has a real `GraphView`-based read path.
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

        // ------------------------------------------------------------------
        // One thin accessor per Wave-1 domain lane (§4.1), written ONCE,
        // completely, here — every accessor is a cheap `Arc`-clone of the
        // SAME registry + authority this `Engine` already holds, never a
        // deep copy. A lane's own `#[pymethods]` block (in its own file)
        // grows; this list does not change for it to do so.
        // ------------------------------------------------------------------

        /// Thin accessor onto the `graph_ops` domain surface (Wave 1 lane
        /// fleshes out `crates/eg-pyengine/src/graph_ops.rs`'s `#[pymethods]`
        /// block; this accessor's shape never needs to change for that).
        fn graph_ops(&self) -> super::graph_ops::PyGraphOpsOps {
            super::graph_ops::PyGraphOpsOps {
                registry: self.registry.clone(),
                authority: self.authority.clone(),
            }
        }

        /// Thin accessor onto the `query` domain surface (Wave 1 lane
        /// fleshes out `crates/eg-pyengine/src/query.rs`'s `#[pymethods]`
        /// block; this accessor's shape never needs to change for that).
        fn query(&self) -> super::query::PyQueryOps {
            super::query::PyQueryOps {
                registry: self.registry.clone(),
                authority: self.authority.clone(),
            }
        }

        /// Thin accessor onto the `txn` domain surface (Wave 1 lane
        /// fleshes out `crates/eg-pyengine/src/txn.rs`'s `#[pymethods]`
        /// block; this accessor's shape never needs to change for that).
        fn txn(&self) -> super::txn::PyTxnOps {
            super::txn::PyTxnOps {
                registry: self.registry.clone(),
                authority: self.authority.clone(),
            }
        }

        /// Thin accessor onto the `finance` domain surface (Wave 1 lane
        /// fleshes out `crates/eg-pyengine/src/finance.rs`'s `#[pymethods]`
        /// block; this accessor's shape never needs to change for that).
        fn finance(&self) -> super::finance::PyFinanceOps {
            super::finance::PyFinanceOps {
                registry: self.registry.clone(),
                authority: self.authority.clone(),
            }
        }

        /// Thin accessor onto the `datascience` domain surface (Wave 1 lane
        /// fleshes out `crates/eg-pyengine/src/datascience.rs`'s
        /// `#[pymethods]` block; this accessor's shape never needs to change
        /// for that).
        fn datascience(&self) -> super::datascience::PyDatascienceOps {
            super::datascience::PyDatascienceOps {
                registry: self.registry.clone(),
                authority: self.authority.clone(),
            }
        }

        /// Thin accessor onto the `mining` domain surface (Wave 1 lane
        /// fleshes out `crates/eg-pyengine/src/mining.rs`'s `#[pymethods]`
        /// block; this accessor's shape never needs to change for that).
        fn mining(&self) -> super::mining::PyMiningOps {
            super::mining::PyMiningOps {
                registry: self.registry.clone(),
                authority: self.authority.clone(),
            }
        }

        /// Thin accessor onto the `graphlearn` domain surface (Wave 1 lane
        /// fleshes out `crates/eg-pyengine/src/graphlearn.rs`'s
        /// `#[pymethods]` block; this accessor's shape never needs to change
        /// for that).
        fn graphlearn(&self) -> super::graphlearn::PyGraphlearnOps {
            super::graphlearn::PyGraphlearnOps {
                registry: self.registry.clone(),
                authority: self.authority.clone(),
            }
        }

        /// Thin accessor onto the `pipeline` domain surface (Wave 1 lane
        /// fleshes out `crates/eg-pyengine/src/pipeline.rs`'s `#[pymethods]`
        /// block; this accessor's shape never needs to change for that).
        fn pipeline(&self) -> super::pipeline::PyPipelineOps {
            super::pipeline::PyPipelineOps {
                registry: self.registry.clone(),
                authority: self.authority.clone(),
            }
        }

        /// Thin accessor onto the `timeseries` domain surface (Wave 1 lane
        /// fleshes out `crates/eg-pyengine/src/timeseries.rs`'s
        /// `#[pymethods]` block; this accessor's shape never needs to change
        /// for that).
        fn timeseries(&self) -> super::timeseries::PyTimeseriesOps {
            super::timeseries::PyTimeseriesOps {
                registry: self.registry.clone(),
                authority: self.authority.clone(),
            }
        }

        /// Thin accessor onto the `blob` domain surface (Wave 1 lane
        /// fleshes out `crates/eg-pyengine/src/blob.rs`'s `#[pymethods]`
        /// block; this accessor's shape never needs to change for that).
        fn blob(&self) -> super::blob::PyBlobOps {
            super::blob::PyBlobOps {
                registry: self.registry.clone(),
                authority: self.authority.clone(),
            }
        }

        /// Thin accessor onto the `kv` domain surface (Wave 1 lane
        /// fleshes out `crates/eg-pyengine/src/kv.rs`'s `#[pymethods]`
        /// block; this accessor's shape never needs to change for that).
        fn kv(&self) -> super::kv::PyKvOps {
            super::kv::PyKvOps {
                registry: self.registry.clone(),
                authority: self.authority.clone(),
            }
        }

        /// Thin accessor onto the `rdf` domain surface (Wave 1 lane
        /// fleshes out `crates/eg-pyengine/src/rdf.rs`'s `#[pymethods]`
        /// block; this accessor's shape never needs to change for that).
        fn rdf(&self) -> super::rdf::PyRdfOps {
            super::rdf::PyRdfOps {
                registry: self.registry.clone(),
                authority: self.authority.clone(),
            }
        }

        /// Thin accessor onto the `streaming` domain surface (Wave 1 lane
        /// fleshes out `crates/eg-pyengine/src/streaming.rs`'s
        /// `#[pymethods]` block; this accessor's shape never needs to change
        /// for that).
        fn streaming(&self) -> super::streaming::PyStreamingOps {
            super::streaming::PyStreamingOps {
                registry: self.registry.clone(),
                authority: self.authority.clone(),
            }
        }

        /// Thin accessor onto the `broker` domain surface (Wave 1 lane
        /// fleshes out `crates/eg-pyengine/src/broker.rs`'s `#[pymethods]`
        /// block; this accessor's shape never needs to change for that).
        fn broker(&self) -> super::broker::PyBrokerOps {
            super::broker::PyBrokerOps {
                registry: self.registry.clone(),
                authority: self.authority.clone(),
            }
        }

        /// Thin accessor onto the `channels` domain surface (Wave 1 lane
        /// fleshes out `crates/eg-pyengine/src/channels.rs`'s `#[pymethods]`
        /// block; this accessor's shape never needs to change for that).
        fn channels(&self) -> super::channels::PyChannelsOps {
            super::channels::PyChannelsOps {
                registry: self.registry.clone(),
                authority: self.authority.clone(),
            }
        }

        /// Thin accessor onto the `jobs` domain surface (Wave 1 lane
        /// fleshes out `crates/eg-pyengine/src/jobs.rs`'s `#[pymethods]`
        /// block; this accessor's shape never needs to change for that).
        fn jobs(&self) -> super::jobs::PyJobsOps {
            super::jobs::PyJobsOps {
                registry: self.registry.clone(),
                authority: self.authority.clone(),
            }
        }

        /// Thin accessor onto the `statechart` domain surface (Wave 1 lane
        /// fleshes out `crates/eg-pyengine/src/statechart.rs`'s
        /// `#[pymethods]` block; this accessor's shape never needs to change
        /// for that).
        fn statechart(&self) -> super::statechart::PyStatechartOps {
            super::statechart::PyStatechartOps {
                registry: self.registry.clone(),
                authority: self.authority.clone(),
            }
        }

        /// Thin accessor onto the `wasm_udf` domain surface (Wave 1 lane
        /// fleshes out `crates/eg-pyengine/src/wasm_udf.rs`'s `#[pymethods]`
        /// block; this accessor's shape never needs to change for that).
        fn wasm_udf(&self) -> super::wasm_udf::PyWasmUdfOps {
            super::wasm_udf::PyWasmUdfOps {
                registry: self.registry.clone(),
                authority: self.authority.clone(),
            }
        }

        /// Thin accessor onto the `sqlite_file` domain surface (Wave 1 lane
        /// fleshes out `crates/eg-pyengine/src/sqlite_file.rs`'s
        /// `#[pymethods]` block; this accessor's shape never needs to change
        /// for that).
        fn sqlite_file(&self) -> super::sqlite_file::PySqliteFileOps {
            super::sqlite_file::PySqliteFileOps {
                registry: self.registry.clone(),
                authority: self.authority.clone(),
            }
        }

        /// Thin accessor onto the `identity` domain surface (Wave 1 lane
        /// fleshes out `crates/eg-pyengine/src/identity.rs`'s `#[pymethods]`
        /// block; this accessor's shape never needs to change for that).
        fn identity(&self) -> super::identity::PyIdentityOps {
            super::identity::PyIdentityOps {
                registry: self.registry.clone(),
                authority: self.authority.clone(),
            }
        }

        /// Thin accessor onto the `rbac` domain surface (Wave 1 lane
        /// fleshes out `crates/eg-pyengine/src/rbac.rs`'s `#[pymethods]`
        /// block; this accessor's shape never needs to change for that).
        fn rbac(&self) -> super::rbac::PyRbacOps {
            super::rbac::PyRbacOps {
                registry: self.registry.clone(),
                authority: self.authority.clone(),
            }
        }

        /// Thin accessor onto the `admin_ctl` domain surface (Wave 1 lane
        /// fleshes out `crates/eg-pyengine/src/admin_ctl.rs`'s
        /// `#[pymethods]` block; this accessor's shape never needs to change
        /// for that).
        fn admin_ctl(&self) -> super::admin_ctl::PyAdminCtlOps {
            super::admin_ctl::PyAdminCtlOps {
                registry: self.registry.clone(),
                authority: self.authority.clone(),
            }
        }

        /// Thin accessor onto the `cluster_ctl` domain surface (Wave 1 lane
        /// fleshes out `crates/eg-pyengine/src/cluster_ctl.rs`'s
        /// `#[pymethods]` block; this accessor's shape never needs to change
        /// for that — see `EG-PYENGINE-PLAN.md` §2.4: this domain's real
        /// methods answer "not applicable in embedded mode," not cluster
        /// admin, since an embedded engine is single-process/single-writer by
        /// construction).
        fn cluster_ctl(&self) -> super::cluster_ctl::PyClusterCtlOps {
            super::cluster_ctl::PyClusterCtlOps {
                registry: self.registry.clone(),
                authority: self.authority.clone(),
            }
        }

        /// Thin accessor onto the `modality` domain surface (Wave 1 lane
        /// fleshes out `crates/eg-pyengine/src/modality.rs`'s `#[pymethods]`
        /// block; this accessor's shape never needs to change for that).
        fn modality(&self) -> super::modality::PyModalityOps {
            super::modality::PyModalityOps {
                registry: self.registry.clone(),
                authority: self.authority.clone(),
            }
        }

        /// Thin accessor onto the `longtail` domain surface (Wave 1 lane
        /// fleshes out `crates/eg-pyengine/src/longtail.rs`'s `#[pymethods]`
        /// block; this accessor's shape never needs to change for that).
        fn longtail(&self) -> super::longtail::PyLongtailOps {
            super::longtail::PyLongtailOps {
                registry: self.registry.clone(),
                authority: self.authority.clone(),
            }
        }
    }

    #[pymodule]
    fn engine(m: &Bound<'_, PyModule>) -> PyResult<()> {
        m.add_class::<PyEngine>()?;
        m.add_class::<crate::graph_ops::PyGraphOpsOps>()?;
        m.add_class::<crate::query::PyQueryOps>()?;
        m.add_class::<crate::txn::PyTxnOps>()?;
        m.add_class::<crate::finance::PyFinanceOps>()?;
        m.add_class::<crate::datascience::PyDatascienceOps>()?;
        m.add_class::<crate::mining::PyMiningOps>()?;
        m.add_class::<crate::graphlearn::PyGraphlearnOps>()?;
        m.add_class::<crate::pipeline::PyPipelineOps>()?;
        m.add_class::<crate::timeseries::PyTimeseriesOps>()?;
        m.add_class::<crate::blob::PyBlobOps>()?;
        m.add_class::<crate::kv::PyKvOps>()?;
        m.add_class::<crate::rdf::PyRdfOps>()?;
        m.add_class::<crate::streaming::PyStreamingOps>()?;
        m.add_class::<crate::broker::PyBrokerOps>()?;
        m.add_class::<crate::channels::PyChannelsOps>()?;
        m.add_class::<crate::jobs::PyJobsOps>()?;
        m.add_class::<crate::statechart::PyStatechartOps>()?;
        m.add_class::<crate::wasm_udf::PyWasmUdfOps>()?;
        m.add_class::<crate::sqlite_file::PySqliteFileOps>()?;
        m.add_class::<crate::identity::PyIdentityOps>()?;
        m.add_class::<crate::rbac::PyRbacOps>()?;
        m.add_class::<crate::admin_ctl::PyAdminCtlOps>()?;
        m.add_class::<crate::cluster_ctl::PyClusterCtlOps>()?;
        m.add_class::<crate::modality::PyModalityOps>()?;
        m.add_class::<crate::longtail::PyLongtailOps>()?;
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
    // `crates/eg-numeric/tests/test_kernel_parity.py`. The RLS contract's own
    // pure-Rust proof lives in `authority.rs`'s own `#[cfg(test)]` module
    // (also reachable without the `python` feature, same reasoning).
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
