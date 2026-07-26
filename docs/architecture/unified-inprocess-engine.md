# The unified in-process engine (PyO3) — design

> Workstream **W-A** of the unified-binary program (`reports/unified-binary-program.md`
> at the workspace root). Implements the evolved edict in `AGENTS.md`: *"Two deployment
> shapes, one performance discipline"*. This doc is the design for the **unified single
> binary** shape — embedding the Rust engine in-process via PyO3 — and the concrete plan
> to prove it, package it, and eventually make it the self-contained default without
> regressing the out-of-process, horizontally-scaled shape that remains the default today.

## 1. Where this sits

Two shapes now exist, and this doc is about the second one:

| | Out-of-process (today's default) | Unified single binary (this doc) |
|---|---|---|
| Transport | Tokio UDS/TCP server, length-prefixed MessagePack, `eg2.` HMAC envelope | A direct Rust function call inside the same OS process |
| Scales | Horizontally — one engine, many `graph-os` clients | Vertically only — one `graph-os`, one embedded engine |
| Wheel target | `bindings = "bin"` (`epistemic-graph-server`) | A pyo3 `cdylib` (`epistemic_graph.engine`), injected into the same wheel |
| Identity boundary | Real — client and engine are different processes/hosts | None needed for the call itself — the caller *is* the trusted process |
| Batching rule | Non-negotiable (round-trip amortization) | **Equally non-negotiable** (lock/allocation amortization — see §5) |
| Status | Shipped, measured (`docs/benchmarks.md`) | **This doc + a compiling, minimally-proven prototype** (`crates/eg-pyengine`) |

Neither shape replaces the other. `AGENTS.md` is explicit that PyO3 is restored as an
**opt-in** path for the self-contained case, not a reversal of the out-of-process default:
the scale-out shape keeps zero pyo3 in its dependency graph (`scripts/check_no_pyo3.sh`),
and this new shape is reached only through the `pyo3-engine` cargo feature, off in
`default`/`full`/`cluster`.

## 2. Why this is even possible without duplicating the engine

The facade crate already contains the exact pattern this workstream needs:
`src/embedded.rs`'s `EmbeddedEngine` (CONCEPT:EG-KG.backend.engine-modes, shipped under the
`embedded` cargo feature, demoed in `examples/embedded.rs`). Its own doc comment says it
better than a summary could:

> SQLite/DuckDB-style: `EmbeddedEngine::open(persist_dir, options)` hands back an
> in-process handle that owns a `GraphRegistry` + (optionally) the redb durable store
> DIRECTLY — **NO Tokio server, NO socket, NO HMAC** (it is in-process, so the caller IS
> the trusted party; there is no network boundary to authenticate). Core ops are plain
> method calls... The ONLY things the embedded transport drops are the network concerns
> the in-process model doesn't need: HMAC auth, the ACL isolation layer (a trusted local
> caller), the Tokio reactor + the per-graph write coalescer.

That is, byte for byte, the architecture this workstream needs — proven, tested, and
already reusing `GraphCore`/`GraphRegistry` (never duplicating the engine). The only gap
is that `EmbeddedEngine` is a **Rust** API, and W-A needs a **Python** one. The obvious
first instinct — "just add `#[pyclass]` to `EmbeddedEngine`" — runs into the one
structural constraint that shapes this entire design, covered next.

**A deliberate refinement worth stating explicitly.** The program doc frames this as
"embed the engine's Tokio server + `eg2.` dispatch in-process." Having found
`EmbeddedEngine` already proving the pattern, this design does something narrower and, on
inspection, more correct: it embeds the **engine core** (`GraphCore`/`GraphRegistry`)
in-process and does **not** stand up a Tokio reactor or run requests through the `eg2.`
dispatch chain at all — because in-process there is no socket for Tokio to listen on and
no network boundary for `eg2.` to authenticate (§7 makes the full argument). Spinning up
an async reactor and an HMAC-checking dispatch layer purely to immediately call it via a
plain function invocation that bypasses the socket would be building infrastructure for a
threat model (an untrusted network peer) that does not exist in-process — exactly the
shape `EmbeddedEngine` already rejected for the Rust API, for the identical reason. This
is a refinement of the framing, not a deviation from the goal: "no socket round-trip" is
satisfied more completely by skipping Tokio/`eg2.` than by keeping them and merely
routing around the socket.

## 3. Why a separate crate, not a module in the facade

Two independent constraints rule out adding pyo3 directly to the facade crate
(`epistemic-graph`, the repo-root package):

1. **maturin binding modes are mutually exclusive.** The main wheel is
   `bindings = "bin"` (`pyproject.toml`) — it ships the `epistemic-graph-server` binary.
   A single maturin invocation builds ONE binding kind; you cannot get a `bin` target
   and a pyo3 `cdylib` out of the same crate/invocation. `crates/eg-numeric` already
   solved this for the numeric kernel: it is **its own crate**, with **its own
   `pyproject.toml`**, built with a **separate** `maturin build -m
   crates/eg-numeric/Cargo.toml --features python` invocation, producing a standalone
   wheel whose compiled extension is then folded into the main wheel
   (`scripts/inject_numeric_kernel.py`). This is the exact shape W-A reuses.

2. **The workspace DAG only points one way.** `AGENTS.md`: *"member crates map 1:1 to
   the acyclic dependency DAG `eg-types → eg-ann → eg-core → eg-compute →
   epistemic-graph`... A crate may only `use` crates to its left; a cycle won't compile,
   which is the enforcement."* The facade sits at the **top** of that DAG. For
   `cargo build --release --features pyo3-engine` to compile a new crate from the repo
   root, that crate has to be an **optional dependency of the facade** (feature-gated).
   A crate cannot simultaneously be a dependency of the facade AND depend on the facade
   — that is a cycle. So the new binding crate (`crates/eg-pyengine`) cannot reuse
   `EmbeddedEngine` as a library dependency; it has to sit **parallel to `eg-compute`**,
   talking to `eg-core` directly, the same layer `EmbeddedEngine` itself talks to.

```
 eg-types → eg-ann → eg-core → eg-compute ─┐
                        │                   ├→ epistemic-graph (facade)
                        └── eg-pyengine ────┘        │  bin target: epistemic-graph-server
                             (feature pyo3-engine,     │  lib target: re-exports eg-{types,core,compute}
                              optional dep of facade)  │  + src/embedded.rs (EmbeddedEngine, Rust API)
                                                        └  #[cfg(feature="pyo3-engine")] pub use eg_pyengine;
```

`crates/eg-pyengine` is therefore **not** a wrapper around `EmbeddedEngine` — it is a
second, minimal implementation of the *same pattern* (open a registry, resolve a graph's
`GraphCore` by name, call its methods), one layer further down the DAG than
`EmbeddedEngine` sits, because that is the only place a `crates/*` member is allowed to
be. It reuses `eg-core` in exactly the sense `AGENTS.md` requires ("do not duplicate the
engine") — no storage, locking, or mutation logic is reimplemented, only ~40 lines of
registry glue that `EmbeddedEngine` also has to have. §11 covers the follow-up option of
hoisting that glue **down into `eg-core` itself**, so both callers share one
implementation instead of two thin copies of it — worth doing once a second caller
(this crate) makes the duplication real rather than hypothetical.

This mirrors `eg-numeric` structurally in every respect: a leaf crate, an internal
`python` feature (off by default) that is the only thing pulling in `pyo3`, a
`crate-type = ["cdylib", "rlib"]` dual surface, and its own standalone `pyproject.toml`.
The facade's own new feature is named `pyo3-engine` (matching this program's naming),
and turns on both the optional dependency and the leaf crate's own `python` feature:

```toml
# crates/eg-pyengine/Cargo.toml
[features]
default = []
python = ["dep:pyo3"]        # mirrors eg-numeric's `python` feature 1:1

# root Cargo.toml (facade)
[dependencies]
eg-pyengine = { path = "crates/eg-pyengine", optional = true }
[features]
pyo3-engine = ["dep:eg-pyengine", "eg-pyengine/python"]   # NOT in default/full/cluster
```

## 4. Same client API surface — only the transport swaps

The instruction from the program doc is precise: `agent_utilities` code must be
**unchanged**. Today, `agent_utilities` (and every other consumer under
`agent-packages/`) talks to `epistemic_graph.client.EpistemicGraphClient`, which exposes
sub-clients — `.nodes`, `.edges`, `.graph`, `.query`, `.finance`, … — each a thin wrapper
whose methods do two things: (1) MessagePack-encode/decode the Python-level
arguments/results, and (2) call `self._client._send(method_name, params)`.

Take `NodeClient` (`epistemic_graph/client.py`) as the concrete example already in the
codebase:

```python
class NodeClient:
    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def add(self, node_id: str, properties: dict[str, Any] | None = None) -> None:
        await self._client._send(
            "AddNode",
            {"node_id": node_id, "properties_msgpack": list(msgpack.packb(properties or {}))},
        )

    async def properties(self, node_id: str) -> dict[str, Any] | None:
        raw_val = await self._client._send("GetNodeProperties", {"node_id": node_id})
        if raw_val is None:
            return None
        if isinstance(raw_val, bytes):
            return msgpack.unpackb(raw_val, raw=False)
        return raw_val
```

The MessagePack encoding of `properties` is **already** identical to what `eg-core`
stores — `Method::AddNode { node_id, properties_msgpack: Vec<u8> }` never decodes the
blob, it stores it opaquely (`GraphCore::add_node(&self, node_id: String,
properties_msgpack: Vec<u8>)`) and hands the same bytes back on read
(`get_node_properties(&self, node_id: &str) -> Option<Vec<u8>>`). That is exactly why
`crates/eg-pyengine`'s pyo3 boundary is designed to take/return raw `bytes`, not a
decoded dict (§6): **the wire encoding does not change when the transport does.** The
only thing that has to change is what `_send`/its equivalent does underneath.

### The shape: a `Transport` protocol, two implementations

`NodeClient`/`EdgeClient`/etc. need **zero changes** if `._send(method, params)` is
lifted to an interface both transports satisfy:

```python
class _Transport(Protocol):
    async def _send(
        self, method: str, params: dict[str, Any] | None = None, *, graph: str | None = None,
    ) -> Any: ...

class SocketTransport:                       # today's EpistemicGraphClient, unchanged
    async def _send(self, method, params=None, *, graph=None) -> Any: ...  # framed UDS/TCP + eg2. envelope

class EmbeddedTransport:                     # NEW — backs epistemic_graph.embedded
    def __init__(self, graph_name: str = "__commons__") -> None:
        from . import engine as _native      # the pyo3 extension this crate builds
        self._engine = _native.Engine()
        self._graph_name = graph_name
        self._engine.create_graph(graph_name)

    async def _send(self, method, params=None, *, graph=None) -> Any:
        g = graph or self._graph_name
        params = params or {}
        if method == "AddNode":
            self._engine.add_node(g, params["node_id"], bytes(params["properties_msgpack"]))
            return None
        if method == "GetNodeProperties":
            return self._engine.get_node_properties(g, params["node_id"])
        raise NotImplementedError(f"EmbeddedTransport: {method} not yet ported")
```

`NodeClient(SocketTransport(...))` and `NodeClient(EmbeddedTransport(...))` are then
interchangeable — same class, same method names, same msgpack encode/decode, different
object underneath `self._client`. `agent_utilities` code that does
`client.nodes.add(...)`/`await client.nodes.properties(...)` never has to know or care
which one it was handed. A `graph-os` config flag (`engine_mode: embedded | socket`,
tying into W-D's transport modes) decides which `Transport` gets constructed at startup;
everything above that line is unmodified.

`EmbeddedTransport._send` is synchronous work wearing an `async def` — deliberately: the
whole point of removing the socket is that there is no I/O to await for an in-memory op,
so the "await" is a no-op yield, not a real suspension. §6 covers the one place this
stops being true (a durable commit) and what changes then.

**Scope note:** the shipped prototype implements the native pyo3 flat layer
(`crates/eg-pyengine`, §6) plus this design; wiring the full `Transport` protocol and
porting every sub-client (`.edges`, `.graph`, `.query`, …) is the concrete follow-up
tracked in §11 — this section specifies the target shape precisely enough that doing so
is mechanical, one sub-client at a time, with a round-trip test per method exactly like
`AGENTS.md`'s "adding a capability" recipe already requires for the wire protocol.

## 5. The batching discipline still applies — in-process does not waive it

`AGENTS.md`'s rule is stated for the socket shape but is **equally true** in-process, for
a different reason:

> **Batch, never per-element.** *N* elements in a loop = *N* round-trips = catastrophic;
> the same work as one batch op = one round-trip.

Removing the socket removes the round-trip cost, but a per-element Python loop calling
`engine.add_node(...)` N times still pays, N times over:

- a Python → Rust FFI boundary crossing (argument marshaling, even for two `str`s and a
  `bytes`),
- a GIL release/re-acquire (`Python::detach`, §6) — cheap per call, **not** free
  at N = 100k,
- a `registry.read()` lock acquisition to resolve the graph's `GraphCore` (parking_lot
  `RwLock`, uncontended-cheap but still not zero), and
- whatever per-op bookkeeping `GraphCore::add_node` itself does (ledger append, dirty
  flag, index maintenance) — the **same per-op cost the out-of-process dispatch already
  pays per element inside a `BatchUpdate`**, just without the framing on top.

`benches/eg096_massive_scale_bench.rs` (already in the repo, feature-light, in-process)
demonstrates exactly this shape: `AddNode` throughput is measured at 10k/50k *elements*
in one criterion iteration, not per call — because even with zero transport, batching how
much work one call does is still the lever that matters at scale. The wire protocol
already has the batch primitives this pattern needs (`Method::BatchUpdate`,
`Method::GetNodePropertiesBatch`, `Method::UnionGetNodeProperties`) — a production
`eg-pyengine` surface must expose the **same** batch methods, not just the
one-node-per-call prototype shipped here. The rule for this crate, stated once so it does
not drift as methods are added (mirrored in the crate's own doc comment):

> Every exposed `#[pymethods]` call is a batch primitive over graph-resident data — ONE
> call does ONE engine mutation/read. A caller with N independent ops calls N times
> because it has N independent ops; bulk work gets a real batch method
> (`add_nodes_batch`, mirroring `Method::BatchUpdate`) instead of a per-element loop, in
> both shapes, for the same reason.

## 6. What ships: `crates/eg-pyengine` (the minimal, real prototype)

The prototype crate wraps exactly enough of `eg-core` to prove the pattern end to end —
create a graph, add a node, read it back — through a real pyo3 boundary:

```rust
// crates/eg-pyengine/src/lib.rs (excerpt; see the file for full doc comments)
pub type SharedRegistry = Arc<parking_lot::RwLock<eg_core::registry::GraphRegistry>>;

pub fn new_registry() -> SharedRegistry { .. }
pub fn create_graph(registry: &SharedRegistry, name: &str) -> Result<(), String> { .. }
pub fn resolve_core(registry: &SharedRegistry, graph: &str)
    -> Result<Arc<eg_core::graph::GraphCore>, String> { .. }

#[cfg(feature = "python")]
mod py {
    #[pyclass(module = "epistemic_graph.engine", name = "Engine")]
    struct PyEngine { registry: SharedRegistry }

    #[pymethods]
    impl PyEngine {
        #[new] fn new() -> Self { .. }
        fn create_graph(&self, py: Python<'_>, name: String) -> PyResult<()> { .. }
        fn add_node(&self, py: Python<'_>, graph: String, node_id: String,
                    properties_msgpack: Vec<u8>) -> PyResult<()> { .. }
        fn get_node_properties(&self, py: Python<'_>, graph: String, node_id: String)
            -> PyResult<Option<Py<PyBytes>>> { .. }
        fn has_node(&self, py: Python<'_>, graph: String, node_id: String) -> bool { .. }
        fn node_count(&self, py: Python<'_>, graph: String) -> usize { .. }
    }

    #[pymodule]
    fn engine(m: &Bound<'_, PyModule>) -> PyResult<()> {
        m.add_class::<PyEngine>()?;
        m.add("__engine__", "eg-pyengine")?;   // discovery marker, mirrors eg-numeric's `__kernel__`
        Ok(())
    }
}
```

The pure (non-pyo3) half — `new_registry`/`create_graph`/`resolve_core` — has no pyo3
dependency at all and is unit-tested with a bare `cargo test -p eg-pyengine` (no `python`
feature needed), exactly like `eg-numeric`'s `linalg`/`reductions` modules. The pyo3 half
is gated in its own `mod py { ... }`, mirroring `eg-numeric`'s own split precisely, for
the same reason: a plain rlib consumer (or a bare `cargo build -p eg-pyengine`) never
pulls pyo3 in.

**GIL discipline.** Every `#[pymethods]` call releases the GIL for the actual engine call
via `Python::detach` — pyo3 0.29's name for the API earlier pyo3 releases called
`Python::allow_threads` (same contract: run a closure with the GIL released; the crate
pins `pyo3 = "0.29"`, confirmed while proving this prototype compiles — see §10/the final
report) — the Rust-side mutation never holds the interpreter lock. For *this* prototype
(synchronous, bounded, in-memory `GraphCore` calls, no I/O) that is a
correctness/consistency practice more than a latency necessity: a `parking_lot::RwLock`
read + a `DashMap` insert is sub-microsecond, so holding the GIL across it would barely
register. It matters for real once the durable path is wired in (see §11): a redb
group-commit **awaits** an off-reactor fsync (`AGENTS.md`'s "commit-before-ack"), and
that wait must not hold the GIL, or a busy embedded engine would stall every other Python
thread in the process (including, critically, `graph-os`'s own asyncio event loop if it
runs on the same interpreter) for the duration of a disk write. The production shape
for that case is a **persistent Tokio runtime owned once by the pyo3 module** (not
spun up per call), with the async commit driven as:

```rust
py.detach(|| RUNTIME.block_on(async_durable_commit(..)))
```

so the GIL is released for the *entire* await, and the blocking wait happens on a Tokio
worker thread, not the Python thread that made the call. This prototype does not need
this yet (no redb wiring — see §11), but the pattern is the one to use when it lands, and
is exactly the pattern `EmbeddedEngine`'s own durable writes already establish (Rust-side,
without pyo3 in between).

## 7. Identity, auth, and the `eg2.` envelope in-process

The out-of-process server treats every request as coming from an **untrusted network
boundary**: the `eg2.` envelope (principal, tenant, audience, effective agent, policy
version, scopes, timestamp, nonce, idempotency key) is HMAC-signed against a signer
registry and checked against a durable replay ledger before a single dispatch arm runs
(`AGENTS.md`, `docs/service_mode.md#authentication-protocol`). None of that exists to
authenticate *content* — it exists to authenticate *origin*, because the caller could be
any process on the wire.

In-process, origin is not in question: the caller **is** the same OS process, the same
address space, the same security boundary as the engine. This is not a new argument —
it is the exact justification `EmbeddedEngine` already documents for dropping HMAC *and*
the ACL isolation layer wholesale ("a trusted local caller"). `eg-pyengine` inherits the
identical reasoning and the identical scope cut for the same reason, which is why the
prototype's `#[pymethods]` carry no `agent_id`/signature/scope parameters at all.

What this means concretely, layer by layer:

- **`eg2.` HMAC envelope, signer registry, replay ledger, nonce/timestamp/skew** — not
  applicable in-process. There is no network packet to forge, replay, or intercept; the
  threat model these defend against does not exist across a plain function call within
  one process. **Do not** build an in-process HMAC check — it would authenticate a
  caller against itself.
- **Tenant/policy-version/audience matching** (`EPISTEMIC_GRAPH_TENANT`,
  `EPISTEMIC_GRAPH_AUDIENCE`, `EPISTEMIC_GRAPH_POLICY_VERSION`) — these are
  deployment-identity assertions ("this server IS tenant X"), not per-call auth. A
  self-contained deployment still has exactly one tenant identity; it is simply bound
  **once**, at `Engine::new()`/`EmbeddedTransport.__init__` construction time (a
  constructor argument or config read), rather than re-verified on every call. This is a
  config-binding question, not a crypto-verification one.
- **Per-agent RBAC / row-level security (`isolation::IsolationLayer`)** — this is the one
  place "in-process ⇒ no auth needed" is **not** the full story, and it is the one gap
  `EmbeddedEngine` shares with this prototype rather than one this crate introduces. RLS
  answers "can agent A see/write property P on node N", a question that still matters
  in-process whenever more than one agent shares **one** embedded engine (the common
  case — one `graph-os` serves many agents). Because there is no network boundary to
  authenticate, the identity for an RLS check does not need a *signature* — but it still
  needs to be **asserted**: the pyo3 call still needs an `agent_id`/scope parameter that
  feeds `IsolationLayer::filter_view` the same way the socket dispatch's
  already-verified `agent_id` does today, just sourced from "the caller told us" instead
  of "the caller cryptographically proved it." Concretely: a production surface adds
  `agent_id: Option<String>` to methods that need RLS, threaded to the same
  `IsolationLayer` `GraphCore` already carries, with `None` meaning "the trusted-caller
  default" (today's `EmbeddedEngine`/prototype behavior — no filtering at all). This is
  tracked as follow-up work (§11), since the prototype's minimal surface
  (`AddNode`/`GetNodeProperties` against `__commons__`-style single-tenant use) does not
  yet need it to prove the embedding pattern.
- **Durable audit chain** (`crate::audit`, hash-chained tamper-evident log, feature
  `security`) — an independent, durability-layer concern, not a transport one. Neither
  `EmbeddedEngine` nor this prototype wires it today; a production in-process engine
  that wants tamper-evidence needs the same audit-chain hook both callers currently lack
  (§11 — another argument for hoisting shared glue down into `eg-core` once there are two
  callers instead of one).
- **CDC / change-notification** (`ChangeNotifier`, the `changes` field on `GraphCore`) —
  transport-independent already: it lives on `GraphCore` itself, so an in-process
  mutation emits the same change events a socket-dispatched one does, for free, with no
  additional wiring. Nothing to do here.

**Net effect:** dropping `eg2.` in-process is not "less secure," it is "a different,
correctly-scoped threat model" — exactly the framing `EmbeddedEngine`'s own docs already
use for the Rust API. The one real, honestly-scoped gap this workstream carries forward
(not introduces) is per-agent RLS in a **multi-agent-sharing-one-embedded-engine**
deployment, tracked explicitly in §11 rather than silently skipped.

## 8. One binary, both artifacts: packaging

`pyproject.toml` today: `bindings = "bin"`, `features = ["full", "ast-extended"]` — one
maturin invocation, one wheel, the `epistemic-graph-server` console-script binary plus the
pure-Python `epistemic_graph/` package. The numeric kernel already proves the pattern for
folding a *second*, independently-built pyo3 extension into that same wheel; this
workstream's engine binding is the second instance of the identical pattern, run
alongside the first:

```
┌─────────────────────────────────────────────────────────────────────────┐
│ 1. maturin build --release --features full,ast-extended                 │
│    (bindings="bin", repo root pyproject.toml)                           │
│    → dist/epistemic_graph-X.Y.Z-*.whl                                   │
│      = epistemic-graph-server binary + pure-python epistemic_graph/     │
├─────────────────────────────────────────────────────────────────────────┤
│ 2. maturin build --release -m crates/eg-numeric/Cargo.toml \            │
│      --features python        (existing)                                │
│    → numeric-wheel/*.whl  (epistemic_graph.numeric)                     │
├─────────────────────────────────────────────────────────────────────────┤
│ 2b. maturin build --release -m crates/eg-pyengine/Cargo.toml \          │
│       --features python       (NEW, this workstream)                    │
│     → engine-wheel/*.whl  (epistemic_graph.engine)                      │
├─────────────────────────────────────────────────────────────────────────┤
│ 3. python scripts/inject_numeric_kernel.py   dist/*.whl numeric-wheel/*.whl │
│ 3b. python scripts/inject_pyengine.py        dist/*.whl engine-wheel/*.whl  │
│     (NEW script, same RECORD-rewrite/mode-preserving zip surgery as 3)   │
├─────────────────────────────────────────────────────────────────────────┤
│ Result: ONE wheel — epistemic-graph-server binary +                     │
│         epistemic_graph/{numeric,engine}.abi3.so + pure-python package  │
└─────────────────────────────────────────────────────────────────────────┘
```

`scripts/inject_pyengine.py` is not shipped by this workstream (see §11) but is a
near-mechanical copy of `scripts/inject_numeric_kernel.py` — same `_find_kernel_extension`
lift (searching for a module basename starting with `engine` instead of `numeric`), same
`RECORD` rewrite, same mode-preservation fix for the console-script executable bit. Both
injections compose freely (order does not matter, since each only reads/rewrites the
target wheel's `RECORD`, never the other's extension). `scripts/check_wheel_completeness.py`
(which already asserts the numeric kernel ships) gets a matching assertion for the engine
extension once §11's packaging work lands.

**Runtime, not install-time, selection.** `pip install epistemic-graph` always gets both
artifacts (the `bin` **and** the extension) once this ships — installing does not commit
you to a transport. `graph-os` picks the transport (`EmbeddedTransport` vs
`SocketTransport`, §4) from its own config at **startup**, which is exactly what W-D
(deployment modes) and W-E (genesis profiles) key off of: a self-contained profile
constructs `EmbeddedTransport` and never spawns `epistemic-graph-server` at all; a
scale-out profile spawns/points at the server binary and constructs `SocketTransport`
as it does today. Neither choice changes what got installed.

## 9. Feature-gating matrix

| Feature | In `default`/`full`? | Pulls pyo3? | Notes |
|---|---|---|---|
| `server` | Yes | No | Tokio UDS/TCP listener + dispatch (today's shape) |
| `redb` | Yes | No | Durable authoritative store |
| `embedded` | Yes (`= ["redb"]`) | No | `EmbeddedEngine` — the Rust in-process API this design's pattern is proven by |
| `cluster` | No (opt-in layer) | No | `raft` + distributed compute — orthogonal axis, composes freely with `pyo3-engine` |
| `full-extras` | No (opt-in layer) | No | GPU/ROS2 — orthogonal axis |
| **`pyo3-engine`** | **No** (opt-in, this workstream) | **Yes**, via `eg-pyengine/python` only | The unified single-binary path |

`pyo3-engine` is an independent axis from `cluster`/`full-extras`: nothing about
embedding the engine in-process conflicts with also being a Raft node or linking the GPU
layer at the *source* level (all three are just optional Cargo features). In practice a
given **deployed** binary is either the out-of-process server or carries the embedded
extension (or, per §8, a wheel that carries both artifacts and chooses at runtime) — the
source-level composability is what keeps this a one-line feature addition rather than a
fork of the build matrix.

Mechanical proof this stays true, not just a convention: `scripts/check_no_pyo3.sh` now
additionally asserts (a) `cargo tree -e normal` (the default/`full` feature set) links no
crate matching `pyo3`, and (b) `pyo3-engine` never appears inside the `default`/`full`/
`cluster` feature definitions in `Cargo.toml` — both fail the gate if a future change
accidentally folds this opt-in path into an aggregate the scale-out build enables.

## 10. Bench: isolating the transport tax

`benches/pyo3_inprocess_vs_uds_bench.rs` (criterion, `required-features = ["server"]`)
answers the specific question this workstream's "no measured downside" gate needs:
holding the actual `GraphCore` mutation constant, what does the socket add?

- **`inprocess`** — calls `GraphCore::add_node`/`get_node_properties` directly. This is
  the Rust-side cost `eg-pyengine`'s `#[pymethods]` pay inside `Python::detach` (the bench
  does not spin up a Python interpreter — it isolates the transport delta the embedding
  removes, not the pyo3 marshaling cost on top, which is orders of magnitude smaller than
  a socket round trip for a `str`/`bytes`-shaped call).
- **`uds_socket`** — wraps the identical call behind a real 4-byte length-prefixed
  MessagePack frame over a real Unix domain socket, on one persistent connection reused
  across every sample (matching how `epistemic_graph.client` actually keeps one
  connection open, not reconnecting per call). It deliberately does **not** implement the
  `eg2.` envelope (HMAC verification, replay-ledger check) — that cost is orthogonal to
  *where the engine runs* and is already captured in the existing end-to-end measurement
  (`docs/benchmarks.md`: `AddNode` p50 ≈ 0.187 ms / p99 ≈ 0.223 ms over UDS,
  `scripts/bench_transport.py`). This bench isolates
  serialize-then-socket-then-deserialize specifically, so its `uds_socket` numbers are
  expected to land **below** 0.187 ms — the gap between the two is roughly what the
  envelope itself costs, a number this workstream does not change and so does not
  re-measure.

Both arms run the *same* op (`AddNode` then `GetNodeProperties`) against the *same* kind
of `GraphCore`, swept at `n = 100` and `n = 1,000` elements per sample (a `Cell<usize>`
offset counter keeps node ids unique across samples, mirroring
`benches/redb_group_commit_bench.rs`'s own idiom).

**Measured this session** (`cargo bench --profile release --features server --bench
pyo3_inprocess_vs_uds_bench --target-dir ./target-isolated`, on a shared, concurrently-
loaded host — see the caveat below):

| Arm | n | time (min / median / max) | median per add+get pair |
|---|---:|---|---:|
| `inprocess` | 100 | 413.4 µs / 534.9 µs / 732.9 µs | 5.35 µs |
| `inprocess` | 1,000 | 4.41 ms / 6.22 ms / 9.42 ms | 6.22 µs |
| `uds_socket` | 100 | 8.00 ms / 8.16 ms / 8.31 ms | 81.6 µs |
| `uds_socket` | 1,000 | 81.3 ms / 83.3 ms / 85.7 ms | 83.3 µs |

The in-process arm ran **~13-15x faster per op** than this bench's envelope-free UDS arm
(≈2.7-3.1 µs/op in-process vs. ≈40.8-41.7 µs/op over the socket, halving the pair cost
above for a single op). This is internally consistent with the existing full end-to-end
baseline: this bench's `uds_socket` numbers (≈41-42 µs/op) land well **below**
`docs/benchmarks.md`'s ≈187 µs p50 for the SAME op over the SAME transport — exactly the
predicted relationship, since that existing number additionally pays the `eg2.` HMAC/
replay-ledger envelope this bench deliberately excludes; the ≈145 µs gap between the two
is a believable order of magnitude for that envelope's own cost, not an inconsistency.

**Caveat.** These numbers were collected on a host running several other concurrent,
CPU-heavy `cargo` compilations at the time (this session's own build activity plus
unrelated processes already running on the shared box) — criterion flagged 2-4 high-
severity outliers per group, and the `inprocess` arm's min/max spread (413 µs to 733 µs
at n=100) reflects that noise. Treat the absolute values as indicative, not a clean-room
result; the ~13-15x relative gap is the robust signal (host noise inflates both arms
together, it does not explain a directional, order-of-magnitude difference). Re-run on a
quiet host for a publishable number. Re-running also requires `--profile release`
explicitly (see the crate's own bench comment / the final report) — a bare `cargo bench`
in this workspace resolves a different fingerprint than `cargo build --release` and
forces a second full dependency recompilation even for byte-identical optimization
settings.

**Reading the result.** The program doc's gate is *"no measured downside before this
becomes default"* — that is a claim about the **unified, self-contained** deployment
shape specifically (one `graph-os`, no horizontal fan-out), not a claim that in-process is
strictly faster in every configuration. The out-of-process shape's entire reason to exist
is horizontal scale-out (one engine, many clients, GIL-free) — a fair comparison is
narrower than "socket vs. no socket": it is "for the self-contained deployment this
feature targets, does removing the socket round trip measurably help, and does it cost
anything else (e.g. GIL contention with `graph-os`'s own event loop) that would eat the
win back." This bench answers the first half (transport tax, isolated); §11 lists what a
follow-up needs to close the second half (a concurrent-load / GIL-contention soak, not a
single-threaded criterion sample).

## 11. Remaining work to productionize (honest gap list)

This prototype proves the embedding *pattern*; it is deliberately not a production
surface. In priority order:

1. **Durability.** The prototype is in-memory only. Wiring redb means either (a)
   depending on `eg-core`'s durable primitives directly from `eg-pyengine` the way
   `EmbeddedEngine` depends on `crate::redb_store`/`crate::mutation_apply` (both live in
   the facade, above `eg-pyengine` in the DAG — not directly reusable, same constraint as
   §3), or (b) — the better fix — **hoisting the shared "resolve graph, apply Method,
   commit durably" glue down into `eg-core` itself** as a small, transport-agnostic
   helper both `EmbeddedEngine` (Rust) and `eg-pyengine` (Python) call, so there is
   genuinely one implementation instead of the two thin, drifting copies this design
   currently accepts as a DAG-forced trade-off. Do this **before** shipping a durable
   `eg-pyengine` surface, not after — it is materially easier to hoist now, with one small
   duplicate, than after a second production surface depends on the duplicate shape.
2. **Real batch methods.** `add_node`/`get_node_properties` per call is the proof shape,
   not the production one (§5). Add `add_nodes_batch`/`get_node_properties_batch`
   mirroring `Method::BatchUpdate`/`GetNodePropertiesBatch` before any real caller adopts
   this path for bulk work.
3. **The Python transport-swap layer.** `epistemic_graph/embedded.py` (or wherever this
   lands) implementing the `_Transport` protocol from §4 and porting `NodeClient`,
   `EdgeClient`, `GraphOperationsClient`, `AnalyticsClient`, etc. one at a time, each with
   the same round-trip test convention `AGENTS.md`'s "adding a capability" recipe already
   requires. Until this exists, `agent_utilities` cannot actually select the embedded
   path — it is transport-ready but not yet transport-selectable.
4. **Per-agent RLS in-process** (§7) — needed the moment more than one agent shares one
   embedded engine with different read/write scopes; not needed for a genuinely
   single-tenant self-contained deployment.
5. **Packaging.** `scripts/inject_pyengine.py` (mirrors `inject_numeric_kernel.py`),
   wired into `.github/workflows/release-build.yml` alongside the existing numeric-kernel
   fold-in step, plus a `scripts/check_wheel_completeness.py` assertion that the shipped
   wheel contains `epistemic_graph/engine*.so` whenever the release build opts into
   `pyo3-engine` (this is a **W-B/W-C-adjacent** follow-up, not this workstream's build —
   W-A's job was proving the crate and the pattern compile and work, not shipping a
   release pipeline).
6. **A persistent Tokio runtime inside the pyo3 module** — needed once (1) lands, so the
   durable commit's await happens on a Tokio worker under `Python::detach`, not on the
   calling Python thread (§6).
7. **Concurrent-load / GIL-contention validation.** This session's bench is
   single-threaded criterion sampling (§10) — it proves the transport-tax removal, not
   the "does an embedded engine under real concurrent agent load stall `graph-os`'s own
   asyncio loop" question the program doc's gate ultimately cares about. That needs a
   Python-side stress harness (many concurrent coroutines calling into the embedded
   engine while a control coroutine measures event-loop responsiveness) — out of scope
   for a Rust-only criterion bench, tracked here rather than skipped silently.
8. **Single-writer-per-persist-dir enforcement.** Once (1) lands, exactly one embedded
   engine may hold a given `persist_dir` open (redb is single-writer-per-file — the same
   constraint `EmbeddedEngine`/the server's `persist_lock` already enforce). Document/
   guard this explicitly for the embedded case (e.g. two `graph-os` processes must never
   both embed against the same directory) before it ships as a supported deployment
   shape.
9. **`EngineResolver` / genesis wiring** (W-D, W-E). `agent-utilities`' engine resolution
   needs a new "embedded" mode alongside its existing socket-autostart behavior
   (`EPISTEMIC_GRAPH_IDLE_SHUTDOWN_SECS`'s shared-tiny-daemon mode); `agent-os-genesis`
   needs to pick this shape for the self-contained default profile. Both are explicitly
   out of scope for W-A (they are W-D/W-E), listed here only so the dependency is visible.

None of the above blocks what this workstream committed to: a feature-gated crate that
compiles, embeds the real engine core with no socket, and is measured against the
out-of-process path on the specific op the edict is about.
