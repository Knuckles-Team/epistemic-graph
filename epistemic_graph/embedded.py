"""`EmbeddedTransport` -- the in-process (native-extension) transport for `epistemic_graph
.client`'s existing sub-client surface (`NodeClient`, `EdgeClient`, `QueryClient`,
...). See `docs/architecture/unified-inprocess-engine.md` §4 ("Same client API
surface -- only the transport swaps") and `plans/pyengine/EG-PYENGINE-PLAN.md`
§4.6 for the design this implements.

This is a NEW file, not a change to `epistemic_graph/client.py` -- the design's
single most important inherited decision (plan §1.1/§2.1) is that `client.py`'s
~40 sub-client classes stay untouched. Every one of them only ever does
``await self._client._send(method, params)``. Today ``self._client`` is always
a live `EpistemicGraphClient` (which owns the UDS/TCP socket AND implements
``_send`` itself, `epistemic_graph/client.py:13260`). `EmbeddedTransport` is a
second, from-scratch implementation of that same ``_send(method, params=None,
*, graph=None)`` shape, calling straight into the in-process native extension
(`epistemic_graph.engine`, built from `crates/eg-pyengine`) instead of a socket.
See the bottom of this file for the EXACT seam a future `client.py` change would
use to accept either transport -- not made here, by design.

## Persist-dir contract (plan §1.9, §9.2 Gate A -- data-loss-critical)

``epistemic-graph`` MUST NOT import ``agent_utilities`` (that would invert the
repo dependency direction), so this module cannot call
``agent_utilities.knowledge_graph.core.graph_compute``'s config helper directly.
Instead it reads the SAME environment variable that function reads today --
confirmed this session by reading
``agent_utilities/knowledge_graph/core/graph_compute.py:1900-1922``:

    persist_dir = setting("GRAPH_SERVICE_PERSIST_DIR")
    if persist_dir is None:
        try:
            from agent_utilities.core.paths import data_dir
            persist_dir = str(data_dir() / "graph_snapshots")
        except Exception:
            persist_dir = None

``setting("GRAPH_SERVICE_PERSIST_DIR")`` resolves that name as a plain
environment variable (confirmed: every other ``GRAPH_SERVICE_*``-prefixed
setting this same codebase reads -- `GRAPH_SERVICE_SOCKET`, `GRAPH_SERVICE_
AUTH_SECRET`, `GRAPH_SERVICE_RPC_TIMEOUT`, etc, `epistemic_graph/client.py` --
is read with a bare ``os.environ.get(...)``, and this repo's own Python code
never imports a ``setting()`` indirection layer). **`GRAPH_SERVICE_PERSIST_DIR`
is therefore a cross-repo contract name shared verbatim with `agent-utilities`
-- renaming it on either side without a coordinated migration silently breaks
the other.**

The `agent_utilities` fallback above -- silently picking `data_dir() /
"graph_snapshots"` when the setting is unset -- is EXACTLY the data-loss hazard
plan §1.9 identifies (that fallback resolves inside the pod's `emptyDir` on the
live unified-in-process deployment, not the PVC) and BUG-PE-003 (confirmed
2026-08-25: `GRAPH_SERVICE_PERSIST_DIR` is absent from the live deployment's
env/ConfigMap today, latent only because the current engine subprocess is
launched with an explicit `--persist-dir` argv instead of this env var).
`EmbeddedTransport` deliberately does NOT reproduce that fallback: an unset
setting (and no explicit `persist_dir=` argument) makes this module refuse to
construct an `EmbeddedTransport` at all, rather than silently running
unpersisted against a path nobody chose. See `_resolve_persist_dir` below.
"""

from __future__ import annotations

import importlib
import os
from collections.abc import Callable
from typing import Any

#: The cross-repo contract name -- see the module docstring's "Persist-dir
#: contract" section. Read directly (no `agent_utilities` import: the wrong
#: dependency direction for this repo, plan §1.9/§4.6).
_PERSIST_DIR_ENV_VAR = "GRAPH_SERVICE_PERSIST_DIR"

#: Explicit, self-documenting opt-out for genuinely ephemeral use (tests, a
#: throwaway local demo) -- mirrors `sqlite3.connect(":memory:")`'s own
#: well-known convention rather than inventing a new keyword argument for it.
_IN_MEMORY_SENTINEL = ":memory:"

#: One list, written ONCE by Wave 0 (plan §4.6) -- every domain lane's module
#: name, in the order the plan enumerates them (§4.6/§5.1). No lane appends
#: to this list after Wave 0 lands; a Wave-1 lane's own module already has an
#: entry here before that lane starts.
_EMBEDDED_OP_MODULES: tuple[str, ...] = (
    "graph_ops",
    "query",
    "txn",
    "finance",
    "datascience",
    "mining",
    "graphlearn",
    "pipeline",
    "timeseries",
    "blob",
    "kv",
    "rdf",
    "streaming",
    "broker",
    "channels",
    "jobs",
    "statechart",
    "wasm_udf",
    "sqlite_file",
    "identity",
    "rbac",
    "admin_ctl",
    "cluster_ctl",
    "modality",
    "longtail",
)


class EmbeddedTransportConfigError(RuntimeError):
    """Raised by `EmbeddedTransport()` when persistence configuration cannot
    be resolved safely -- see `_resolve_persist_dir`. Deliberately a
    `RuntimeError` subclass (matching the generic base class the socket
    transport raises for a plain engine error, `client.py:13399`), not a new
    parallel hierarchy (plan §4.4's rule, applied here too even though this
    specific error has no socket-side equivalent -- it can only happen
    embedded, since a socket client never chooses a persist directory).
    """


def _resolve_persist_dir(explicit: str | None) -> str | None:
    """Resolve the directory `EmbeddedTransport` should persist through, or
    `None` for an explicit, deliberate in-memory-only engine.

    Precedence: an explicit `persist_dir=` argument always wins (including
    the `":memory:"` sentinel); otherwise reads `GRAPH_SERVICE_PERSIST_DIR`
    (see the module docstring). **Never** falls back to a path nobody
    configured -- an unset setting with no explicit argument raises
    `EmbeddedTransportConfigError` (plan §1.9's "(a) refuse to start" option)
    rather than silently picking `agent_utilities.core.paths.data_dir()`'s
    equivalent the way today's subprocess-autostart path does.
    """
    if explicit == _IN_MEMORY_SENTINEL:
        return None
    if explicit is not None:
        return explicit
    env_value = os.environ.get(_PERSIST_DIR_ENV_VAR, "")
    if env_value:
        return env_value
    raise EmbeddedTransportConfigError(
        f"EmbeddedTransport: {_PERSIST_DIR_ENV_VAR} is not set and no "
        "persist_dir was given -- refusing to start rather than silently "
        "running unpersisted (or, worse, picking an unmounted default path: "
        "this is the exact data-loss hazard plan EG-PYENGINE-PLAN.md §1.9/"
        "§9.2 Gate A and BUG-PE-003 describe for the equivalent agent_utilities"
        ".knowledge_graph.core.graph_compute fallback). Pass "
        f"persist_dir={_IN_MEMORY_SENTINEL!r} explicitly if an ephemeral, "
        "unpersisted engine is genuinely what you want (e.g. a test)."
    )


class EmbeddedTransport:
    """The in-process transport: `_send` is a dict lookup into a dispatch
    table built ONCE at construction (never per call -- the whole point of
    this transport existing is to beat the socket path's own measured
    round-trip floor, so no per-call module import/introspection/logging
    belongs on this path).

    ``agent_id``/``tenant`` bind the DEFAULT identity for the lifetime of
    this instance -- the common case for a genuinely single-tenant embedded
    deployment (plan §4.3). ``_send``'s own ``agent_id`` keyword (below) is a
    PER-CALL override of that default, added once `crates/eg-pyengine`
    started accepting one on its RLS-relevant methods
    (`authority::EmbeddedAuthority::can_see_properties`, commit
    `b48ee56c`): the mechanism BUG-PE-022 needed so two principals can share
    ONE embedded engine/`persist_dir` (a second same-process open of one
    `persist_dir` is denied at the OS advisory-lock level, so two
    `EmbeddedTransport`s can never observe each other's writes -- see
    `tests/parity/conftest.py`'s `pair_factory`).
    """

    def __init__(
        self,
        graph_name: str = "__commons__",
        *,
        persist_dir: str | None = None,
        agent_id: str | None = None,
        tenant: str | None = None,
    ) -> None:
        # Dynamic (not `from . import engine`): the compiled native extension
        # (`crates/eg-pyengine --features python`) is not always present in
        # the source tree mypy statically analyzes (this repo's own `.mypy
        # .ini`-equivalent config has no stub for it, and Wave 0 does not
        # build it) -- a static import would make mypy fail with "Module
        # 'epistemic_graph' has no attribute 'engine'" on every checkout
        # that hasn't built the extension, which is not a real defect.
        _native = importlib.import_module("epistemic_graph.engine")

        resolved_persist_dir = _resolve_persist_dir(persist_dir)
        self._engine = _native.Engine(
            persist_dir=resolved_persist_dir, agent_id=agent_id, tenant=tenant
        )
        self._graph_name = graph_name
        self._engine.create_graph(graph_name)
        # `Callable[..., Any]` (not a fixed arity) deliberately: most domain
        # modules' `build_dispatch` return an empty dict (Wave-1 stubs, never
        # actually called), and the one Wave-0 module with real closures
        # (`_embedded_ops/graph_ops.py`) accepts the extra `agent_id`
        # parameter `_send` passes below -- a fixed 2-arg `Callable` type
        # here would force every stub file to also declare the 3rd
        # parameter it never uses, for no runtime benefit (their handlers
        # are never invoked; `_send` raises `NotImplementedError` first).
        self._dispatch: dict[str, Callable[..., Any]] = {}
        for _mod_name in _EMBEDDED_OP_MODULES:
            _mod = importlib.import_module(f"epistemic_graph._embedded_ops.{_mod_name}")
            self._dispatch.update(_mod.build_dispatch(self._engine))

    async def _send(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        graph: str | None = None,
        agent_id: str | None = None,
    ) -> Any:
        """``agent_id``, when given, OVERRIDES this transport's construction-time
        identity for THIS call only -- threaded straight through to the
        dispatch handler, which passes it to whichever native `PyEngine`
        method accepts an `agent_id` override (currently
        `get_node_properties`/`has_node`, `crates/eg-pyengine/src/lib.rs`).
        `None` (the default) preserves today's construction-time-only
        behavior byte-for-byte. A handler for a method with no override
        support simply ignores the extra argument.
        """
        handler = self._dispatch.get(method)
        if handler is None:
            raise NotImplementedError(f"EmbeddedTransport: {method} not yet ported")
        return handler(graph or self._graph_name, params or {}, agent_id)


# ─────────────────────────────────────────────────────────────────────────
# The transport-swap seam this module sets up -- NOT implemented here.
# ─────────────────────────────────────────────────────────────────────────
#
# Today, `EpistemicGraphClient` (`epistemic_graph/client.py`) IS the socket
# transport: its `__init__` (`client.py:12318` onward) takes an already-open
# `asyncio.StreamReader`/`StreamWriter` pair and stores them directly on
# `self`, and its own `_send` (`client.py:13260-13407`) both shapes the
# request (binds `agent_id`, computes the HMAC `auth_token`, handles the
# `ApplyChangeEnvelope`/`ApplyChangeEnvelopes` request-id binding) AND does
# the socket I/O (frames, writes, awaits the demux'd response) in one method
# body. Every sub-client (`NodeClient`, `EdgeClient`, ...) holds a reference
# to the `EpistemicGraphClient` instance as `self._client` and calls
# `self._client._send(method, params)` -- that call shape is `_send`'s ENTIRE
# public contract as far as a sub-client is concerned, and it already matches
# `EmbeddedTransport._send`'s signature above (`(method, params=None, *,
# graph=None)`) except for the extra `idempotency_key` keyword client.py's
# version also accepts (used only by the two `ApplyChangeEnvelope*` methods).
#
# So the minimal, mechanical follow-up change (a LATER lane's job, out of
# scope here per the task brief) is:
#
#   1. Split `EpistemicGraphClient._send`'s body at `client.py:13260` into
#      two pieces: keep the request-shaping prologue (agent_id binding, HMAC
#      auth_token, the envelope-binding special cases) as-is on
#      `EpistemicGraphClient`, but move the "do the socket I/O and return the
#      decoded result" tail into a `SocketTransport` object satisfying the
#      SAME `_send(method, params=None, *, graph=None)` shape.
#   2. Add a `transport: _Transport | None = None` parameter to
#      `EpistemicGraphClient.__init__`/`.connect()` (default `None` = build a
#      `SocketTransport` from the existing reader/writer args, preserving
#      every current call site byte-for-byte); when given, skip the
#      reader/writer setup and store the provided transport instead.
#   3. `EpistemicGraphClient._send` becomes a thin dispatcher: shape the
#      request as it does today, then `return await self._transport._send(...)`
#      instead of writing to `self._writer` directly.
#
# A construction path like
# `EpistemicGraphClient(transport=EmbeddedTransport(...), verified_context=...)`
# then makes every existing sub-client work unmodified over the embedded
# engine -- `agent_utilities` code calling `client.nodes.add(...)` never has
# to know which transport backs it (`docs/architecture/unified-inprocess-
# engine.md` §4's own framing). This file does not touch `client.py` to keep
# that change small, reviewable, and owned by whichever lane picks it up.
