"""The differential parity harness -- the plan's correctness backbone (§3).

FOUNDATION-owned (Wave 0, plan §3.1/§4.6). No lane edits this file after
Wave 0 lands -- every Wave-1 lane owns only its own `test_parity_<domain>.py`
file(s) (plan §5.0's file-disjointness rule).

Two functions, plus one small decode helper both share:

  * `assert_parity` -- runs one operation through both transports (a socket
    `EpistemicGraphClient` and an `EmbeddedTransport`) and asserts equal
    DECODED results.
  * `assert_rls_isolation` -- reads the same thing as two different
    principals, through both transports, and asserts they see the same
    thing as each other (never a superset/subset mismatch, plan §3.2).

Both compare DECODED values, never raw bytes: msgpack framing differs
between the socket transport (length-prefixed frame + `eg2.` HMAC envelope)
and the embedded transport (a plain in-process return value) even when the
underlying data is identical (plan §3, §3.2).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import msgpack


@dataclass(frozen=True)
class TransportPair:
    """One (socket, embedded) client pair, both bound to the same identity
    and target graph -- what `assert_parity`/`assert_rls_isolation` compare.

    `embedded` is typically a `BoundEmbeddedTransport` (below), NOT a raw
    `EmbeddedTransport` -- see that class's doc for why (BUG-PE-022)."""

    socket: Any
    embedded: Any


class BoundEmbeddedTransport:
    """Binds ONE agent_id's per-call override onto a SHARED
    `EmbeddedTransport` (one native `Engine`/one `persist_dir`) -- the
    one-engine-two-identities shape BUG-PE-022 needed.

    Two principals can only observe each other's writes if they share one
    embedded `Engine`: the Rust lane's `src/persist_lock.rs` advisory flock
    is scoped to the OS open-file-description, not the process, so a SECOND
    same-process open of one `persist_dir` (what building a separate
    `EmbeddedTransport` per principal used to do, `EG-PYENGINE-PLAN.md`'s
    BUG-PE-022) is denied outright -- it can never reach the point of
    comparing what two principals see. `crates/eg-pyengine` closed the
    actual gap this exists to bridge by adding a per-call `agent_id`
    override to the RLS-relevant `PyEngine` methods (commit `b48ee56c`,
    `authority::EmbeddedAuthority::can_see_properties`); `tests/parity/
    conftest.py`'s `pair_factory` builds ONE `EmbeddedTransport` per test and
    hands out one `BoundEmbeddedTransport` per agent_id, each forwarding to
    the same underlying transport with its own identity as the `agent_id`
    override on every `_send` call.

    Matches `EmbeddedTransport._send`'s own `(method, params=None, *,
    graph=None)` shape exactly, so `assert_parity`/`assert_rls_isolation`'s
    `_try_send` calls it identically to any other transport -- callers never
    need to know whether an embedded pair member is bound or not.
    """

    def __init__(self, shared: Any, agent_id: str) -> None:
        self._shared = shared
        self._agent_id = agent_id

    async def _send(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        graph: str | None = None,
    ) -> Any:
        return await self._shared._send(
            method, params, graph=graph, agent_id=self._agent_id
        )


def _decode(value: Any) -> Any:
    """Msgpack-decode a raw `bytes`/`bytearray` result; pass anything else
    through unchanged. `client.py`'s own sub-clients decode selectively per
    method (e.g. `NodeClient.properties`, `client.py:2704-2711`) -- comparing
    decoded values is the only comparison that is correct regardless of
    which transport is on the other end."""
    if isinstance(value, (bytes, bytearray)):
        return msgpack.unpackb(bytes(value), raw=False)
    return value


async def _try_send(
    transport: Any, method: str, params: dict[str, Any] | None, graph: str | None
) -> tuple[Any, BaseException | None]:
    """Call `transport._send(...)`, returning `(decoded_result, None)` on
    success or `(None, exception)` on failure -- never raises itself, so a
    caller can compare the outcome SHAPE (raised vs returned) across two
    transports before deciding whether either is a real test failure."""
    try:
        return _decode(await transport._send(method, params, graph=graph)), None
    except (
        Exception
    ) as exc:  # intentionally broad: the exception is compared below, never swallowed
        return None, exc


async def assert_parity(
    pair: TransportPair,
    method: str,
    params: dict[str, Any] | None = None,
    *,
    graph: str | None = None,
) -> Any:
    """Run `method(params)` through both transports in `pair`; assert equal
    decoded results (or, on failure, the same exception CLASS -- message
    text may legitimately differ, since the embedded path has no socket
    peer/transport detail to include, plan §3.2 -- and, where the wire error
    carries an `UPPER_CASE:` semantic prefix, the same prefix). Returns the
    shared, decoded result on success.

    NOTE on scope: the plan's own sketch (§4.6) additionally names a
    `principal=` keyword on this function. It is intentionally NOT
    implemented here, even though `EmbeddedTransport`/`PyEngine` gained a
    per-call `agent_id` override since this was first written (BUG-PE-022,
    `crates/eg-pyengine` commit `b48ee56c`): `assert_parity` compares ONE
    identity's view across two transports, and `pair.socket`/`pair.embedded`
    are already bound to that one identity (a `BoundEmbeddedTransport` for
    the embedded side, see `TransportPair`'s doc) -- a `principal=` keyword
    here would only ever re-bind both sides to the SAME identity `pair` was
    already built with, which is not a useful capability. Multi-principal
    comparison stays `assert_rls_isolation`'s job -- it takes two already-
    built `TransportPair`s (one per principal, both now safely sharing one
    embedded `Engine`/`persist_dir`, see `pair_factory`) instead of a
    per-call identity switch.
    """
    socket_result, socket_exc = await _try_send(pair.socket, method, params, graph)
    embedded_result, embedded_exc = await _try_send(
        pair.embedded, method, params, graph
    )

    if socket_exc is not None or embedded_exc is not None:
        assert type(socket_exc) is type(embedded_exc), (
            f"{method}: exception class mismatch -- socket raised "
            f"{type(socket_exc).__name__ if socket_exc else None!r}, embedded raised "
            f"{type(embedded_exc).__name__ if embedded_exc else None!r}"
        )
        socket_prefix = str(socket_exc).split(":", 1)[0] if socket_exc else None
        if socket_prefix is not None and socket_prefix.isupper():
            embedded_prefix = (
                str(embedded_exc).split(":", 1)[0] if embedded_exc else None
            )
            assert socket_prefix == embedded_prefix, (
                f"{method}: semantic error-code prefix mismatch -- "
                f"socket={socket_prefix!r} embedded={embedded_prefix!r}"
            )
        assert socket_exc is not None
        raise socket_exc

    assert (
        socket_result == embedded_result
    ), f"{method}: result mismatch -- socket={socket_result!r} embedded={embedded_result!r}"
    return socket_result


async def assert_rls_isolation(
    method: str,
    params: dict[str, Any],
    *,
    owner: TransportPair,
    other: TransportPair,
    graph: str | None = None,
) -> None:
    """Read `method(params)` as `owner` and as `other`, through BOTH
    transports, asserting `other` sees NOTHING (either transport may enforce
    this as a raised access-denied exception or as a falsy/empty result --
    either style is accepted, but the two transports must agree on which
    one) while `owner` sees the SAME thing on both transports. The caller
    writes the data being read (as `owner`) BEFORE calling this -- this
    function only checks the READ side of isolation.

    Never asserts a superset or subset relationship (plan §3.2's rule) --
    `other`'s two outcomes (socket, embedded) are compared to EACH OTHER,
    not to `owner`'s.
    """
    owner_socket, owner_socket_exc = await _try_send(
        owner.socket, method, params, graph
    )
    owner_embedded, owner_embedded_exc = await _try_send(
        owner.embedded, method, params, graph
    )
    assert (
        owner_socket_exc is None
    ), f"{method}: owner's own socket read failed: {owner_socket_exc!r}"
    assert (
        owner_embedded_exc is None
    ), f"{method}: owner's own embedded read failed: {owner_embedded_exc!r}"
    assert owner_socket == owner_embedded, (
        f"{method}: owner result mismatch across transports -- "
        f"socket={owner_socket!r} embedded={owner_embedded!r}"
    )
    assert (
        owner_socket
    ), f"{method}: owner unexpectedly saw nothing -- was the data actually written?"

    other_socket, other_socket_exc = await _try_send(
        other.socket, method, params, graph
    )
    other_embedded, other_embedded_exc = await _try_send(
        other.embedded, method, params, graph
    )

    assert type(other_socket_exc) is type(other_embedded_exc), (
        f"{method}: other's raise-vs-return shape differs across transports -- "
        f"socket={other_socket_exc!r} embedded={other_embedded_exc!r}"
    )
    if other_socket_exc is None:
        assert (
            not other_socket
        ), f"{method}: other unexpectedly saw owner's data (socket)"
        assert (
            not other_embedded
        ), f"{method}: other unexpectedly saw owner's data (embedded)"
