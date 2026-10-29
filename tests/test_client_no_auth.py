"""EG-IDENTITY-R003.1: explicit anonymous local wire, without invented claims."""

import asyncio
import contextlib
import json
from typing import Literal, cast

import msgpack
import pytest
from conftest import request_context

from medulla.client import EpistemicGraphClient

pytestmark = pytest.mark.no_engine


@contextlib.asynccontextmanager
async def _server():
    frames, writers, tasks = [], [], set()

    async def serve(reader, writer):
        task = asyncio.current_task()
        assert task is not None
        tasks.add(task)
        writers.append(writer)
        try:
            while True:
                size = int.from_bytes(await reader.readexactly(4), "big")
                request = msgpack.unpackb(await reader.readexactly(size), raw=False)
                frames.append(request)
                data = msgpack.packb({"id": request["id"], "result": 1})
                writer.write(len(data).to_bytes(4, "big") + data)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()
            tasks.discard(task)

    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    try:
        yield f"127.0.0.1:{server.sockets[0].getsockname()[1]}", frames
    finally:
        server.close()
        for writer in writers:
            writer.close()
        if tasks:
            await asyncio.gather(*tuple(tasks))
        await server.wait_closed()


def test_anonymous_wire_and_verified_default(monkeypatch):
    async def run():
        async with _server() as (address, frames):
            anonymous = await EpistemicGraphClient.connect(
                tcp_addr=address, auth_mode="none"
            )
            authenticated = await EpistemicGraphClient.connect(
                tcp_addr=address,
                auth_secret="synthetic-test-secret",
                verified_context=request_context(),
            )
            try:
                assert anonymous.authenticated is False
                assert anonymous.auth_mode == "none"
                assert anonymous._verified_context is None
                assert not anonymous._auth_secret
                assert await anonymous._send("NodeCount") == 1
                assert frames[-1]["auth_token"] == ""
                assert frames[-1]["agent_id"] is None
                assert "context" not in frames[-1]
                assert authenticated.authenticated is True
                assert await authenticated._send("NodeCount") == 1
                token = frames[-1]["auth_token"]
                assert token.startswith("eg2.")
                claims = json.loads(bytes.fromhex(token[4:]))["context"]
                assert claims["principal"] == request_context()["principal"]
                with pytest.raises(ValueError, match="verified context"):
                    with anonymous.use_verified_context(request_context()):
                        pass
                with pytest.raises(ValueError, match="verified identity"):
                    anonymous.fresh_bolt_auth_token()
                await anonymous._send("NodeCount", idempotency_key="synthetic")
                assert (
                    frames[-1]["auth_token"] == 'anon1.{"idempotency_key":"synthetic"}'
                )
                assert len(frames) == 3
                # A reconnect stays anonymous; no cached identity is adopted.
                async with anonymous._lock:
                    await anonymous._reconnect()
                assert await anonymous._send("NodeCount") == 1
                assert frames[-1]["auth_token"] == ""
                assert frames[-1]["agent_id"] is None
            finally:
                await anonymous.close()
                await authenticated.close()

    monkeypatch.setenv("GRAPH_SERVICE_AUTH_SECRET", "ignored-in-explicit-none")
    monkeypatch.delenv("GRAPH_SERVICE_TLS", raising=False)
    asyncio.run(run())


@pytest.mark.parametrize(
    "kwargs",
    [
        {"auth_secret": "synthetic"},
        {"auth_secret": ""},
        {"verified_context": request_context()},
        {"node_id": "node:synthetic"},
        {"write_admission": lambda: None},
    ],
)
def test_anonymous_rejects_mixed_authority_before_connect(kwargs):
    with pytest.raises(ValueError):
        asyncio.run(EpistemicGraphClient.connect(auth_mode="none", **kwargs))


@pytest.mark.parametrize(
    "address",
    [
        "example.com:9000",
        "localhost:9000",
        "0.0.0.0:9000",
        "192.0.2.1:9000",
        "[::]:9000",
        "[2001:db8::1]:9000",
        "127.0.0.1:0",
        "bad-address",
    ],
)
def test_anonymous_rejects_nonlocal_target_before_connect(address):
    with pytest.raises(ValueError, match="literal loopback"):
        asyncio.run(EpistemicGraphClient.connect(auth_mode="none", tcp_addr=address))


def test_anonymous_loopback_transport():
    async def run():
        accepted = []

        async def serve(reader, writer):
            accepted.append(writer)
            await reader.read()
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_server(serve, "127.0.0.1", 0)
        try:
            port = server.sockets[0].getsockname()[1]
            client = await EpistemicGraphClient.connect(
                auth_mode="none", tcp_addr=f"127.0.0.1:{port}"
            )
            assert not client.authenticated
            await client.close()
        finally:
            server.close()
            await server.wait_closed()
            for writer in accepted:
                writer.close()

    asyncio.run(run())


def test_default_still_requires_credentials(monkeypatch):
    monkeypatch.delenv("GRAPH_SERVICE_AUTH_SECRET", raising=False)
    with pytest.raises(ValueError, match="authentication secret"):
        asyncio.run(EpistemicGraphClient.connect())
    with pytest.raises(ValueError, match="auth_mode"):
        asyncio.run(
            EpistemicGraphClient.connect(
                auth_mode=cast(Literal["authenticated", "none"], "auto")
            )
        )


def test_anonymous_requires_matching_server_without_retry():
    async def run():
        frames = []
        tasks = set()

        async def reject(reader, writer):
            task = asyncio.current_task()
            assert task is not None
            tasks.add(task)
            try:
                size = int.from_bytes(await reader.readexactly(4), "big")
                frames.append(
                    msgpack.unpackb(await reader.readexactly(size), raw=False)
                )
                # An authenticated engine closes on an unsigned request. The client
                # must surface this, not invent credentials or replay a mutation.
            finally:
                writer.close()
                await writer.wait_closed()
                tasks.discard(task)

        server = await asyncio.start_server(reject, "127.0.0.1", 0)
        client = await EpistemicGraphClient.connect(
            auth_mode="none", tcp_addr=f"127.0.0.1:{server.sockets[0].getsockname()[1]}"
        )
        try:
            with pytest.raises(ConnectionError):
                await client._send(
                    "AddNode", {"node_id": "synthetic", "properties_msgpack": b"\x80"}
                )
            assert len(frames) == 1
            assert frames[0]["auth_token"] == ""
            assert not client.authenticated
        finally:
            await client.close()
            server.close()
            await server.wait_closed()
            if tasks:
                await asyncio.gather(*tuple(tasks))

    asyncio.run(run())


def test_none_rejects_tls_and_environment_does_not_select_none(monkeypatch):
    monkeypatch.setenv("GRAPH_SERVICE_AUTH_MODE", "none")
    monkeypatch.delenv("GRAPH_SERVICE_AUTH_SECRET", raising=False)
    with pytest.raises(ValueError, match="authentication secret"):
        asyncio.run(EpistemicGraphClient.connect())
    with pytest.raises(ValueError, match="plaintext"):
        asyncio.run(EpistemicGraphClient.connect(auth_mode="none", tls=True))


@pytest.mark.parametrize(
    "key", ["", "   ", "x" * 257, "é" * 129, "a\n", "a\x00", "a\x7f", "a\x85", 42]
)
def test_anonymous_rejects_invalid_replay_metadata(key):
    with pytest.raises(ValueError, match="idempotency key"):
        EpistemicGraphClient._anonymous_correlation(key)


def test_anonymous_highlevel_changes_carry_replay_and_local_namespace():
    from test_verified_change_client import _envelope

    async def run():
        async with _server() as (address, frames):
            client = await EpistemicGraphClient.connect(
                auth_mode="none", tcp_addr=address
            )
            try:
                original = _envelope()
                await client.changes.apply(original)
                first = frames[-1]
                assert first["method"] == "ApplyChangeEnvelopeDraft"
                assert (
                    first["auth_token"]
                    == 'anon1.{"idempotency_key":"idempotency-fixture"}'
                )
                assert first["params"]["draft"]["policies"][0]["tenant"] == "local"
                assert "tenant" not in original["policies"][0]
                assert first["agent_id"] is None
                await client.changes.apply(original)
                assert frames[-1]["auth_token"] == first["auth_token"]
                assert frames[-1]["params"] == first["params"]
                await client.changes.get("envelope-fixture")
                assert frames[-1]["params"]["tenant"] == "local"
                assert client._verified_context is None
                assert not client.authenticated
            finally:
                await client.close()

    asyncio.run(run())


def test_generated_authentication_method_preserves_closed_enum():
    """EG-IDENTITY-R003.1 adds none without weakening authenticated contracts."""
    from enum import Enum

    from medulla.generated.models import RequestContextAuthenticationMethod

    assert isinstance(RequestContextAuthenticationMethod, type)
    assert issubclass(RequestContextAuthenticationMethod, Enum)
    assert {member.value for member in RequestContextAuthenticationMethod} == {
        "workload_identity",
        "oidc",
        "mutual_tls",
        "local_process",
        "none",
    }
    for value in ("workload_identity", "oidc", "mutual_tls", "local_process", "none"):
        assert RequestContextAuthenticationMethod(value).value == value
    for name in ("WORKLOAD_IDENTITY", "OIDC", "MUTUAL_TLS", "LOCAL_PROCESS"):
        assert getattr(RequestContextAuthenticationMethod, name).value == name.lower()
    for value in ("unknown", "", "anonymous", "NONE", "oidc-extra"):
        with pytest.raises(ValueError):
            RequestContextAuthenticationMethod(value)


@pytest.mark.parametrize(
    "kwargs,error,message",
    [
        (
            {"auth_mode": "none", "auth_secret": "", "tcp_addr": "bad"},
            ValueError,
            "credentials",
        ),
        (
            {"auth_mode": "none", "tcp_addr": "bad", "node_id": "node"},
            ValueError,
            "literal loopback",
        ),
        ({"auth_mode": "none", "node_id": "node"}, ValueError, "verified authority"),
        ({}, ValueError, "authentication secret"),
        ({"auth_secret": "synthetic"}, TypeError, "must be a mapping"),
    ],
)
def test_connection_authority_refactor_preserves_refusal_order(
    monkeypatch, kwargs, error, message
):
    monkeypatch.delenv("GRAPH_SERVICE_AUTH_SECRET", raising=False)
    with pytest.raises(error, match=message):
        asyncio.run(EpistemicGraphClient.connect(**kwargs))


@pytest.mark.parametrize(
    "explicit,environment,expected",
    [
        (None, " host.example ", "host.example"),
        ("", "host.example", "host.example"),
        (" ", "host.example", None),
        (" explicit.example ", "host.example", "explicit.example"),
        (None, "", None),
    ],
)
def test_connection_tls_hostname_refactor_preserves_precedence(
    monkeypatch, explicit, environment, expected
):
    from medulla.client import _connection_tls_server_hostname

    monkeypatch.setenv("GRAPH_SERVICE_TLS_SERVER_NAME", environment)
    assert _connection_tls_server_hostname(explicit) == expected
