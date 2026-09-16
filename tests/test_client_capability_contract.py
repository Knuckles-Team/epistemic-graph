"""The published client must identify its live capability surface."""

from __future__ import annotations

import json

import pytest

from epistemic_graph.client import ConsensusClient, WorkItemClient
from epistemic_graph.client_capabilities import (
    CLIENT_CAPABILITY_SCHEMA_VERSION,
    CONSENSUS_GET_IDENTITY_CAPABILITY,
    SQL_SOURCE_PREPARATION_CAPABILITY,
    WORK_ITEM_METADATA_CAS_CAPABILITY,
    ClientCapabilityError,
    client_build_identity,
    client_capability_manifest,
    require_client_capabilities,
)

pytestmark = pytest.mark.no_engine


def _without_sql_source(capabilities: dict[str, bool]) -> dict[str, bool]:
    """The SQL source capability depends on the native kernel; its own tests pin it."""
    return {
        key: value
        for key, value in capabilities.items()
        if key != SQL_SOURCE_PREPARATION_CAPABILITY
    }


def test_manifest_is_deterministic_and_advertises_live_client_capabilities() -> None:
    first = client_capability_manifest()
    second = client_capability_manifest()

    assert first == second
    assert first["schema_version"] == CLIENT_CAPABILITY_SCHEMA_VERSION
    assert first["package"] == "epistemic-graph"
    assert first["package_version"]
    assert first["client_build_identity"] == client_build_identity()
    assert _without_sql_source(first["capabilities"]) == {
        CONSENSUS_GET_IDENTITY_CAPABILITY: True,
        WORK_ITEM_METADATA_CAS_CAPABILITY: True,
    }
    json.dumps(first, sort_keys=True)
    assert (
        require_client_capabilities(
            (WORK_ITEM_METADATA_CAS_CAPABILITY, CONSENSUS_GET_IDENTITY_CAPABILITY)
        )
        == first
    )


def test_client_without_metadata_cas_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stale installed wheel cannot claim CAS when its method is absent."""

    supported = client_capability_manifest()
    monkeypatch.delattr(WorkItemClient, "cas_metadata")

    manifest = client_capability_manifest()
    assert _without_sql_source(manifest["capabilities"]) == {
        CONSENSUS_GET_IDENTITY_CAPABILITY: True,
        WORK_ITEM_METADATA_CAS_CAPABILITY: False,
    }
    assert manifest["client_build_identity"] != supported["client_build_identity"]

    with pytest.raises(ClientCapabilityError, match="work_items[.]cas_metadata"):
        require_client_capabilities((WORK_ITEM_METADATA_CAS_CAPABILITY,))


def test_unknown_required_capability_fails_closed() -> None:
    with pytest.raises(ClientCapabilityError, match="unknown.capability"):
        require_client_capabilities(("unknown.capability",))


def test_same_version_client_without_get_identity_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stale 2.27.0 wheel must not pass when its new RPC is absent.

    The distribution version intentionally remains unchanged in this fixture:
    this reproduces the incident where a same-version wheel was source-compatible
    enough to install but lacked ``ConsensusClient.get_identity``.
    """

    supported = client_capability_manifest()
    monkeypatch.delattr(ConsensusClient, "get_identity")

    manifest = client_capability_manifest()
    assert manifest["package_version"] == supported["package_version"]
    assert _without_sql_source(manifest["capabilities"]) == {
        CONSENSUS_GET_IDENTITY_CAPABILITY: False,
        WORK_ITEM_METADATA_CAS_CAPABILITY: True,
    }
    assert manifest["client_build_identity"] != supported["client_build_identity"]

    with pytest.raises(ClientCapabilityError, match="consensus[.]get_identity"):
        require_client_capabilities((CONSENSUS_GET_IDENTITY_CAPABILITY,))


def test_missing_native_sql_codec_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    import epistemic_graph.client_capabilities as capabilities

    def missing_codec() -> None:
        raise ClientCapabilityError("missing native codec")

    monkeypatch.setattr(capabilities, "_sql_source_native_codec", missing_codec)
    manifest = client_capability_manifest()
    assert manifest["capabilities"][SQL_SOURCE_PREPARATION_CAPABILITY] is False
    with pytest.raises(ClientCapabilityError, match="query[.]prepare_sql_source_batch"):
        require_client_capabilities((SQL_SOURCE_PREPARATION_CAPABILITY,))
