"""The published client must identify its live WorkItem capability surface."""

from __future__ import annotations

import json

import pytest

from epistemic_graph.client import WorkItemClient
from epistemic_graph.client_capabilities import (
    CLIENT_CAPABILITY_SCHEMA_VERSION,
    WORK_ITEM_METADATA_CAS_CAPABILITY,
    ClientCapabilityError,
    client_build_identity,
    client_capability_manifest,
    require_client_capabilities,
)

pytestmark = pytest.mark.no_engine


def test_manifest_is_deterministic_and_advertises_native_metadata_cas() -> None:
    first = client_capability_manifest()
    second = client_capability_manifest()

    assert first == second
    assert first["schema_version"] == CLIENT_CAPABILITY_SCHEMA_VERSION
    assert first["package"] == "epistemic-graph"
    assert first["package_version"]
    assert first["client_build_identity"] == client_build_identity()
    assert first["capabilities"] == {WORK_ITEM_METADATA_CAS_CAPABILITY: True}
    json.dumps(first, sort_keys=True)
    assert require_client_capabilities((WORK_ITEM_METADATA_CAS_CAPABILITY,)) == first


def test_client_without_metadata_cas_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stale installed wheel cannot claim CAS when its method is absent."""

    supported = client_capability_manifest()
    monkeypatch.delattr(WorkItemClient, "cas_metadata")

    manifest = client_capability_manifest()
    assert manifest["capabilities"] == {WORK_ITEM_METADATA_CAS_CAPABILITY: False}
    assert manifest["client_build_identity"] != supported["client_build_identity"]

    with pytest.raises(ClientCapabilityError, match="work_items[.]cas_metadata"):
        require_client_capabilities((WORK_ITEM_METADATA_CAS_CAPABILITY,))


def test_unknown_required_capability_fails_closed() -> None:
    with pytest.raises(ClientCapabilityError, match="unknown.capability"):
        require_client_capabilities(("unknown.capability",))
