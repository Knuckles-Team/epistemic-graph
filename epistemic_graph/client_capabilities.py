"""Machine-checkable identity for the published Python client surface.

The package version identifies the published artifact, while the capability
map is derived from the client class that is actually importable.  Keeping the
two facts together lets an image or application preflight reject a stale wheel
instead of treating a matching distribution version as proof of API parity.
The capability probe is deliberately fail-closed: an absent or non-callable
method is never advertised as supported.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from importlib.metadata import PackageNotFoundError, version
from typing import Any, Final

from .client import WorkItemClient

PACKAGE_NAME: Final = "epistemic-graph"
CLIENT_CAPABILITY_SCHEMA_VERSION: Final = 1
WORK_ITEM_METADATA_CAS_CAPABILITY: Final = "work_items.cas_metadata"


class ClientCapabilityError(RuntimeError):
    """Raised when an installed client cannot satisfy a required capability."""


def _package_version() -> str:
    try:
        package_version = version(PACKAGE_NAME)
    except PackageNotFoundError as exc:
        raise ClientCapabilityError(
            f"{PACKAGE_NAME} distribution metadata is unavailable"
        ) from exc
    if not package_version:
        raise ClientCapabilityError(f"{PACKAGE_NAME} distribution version is empty")
    return package_version


def _capabilities() -> dict[str, bool]:
    """Report only capabilities present on this imported client class."""

    return {
        WORK_ITEM_METADATA_CAS_CAPABILITY: callable(
            getattr(WorkItemClient, "cas_metadata", None)
        )
    }


def _build_identity(package_version: str, capabilities: dict[str, bool]) -> str:
    identity_payload = {
        "schema_version": CLIENT_CAPABILITY_SCHEMA_VERSION,
        "package": PACKAGE_NAME,
        "package_version": package_version,
        "capabilities": capabilities,
    }
    canonical = json.dumps(
        identity_payload, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    digest = hashlib.sha256(canonical).hexdigest()
    return f"{PACKAGE_NAME}-client/{package_version};capabilities-sha256={digest}"


def client_capability_manifest() -> dict[str, Any]:
    """Return the deterministic package identity and live client capabilities.

    The returned mapping is JSON-serializable so deployment/image preflight can
    inspect it without importing implementation details.  The identity digest
    covers the package version and every capability result; an older client
    lacking ``WorkItemClient.cas_metadata`` therefore produces a different
    identity and, more importantly, reports the required capability as false.
    """

    package_version = _package_version()
    capabilities = _capabilities()
    return {
        "schema_version": CLIENT_CAPABILITY_SCHEMA_VERSION,
        "package": PACKAGE_NAME,
        "package_version": package_version,
        "client_build_identity": _build_identity(package_version, capabilities),
        "capabilities": capabilities,
    }


def client_build_identity() -> str:
    """Return the deterministic identity for this imported client artifact."""

    return str(client_capability_manifest()["client_build_identity"])


def require_client_capabilities(required: Iterable[str]) -> dict[str, Any]:
    """Fail closed unless every named capability is implemented by this client."""

    if isinstance(required, str):
        raise TypeError("required capabilities must be an iterable of names")
    required_names = tuple(sorted(set(required)))
    manifest = client_capability_manifest()
    capabilities = manifest["capabilities"]
    missing = [name for name in required_names if capabilities.get(name) is not True]
    if missing:
        identity = manifest["client_build_identity"]
        raise ClientCapabilityError(
            "client artifact is missing required capabilities: "
            f"{', '.join(missing)} (build_identity={identity})"
        )
    return manifest
