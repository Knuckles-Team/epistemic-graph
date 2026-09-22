"""Public Python client package for epistemic-graph."""

from importlib.metadata import PackageNotFoundError, distribution
from pathlib import Path

# Ordering note: `_add_editable_native_overlay` below must run before anything
# in this package's import graph resolves `epistemic_graph.numeric` — an
# editable Maturin install keeps that compiled extension beside the
# distribution metadata, not in this source tree, so a submodule import
# reaching it too early would fail to find it. None of `.client`,
# `.client_capabilities`, or `.parser` (nor anything they import) touches
# `epistemic_graph.numeric` at module-import time — only lazily, inside
# function bodies, well after package init has finished — so it is safe for
# the public re-exports below to be ordinary top-of-file imports with no
# runtime statement between `from __future__` and the last `from .xxx import`,
# and for the overlay setup to run afterward as one clearly-scoped block.
from .client import (
    EpistemicGraphClient,
    KnowledgeStreamBatch,
    KnowledgeStreamClient,
    KnowledgeStreamCursor,
    KnowledgeStreamQuery,
    ModalityApplyOutcome,
    ModalityAuthority,
    RequestContextClaims,
    ResultTooLargeError,
    ServedModalityCapabilities,
    ServedModalityClient,
    ServedModalityEvent,
    ServedModalityPage,
    ServedModalityStats,
    ServerRegistryClient,
    StaleRouteError,
    SyncEpistemicGraphClient,
    validate_request_context,
)
from .client_capabilities import (
    CLIENT_CAPABILITY_SCHEMA_VERSION,
    CONSENSUS_GET_IDENTITY_CAPABILITY,
    WORK_ITEM_METADATA_CAS_CAPABILITY,
    ClientCapabilityError,
    client_build_identity,
    client_capability_manifest,
    require_client_capabilities,
)
from .connector_pack import (
    ConnectorPackArchive,
    ConnectorPackArchiveBuilder,
    ConnectorPackClient,
    ConnectorPackEntryContent,
    ConnectorPackWriteError,
    PackImportResult,
    PackImportResultImported,
    PackImportResultRejected,
    PackImportResultUnchanged,
    PackWriteErrorCode,
    connector_pack_import_key,
    pack_digest,
)
from .generated.server_registry import (
    RegisteredServerCursor,
    RegisteredServerListPage,
    RegisteredServerListRequest,
    RegisteredServerView,
)
from .parser import RustASTParser


def _add_editable_native_overlay() -> None:
    """Let a Maturin editable install discover its folded native submodule.

    The editable wheel's Python package resolves to this source tree, whereas its
    separately-built ``numeric`` extension is installed beside the distribution
    metadata.  Appending that owned package directory keeps the required native
    module available without placing a generated binary in the checkout.
    """

    source = Path(__file__).resolve().parent
    try:
        overlay = Path(
            str(distribution("epistemic-graph").locate_file("epistemic_graph"))
        )
    except PackageNotFoundError:
        return
    if overlay != source and overlay.is_dir():
        __path__.append(str(overlay))


_add_editable_native_overlay()

__all__ = [
    "EpistemicGraphClient",
    "ConnectorPackClient",
    "ConnectorPackArchive",
    "ConnectorPackArchiveBuilder",
    "ConnectorPackEntryContent",
    "ConnectorPackWriteError",
    "PackImportResult",
    "PackImportResultImported",
    "PackImportResultRejected",
    "PackImportResultUnchanged",
    "PackWriteErrorCode",
    "connector_pack_import_key",
    "pack_digest",
    "RegisteredServerCursor",
    "RegisteredServerListPage",
    "RegisteredServerListRequest",
    "RegisteredServerView",
    "ServerRegistryClient",
    "SyncEpistemicGraphClient",
    "KnowledgeStreamClient",
    "KnowledgeStreamQuery",
    "KnowledgeStreamCursor",
    "KnowledgeStreamBatch",
    "ServedModalityClient",
    "ModalityAuthority",
    "ModalityApplyOutcome",
    "ServedModalityPage",
    "ServedModalityEvent",
    "ServedModalityStats",
    "ServedModalityCapabilities",
    "RequestContextClaims",
    "validate_request_context",
    "ResultTooLargeError",
    "StaleRouteError",
    "RustASTParser",
    "CLIENT_CAPABILITY_SCHEMA_VERSION",
    "CONSENSUS_GET_IDENTITY_CAPABILITY",
    "WORK_ITEM_METADATA_CAS_CAPABILITY",
    "ClientCapabilityError",
    "client_build_identity",
    "client_capability_manifest",
    "require_client_capabilities",
]
