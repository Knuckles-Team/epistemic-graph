"""Public Python client package for epistemic-graph."""

from importlib.metadata import PackageNotFoundError, distribution
from pathlib import Path


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
    StaleRouteError,
    SyncEpistemicGraphClient,
    validate_request_context,
)
from .client_capabilities import (
    CLIENT_CAPABILITY_SCHEMA_VERSION,
    WORK_ITEM_METADATA_CAS_CAPABILITY,
    ClientCapabilityError,
    client_build_identity,
    client_capability_manifest,
    require_client_capabilities,
)
from .parser import RustASTParser

__all__ = [
    "EpistemicGraphClient",
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
    "WORK_ITEM_METADATA_CAS_CAPABILITY",
    "ClientCapabilityError",
    "client_build_identity",
    "client_capability_manifest",
    "require_client_capabilities",
]
