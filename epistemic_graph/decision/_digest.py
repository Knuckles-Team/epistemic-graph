"""The decision surface's one digest scheme, in pure Python.

``sha256(domain || 0x00 || compact-JSON(value))``, rendered ``sha256:<hex>``.
The engine encodes every value with ``serde_json`` in declaration order and
holds no float and no non-string map key, so a record decoded with ``json``
(which keeps key order) and re-encoded compactly reproduces the exact bytes.
"""

from __future__ import annotations

import copy
import hashlib
import json
from typing import Any

DIGEST_TEXT_PREFIX = "sha256:"
DECISION_RECORD_DIGEST_DOMAIN = "eg/decision-record/v1"
DECISION_INPUTS_DIGEST_DOMAIN = "eg/decision-inputs/v1"
DECISION_CATALOG_DIGEST_DOMAIN = "eg/decision-catalog/v1"
DECISION_POLICY_DIGEST_DOMAIN = "eg/decision-policy/v1"
DECISION_COMPONENT_ID_PREFIX = "decision:"


def compact_json(value: Any) -> bytes:
    """The engine's JSON encoding of ``value``: compact, key order kept."""
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def raw_digest(domain: str, value: Any) -> str:
    """The hex SHA-256 of ``domain || 0x00 || compact-JSON(value)``."""
    hasher = hashlib.sha256()
    hasher.update(domain.encode("utf-8"))
    hasher.update(b"\x00")
    hasher.update(compact_json(value))
    return hasher.hexdigest()


def digest_text(domain: str, value: Any) -> str:
    """``sha256:<hex>`` of ``value`` under ``domain``."""
    return DIGEST_TEXT_PREFIX + raw_digest(domain, value)


def record_digest(record: dict[str, Any]) -> str:
    """A record's digest: computed with ``record_digest`` and ``record_id`` cleared."""
    subject = copy.deepcopy(record)
    subject["record_digest"] = ""
    subject["record_id"] = ""
    return digest_text(DECISION_RECORD_DIGEST_DOMAIN, subject)


def record_id(digest: str) -> str:
    """The component id a record with ``digest`` is committed under."""
    return DECISION_COMPONENT_ID_PREFIX + digest.removeprefix(DIGEST_TEXT_PREFIX)


def inputs_digest(inputs: dict[str, Any]) -> str:
    """The digest of a decision's complete input set."""
    return digest_text(DECISION_INPUTS_DIGEST_DOMAIN, inputs)


def policy_digest(policy: dict[str, Any]) -> str:
    """The digest of a decision policy body."""
    return digest_text(DECISION_POLICY_DIGEST_DOMAIN, policy)


def catalog_digest(candidates: list[dict[str, Any]]) -> str:
    """The digest of the candidate catalog: ``(id, definition digest, lifecycle)``."""
    members = sorted(
        (
            {
                "component_id": candidate["component_id"],
                "definition_digest": candidate["definition_digest"],
                "lifecycle": candidate["lifecycle"],
            }
            for candidate in candidates
        ),
        key=lambda member: (member["component_id"], member["definition_digest"]),
    )
    return digest_text(DECISION_CATALOG_DIGEST_DOMAIN, members)
