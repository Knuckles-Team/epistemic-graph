"""Byte-level digests shared by the synthetic generators.

`framed` is the pure-Python form of the engine's `Digest256::framed`
(`crates/eg-types/src/contract/crypto.rs`): a domain-separated SHA-256 over
length-prefixed fields. Generators use it for their own stream and fixture
identities, and the connector-pack generator uses it for the normative pack
digest layout.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from typing import Any

FRAMED_TAG = b"eg/framed-sha256/v1\x00"
DIGEST_PREFIX = "sha256:"
_U32 = 1 << 32
_U64 = 1 << 64


def u64_be(value: int) -> bytes:
    """Encode an unsigned 64-bit integer, refusing anything outside the range."""
    if not 0 <= value < _U64:
        raise ValueError(f"{value} is not an unsigned 64-bit integer")
    return value.to_bytes(8, "big")


def framed(domain: bytes, fields: Sequence[bytes]) -> bytes:
    """Return the 32-byte framed digest of ``fields`` under ``domain``."""
    if not domain or len(domain) >= _U32:
        raise ValueError("digest domain is empty or too large")
    hasher = hashlib.sha256(FRAMED_TAG)
    hasher.update(len(domain).to_bytes(4, "big"))
    hasher.update(domain)
    hasher.update(len(fields).to_bytes(4, "big"))
    for field in fields:
        hasher.update(u64_be(len(field)))
        hasher.update(field)
    return hasher.digest()


def sha256_raw(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def sha256_text(data: bytes) -> str:
    """The `sha256:<hex>` text form used in component fields and records."""
    return DIGEST_PREFIX + hashlib.sha256(data).hexdigest()


def canonical_json(value: Any) -> bytes:
    """The connector SDK serialization rule (PACK-IMPORT-DESIGN §3.1)."""
    text = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return text.encode("utf-8")
