//! Render canonical digest helpers shared by generated DTO modules.

use super::HEADER;

pub(super) fn digest_module() -> String {
    let mut out = String::from(HEADER);
    out.push_str(
        r#""""Canonical MessagePack and framed digest helpers shared by generated DTOs."""

from __future__ import annotations

import hashlib
import struct
from collections.abc import Collection, Mapping, Sequence
from typing import Any

import msgpack


def _canonical_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _canonical_json(value[key]) for key in sorted(value)}
    if isinstance(value, list | tuple):
        return [_canonical_json(item) for item in value]
    return value


def _path_parts(path: str) -> tuple[str, ...]:
    return tuple(part for part in path.replace("[*]", ".*").split(".") if part)


def _canonicalize_at(value: Any, parts: tuple[str, ...]) -> Any:
    if value is None:
        return value
    if not parts:
        return _canonical_json(value)
    head, *tail = parts
    remaining = tuple(tail)
    if head == "*":
        if not isinstance(value, list | tuple):
            raise ValueError("canonical JSON wildcard requires an array")
        return [_canonicalize_at(item, remaining) for item in value]
    if not isinstance(value, Mapping) or head not in value:
        raise ValueError(f"canonical JSON path component is absent: {head}")
    result = dict(value)
    result[head] = _canonicalize_at(result[head], remaining)
    return result


def _omit_none_at(value: Any, parts: tuple[str, ...]) -> Any:
    if value is None or not parts:
        return value
    head, *tail = parts
    remaining = tuple(tail)
    if head == "*":
        if not isinstance(value, list | tuple):
            raise ValueError("omit-none wildcard requires an array")
        return [_omit_none_at(item, remaining) for item in value]
    if not isinstance(value, Mapping):
        raise ValueError(f"omit-none path requires an object at: {head}")
    result = dict(value)
    if head not in result:
        return result
    if not remaining and result[head] is None:
        del result[head]
    else:
        result[head] = _omit_none_at(result[head], remaining)
    return result


def _order_named_struct_at(
    value: Any,
    parts: tuple[str, ...],
    fields: Sequence[str],
) -> Any:
    if value is None:
        return value
    if not parts:
        if not isinstance(value, Mapping):
            raise ValueError("named struct path requires an object")
        unknown = set(value).difference(fields)
        if unknown:
            raise ValueError(f"named struct has undeclared fields: {sorted(unknown)}")
        return {field: value[field] for field in fields if field in value}
    head, *tail = parts
    remaining = tuple(tail)
    if head == "*":
        if not isinstance(value, list | tuple):
            raise ValueError("named struct wildcard requires an array")
        return [_order_named_struct_at(item, remaining, fields) for item in value]
    if not isinstance(value, Mapping) or head not in value:
        raise ValueError(f"named struct path component is absent: {head}")
    result = dict(value)
    result[head] = _order_named_struct_at(result[head], remaining, fields)
    return result


def canonical_msgpack(value: Any) -> bytes:
    """Encode a JSON-shaped value with every mapping recursively key-sorted."""
    return msgpack.packb(_canonical_json(value), use_bin_type=True)


def named_msgpack(
    value: Mapping[str, Any],
    *,
    canonical_json_fields: Collection[str] = (),
    canonical_json_paths: Collection[str] = (),
    omit_none_paths: Collection[str] = (),
    named_struct_paths: Mapping[str, Sequence[str]] | None = None,
) -> bytes:
    """Encode a named Rust-struct projection, preserving declaration order."""
    projection = dict(value)
    for field in canonical_json_fields:
        if field in projection:
            projection[field] = _canonical_json(projection[field])
    for path in canonical_json_paths:
        projection = _canonicalize_at(projection, _path_parts(path))
    for path in omit_none_paths:
        projection = _omit_none_at(projection, _path_parts(path))
    for path, fields in (named_struct_paths or {}).items():
        projection = _order_named_struct_at(
            projection,
            _path_parts(path),
            fields,
        )
    return msgpack.packb(projection, use_bin_type=True)


def framed_sha256(domain: bytes, fields: Sequence[bytes]) -> str:
    """Return EG's length-framed SHA-256 as 64 lowercase hexadecimal digits."""
    if not domain or len(domain) > 0xFFFFFFFF:
        raise ValueError("digest domain is empty or too large")
    if len(fields) > 0xFFFFFFFF:
        raise ValueError("digest field count exceeds the framing limit")
    digest = hashlib.sha256()
    digest.update(b"eg/framed-sha256/v1\0")
    digest.update(struct.pack(">I", len(domain)))
    digest.update(domain)
    digest.update(struct.pack(">I", len(fields)))
    for field in fields:
        digest.update(struct.pack(">Q", len(field)))
        digest.update(field)
    return digest.hexdigest()


def framed_named_msgpack_digest(
    domain: bytes,
    value: Mapping[str, Any],
    *,
    canonical_json_fields: Collection[str] = (),
    canonical_json_paths: Collection[str] = (),
    omit_none_paths: Collection[str] = (),
    named_struct_paths: Mapping[str, Sequence[str]] | None = None,
) -> str:
    """Frame and digest one named MessagePack model projection."""
    encoded = named_msgpack(
        value,
        canonical_json_fields=canonical_json_fields,
        canonical_json_paths=canonical_json_paths,
        omit_none_paths=omit_none_paths,
        named_struct_paths=named_struct_paths,
    )
    return framed_sha256(domain, [encoded])
"#,
    );
    out
}
