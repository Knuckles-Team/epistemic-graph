"""Resolve versioned evidence citations without changing their source data."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol, TypeVar


class CitationFragment(Protocol):
    fragment_id: str
    address: str
    content_hash: str
    text: str


FragmentT = TypeVar("FragmentT", bound=CitationFragment)


def resolve_fragment(
    fragments: Sequence[FragmentT],
    *,
    fragment_id: str = "",
    content_hash: str = "",
) -> FragmentT | None:
    """Resolve by address, then by unique content when an address moved."""
    if fragment_id:
        for fragment in fragments:
            if fragment.fragment_id == fragment_id:
                return fragment
    if content_hash:
        matches = [
            fragment for fragment in fragments if fragment.content_hash == content_hash
        ]
        if len(matches) == 1:
            return matches[0]
    return None


def citation_status(
    fragments: Sequence[CitationFragment],
    *,
    fragment_id: str = "",
    content_hash: str = "",
) -> dict[str, Any]:
    """Report current, moved, stale, or lost without guessing ambiguity."""
    at_address = next(
        (
            fragment
            for fragment in fragments
            if fragment_id and fragment.fragment_id == fragment_id
        ),
        None,
    )
    if at_address is not None and (
        not content_hash or at_address.content_hash == content_hash
    ):
        return {
            "status": "current",
            "fragment_id": at_address.fragment_id,
            "address": at_address.address,
            "content_hash": at_address.content_hash,
            "cited_content_hash": content_hash,
            "text": at_address.text,
        }
    relocated = resolve_fragment(fragments, content_hash=content_hash)
    if relocated is not None and relocated is not at_address:
        return {
            "status": "moved",
            "fragment_id": relocated.fragment_id,
            "address": relocated.address,
            "content_hash": relocated.content_hash,
            "cited_content_hash": content_hash,
            "text": relocated.text,
        }
    if at_address is not None:
        return {
            "status": "stale",
            "fragment_id": at_address.fragment_id,
            "address": at_address.address,
            "content_hash": at_address.content_hash,
            "cited_content_hash": content_hash,
            "text": at_address.text,
        }
    return {
        "status": "lost",
        "fragment_id": "",
        "address": "",
        "content_hash": "",
        "cited_content_hash": content_hash,
        "text": "",
    }
