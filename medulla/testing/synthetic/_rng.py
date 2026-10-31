"""A deterministic, integer-only random stream.

Python's `random` module is deterministic for one interpreter, but its derived
helpers have changed between releases and it is not reproducible from another
language. Every value here is a SHA-256 of (seed, label, counter) under the
engine's framed-digest layout, so a Rust test can regenerate the same stream,
and no float ever decides an outcome.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TypeVar

from ._digest import framed, u64_be

T = TypeVar("T")

_DOMAIN = b"eg/synthetic-stream/v1"
_U64 = 1 << 64


class SeededStream:
    """An independent stream of unsigned integers for one (seed, label)."""

    __slots__ = ("_counter", "_label", "_seed")

    def __init__(self, seed: int, label: str) -> None:
        if not 0 <= seed < _U64:
            raise ValueError("seed must be an unsigned 64-bit integer")
        if not label:
            raise ValueError("stream label must be non-empty")
        self._seed = seed
        self._label = label
        self._counter = 0

    def child(self, label: str) -> SeededStream:
        """A stream whose values do not depend on how much of this one was used."""
        return SeededStream(self._seed, f"{self._label}/{label}")

    def _block(self) -> bytes:
        fields = (u64_be(self._seed), self._label.encode(), u64_be(self._counter))
        self._counter += 1
        return framed(_DOMAIN, fields)

    def next_u64(self) -> int:
        return int.from_bytes(self._block()[:8], "big")

    def below(self, bound: int) -> int:
        """A uniform integer in ``[0, bound)``, by rejection so there is no bias."""
        if not 0 < bound <= _U64:
            raise ValueError("bound must be in 1..=2**64")
        limit = _U64 - (_U64 % bound)
        while True:
            value = self.next_u64()
            if value < limit:
                return value % bound

    def between(self, low: int, high: int) -> int:
        """A uniform integer in the inclusive range ``[low, high]``."""
        if high < low:
            raise ValueError("empty range")
        return low + self.below(high - low + 1)

    def chance(self, numerator: int, denominator: int) -> bool:
        """True with probability exactly ``numerator / denominator``."""
        if not 0 <= numerator <= denominator or denominator == 0:
            raise ValueError("chance needs 0 <= numerator <= denominator > 0")
        return self.below(denominator) < numerator

    def choice(self, items: Sequence[T]) -> T:
        if not items:
            raise ValueError("cannot choose from an empty sequence")
        return items[self.below(len(items))]

    def shuffled(self, items: Sequence[T]) -> list[T]:
        """A Fisher-Yates permutation of a copy of ``items``."""
        result = list(items)
        for index in range(len(result) - 1, 0, -1):
            swap = self.below(index + 1)
            result[index], result[swap] = result[swap], result[index]
        return result

    def sample(self, items: Sequence[T], count: int) -> list[T]:
        """``count`` distinct items, in stream order."""
        if not 0 <= count <= len(items):
            raise ValueError("sample size outside the population")
        return self.shuffled(items)[:count]

    def token(self, length: int) -> bytes:
        """``length`` pseudo-random bytes."""
        out = bytearray()
        while len(out) < length:
            out += self._block()
        return bytes(out[:length])
