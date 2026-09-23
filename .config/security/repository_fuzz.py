#!/usr/bin/env python3

"""Bounded, dependency-free fuzz smoke test for repository config parsers."""

from __future__ import annotations

import json
import sys
from collections.abc import Iterator
from itertools import count, islice
from pathlib import Path

import tomllib

MAX_FILES = 16
MAX_SOURCE_BYTES = 1024 * 1024
MAX_CASES = 64


def _corpus(root: Path) -> list[tuple[str, bytes]]:
    values: list[tuple[str, bytes]] = []
    try:
        candidates = sorted(
            path
            for path in root.iterdir()
            if path.suffix.casefold() in {".json", ".toml"}
        )[:MAX_FILES]
    except OSError:
        candidates = []
    for path in candidates:
        suffix = path.suffix.casefold()
        try:
            size = path.stat().st_size
            if path.is_symlink() or not 0 < size <= MAX_SOURCE_BYTES:
                continue
            values.append((suffix, path.read_bytes()))
        except OSError:
            continue
    return values


def _mutations(payload: bytes) -> tuple[bytes, ...]:
    midpoint = max(1, len(payload) // 2)
    return (
        payload[:midpoint],
        payload + b"\x00",
        b"{" * 128 + payload[:64],
        b"[" * 128 + payload[:64],
        payload[:32] + b"\xff\xfe" + payload[32:64],
        b"null trailing",
        b"key = [1, 2,",
        b"\n" * 4096 + payload[:128],
    )


def _exercise(suffix: str, payload: bytes) -> None:
    text = payload.decode("utf-8")
    if suffix == ".json":
        json.loads(text)
    else:
        tomllib.loads(text)


def _cases(corpus: list[tuple[str, bytes]]) -> Iterator[tuple[str, bytes]]:
    """Cycle every mutation of every corpus entry, bounded to ``MAX_CASES``."""
    return islice(
        (
            (suffix, mutation)
            for _ in count()
            for suffix, payload in corpus
            for mutation in _mutations(payload)
        ),
        MAX_CASES,
    )


def _crashed(suffix: str, payload: bytes) -> bool:
    """Exercise one case; only a non-decode exception counts as a crash."""
    try:
        _exercise(suffix, payload)
    except (UnicodeDecodeError, json.JSONDecodeError, tomllib.TOMLDecodeError):
        return False
    except Exception:
        return True
    return False


def _write_report(output: Path, cases: int, crashes: int) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(
            {
                "version": 1,
                "kind": "fuzz",
                "passed": crashes == 0,
                "cases": cases,
                "failures": 0,
                "crashes": crashes,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def main() -> int:
    if len(sys.argv) != 2:
        return 2
    corpus = _corpus(Path.cwd()) or [(".json", b'{"seed": true}')]
    cases = 0
    crashes = 0
    for suffix, mutation in _cases(corpus):
        crashes += _crashed(suffix, mutation)
        cases += 1
    _write_report(Path(sys.argv[1]), cases, crashes)
    return 0 if crashes == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
