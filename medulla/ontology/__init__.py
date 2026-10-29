"""Read API for the core ontology modules, shapes and catalog shipped in the wheel.

The Turtle files are not stored here in git. ``scripts/inject_ontology.py`` copies
``crates/eg-core/ontology/*.ttl`` (the single source) into this package at wheel
build time and fails the build unless the packaged set equals the source set. In a
source checkout without a built wheel the same files are read from the crate.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from importlib import resources
from pathlib import Path

CATALOG_NAME = "catalog-v1.ttl"
SHAPES_SUFFIX = ".shapes.ttl"
_IMPORT = re.compile(r"<([^<>\s]+)>")
_ONTOLOGY_DECL = re.compile(r"<([^<>\s]+)>\s+a\s+owl:Ontology\b")
_SOURCE = Path(__file__).resolve().parents[2] / "crates" / "eg-core" / "ontology"


def _root():  # a Traversable (package resource dir) or a Path
    packaged = resources.files(__name__)
    if any(p.name.endswith(".ttl") for p in packaged.iterdir()):
        return packaged
    if _SOURCE.is_dir():
        return _SOURCE
    raise FileNotFoundError("core ontology files are missing from this installation")


def names() -> list[str]:
    """Sorted file names of every packaged ``.ttl`` file (modules, shapes, catalog)."""

    return sorted(p.name for p in _root().iterdir() if p.name.endswith(".ttl"))


def read(name: str) -> bytes:
    """Bytes of one packaged file; ``FileNotFoundError`` for an unknown name."""

    if name not in names():
        raise FileNotFoundError(name)
    return _root().joinpath(name).read_bytes()


def files() -> Iterator[tuple[str, bytes]]:
    """Yield ``(name, bytes)`` for every packaged ontology, shapes and catalog file."""

    for name in names():
        yield name, read(name)


def shape_names() -> list[str]:
    """File names of the SHACL shapes files."""

    return [n for n in names() if n.endswith(SHAPES_SUFFIX)]


def catalog() -> bytes:
    """Bytes of the aggregate module catalog."""

    return read(CATALOG_NAME)


def catalog_order() -> list[str]:
    """Module IRIs in the catalog's ``owl:imports`` order (the load order)."""

    text = catalog().decode()
    start = text.index("owl:imports")
    return _IMPORT.findall(text[start:])


def module_files() -> dict[str, str]:
    """Map each module IRI to the file that declares it as ``owl:Ontology``."""

    found: dict[str, str] = {}
    for name in names():
        if name == CATALOG_NAME or name.endswith(SHAPES_SUFFIX):
            continue
        match = _ONTOLOGY_DECL.search(read(name).decode())
        if match:
            found[match.group(1)] = name
    return found
