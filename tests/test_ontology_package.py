"""The core ontology, shapes and catalog ship in the wheel and are readable by API."""

from __future__ import annotations

import os
import zipfile
from pathlib import Path

import pytest

from medulla import ontology
from scripts import inject_ontology

pytestmark = [pytest.mark.no_engine, pytest.mark.spec("EG-CONTRACT-R015.1")]

SOURCE = Path(__file__).resolve().parent.parent / "crates/eg-core/ontology"


def _bare_wheel(path: Path) -> Path:
    record = "pkg-1.dist-info/RECORD"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("medulla/__init__.py", b"")
        archive.writestr(record, "")
    return path


def test_api_returns_every_source_file() -> None:
    expected = {p.name: p.read_bytes() for p in SOURCE.glob("*.ttl")}
    assert dict(ontology.files()) == expected
    assert ontology.CATALOG_NAME in expected
    assert ontology.shape_names()


def test_catalog_order_names_every_module_file() -> None:
    order = ontology.catalog_order()
    assert order and len(order) == len(set(order))
    assert set(order) <= set(ontology.module_files())


def test_unknown_name_is_refused() -> None:
    with pytest.raises(FileNotFoundError):
        ontology.read("missing-v1.ttl")


def test_inject_packages_exact_source_set(tmp_path: Path) -> None:
    wheel = _bare_wheel(tmp_path / "pkg-1-py3-none-any.whl")
    inject_ontology.inject(wheel)
    assert inject_ontology.packaged_set(wheel) == inject_ontology.source_set()
    with zipfile.ZipFile(wheel) as archive:
        record = archive.read("pkg-1.dist-info/RECORD").decode()
    assert "medulla/ontology/catalog-v1.ttl,sha256=" in record


def test_verify_rejects_missing_changed_and_extra(tmp_path: Path) -> None:
    wheel = _bare_wheel(tmp_path / "pkg-1-py3-none-any.whl")
    inject_ontology.inject(wheel)
    source = tmp_path / "src"
    source.mkdir()
    for name, data in inject_ontology.source_set().items():
        (source / name).write_bytes(data)
    (source / "added-v1.ttl").write_text("# new\n")
    (source / "catalog-v1.ttl").write_text("# changed\n")
    (source / "a2a-v1.ttl").unlink()
    with pytest.raises(SystemExit) as failure:
        inject_ontology.verify(wheel, source)
    message = str(failure.value)
    assert "added-v1.ttl" in message
    assert "catalog-v1.ttl" in message
    assert "a2a-v1.ttl" in message


@pytest.mark.skipif(
    "EG_ONTOLOGY_WHEEL" not in os.environ, reason="needs a built wheel path"
)
def test_built_wheel_contains_every_source_file() -> None:
    wheel = Path(os.environ["EG_ONTOLOGY_WHEEL"])
    inject_ontology.verify(wheel)
    assert len(inject_ontology.packaged_set(wheel)) == len(list(SOURCE.glob("*.ttl")))
