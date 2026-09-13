"""Source-universe regressions for the contract method reachability gate."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]


def _script():
    path = ROOT / "scripts" / "check_contract_method_reachability.py"
    sys.path.insert(0, str(ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("contract_method_reachability", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _plant_protocol(root: Path, *, declare_chunk: bool, chunk: str) -> None:
    source = root / "crates" / "eg-types" / "src"
    method = source / "protocol" / "method"
    method.mkdir(parents=True)
    (source / "protocol.rs").write_text("mod method;\n", encoding="utf-8")
    declaration = "mod method_00;\n" if declare_chunk else ""
    (method / "mod.rs").write_text(
        f"{declaration}mod method_finish;\n"
        "pub(crate) use method_00::__eg_method_chunk_0;\n"
        "pub(crate) use method_finish::__eg_method_finish;\n"
        "__eg_method_chunk_0!();\n",
        encoding="utf-8",
    )
    (method / "method_00.rs").write_text(chunk, encoding="utf-8")
    (method / "method_finish.rs").write_text(
        "macro_rules! __eg_method_finish {\n"
        "    (@acc [$($variants:tt)*]) => {\n"
        "        pub enum Method { $($variants)* }\n"
        "    };\n"
        "}\n"
        "pub(crate) use __eg_method_finish;\n",
        encoding="utf-8",
    )


def test_method_variants_include_declared_children_and_ignore_comment_spoof(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate = _script()
    _plant_protocol(
        tmp_path,
        declare_chunk=True,
        chunk=(
            "macro_rules! __eg_method_chunk_0 {\n"
            "    () => {\n"
            "        __eg_method_finish!(@acc [\n"
            "    Alpha,\n"
            "/*\n"
            "    FakeFromComment,\n"
            "*/\n"
            '    "QuotedNotAVariant,",\n'
            "        ]);\n"
            "    };\n"
            "}\n"
            "pub(crate) use __eg_method_chunk_0;\n"
        ),
    )
    monkeypatch.setattr(gate, "ROOT", tmp_path)

    assert gate.protocol_variants() == {"Alpha"}


def test_omitted_declared_child_fails_closed(tmp_path: Path, monkeypatch) -> None:
    gate = _script()
    _plant_protocol(
        tmp_path,
        declare_chunk=False,
        chunk=(
            "macro_rules! __eg_method_chunk_0 { () => {\n"
            "    Alpha,\n"
            "}; }\n"
            "pub(crate) use __eg_method_chunk_0;\n"
        ),
    )
    monkeypatch.setattr(gate, "ROOT", tmp_path)

    with pytest.raises(gate.GateError, match="orphan Rust module files"):
        gate.protocol_variants()
