"""Tests for the release wheel-reproducibility diagnostic (``scripts/compare_wheel_reproducibility``).

The gate this replaces was a bare inline ``python -c`` with three ``assert``s
and no diagnostic output on failure -- exactly the shape that made the
``windows-x86_64``-only reproducibility bug undiagnosable from the CI log
alone. This suite proves the replacement script (a) still fails closed with
the SAME three protected assertion messages the old gate used (log/monitoring
match on these), and (b) actually classifies a real mismatch correctly: a
CONTENT difference (decompressed bytes differ) versus a CONTAINER-ONLY
difference (decompressed bytes identical, only stored metadata/compression
differs). A gate that cannot demonstrably fail -- and fail informatively -- is
not a gate.
"""

from __future__ import annotations

import subprocess
import sys
import zipfile
from pathlib import Path, PurePosixPath

import pytest

from scripts.compare_wheel_reproducibility import (
    _first_diff_offset,
    _read_wheel,
    _redact,
    _resolve_wheel_group,
    compare,
)
from scripts.compare_wheel_reproducibility import main as compare_main

# This suite exercises only the static comparison script against in-memory zip
# fixtures -- no native engine involved. Matches the adjacent release-artifact
# test modules (test_wheel_privacy.py, test_check_wheel_completeness.py).
pytestmark = pytest.mark.no_engine

WHEEL_NAME = "epistemic_graph-9.9.9-py3-none-any.whl"


def _make_wheel(
    directory: Path,
    *,
    members: dict[str, bytes],
    modes: dict[str, int] | None = None,
    name: str = WHEEL_NAME,
) -> Path:
    wheel = directory / name
    with zipfile.ZipFile(wheel, "w", zipfile.ZIP_DEFLATED) as archive:
        for member_name, payload in sorted(members.items()):
            if modes and member_name in modes:
                info = zipfile.ZipInfo(member_name)
                info.compress_type = zipfile.ZIP_DEFLATED
                info.external_attr = modes[member_name] << 16
                archive.writestr(info, payload)
            else:
                archive.writestr(member_name, payload)
    return wheel


_BASE_MEMBERS = {
    "epistemic_graph/__init__.py": b"print('hello')\n",
    "epistemic_graph/server.exe": b"\x00" * 32 + b"NATIVE-BINARY-PAYLOAD" + b"\x00" * 32,
    "epistemic_graph-9.9.9.dist-info/METADATA": b"Metadata-Version: 2.3\nName: epistemic-graph\n",
}


def test_resolve_wheel_group_accepts_directory_and_file(tmp_path: Path):
    directory = tmp_path / "dist"
    directory.mkdir()
    wheel = _make_wheel(directory, members=_BASE_MEMBERS)

    assert _resolve_wheel_group(directory) == [wheel]
    assert _resolve_wheel_group(wheel) == [wheel]


def test_identical_wheels_pass_and_print_the_success_marker(tmp_path: Path, capsys):
    primary_dir = tmp_path / "dist-primary"
    reproduction_dir = tmp_path / "dist-reproduction"
    primary_dir.mkdir()
    reproduction_dir.mkdir()
    _make_wheel(primary_dir, members=_BASE_MEMBERS)
    _make_wheel(reproduction_dir, members=_BASE_MEMBERS)

    compare(primary_dir, reproduction_dir)

    out = capsys.readouterr().out
    assert "release_wheel_reproducibility=passed" in out


def test_cli_success_exits_zero(tmp_path: Path):
    primary_dir = tmp_path / "dist-primary"
    reproduction_dir = tmp_path / "dist-reproduction"
    primary_dir.mkdir()
    reproduction_dir.mkdir()
    _make_wheel(primary_dir, members=_BASE_MEMBERS)
    _make_wheel(reproduction_dir, members=_BASE_MEMBERS)

    assert compare_main([str(primary_dir), str(reproduction_dir)]) == 0


def test_cardinality_mismatch_raises_the_exact_protected_message(tmp_path: Path):
    primary_dir = tmp_path / "dist-primary"
    reproduction_dir = tmp_path / "dist-reproduction"
    primary_dir.mkdir()
    reproduction_dir.mkdir()
    _make_wheel(primary_dir, members=_BASE_MEMBERS)
    # No wheel at all in reproduction_dir -> cardinality mismatch (0 vs 1).

    with pytest.raises(AssertionError, match="^release wheel cardinality mismatch$"):
        compare(primary_dir, reproduction_dir)


def test_filename_mismatch_raises_the_exact_protected_message(tmp_path: Path):
    primary_dir = tmp_path / "dist-primary"
    reproduction_dir = tmp_path / "dist-reproduction"
    primary_dir.mkdir()
    reproduction_dir.mkdir()
    _make_wheel(primary_dir, members=_BASE_MEMBERS, name="epistemic_graph-9.9.9-py3-none-any.whl")
    _make_wheel(
        reproduction_dir,
        members=_BASE_MEMBERS,
        name="epistemic_graph-9.9.8-py3-none-any.whl",
    )

    with pytest.raises(AssertionError, match="^release wheel filename mismatch$"):
        compare(primary_dir, reproduction_dir)


def test_digest_mismatch_raises_the_exact_protected_message(tmp_path: Path):
    primary_dir = tmp_path / "dist-primary"
    reproduction_dir = tmp_path / "dist-reproduction"
    primary_dir.mkdir()
    reproduction_dir.mkdir()
    _make_wheel(primary_dir, members=_BASE_MEMBERS)
    reproduction_members = dict(_BASE_MEMBERS)
    reproduction_members["epistemic_graph/__init__.py"] = b"print('different')\n"
    _make_wheel(reproduction_dir, members=reproduction_members)

    with pytest.raises(AssertionError, match="^release wheel digest mismatch$"):
        compare(primary_dir, reproduction_dir)


def test_cli_mismatch_exits_nonzero_as_a_subprocess(tmp_path: Path):
    """A gate that cannot demonstrably fail is not a gate: prove it end-to-end via the CLI."""

    primary_dir = tmp_path / "dist-primary"
    reproduction_dir = tmp_path / "dist-reproduction"
    primary_dir.mkdir()
    reproduction_dir.mkdir()
    _make_wheel(primary_dir, members=_BASE_MEMBERS)
    reproduction_members = dict(_BASE_MEMBERS)
    reproduction_members["epistemic_graph/__init__.py"] = b"print('different')\n"
    _make_wheel(reproduction_dir, members=reproduction_members)

    script = Path(__file__).resolve().parents[1] / "scripts" / "compare_wheel_reproducibility.py"
    result = subprocess.run(
        [sys.executable, str(script), str(primary_dir), str(reproduction_dir)],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert "release wheel digest mismatch" in result.stderr
    assert "CONTENT DIFFERS: epistemic_graph/__init__.py" in result.stdout
    assert "SUMMARY: content_differing_members=1 container_only_members=0" in result.stdout


def test_content_difference_is_classified_as_content(tmp_path: Path, capsys):
    """A member whose decompressed bytes differ must be classified CONTENT."""

    primary_dir = tmp_path / "dist-primary"
    reproduction_dir = tmp_path / "dist-reproduction"
    primary_dir.mkdir()
    reproduction_dir.mkdir()
    _make_wheel(primary_dir, members=_BASE_MEMBERS)
    reproduction_members = dict(_BASE_MEMBERS)
    reproduction_members["epistemic_graph/server.exe"] = (
        b"\x00" * 32 + b"NATIVE-BINARY-DIFFERS" + b"\x00" * 32
    )
    _make_wheel(reproduction_dir, members=reproduction_members)

    with pytest.raises(AssertionError, match="^release wheel digest mismatch$"):
        compare(primary_dir, reproduction_dir)

    out = capsys.readouterr().out
    assert "CONTENT DIFFERS: epistemic_graph/server.exe" in out
    assert "first differing byte offset:" in out
    assert "SUMMARY: content_differing_members=1 container_only_members=0" in out
    # Only the touched member is classified -- an untouched member must never
    # get its own CONTENT DIFFERS / CONTAINER-ONLY section.
    assert "CONTENT DIFFERS: epistemic_graph/__init__.py" not in out
    assert "CONTAINER-ONLY" not in out


def test_container_only_difference_is_classified_separately_from_content(tmp_path: Path, capsys):
    """Identical decompressed bytes but different ZipInfo metadata must be CONTAINER-ONLY, not CONTENT."""

    primary_dir = tmp_path / "dist-primary"
    reproduction_dir = tmp_path / "dist-reproduction"
    primary_dir.mkdir()
    reproduction_dir.mkdir()

    # primary: default compression + default mode.
    _make_wheel(primary_dir, members=_BASE_MEMBERS)

    # reproduction: byte-identical content in every member, but one member is
    # stored with a different Unix mode bit (a container-only difference) plus
    # a different digest-relevant member elsewhere so the overall wheel digest
    # still differs and the report actually runs.
    reproduction_members = dict(_BASE_MEMBERS)
    reproduction_members["epistemic_graph/README-EXTRA.txt"] = b"padding to force a digest change\n"
    _make_wheel(
        reproduction_dir,
        members=reproduction_members,
        modes={"epistemic_graph/server.exe": 0o755},
    )

    with pytest.raises(AssertionError, match="^release wheel digest mismatch$"):
        compare(primary_dir, reproduction_dir)

    out = capsys.readouterr().out
    assert "CONTAINER-ONLY (decompressed bytes IDENTICAL): epistemic_graph/server.exe" in out
    assert "CONTENT DIFFERS: epistemic_graph/server.exe" not in out
    assert "members ONLY in reproduction (1):" in out
    assert "epistemic_graph/README-EXTRA.txt" in out
    assert "SUMMARY: content_differing_members=0 container_only_members=1" in out


def test_hexdump_redacts_path_shaped_sensitive_substrings():
    # Built from separate path components (matching test_wheel_privacy.py's
    # own fixture idiom) rather than one literal joined string, so the
    # fixture path never appears in tracked source as a contiguous
    # home-path-shaped substring.
    sensitive_path = str(
        PurePosixPath("/", "home", "fixture-builder", "source", "lib.rs")
    ).encode()
    payload = b"prefix " + sensitive_path + b" suffix"
    redacted = _redact(payload)

    assert sensitive_path not in redacted
    assert b"prefix " in redacted
    assert b" suffix" in redacted
    assert len(redacted) == len(payload)


def test_first_diff_offset_handles_prefix_relationship():
    assert _first_diff_offset(b"abc", b"abcdef") == 3
    assert _first_diff_offset(b"abc", b"abc") is None
    assert _first_diff_offset(b"abx", b"aby") == 2


def test_read_wheel_returns_members_and_comment(tmp_path: Path):
    directory = tmp_path / "dist"
    directory.mkdir()
    wheel = _make_wheel(directory, members=_BASE_MEMBERS)

    members, comment = _read_wheel(wheel)

    assert set(members) == set(_BASE_MEMBERS)
    assert comment == b""
