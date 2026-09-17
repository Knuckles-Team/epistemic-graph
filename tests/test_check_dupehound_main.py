"""Behavior tests for scripts/check_dupehound.py's `main` and the helpers it
was decomposed into (`_run_dupehound`, `_load_reviewed_register`,
`_report_rotted_register`, `_report_clean_or_unregistered`).

`main` drives a real `dupehound` binary and a real git diff, neither of which
this pure-Python lane should invoke, so every external boundary (path
selection, the binary invocation, and the reviewed-distinct register) is
monkeypatched with deterministic fakes -- a change that regresses any of
`main`'s decision branches (no changed source / clean / rotted register /
unregistered clone) must fail this test.
"""

from __future__ import annotations

import json
import subprocess

import pytest

# Pure/static test -- never needs the shared native engine (see
# conftest.py's session-scoped `start_epistemic_graph_server` fixture,
# which this marker exempts this module from triggering).
pytestmark = pytest.mark.no_engine

_FINDING = {
    "file": "src/new.py",
    "line": 4,
    "name": "new",
    "similarity": 0.91,
    "original_file": "src/old.py",
    "original_line": 4,
    "original_name": "old",
}


@pytest.fixture
def dupehound(load_script):
    return load_script("check_dupehound")


def _stub_environment(dupehound, monkeypatch, *, paths=("src/new.py",)):
    """Bypass git/tool resolution: force a fixed changed-path set and a fake
    binary, so `main` reaches its own decision logic."""
    monkeypatch.setattr(dupehound, "changed_paths", lambda base_ref: list(paths))
    monkeypatch.setattr(
        dupehound,
        "selected_paths",
        lambda changed, config: sorted(changed) if paths else [],
    )
    monkeypatch.setattr(
        dupehound, "_resolve_dupehound", lambda config: "/fake/dupehound"
    )
    monkeypatch.setattr(dupehound, "_check_version", lambda executable, config: None)


def test_main_reports_ok_when_no_changed_supported_source(
    dupehound, monkeypatch, capsys
):
    _stub_environment(dupehound, monkeypatch, paths=())

    code = dupehound.main([])

    assert code == 0
    assert "no changed supported-language source" in capsys.readouterr().out


@pytest.mark.parametrize(
    ("findings", "register", "partitioned", "expected_code", "expected_output"),
    [
        pytest.param(
            [],
            [],
            ([], [], [], []),
            0,
            ["no changed function reimplementation"],
            id="dupehound-finds-nothing",
        ),
        pytest.param(
            [_FINDING],
            [],
            ([_FINDING], [], [], []),
            1,
            [
                "FAIL: 1 structural clone(s)",
                "src/new.py:4 new reimplements src/old.py:4 old",
            ],
            id="unregistered-clone",
        ),
        pytest.param(
            [],
            ["fake-pair"],
            ([], [], [], ["fake-pair"]),
            1,
            ["reviewed-distinct entr(ies) no longer"],
            id="rotted-unused-register-entry",
        ),
        pytest.param(
            [],
            ["fake-pair"],
            ([], [_FINDING], [], []),
            1,
            ["reviewed-distinct pair(s) changed"],
            id="rotted-changed-register-entry",
        ),
        pytest.param(
            [],
            ["fake-pair"],
            ([], [], [], []),
            0,
            ["1 reviewed-distinct pair(s) still hold"],
            id="register-still-holds",
        ),
    ],
)
def test_main_verdicts(
    dupehound,
    monkeypatch,
    capsys,
    findings,
    register,
    partitioned,
    expected_code,
    expected_output,
):
    _stub_environment(dupehound, monkeypatch)
    payload = json.dumps({"schema_version": 1, "findings": findings})
    monkeypatch.setattr(
        dupehound,
        "_run_dupehound",
        lambda executable, config, args: subprocess.CompletedProcess(
            args=["dupehound"],
            returncode=1 if findings else 0,
            stdout=payload,
            stderr="",
        ),
    )
    monkeypatch.setattr(dupehound.dupehound_ledger, "load_register", lambda: register)
    # Real `partition` calls `resolved_reason`, which stats the finding's
    # (real) files -- there are none, so pin the wiring in isolation with a
    # direct fake that also proves main hands it the parsed findings and the
    # loaded register.
    partition_calls = []

    def fake_partition(parsed, loaded):
        partition_calls.append((parsed, loaded))
        return partitioned

    monkeypatch.setattr(dupehound.dupehound_ledger, "partition", fake_partition)

    code = dupehound.main([])

    out = capsys.readouterr().out
    assert code == expected_code
    for fragment in expected_output:
        assert fragment in out
    assert partition_calls == [(findings, register)]
