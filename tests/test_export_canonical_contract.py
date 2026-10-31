"""Fresh hosted contract exports retain parity failures and bounded provenance."""

import importlib.util
import json
import subprocess
from pathlib import Path

import pytest
import yaml

pytestmark = [pytest.mark.no_engine, pytest.mark.spec("MEDULLA-CONTRACT-R006.3")]
ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def exporter(monkeypatch, tmp_path):
    spec = importlib.util.spec_from_file_location(
        "exporter", ROOT / "scripts/export_canonical_contract.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    for name in module.RECIPE:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("source")

    def output(command, **kwargs):
        if command[0] == "rustc":
            return "rustc test\n"
        if command[1:3] == ["rev-parse", "HEAD"]:
            return "a" * 40
        if command[1:3] == ["rev-parse", "HEAD^{tree}"]:
            return "b" * 40
        return ""

    monkeypatch.setattr(module.subprocess, "check_output", output)
    return module


def produce(module, root):
    body = root / "contract/methods.json"
    body.parent.mkdir(parents=True, exist_ok=True)
    body.write_text("{}")
    receipt = {"artifact_digests": {"contract/methods.json": module.digest(body)}}
    for name in module.EXTRAS:
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(receipt) if name.endswith("receipt.json") else "generated"
        )


def test_export_records_exact_source_recipe_and_artifacts(
    exporter, monkeypatch, tmp_path
):
    marker = tmp_path / "output"
    monkeypatch.setenv("GITHUB_OUTPUT", str(marker))
    monkeypatch.setattr(
        exporter.subprocess, "run", lambda *_a, **_kw: produce(exporter, tmp_path)
    )
    exporter.export(tmp_path, tmp_path / "export")
    proof = json.loads((tmp_path / "export/provenance.json").read_text())
    assert proof["source_revision"] == "a" * 40
    assert proof["source_tree"] == "b" * 40
    assert set(proof["artifact_sha256"]) == exporter.EXTRAS | {"contract/methods.json"}
    assert set(proof["recipe_sha256"]) == set(exporter.RECIPE)
    assert marker.read_text() == "fresh_export=true\n"


def test_failed_generator_never_exports(exporter, monkeypatch, tmp_path):
    def fail(*args, **kwargs):
        raise subprocess.CalledProcessError(1, exporter.COMMAND)

    monkeypatch.setattr(exporter.subprocess, "run", fail)
    with pytest.raises(subprocess.CalledProcessError):
        exporter.export(tmp_path, tmp_path / "export")
    assert not (tmp_path / "export").exists()


@pytest.mark.parametrize(
    "name",
    [
        "../secret",
        "/secret",
        "contract/../../secret.json",
        "source.rs",
        "medulla/generated/key.pem",
    ],
)
def test_path_allowlist_refuses_non_contracts(exporter, name):
    assert not exporter.allowed(name)


def test_digest_mismatch_never_exports(exporter, monkeypatch, tmp_path):
    def corrupt(*args, **kwargs):
        produce(exporter, tmp_path)
        (tmp_path / "contract/methods.json").write_text("changed")

    monkeypatch.setattr(exporter.subprocess, "run", corrupt)
    with pytest.raises(ValueError, match="digest mismatch"):
        exporter.export(tmp_path, tmp_path / "export")
    assert not (tmp_path / "export").exists()


def test_workflow_preserves_failed_parity():
    jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
    job = jobs["gates-contract"]
    assert job["needs"] == "changes"
    # Contract parity blocks every pull request; `gates` runs after merge.
    assert job["if"] == "needs.changes.outputs.code == 'true'"
    assert job["steps"][0] == jobs["gates"]["steps"][0]
    assert all(step in jobs["gates"]["steps"] for step in job["steps"][:5])
    assert "gates-contract" in jobs["engine-main-wheel"]["needs"]
    parity = next(
        step for step in job["steps"] if step.get("id") == "engine-contract-preflight"
    )
    export = next(
        step for step in job["steps"] if step.get("id") == "engine-contract-export"
    )
    upload = job["steps"][-1]
    assert parity["if"] == "${{ !cancelled() }}"
    assert (
        parity["run"] == "cargo run -q --locked -p medulla-capabilities --features contract "
        "--bin gen_contract -- --check"
    )
    assert "steps.engine-contract-preflight.outcome == 'failure'" in export["if"]
    assert "steps.engine-contract-export.outputs.fresh_export == 'true'" in upload["if"]
    assert all("continue-on-error" not in step for step in (parity, export, upload))

    retained = next(
        step
        for step in jobs["gates"]["steps"]
        if step.get("id") == "engine-contract-parity"
    )
    assert retained["run"] == parity["run"]
    assert "continue-on-error" not in retained


def test_workflow_has_one_canonical_export_authority():
    jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
    assert (
        sum(
            step.get("id") == "engine-contract-preflight"
            for entry in jobs.values()
            for step in entry.get("steps", [])
        )
        == 1
    )


def test_dirty_source_refuses_before_generation(exporter, monkeypatch, tmp_path):
    monkeypatch.setattr(
        exporter.subprocess, "check_output", lambda *a, **k: " M source.rs"
    )
    monkeypatch.setattr(
        exporter.subprocess, "run", lambda *a, **k: pytest.fail("must not build")
    )
    with pytest.raises(ValueError, match="clean tracked"):
        exporter.export(tmp_path, tmp_path / "export")


def test_symlink_refuses_before_upload(exporter, monkeypatch, tmp_path):
    def linked(*args, **kwargs):
        produce(exporter, tmp_path)
        body = tmp_path / "contract/methods.json"
        body.rename(tmp_path / "outside.json")
        body.symlink_to(tmp_path / "outside.json")

    monkeypatch.setattr(exporter.subprocess, "run", linked)
    with pytest.raises(ValueError, match="symlinks"):
        exporter.export(tmp_path, tmp_path / "export")
    assert not (tmp_path / "export").exists()


def test_existing_export_is_never_reused(exporter, tmp_path):
    directory = tmp_path / "export"
    directory.mkdir()
    with pytest.raises(ValueError, match="fresh"):
        exporter.export(tmp_path, directory)


def test_every_current_canonical_receipt_path_is_allowed(exporter):
    receipt = json.loads((ROOT / "contract/receipt.json").read_text())
    assert all(exporter.allowed(path) for path in receipt["artifact_digests"])


def test_untracked_source_refuses_before_generation(exporter, monkeypatch, tmp_path):
    def status(command, **kwargs):
        assert "--untracked-files=normal" in command
        return "?? source.rs"

    monkeypatch.setattr(exporter.subprocess, "check_output", status)
    with pytest.raises(ValueError, match="clean tracked"):
        exporter.export(tmp_path, tmp_path / "export")


@pytest.mark.parametrize("operation", ["ls-files", "diff"])
def test_generator_unexpected_source_refuses_export(
    exporter, monkeypatch, tmp_path, operation
):
    original = exporter.subprocess.check_output

    def output(command, **kwargs):
        if command[1:2] == [operation]:
            if operation == "diff":
                assert command[-1] == "HEAD"
            return "unexpected.rs"
        return original(command, **kwargs)

    monkeypatch.setattr(exporter.subprocess, "check_output", output)
    monkeypatch.setattr(
        exporter.subprocess, "run", lambda *_a, **_kw: produce(exporter, tmp_path)
    )
    with pytest.raises(ValueError, match="non-contract"):
        exporter.export(tmp_path, tmp_path / "export")
