"""Candidate official-wheel identity and provenance are fail closed."""

import json
import subprocess
import zipfile
from pathlib import Path

import pytest
import yaml

from scripts import candidate_server_wheel_provenance as proof

pytestmark = pytest.mark.no_engine
ROOT = Path(__file__).resolve().parents[1]


def wheel(directory: Path, name="medulla-stem", version="2.27.10", scripts=1):
    path = directory / "medulla_stem-2.27.10-py3-none-manylinux_2_28_x86_64.whl"
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("epistemic_graph_server/__init__.py", b"locator fixture")
        z.writestr(
            "medulla_stem-2.27.10.dist-info/METADATA",
            f"Name: {name}\nVersion: {version}\n",
        )
        if scripts:
            z.writestr(
                "medulla_stem.data/scripts/epistemic-graph-server", b"test fixture"
            )
        if scripts > 1:
            z.writestr("medulla_stem.data/scripts/other", b"test fixture")
    return path


@pytest.mark.parametrize(
    "name,version,scripts",
    [
        ("epistemic-graph-server", "2.27.10", 1),
        ("medulla-stem", "2.27.9", 1),
        ("medulla-stem", "2.27.10", 0),
        ("medulla-stem", "2.27.10", 2),
    ],
)
def test_wrong_package_or_script_refused(tmp_path, name, version, scripts):
    with pytest.raises(ValueError):
        proof.audit(wheel(tmp_path, name, version, scripts), "2.27.10")


def test_identity_and_platform(tmp_path):
    path = wheel(tmp_path)
    proof.audit(path, "2.27.10")
    moved = path.with_name("medulla_stem-2.27.10-py3-none-win_amd64.whl")
    path.rename(moved)
    with pytest.raises(ValueError):
        proof.audit(moved, "2.27.10")


def test_export_binds_clean_source_and_exact_bytes(tmp_path):
    root = tmp_path / "repo"
    root.mkdir()
    for name in proof.RECIPE:
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("fixture\n")
    (root / "Cargo.toml").write_text('[package]\nversion="2.27.10"\n')
    (root / proof.RECIPE[5]).write_text(
        '[project]\nname="medulla-stem"\nversion="2.27.10"\n'
    )

    def git(*args):
        return subprocess.check_output(["git", *args], cwd=root, text=True).strip()

    git("init", "-q")
    git("add", *proof.RECIPE)
    git(
        "-c",
        "user.name=Fixture",
        "-c",
        "user.email=fixture@example.invalid",
        "-c",
        "commit.gpgsign=false",
        "commit",
        "-qm",
        "fixture",
    )
    out = tmp_path / "out"
    out.mkdir()
    path = wheel(out)
    result = proof.export(root, out)
    assert result["source_revision"] == git("rev-parse", "HEAD")
    assert result["source_tree"] == git("rev-parse", "HEAD^{tree}")
    assert result["artifact_sha256"] == {path.name: proof.sha256(path.read_bytes())}
    assert result["recipe_sha256"] == {
        n: proof.sha256((root / n).read_bytes()) for n in proof.RECIPE
    }
    assert json.loads((out / "provenance.json").read_text()) == result
    (root / "Cargo.lock").write_text("changed\n")
    with pytest.raises(ValueError, match="unchanged tracked"):
        proof.export(root, out)


def test_job_uses_official_recipe_without_publication():
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    job = workflow["jobs"]["candidate-server-wheel"]
    assert job["if"] == (
        "needs.changes.outputs.code == 'true' && "
        "github.event_name == 'pull_request' && "
        "needs.changes.outputs.full_native == 'true'"
    )
    assert job["timeout-minutes"] == 360
    assert job["runs-on"] == "ubuntu-latest"
    assert (
        "permissions" not in job
        and "secrets" not in job
        and "continue-on-error" not in job
    )
    steps = job["steps"]
    assert steps[0]["with"] == {
        "ref": "${{ github.sha }}",
        "persist-credentials": False,
    }
    assert steps[1]["uses"] == "./.github/actions/build-server-wheel"
    assert steps[1]["with"] == {"cache-save": "false"}
    assert "twine" not in str(job) and "pypa/gh-action-pypi-publish" not in str(job)
    assert steps[-1]["with"]["if-no-files-found"] == "error"


@pytest.mark.parametrize(
    "cap", ["MAX_ARCHIVE", "MAX_MEMBER", "MAX_TOTAL", "MAX_METADATA", "MAX_MEMBERS"]
)
def test_archive_limits_refuse_before_install(tmp_path, monkeypatch, cap):
    path = wheel(tmp_path)
    monkeypatch.setattr(proof, cap, 1)
    with pytest.raises(ValueError):
        proof.audit(path, "2.27.10")


def test_stream_hash_refuses_unadvertised_expansion():
    import io

    with pytest.raises(ValueError, match="byte limit"):
        proof.stream_hash(io.BytesIO(b"oversized"), 3)


@pytest.mark.parametrize(
    "member", ["../escape", "/absolute", "epistemic_graph_server/__init__.py"]
)
def test_unsafe_or_duplicate_members_refused(tmp_path, member):
    path = wheel(tmp_path)
    with zipfile.ZipFile(path, "a") as archive:
        archive.writestr(member, b"unsafe")
    with pytest.raises(ValueError):
        proof.audit(path, "2.27.10")


@pytest.mark.parametrize(
    "fault",
    [None, "module_origin", "binary_origin", "binary_bytes", "module_bytes", "no_venv"],
)
def test_installed_evidence_rejects_shadowing_and_wrong_payload(
    tmp_path, monkeypatch, fault
):
    import types

    prefix = tmp_path / "venv"
    bindir = prefix / "bin"
    purelib = prefix / "lib/site-packages"
    bindir.mkdir(parents=True)
    (purelib / "epistemic_graph_server").mkdir(parents=True)
    binary = bindir / "epistemic-graph-server"
    module = purelib / "epistemic_graph_server/__init__.py"
    binary.write_bytes(b"native")
    module.write_bytes(b"locator")
    payload = {"binary": proof.file_hash(binary), "module": proof.file_hash(module)}
    monkeypatch.setattr(proof.sys, "prefix", str(prefix))
    monkeypatch.setattr(
        proof.sys, "base_prefix", str(prefix) if fault == "no_venv" else "/base"
    )
    monkeypatch.setattr(
        proof.sysconfig,
        "get_path",
        lambda key: str(bindir if key == "scripts" else purelib),
    )
    fake = types.SimpleNamespace(
        __file__=str(tmp_path / "foreign.py")
        if fault == "module_origin"
        else str(module),
        binary_path=lambda: (
            tmp_path / "foreign" if fault == "binary_origin" else binary
        ),
    )
    monkeypatch.setattr(proof.importlib, "import_module", lambda _: fake)
    if fault == "binary_bytes":
        binary.write_bytes(b"different")
    if fault == "module_bytes":
        module.write_bytes(b"different")
    if fault is None:
        assert proof.verify_installed(payload)["verified"] is True
    else:
        with pytest.raises(ValueError):
            proof.verify_installed(payload)
