"""ARM64 linker selection contracts without native compilation or network."""

import os
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / "scripts/configure_arm64_linker.sh"


def fixture_tools(tmp_path: Path, *, version: str = "LLD 22.1.2") -> Path:
    tools = tmp_path / "bin"
    tools.mkdir()
    shim = tmp_path / "lib/rustlib/aarch64-unknown-linux-gnu/bin/gcc-ld"
    shim.mkdir(parents=True)
    scripts = {
        tools / "uname": "echo aarch64",
        tools / "rustc": (
            f'case "$*" in "--print sysroot") echo "{tmp_path}";;'
            ' *) echo "rustc 1.96.0";; esac'
        ),
        tools / "cc": f'echo "{version}"',
        tools / "ld": "echo 'GNU ld fixture'",
        shim / "ld.lld": f'echo "{version}"',
    }
    for path, text in scripts.items():
        path.write_text("#!/bin/sh\n" + text + "\n")
        path.chmod(0o755)
    return tools


def invoke(tools: Path, flags: str, target: str = "aarch64-unknown-linux-gnu"):
    return subprocess.run(
        [
            "bash",
            "-euc",
            'source "$1" "$2"; printf "FLAGS=%s\\n" "$CARGO_ENCODED_RUSTFLAGS"',
            "probe",
            str(HELPER),
            target,
        ],
        env={
            **os.environ,
            "PATH": f"{tools}:{os.environ['PATH']}",
            "CARGO_ENCODED_RUSTFLAGS": flags,
        },
        text=True,
        capture_output=True,
        check=False,
    )


def test_preserves_encoded_flags_and_selects_container_shim(tmp_path):
    tools = fixture_tools(tmp_path)
    original = "--remap-path-prefix=/source with spaces=/build/source\x1f-Cdebuginfo=0"
    result = invoke(tools, original)
    assert result.returncode == 0, result.stderr
    suffix = (
        f"\x1f-Clink-arg=-B{tmp_path}/lib/rustlib/"
        "aarch64-unknown-linux-gnu/bin/gcc-ld\x1f-Clink-arg=-fuse-ld=lld"
    )
    assert f"FLAGS={original}{suffix}\n" in result.stdout


@pytest.mark.parametrize(
    "target",
    ["x86_64-unknown-linux-gnu", "aarch64-apple-darwin", "x86_64-pc-windows-msvc"],
)
def test_other_targets_keep_flags_unchanged(tmp_path, target):
    result = invoke(tmp_path, "keep-this", target)
    assert result.returncode == 0
    assert result.stdout == "FLAGS=keep-this\n"


@pytest.mark.parametrize(
    "failure", ["missing", "version", "driver", "remaps", "host", "rust"]
)
def test_fails_closed(tmp_path, failure):
    tools = fixture_tools(tmp_path)
    flags = "--remap-path-prefix=/a=/b"
    if failure == "missing":
        (tmp_path / "lib/rustlib/aarch64-unknown-linux-gnu/bin/gcc-ld/ld.lld").unlink()
    elif failure == "version":
        (
            tmp_path / "lib/rustlib/aarch64-unknown-linux-gnu/bin/gcc-ld/ld.lld"
        ).write_text("#!/bin/sh\necho 'LLD 99.0.0'\n")
    elif failure == "driver":
        (tools / "cc").write_text("#!/bin/sh\necho 'GNU ld fixture'\n")
    elif failure == "remaps":
        flags = ""
    elif failure == "host":
        (tools / "uname").write_text("#!/bin/sh\necho x86_64\n")
    elif failure == "rust":
        (tools / "rustc").write_text("#!/bin/sh\necho 'rustc 1.98.0'\n")
    assert invoke(tools, flags).returncode != 0


def test_all_three_maturin_containers_select_linker_after_setup():
    action = yaml.safe_load(
        (ROOT / ".github/actions/folded-wheel/action.yml").read_text()
    )
    builds = [
        step
        for step in action["runs"]["steps"]
        if step.get("uses", "") == "./.github/actions/release-maturin"
    ]
    assert len(builds) == 3
    shared = yaml.safe_load(
        (ROOT / ".github/actions/release-maturin/action.yml").read_text()
    )["runs"]["steps"][0]
    assert (
        shared["uses"] == "PyO3/maturin-action@e83996d129638aa358a18fbd1dfb82f0b0fb5d3b"
    )
    assert (
        shared["with"]["before-script-linux"]
        == "source scripts/configure_arm64_linker.sh '${{ env.EG_WHEEL_TARGET }}'"
    )


def test_probe_is_bounded_hosted_and_nonpublishing():
    workflow = yaml.safe_load(
        (ROOT / ".github/workflows/arm64-linker-probe.yml").read_text()
    )
    job = workflow["jobs"]["probe"]
    assert job["runs-on"] == "ubuntu-24.04-arm"
    assert job["timeout-minutes"] == 10
    assert workflow["permissions"] == {"contents": "read"}
    step = next(
        step
        for step in job["steps"]
        if step.get("uses", "").startswith("PyO3/maturin-action@")
    )
    assert step["with"]["container"].endswith(
        "@sha256:acc4e63610fef1da3d687322793665205415c2b22c0d2e403f1a44eb834d63fc"
    )
    assert step["with"]["maturin-version"] == "1.15.0"
    assert all("upload" not in step.get("uses", "") for step in job["steps"])


def test_pass_target_is_mounted_and_reproduction_starts_clean(tmp_path):
    action = yaml.safe_load(
        (ROOT / ".github/actions/folded-wheel/action.yml").read_text()
    )
    steps = action["runs"]["steps"]
    target = tmp_path / "epistemic-graph-release-target"
    env_file = tmp_path / "github-env"
    env_file.touch()
    environment = {
        **os.environ,
        "RUNNER_TEMP": str(tmp_path),
        "GITHUB_ENV": str(env_file),
    }
    # Execute the actual selector twice: a primary artifact must not survive
    # the reproduction pass, while the directory/path stays consistent.
    for _ in range(2):
        target.mkdir(parents=True, exist_ok=True)
        marker = target / "previous-pass-artifact"
        marker.write_text("must not be reused")
        subprocess.run(
            ["bash", "-eu", "-c", steps[0]["run"]], env=environment, check=True
        )
        assert target.is_dir() and not marker.exists()
    assert env_file.read_text().splitlines() == [f"CARGO_TARGET_DIR={target}"] * 2
    builds = [
        step
        for step in steps
        if step.get("uses", "") == "./.github/actions/release-maturin"
    ]
    assert len(builds) == 3
    shared = yaml.safe_load(
        (ROOT / ".github/actions/release-maturin/action.yml").read_text()
    )["runs"]["steps"][0]
    assert shared["with"]["docker-options"] == (
        '--volume "${{ env.CARGO_TARGET_DIR }}:${{ env.CARGO_TARGET_DIR }}"'
    )


@pytest.mark.parametrize(
    "target",
    ["aarch64-unknown-linux-gnu", "x86_64-unknown-linux-gnu", "aarch64-apple-darwin"],
)
def test_only_linux_arm_uses_exact_probed_environment(tmp_path, target):
    action = yaml.safe_load(
        (ROOT / ".github/actions/folded-wheel/action.yml").read_text()
    )
    steps = action["runs"]["steps"]
    outputs = tmp_path / "outputs"
    outputs.touch()
    environment = {
        **os.environ,
        "RUNNER_TEMP": str(tmp_path),
        "GITHUB_ENV": str(tmp_path / "env"),
        "GITHUB_OUTPUT": str(outputs),
        "EG_WHEEL_TARGET": target,
    }
    subprocess.run(["bash", "-eu", "-c", steps[0]["run"]], env=environment, check=True)
    if target == "aarch64-unknown-linux-gnu":
        assert outputs.read_text().splitlines() == [
            "container=quay.io/pypa/manylinux_2_28_aarch64@sha256:acc4e63610fef1da3d687322793665205415c2b22c0d2e403f1a44eb834d63fc",
            "maturin-version=1.15.0",
        ]
    else:
        assert outputs.read_text() == ""
    for step in steps:
        if step.get("uses", "") == "./.github/actions/release-maturin":
            assert (
                step["with"]["container"]
                == "${{ steps.build-environment.outputs.container }}"
            )
            assert (
                step["with"]["maturin-version"]
                == "${{ steps.build-environment.outputs.maturin-version }}"
            )
