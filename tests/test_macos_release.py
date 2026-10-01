"""Native Mac helper guards and bounded probe contracts."""

import os
import subprocess
from pathlib import Path

import pytest
import tomllib
import yaml

ROOT = Path(__file__).resolve().parents[1]


def invoke_helper(
    tmp_path: Path, *, lld: str = "22.1.2", flags: str = "remap", sdk: str = "14.5"
):
    tools = tmp_path / "tools"
    tools.mkdir()
    driver = tools / "driver.py"
    driver.write_text(
        "#!/usr/bin/env python3\nimport os,sys\nfrom pathlib import Path\n"
        "name=Path(sys.argv[0]).name\n"
        "if name=='uname': print('Darwin' if '-s' in sys.argv else 'arm64')\n"
        "elif name=='rustc':\n"
        " print(os.environ['FIXTURE_ROOT'] if '--print' in sys.argv "
        "else 'rustc 1.96.0')\n"
        "elif name=='ld64.lld': print('LLD '+os.environ['FIXTURE_LLD'])\n"
        "elif name=='xcrun':\n"
        " print(os.environ['FIXTURE_SDK'] if '--show-sdk-version' in sys.argv "
        "else os.environ['FIXTURE_ROOT']+'/sdk')\n"
        "else: print('Xcode fixture')\n"
    )
    driver.chmod(0o755)
    shim = tmp_path / "lib/rustlib/aarch64-apple-darwin/bin/gcc-ld/ld64.lld"
    shim.parent.mkdir(parents=True)
    shim.symlink_to(driver)
    for command in ("uname", "rustc", "xcrun", "xcodebuild"):
        (tools / command).symlink_to(driver)
    return subprocess.run(
        [
            "bash",
            "-euc",
            'source "$1" aarch64-apple-darwin; '
            'printf "RESULT=%s\\n" "$CARGO_ENCODED_RUSTFLAGS"',
            "fixture",
            str(ROOT / "scripts/configure_macos_linker.sh"),
        ],
        env={
            **os.environ,
            "PATH": f"{tools}:{os.environ['PATH']}",
            "FIXTURE_ROOT": str(tmp_path),
            "FIXTURE_LLD": lld,
            "FIXTURE_SDK": sdk,
            "CARGO_ENCODED_RUSTFLAGS": flags,
        },
        capture_output=True,
        text=True,
        check=False,
    )


def test_macho_preserves_remaps_and_uses_installed_flavor_shim(tmp_path):
    flags = "--remap-path-prefix=/source with spaces=/source\x1f-Cdebuginfo=0"
    result = invoke_helper(tmp_path, flags=flags)
    assert result.returncode == 0, result.stderr
    shim = tmp_path / "lib/rustlib/aarch64-apple-darwin/bin/gcc-ld/ld64.lld"
    assert f"RESULT={flags}\x1f-Clink-arg=-fuse-ld={shim}\n" in result.stdout
    assert shim.is_symlink(), "the installed flavor wrapper must not be relocated"


@pytest.mark.parametrize(
    "overrides", [{"lld": "99.0.0"}, {"flags": ""}, {"sdk": "99.0"}]
)
def test_macho_refuses_unverified_inputs(tmp_path, overrides):
    assert invoke_helper(tmp_path, **overrides).returncode != 0


def test_mac_probe_has_only_bounded_hosted_native_targets():
    workflow = yaml.safe_load(
        (ROOT / ".github/workflows/macos-release-probe.yml").read_text()
    )
    job = workflow["jobs"]["probe"]
    assert job["timeout-minutes"] == 15
    assert {row["runner"] for row in job["strategy"]["matrix"]["include"]} == {
        "macos-14",
        "macos-15-intel",
    }
    assert workflow["permissions"] == {"contents": "read"}
    assert job["env"]["RUSTUP_TOOLCHAIN"] == "1.96.0"
    assert not any("upload" in step.get("uses", "") for step in job["steps"])


def test_prebuild_is_after_clean_target_and_before_server_packaging():
    action = yaml.safe_load(
        (ROOT / ".github/actions/folded-wheel/action.yml").read_text()
    )
    steps = action["runs"]["steps"]
    prebuild = next(
        i for i, step in enumerate(steps) if step["name"].startswith("Prebuild all")
    )
    server = next(
        i for i, step in enumerate(steps) if step["name"] == "Build server wheel"
    )
    assert 0 < prebuild < server
    assert 'rm -rf -- "$target"' in steps[0]["run"]
    assert (
        steps[prebuild]["run"]
        == "bash scripts/prebuild_macos_bins.sh '${{ inputs.pass }}'"
    )
    command = (ROOT / "scripts/prebuild_macos_bins.sh").read_text()
    assert "/usr/bin/time -l cargo build --release --locked --bins --timings" in command
    assert '--features "${MATURIN_FEATURES:?}"' in command
    assert '--target "${EG_WHEEL_TARGET:?}"' in command
    assert 'read -r -a jobs <<< "${EG_WHEEL_JOBS_ARG:?}"' in command
    assert "status=${PIPESTATUS[0]}" in command
    assert 'exit "$status"' in command
    assert "endsWith(env.EG_WHEEL_TARGET, '-apple-darwin')" in steps[prebuild]["if"]
    assert "server-packaging-args" in steps[server]["with"]["args"]
    profile = tomllib.loads((ROOT / "Cargo.toml").read_text())["profile"]["release"]
    assert profile["strip"] is True
    assert (
        profile["opt-level"],
        profile["lto"],
        profile["codegen-units"],
        profile["panic"],
    ) == (3, "thin", 1, "unwind")


def test_pass_metrics_are_retained_outside_wheel_and_target_directories():
    workflow = yaml.safe_load((ROOT / ".github/workflows/release.yml").read_text())
    upload = next(
        step
        for step in workflow["jobs"]["build"]["steps"]
        if step.get("name") == "Retain Mac prebuild measurements"
    )
    assert upload["if"] == "${{ always() && endsWith(matrix.target, '-apple-darwin') }}"
    assert upload["with"]["path"] == "${{ runner.temp }}/eg-macos-build-metrics"
    assert upload["with"]["name"].startswith("macos-build-metrics-")
    wrapper = (ROOT / "scripts/prebuild_macos_bins.sh").read_text()
    assert 'metrics="${RUNNER_TEMP:?}/eg-macos-build-metrics/$pass"' in wrapper
    assert '"$metrics/exit-status.txt"' in wrapper
    assert '"$CARGO_TARGET_DIR/cargo-timings"' in wrapper
