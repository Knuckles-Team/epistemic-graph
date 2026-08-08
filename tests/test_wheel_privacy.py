"""Deterministic release-artifact privacy gates (no engine required)."""

from __future__ import annotations

import zipfile
from pathlib import Path, PurePosixPath, PureWindowsPath

import pytest

from scripts.check_wheel_privacy import audit_wheel, runtime_deny_prefixes
from scripts.check_wheel_privacy import main as audit_main
from scripts.configure_rust_path_remap import (
    UNIT_SEPARATOR,
    encoded_rustflags,
    native_prefix_flags,
    path_remaps,
)
from scripts.normalize_wheel_build_paths import normalize_wheel_build_paths
from scripts.normalize_wheel_sbom import normalize_wheel

# This file's own docstring says "no engine required" -- it exercises pure-Python
# release-artifact scripts against in-memory zip fixtures, nothing else. Without
# this marker, conftest.py's session-scoped autouse `start_epistemic_graph_server`
# fixture doesn't know that and pays a full `cargo build --features full` before a
# single test here runs, exactly like test_build_backend.py and
# test_release_full_wheel_contract.py (the other static release-artifact test
# files) already declare.
pytestmark = pytest.mark.no_engine


def _neutral_metadata(*, body: str = "") -> bytes:
    return (
        "Metadata-Version: 2.4\n"
        "Name: fixture-package\n"
        "Version: 1.0.0\n"
        "Author-email: Repository Maintainers <maintainers@example.invalid>\n"
        "\n"
        f"{body}"
    ).encode()


def _wheel(
    tmp_path: Path,
    members: dict[str, bytes],
    *,
    modes: dict[str, int] | None = None,
) -> Path:
    wheel = tmp_path / "fixture_package-1.0.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, payload in sorted(members.items()):
            if modes and name in modes:
                info = zipfile.ZipInfo(name)
                info.compress_type = zipfile.ZIP_DEFLATED
                info.external_attr = modes[name] << 16
                archive.writestr(info, payload)
            else:
                archive.writestr(name, payload)
    return wheel


def _categories(path: Path, **kwargs: object) -> set[str]:
    result = audit_wheel(path, environ={}, **kwargs)
    return {finding.category for finding in result.findings}


def test_encoded_flags_preserve_existing_encoded_flags_and_remap_specific_first():
    checkout = PurePosixPath("/", "srv", "fixture-source")
    home = PurePosixPath("/", "srv")
    existing = UNIT_SEPARATOR.join(("-C", "debuginfo=0"))

    encoded, preserved, remaps = encoded_rustflags(
        {
            "CARGO_ENCODED_RUSTFLAGS": existing,
            "GITHUB_WORKSPACE": str(checkout),
            "HOME": str(home),
        },
        checkout=checkout,
    )
    flags = encoded.split(UNIT_SEPARATOR)

    assert flags[:2] == ["-C", "debuginfo=0"]
    assert preserved == 2
    # 4, not 2: checkout + HOME (env-derived) PLUS the two unconditional
    # container roots (/root, /github/home) that ALWAYS_DENIED_ROOTS adds
    # regardless of environment -- see configure_rust_path_remap.py.
    assert remaps == 4
    assert flags[2].endswith("=/build/source")
    assert all(flag.endswith("=/build/home") for flag in flags[3:6])
    assert {flag.rpartition("=/build/home")[0] for flag in flags[3:6]} == {
        "--remap-path-prefix=/github/home",
        "--remap-path-prefix=/root",
        "--remap-path-prefix=/srv",
    }


def test_encoded_flags_convert_plain_flags_without_losing_quoted_values():
    encoded, preserved, remaps = encoded_rustflags(
        {"RUSTFLAGS": "-C opt-level=3 '-Ctarget-cpu=x86-64-v2'"},
        checkout=PurePosixPath("/", "srv", "fixture-source"),
    )
    flags = encoded.split(UNIT_SEPARATOR)

    assert flags[:3] == ["-C", "opt-level=3", "-Ctarget-cpu=x86-64-v2"]
    assert preserved == 3
    # 3, not 1: checkout PLUS the two unconditional container roots.
    assert remaps == 3


def test_native_flags_preserve_existing_values_and_remap_build_sources():
    checkout = PurePosixPath("/", "srv", "fixture-source")
    flags = native_prefix_flags(
        "CFLAGS",
        {"CFLAGS": "-O2 -fno-omit-frame-pointer"},
        checkout=checkout,
    )

    assert flags.startswith("-O2 -fno-omit-frame-pointer ")
    assert f"-ffile-prefix-map={checkout}=/build/source" in flags
    # The unconditional container roots ride along on every remap call, not
    # just the checkout -- native (C/C++) build scripts run inside the same
    # manylinux container and need the same protection rustc's
    # --remap-path-prefix gets.
    assert "-ffile-prefix-map=/root=/build/home" in flags
    assert "-ffile-prefix-map=/github/home=/build/home" in flags


def test_privileged_container_roots_are_always_denied_even_with_no_environment():
    """Regression test: manylinux/maturin-action builds hand off to a Docker
    container that always runs as root (HOME=/root, CARGO_HOME=/root/.cargo,
    RUSTUP_HOME=/root/.rustup), a path structure no environment variable this
    script can read ever names -- the outer runner's own env is irrelevant to
    what the container uses. A dependency fetched fresh into that container's
    registry cache embeds `/root/.cargo/registry/src/.../<crate>/src/....rs`
    into panic!/track_caller/file!() locations, and it must be remapped
    unconditionally, not merely when some environment variable says so."""

    remaps = path_remaps({})

    assert ("/root", "/build/home") in remaps
    assert ("/github/home", "/build/home") in remaps


def test_cargo_target_dir_is_remapped_and_denied_at_runtime():
    target = PurePosixPath("/var", "tmp", "fixture-target")

    remaps = path_remaps({"CARGO_TARGET_DIR": str(target)})

    assert (str(target), "/build/cargo-target") in remaps
    assert str(target) in runtime_deny_prefixes({"CARGO_TARGET_DIR": str(target)})


def test_sanitized_wheel_allows_third_party_attribution_emails(tmp_path: Path):
    wheel = _wheel(
        tmp_path,
        {
            "fixture_package-1.0.0.dist-info/METADATA": _neutral_metadata(
                body="License attribution: Contributor <third.party@example.org>\n"
            ),
            "fixture_package-1.0.0.dist-info/sbom.spdx.json": (
                b'{"supplier":"third.party@example.org","source":"/build/source"}'
            ),
            "fixture_package/native.so": b"ELF\x00/build/cargo/registry\x00",
        },
    )

    result = audit_wheel(wheel, environ={})

    assert result.member_count == 3
    assert not result.findings


@pytest.mark.parametrize(
    ("member", "payload", "expected"),
    [
        (
            "fixture_package/native.so",
            str(PurePosixPath("/", "home", "fixture-builder", "source"))
            .encode()
            .join((b"ELF\x00", b"\x00")),
            "posix-home-prefix",
        ),
        (
            "fixture_package/sbom.cdx.json",
            str(
                PurePosixPath(
                    "/", "mnt", "z", "Users", "fixture-builder", "Workspace", "source"
                )
            ).encode(),
            "wsl-home-prefix",
        ),
        (
            "fixture_package/native.pdb",
            str(
                PureWindowsPath(
                    "Z:/", "Users", "fixture-builder", "Workspace", "source"
                )
            ).encode("utf-16le"),
            "windows-home-prefix",
        ),
    ],
)
def test_wheel_members_are_scanned_regardless_of_content_type(
    tmp_path: Path,
    member: str,
    payload: bytes,
    expected: str,
):
    wheel = _wheel(
        tmp_path,
        {
            "fixture_package-1.0.0.dist-info/METADATA": _neutral_metadata(),
            member: payload,
        },
    )

    assert expected in _categories(wheel)


def test_runtime_prefix_detects_nonstandard_build_root(tmp_path: Path):
    build_root = PurePosixPath("/", "var", "lib", "fixture-builder", "source")
    wheel = _wheel(
        tmp_path,
        {
            "fixture_package-1.0.0.dist-info/METADATA": _neutral_metadata(),
            "fixture_package/native.so": f"ELF\x00{build_root}/src/lib.rs".encode(),
        },
    )

    assert "runtime-build-prefix" in _categories(
        wheel,
        deny_prefixes=(str(build_root),),
    )


def test_runtime_prefix_requires_a_path_boundary(tmp_path: Path):
    build_root = PurePosixPath("/", "root")
    wheel = _wheel(
        tmp_path,
        {
            "fixture_package-1.0.0.dist-info/METADATA": _neutral_metadata(),
            "fixture_package/native.so": b"ELF\x00/build/source/src/root_cause.rs\x00",
        },
    )

    assert "runtime-build-prefix" not in _categories(
        wheel,
        deny_prefixes=(str(build_root),),
    )


@pytest.mark.parametrize(
    "member",
    (
        "fixture_package/__pycache__/module.cpython-312.pyc",
        "fixture_package/module.pyc",
        "fixture_package/module.pyo",
    ),
)
def test_python_bytecode_members_are_rejected(tmp_path: Path, member: str):
    wheel = _wheel(
        tmp_path,
        {
            "fixture_package-1.0.0.dist-info/METADATA": _neutral_metadata(),
            member: b"compiled-python-cache",
        },
    )

    assert "python-bytecode-member" in _categories(wheel)


def test_build_path_normalizer_rewrites_native_payload_and_rebuilds_record(
    tmp_path: Path,
):
    build_root = PurePosixPath("/", "var", "lib", "fixture-builder", "source")
    executable = "fixture_package-1.0.0.data/scripts/fixture-server"
    wheel = _wheel(
        tmp_path,
        {
            "fixture_package-1.0.0.dist-info/METADATA": _neutral_metadata(),
            "fixture_package-1.0.0.dist-info/RECORD": b"stale-record\n",
            executable: (
                f"ELF\x00{build_root}/native/parser.c\x00"
                "/build/source/src/root_cause.rs\x00"
            ).encode(),
        },
        modes={executable: 0o100755},
    )

    assert "runtime-build-prefix" in _categories(
        wheel, deny_prefixes=(str(build_root),)
    )
    before_size = len(f"{build_root}".encode())
    assert (
        normalize_wheel_build_paths(
            wheel,
            environ={"HOME": str(build_root)},
        )
        == 1
    )

    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        payload = archive.read(executable)
        record = archive.read("fixture_package-1.0.0.dist-info/RECORD")
        mode = archive.getinfo(executable).external_attr >> 16
    assert names[-1].endswith(".dist-info/RECORD")
    assert str(build_root).encode() not in payload
    assert b"/build/source/src/root_cause.rs" in payload
    neutral_prefix = payload.split(b"ELF\x00", 1)[1].split(b"/native", 1)[0]
    assert len(neutral_prefix) == before_size
    assert mode & 0o111
    assert b"stale-record" not in record
    assert b"sha256=" in record
    assert not audit_wheel(
        wheel,
        environ={},
        deny_prefixes=(str(build_root),),
    ).findings
    assert (
        normalize_wheel_build_paths(
            wheel,
            environ={"HOME": str(build_root)},
        )
        == 0
    )


def test_build_path_normalizer_rewrites_cargo_target_dir(tmp_path: Path):
    target = PurePosixPath("/var", "tmp", "fixture-target")
    wheel = _wheel(
        tmp_path,
        {
            "fixture_package-1.0.0.dist-info/METADATA": _neutral_metadata(),
            "fixture_package-1.0.0.dist-info/RECORD": b"stale-record\n",
            "fixture_package/native.so": f"ELF\x00{target}/release/build.rs\x00".encode(),
        },
    )
    environ = {"CARGO_TARGET_DIR": str(target)}

    assert "runtime-build-prefix" in _categories(
        wheel, deny_prefixes=runtime_deny_prefixes(environ)
    )
    assert normalize_wheel_build_paths(wheel, environ=environ) == 1
    assert not audit_wheel(wheel, environ=environ).findings


def test_build_path_normalizer_rewrites_utf16le_windows_payload(tmp_path: Path):
    build_root = str(
        PureWindowsPath("Z:/", "Users", "fixture-builder", "Workspace", "source")
    )
    member = "fixture_package/native.pdb"
    wheel = _wheel(
        tmp_path,
        {
            "fixture_package-1.0.0.dist-info/METADATA": _neutral_metadata(),
            "fixture_package-1.0.0.dist-info/RECORD": b"stale-record\n",
            member: (build_root + r"\native\parser.c").encode("utf-16le"),
        },
    )

    assert "runtime-build-prefix" in _categories(wheel, deny_prefixes=(build_root,))
    assert (
        normalize_wheel_build_paths(
            wheel,
            environ={"USERPROFILE": build_root},
        )
        == 1
    )
    with zipfile.ZipFile(wheel) as archive:
        payload = archive.read(member)
    assert build_root.encode("utf-16le") not in payload
    assert len(payload) == len((build_root + r"\native\parser.c").encode("utf-16le"))
    assert not audit_wheel(
        wheel,
        environ={},
        deny_prefixes=(build_root,),
    ).findings


def test_sbom_normalizer_rewrites_local_refs_and_rebuilds_record(tmp_path: Path):
    source_root = PurePosixPath("/", "srv", "fixture-source")
    reference = f"path+file://{source_root}#1.0.0"
    wheel = _wheel(
        tmp_path,
        {
            "fixture_package-1.0.0.dist-info/METADATA": _neutral_metadata(),
            "fixture_package-1.0.0.dist-info/RECORD": b"stale-record\n",
            "fixture_package-1.0.0.dist-info/sboms/package.cyclonedx.json": (
                '{"metadata":{"component":{"bom-ref":"'
                + reference
                + '"}},"dependencies":[{"ref":"'
                + reference
                + '"}]}'
            ).encode(),
        },
    )

    assert _categories(wheel) == {"filesystem-sbom-reference"}
    assert normalize_wheel(wheel, environ={}, checkout=source_root) == 1

    with zipfile.ZipFile(wheel) as archive:
        sbom = archive.read(
            "fixture_package-1.0.0.dist-info/sboms/package.cyclonedx.json"
        )
        record = archive.read("fixture_package-1.0.0.dist-info/RECORD")
    assert b"path+file://" not in sbom
    assert sbom.count(b"repo://source#1.0.0") == 2
    assert b"sha256=" in record
    assert not audit_wheel(wheel, environ={}).findings
    assert normalize_wheel(wheel, environ={}, checkout=source_root) == 0


def test_sbom_home_alias_is_not_shaped_like_a_posix_home_path(tmp_path: Path):
    home = PurePosixPath("/", "home", "fixture-builder")
    source_root = PurePosixPath("/", "srv", "fixture-source")
    reference = f"path+file://{home / 'cache' / 'crate'}#1.0.0"
    wheel = _wheel(
        tmp_path,
        {
            "fixture_package-1.0.0.dist-info/METADATA": _neutral_metadata(),
            "fixture_package-1.0.0.dist-info/RECORD": b"stale-record\n",
            "fixture_package-1.0.0.dist-info/sboms/package.cyclonedx.json": (
                '{"metadata":{"component":{"bom-ref":"' + reference + '"}}}'
            ).encode(),
        },
    )

    assert (
        normalize_wheel(
            wheel,
            environ={"HOME": str(home)},
            checkout=source_root,
        )
        == 1
    )
    with zipfile.ZipFile(wheel) as archive:
        sbom = archive.read(
            "fixture_package-1.0.0.dist-info/sboms/package.cyclonedx.json"
        )
    assert b"build://identity-neutral-root/cache/crate#1.0.0" in sbom
    assert not audit_wheel(wheel, environ={}).findings


def test_absolute_path_file_is_rejected_even_when_prefix_is_neutral(tmp_path: Path):
    wheel = _wheel(
        tmp_path,
        {
            "fixture_package-1.0.0.dist-info/METADATA": _neutral_metadata(),
            "fixture_package.pth": b"/build/source\n",
        },
    )

    assert _categories(wheel) == {"absolute-pth-reference"}


def test_only_core_metadata_identity_fields_must_be_neutral(tmp_path: Path):
    metadata = (
        b"Metadata-Version: 2.4\n"
        b"Name: fixture-package\n"
        b"Version: 1.0.0\n"
        b"Author-email: Fixture Identity <fixture.identity@example.org>\n"
        b"\n"
        b"Attribution: Third Party <third.party@example.org>\n"
    )
    wheel = _wheel(
        tmp_path,
        {"fixture_package-1.0.0.dist-info/METADATA": metadata},
    )

    assert _categories(wheel) == {"first-party-metadata-identity"}


def test_cli_never_echoes_a_sensitive_prefix(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
):
    build_root = PurePosixPath("/", "var", "lib", "fixture-builder", "source")
    wheel = _wheel(
        tmp_path,
        {
            "fixture_package-1.0.0.dist-info/METADATA": _neutral_metadata(),
            "fixture_package/native.so": f"ELF\x00{build_root}/src/lib.rs".encode(),
        },
    )

    assert audit_main([str(wheel), "--deny-prefix", str(build_root)]) == 1
    captured = capsys.readouterr()
    assert str(build_root) not in captured.out
    assert str(build_root) not in captured.err
    assert "category=runtime-build-prefix" in captured.err
