"""Deterministic release-artifact privacy gates (no engine required)."""

from __future__ import annotations

import sys
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
from scripts.normalize_wheel_build_paths import main as normalize_build_paths_main
from scripts.normalize_wheel_sbom import normalize_wheel

# This file's own module docstring says "no engine required" -- every test here
# exercises pure-Python release-artifact scripts against in-memory zip fixtures,
# nothing else. Without this marker, conftest.py's session-scoped autouse
# `start_epistemic_graph_server` fixture doesn't know that and pays a full
# `cargo build --features full` before a single test here runs, exactly like
# `test_build_backend.py` and `test_release_full_wheel_contract.py` (the other
# static release-artifact test files) already declare.
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
    # 4, not 2: the explicit checkout/HOME (environment-derived) roots PLUS the
    # two unconditional container roots (/root, /github/home) that
    # ALWAYS_DENIED_ROOTS adds regardless of environment -- see
    # configure_rust_path_remap.py.
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
    # 3, not 1: the checkout PLUS the two unconditional container roots.
    assert remaps == 3


def test_native_flags_preserve_existing_values_and_remap_build_sources():
    checkout = PurePosixPath("/", "srv", "fixture-source")
    # `CC=sys.executable` makes compiler *resolution* deterministic (a Python
    # interpreter is always on PATH/absolute-resolvable in a pytest run,
    # unlike a system `cc`) without depending on this host actually having a
    # gcc/clang installed. `probe=lambda *_: True` stubs the *feature-detect*
    # step so this test asserts the flag-preservation/remap contract only --
    # the probe's real accept/reject behavior against a real compiler is
    # covered separately below.
    flags = native_prefix_flags(
        "CFLAGS",
        {"CFLAGS": "-O2 -fno-omit-frame-pointer", "CC": sys.executable},
        checkout=checkout,
        probe=lambda *_args, **_kwargs: True,
    )

    assert flags.startswith("-O2 -fno-omit-frame-pointer ")
    assert f"-ffile-prefix-map={checkout}=/build/source" in flags
    # The unconditional container roots ride along on every remap call, not
    # just the checkout -- native (C/C++) build scripts run inside the same
    # manylinux container and need the same protection rustc's
    # --remap-path-prefix gets.
    assert "-ffile-prefix-map=/root=/build/home" in flags
    assert "-ffile-prefix-map=/github/home=/build/home" in flags


def test_native_flags_fall_back_when_compiler_rejects_ffile_prefix_map(tmp_path):
    """Direct regression test for the aarch64 release-leg failure this module
    exists to prevent: `aarch64-unknown-linux-gnu-gcc: error: unrecognized
    command line option '-ffile-prefix-map=...'`.

    Builds a real (fake) compiler executable that mimics an old/cross gcc --
    accepts -fdebug-prefix-map, rejects -ffile-prefix-map exactly the way the
    manylinux aarch64 cross toolchain did -- and drives it through the actual
    subprocess-probing code path (no probe stub), proving the negative path
    concretely rather than merely asserting it in prose.
    """

    old_cross_gcc = tmp_path / "aarch64-unknown-linux-gnu-gcc"
    old_cross_gcc.write_text(
        "#!/bin/sh\n"
        "for arg in \"$@\"; do\n"
        "  case \"$arg\" in\n"
        "    -ffile-prefix-map=*)\n"
        "      echo \"$0: error: unrecognized command line option '$arg'\" >&2\n"
        "      exit 1\n"
        "      ;;\n"
        "  esac\n"
        "done\n"
        "exit 0\n"
    )
    old_cross_gcc.chmod(0o755)

    checkout = PurePosixPath("/", "srv", "fixture-source")
    flags = native_prefix_flags(
        "CFLAGS_aarch64_unknown_linux_gnu",
        {"CC_aarch64_unknown_linux_gnu": str(old_cross_gcc)},
        checkout=checkout,
        target="aarch64-unknown-linux-gnu",
    )

    # The flag this compiler proved it rejects must NEVER appear.
    assert "-ffile-prefix-map=" not in flags
    # The weaker but universally-supported fallback takes its place.
    assert f"-fdebug-prefix-map={checkout}=/build/source" in flags
    assert "-fdebug-prefix-map=/root=/build/home" in flags


def test_native_flags_keep_ffile_prefix_map_when_compiler_accepts_it(tmp_path):
    """Positive counterpart: a compiler that DOES accept -ffile-prefix-map
    (e.g. any GCC 8+/modern Clang, matching the linux-x86_64 leg that already
    passes) keeps the full macro+debug remap, unweakened."""

    modern_gcc = tmp_path / "x86_64-unknown-linux-gnu-gcc"
    modern_gcc.write_text("#!/bin/sh\nexit 0\n")
    modern_gcc.chmod(0o755)

    checkout = PurePosixPath("/", "srv", "fixture-source")
    flags = native_prefix_flags(
        "CFLAGS_x86_64_unknown_linux_gnu",
        {"CC_x86_64_unknown_linux_gnu": str(modern_gcc)},
        checkout=checkout,
        target="x86_64-unknown-linux-gnu",
    )

    assert f"-ffile-prefix-map={checkout}=/build/source" in flags
    assert "-fdebug-prefix-map=" not in flags


def test_native_flags_omit_when_compiler_accepts_neither_prefix_map_flag(tmp_path):
    """A compiler that rejects both macros gets neither flag -- the script
    must fail safe (log and omit) rather than fail the whole build."""

    ancient_cc = tmp_path / "ancient-target-gcc"
    ancient_cc.write_text(
        "#!/bin/sh\n"
        "echo \"$0: error: unrecognized command line option\" >&2\n"
        "exit 1\n"
    )
    ancient_cc.chmod(0o755)

    checkout = PurePosixPath("/", "srv", "fixture-source")
    flags = native_prefix_flags(
        "CFLAGS_ancient_target",
        {"CC_ancient_target": str(ancient_cc)},
        checkout=checkout,
        target="ancient-target",
    )

    assert "-ffile-prefix-map=" not in flags
    assert "-fdebug-prefix-map=" not in flags


def test_native_flags_never_borrow_host_compiler_for_an_unresolvable_target():
    """Regression guard for the mechanism that caused the original bug: a
    generic `CFLAGS`/`cc` on the outer runner must never stand in for an
    unresolvable CROSS target's compiler -- that is precisely how an
    unverified flag reached the wrong toolchain in the first place. When a
    target is given and no compiler for it can be resolved, the conservative
    -fdebug-prefix-map fallback is used, not a probe of some unrelated `cc`.
    """

    checkout = PurePosixPath("/", "srv", "fixture-source")
    # No CC_<target> override, and `improbable-target-triple-gcc` will not
    # exist on PATH -- deliberately unresolvable.
    flags = native_prefix_flags(
        "CFLAGS_improbable_target_triple",
        {"CFLAGS": "should-not-be-read"},
        checkout=checkout,
        target="improbable-target-triple",
    )

    assert "-ffile-prefix-map=" not in flags
    assert "-fdebug-prefix-map=" in flags


def test_scoped_flags_variable_naming():
    from scripts.configure_rust_path_remap import _scoped_flags_variable

    assert _scoped_flags_variable("CFLAGS", None) == "CFLAGS"
    assert (
        _scoped_flags_variable("CFLAGS", "aarch64-unknown-linux-gnu")
        == "CFLAGS_aarch64_unknown_linux_gnu"
    )


def test_cargo_target_dir_is_remapped_and_denied_at_runtime():
    target = PurePosixPath("/var", "tmp", "fixture-target")

    remaps = path_remaps({"CARGO_TARGET_DIR": str(target)})

    assert (str(target), "/build/cargo-target") in remaps
    assert str(target) in runtime_deny_prefixes({"CARGO_TARGET_DIR": str(target)})


def test_privileged_container_roots_are_always_denied_even_with_no_environment():
    """The linux manylinux release legs hand off compilation to a throwaway
    root-owned `docker run` (`PyO3/maturin-action` with `manylinux: 2_28`).
    That container's `HOME` is unconditionally `/root`, and `maturin-action`
    deliberately excludes `CARGO_HOME` from the env vars it forwards into the
    container (its own `FORBIDDEN_ENVS`), so the container always resolves a
    fresh `CARGO_HOME` of `/root/.cargo` regardless of what this script's
    caller — running on the outer runner, which never has `HOME=/root` — sees
    in its own environment. No environment variable this process reads can
    ever name that path, so it must be denied unconditionally, not merely
    when some environment variable says so."""

    remaps = path_remaps({})

    assert ("/root", "/build/home") in remaps
    assert ("/github/home", "/build/home") in remaps


def test_privileged_container_roots_survive_alongside_environment_roots():
    checkout = PurePosixPath("/", "srv", "fixture-source")

    remaps = path_remaps({"HOME": str(checkout)}, checkout=checkout)

    # The explicit checkout/HOME root from the environment coexists with the
    # two unconditional container roots -- they are never dropped by
    # deduplication (their source strings, "/root" and "/github/home", never
    # collide with anything environment-derived here).
    assert (str(checkout), "/build/source") in remaps
    assert ("/root", "/build/home") in remaps
    assert ("/github/home", "/build/home") in remaps


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


def test_planted_root_container_leak_is_caught_by_the_audit_before_any_fix(
    tmp_path: Path,
):
    """A privacy gate that passes only because it stopped looking is worse than
    the leak it was meant to catch. This plants the exact class of leak the
    manylinux/maturin-action release legs actually produce -- a Cargo registry
    source path under a root-owned container's `/root/.cargo` -- directly into
    an otherwise-untouched wheel member (no normalizer run first) and proves
    `check_wheel_privacy.py`'s own `privileged-home-prefix` pattern still flags
    it. This category is a broad PATTERN match (unlike the exact-prefix
    `runtime-build-prefix` category), so it fires with NO `deny_prefixes` and
    NO environment at all -- it is not contingent on the build-layer fix below
    having run first."""

    wheel = _wheel(
        tmp_path,
        {
            "fixture_package-1.0.0.dist-info/METADATA": _neutral_metadata(),
            "fixture_package/native.so": (
                b"ELF\x00/root/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f"
                b"/some-dep-1.2.3/src/lib.rs\x00"
            ),
        },
    )

    assert "privileged-home-prefix" in _categories(wheel)
    result = audit_wheel(wheel, environ={})
    assert any(f.category == "privileged-home-prefix" for f in result.findings)


def test_build_path_normalizer_scrubs_root_container_leak_unconditionally(
    tmp_path: Path,
):
    """The build-layer fix: `normalize_wheel_build_paths` derives its rewrite
    roots from `configure_rust_path_remap.path_remaps()`, so once `/root` and
    `/github/home` are unconditionally denied there (see
    `test_privileged_container_roots_are_always_denied_even_with_no_environment`),
    this normalizer scrubs a `/root/.cargo/registry/...` payload with an EMPTY
    environment -- exactly the case the manylinux legs hit, where no env var
    this process reads ever names the container's HOME. Before that fix this
    payload survived normalization untouched (the operator-observed CI
    failure: `normalize_wheel_sbom.py`/`normalize_wheel_build_paths.py` both
    reported 0 rewritten occurrences immediately before `check_wheel_privacy.py`
    failed on exactly these members)."""

    wheel = _wheel(
        tmp_path,
        {
            "fixture_package-1.0.0.dist-info/METADATA": _neutral_metadata(),
            "fixture_package-1.0.0.dist-info/RECORD": b"stale-record\n",
            "fixture_package/native.so": (
                b"ELF\x00/root/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f"
                b"/some-dep-1.2.3/src/lib.rs\x00"
            ),
        },
    )

    assert "privileged-home-prefix" in _categories(wheel)
    assert normalize_wheel_build_paths(wheel, environ={}) == 1

    with zipfile.ZipFile(wheel) as archive:
        payload = archive.read("fixture_package/native.so")
    assert b"/root" not in payload
    assert not audit_wheel(wheel, environ={}).findings


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


def test_sbom_normalizer_handles_cargo_target_dir_without_crashing(tmp_path: Path):
    """Regression test for D-W5EG-2: `normalize_wheel_sbom.py`'s `_URI_ALIASES`
    dict is what every `path_remaps()` replacement value must resolve through
    (`_root_aliases` does a bare `_URI_ALIASES[replacement]` lookup with no
    default). Every OTHER `ENVIRONMENT_ROOTS` replacement value
    (`/build/source`, `/build/runner-workspace`, `/build/cargo`,
    `/build/rustup`, `/build/tools`, `/build/temp`, `/build/local-data`,
    `/build/app-data`, `/build/home`) had a matching alias already, but
    `/build/cargo-target` (the `CARGO_TARGET_DIR` target) did not -- so setting
    `CARGO_TARGET_DIR`, which the mandatory isolated-build-target discipline
    means is ALWAYS set for a compliant local build, crashed this normalizer
    with a bare `KeyError` before it ever reached a single wheel member."""

    target = PurePosixPath("/var", "tmp", "fixture-target")
    reference = f"path+file://{target / 'release' / 'crate'}#1.0.0"
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
        normalize_wheel(wheel, environ={"CARGO_TARGET_DIR": str(target)}, checkout=None)
        == 1
    )
    with zipfile.ZipFile(wheel) as archive:
        sbom = archive.read(
            "fixture_package-1.0.0.dist-info/sboms/package.cyclonedx.json"
        )
    assert b"build://cargo-target/release/crate#1.0.0" in sbom
    assert not audit_wheel(wheel, environ={"CARGO_TARGET_DIR": str(target)}).findings


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


def test_build_path_normalizer_closes_source_before_replacing_on_windows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Windows refuses to replace/delete a file while ANY handle onto it is
    still open (`PermissionError` / WinError 32). `normalize_wheel_build_paths`
    used to call `Path.replace` on the freshly rewritten temp file while the
    *source* `zipfile.ZipFile` -- opened for reading the original wheel -- was
    still open inside its own enclosing `with` block (it stayed open only
    because the write phase needed `source.read()`). POSIX happily replaces a
    file out from under an open handle, so this was invisible on the
    Linux/macOS release legs; it is exactly the windows-x86_64 leg's real
    failure behind `scripts/normalize_wheel_build_paths.py`'s generic
    "could not complete" line (`.github/workflows/release.yml`, "Normalize
    and audit primary wheel" step).

    This reproduces the failure mode without a Windows runner: it fails any
    `Path.replace` of the ``.build-paths-tmp`` file while a read-mode
    `zipfile.ZipFile` handle is still tracked as open, and only passes once
    the source handle is closed before the replace -- which is exactly what
    the fix does.
    """

    build_root = PurePosixPath("/", "var", "lib", "fixture-builder", "source")
    wheel = _wheel(
        tmp_path,
        {
            "fixture_package-1.0.0.dist-info/METADATA": _neutral_metadata(),
            "fixture_package-1.0.0.dist-info/RECORD": b"stale-record\n",
            "fixture_package/native.so": (
                f"ELF\x00{build_root}/native/parser.c\x00"
            ).encode(),
        },
    )

    open_read_handles: list[zipfile.ZipFile] = []
    real_init = zipfile.ZipFile.__init__
    real_close = zipfile.ZipFile.close

    def tracking_init(self, file, mode="r", *args, **kwargs):
        real_init(self, file, mode, *args, **kwargs)
        if mode == "r":
            open_read_handles.append(self)

    def tracking_close(self):
        real_close(self)
        if self in open_read_handles:
            open_read_handles.remove(self)

    monkeypatch.setattr(zipfile.ZipFile, "__init__", tracking_init)
    monkeypatch.setattr(zipfile.ZipFile, "close", tracking_close)

    real_replace = Path.replace

    def guarded_replace(self, target):
        if str(self).endswith(".build-paths-tmp") and open_read_handles:
            raise PermissionError(
                13,
                "The process cannot access the file because it is being "
                "used by another process",
            )
        return real_replace(self, target)

    monkeypatch.setattr(Path, "replace", guarded_replace)

    assert (
        normalize_wheel_build_paths(wheel, environ={"HOME": str(build_root)}) == 1
    )
    with zipfile.ZipFile(wheel) as archive:
        payload = archive.read("fixture_package/native.so")
    assert str(build_root).encode() not in payload


def test_cli_reports_real_cause_instead_of_a_generic_message(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    """The CLI used to catch every failure behind one generic "FAIL: wheel
    build-path normalization could not complete" line with no wheel path, no
    exception type, and no message -- exactly why the windows-x86_64 release
    leg's real failure was undiagnosable from the CI log. It must now report
    which wheel failed and the real exception chain (type + message), not a
    static string."""

    wheel = _wheel(
        tmp_path,
        {
            "fixture_package-1.0.0.dist-info/METADATA": _neutral_metadata(),
            "fixture_package-1.0.0.dist-info/RECORD": b"stale-record\n",
            "fixture_package/native.so": (
                b"ELF\x00/root/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f"
                b"/some-dep-1.2.3/src/lib.rs\x00"
            ),
        },
    )

    real_replace = Path.replace

    def boom(self, target):
        if str(self).endswith(".build-paths-tmp"):
            raise PermissionError(
                13,
                "The process cannot access the file because it is being "
                "used by another process",
            )
        return real_replace(self, target)

    monkeypatch.setattr(Path, "replace", boom)

    assert normalize_build_paths_main([str(wheel)]) == 1
    captured = capsys.readouterr()
    assert str(wheel) in captured.err
    assert "PermissionError" in captured.err
    assert "used by another process" in captured.err
    assert captured.err.strip() != (
        "FAIL: wheel build-path normalization could not complete"
    )


def test_msvc_leg_gets_deterministic_link_flag_and_others_do_not() -> None:
    """`/Brepro` is emitted for the MSVC target only (E1).

    MSVC `link.exe` stamps a wall-clock `TimeDateStamp` into the PE header and a
    fresh CodeView GUID on every link, so two byte-identical inputs still yield
    two different binaries -- which is what fails the release workflow's
    "Require byte-identical folded wheels" step on windows-x86_64 while the
    linux/macos legs pass. `/Brepro` makes both content-derived.

    It is a `link.exe` switch, so emitting it on a leg that links with `ld`/`lld`
    would break that leg's build outright. This pins BOTH halves: present for
    MSVC, absent everywhere else.
    """

    msvc, _, _ = encoded_rustflags(
        {}, checkout="/build/source", target="x86_64-pc-windows-msvc"
    )
    assert "-Clink-arg=/Brepro" in msvc.split("\x1f")

    for other in (
        "x86_64-unknown-linux-gnu",
        "aarch64-unknown-linux-gnu",
        "aarch64-apple-darwin",
        "x86_64-apple-darwin",
        None,
    ):
        flags, _, _ = encoded_rustflags({}, checkout="/build/source", target=other)
        assert "-Clink-arg=/Brepro" not in flags.split("\x1f"), other


def test_deterministic_link_flag_is_not_duplicated_when_already_present() -> None:
    """A caller-supplied `/Brepro` is preserved, not emitted twice.

    Duplicate link args are accepted by `link.exe` but would make the encoded
    flag string differ between a leg that pre-set it and one that did not --
    itself a reproducibility hazard.
    """

    flags, _, _ = encoded_rustflags(
        {"RUSTFLAGS": "-Clink-arg=/Brepro"},
        checkout="/build/source",
        target="x86_64-pc-windows-msvc",
    )
    assert flags.split("\x1f").count("-Clink-arg=/Brepro") == 1
