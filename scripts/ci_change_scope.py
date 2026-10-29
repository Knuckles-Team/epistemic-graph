#!/usr/bin/env python3
"""Conservatively identify documentation-only PRs; other events run full CI."""

from __future__ import annotations

import argparse
import re
import subprocess
from collections.abc import Callable
from pathlib import PurePosixPath

ROOT_DOCUMENTS = frozenset(
    {"README.md", "CHANGELOG.md", "CONTRIBUTING.md", "AGENTS.md", "CLAUDE.md"}
)


def documentation_path(path: str) -> bool:
    """Only non-executable prose and generated spec status can bypass builds."""
    parts = PurePosixPath(path).parts
    if path in ROOT_DOCUMENTS:
        return True
    if len(parts) < 2 or parts[0] not in {"docs", "specs", ".specify", "architecture"}:
        return False
    return path.endswith(".md") or (
        len(parts) == 3 and parts[0] == "specs" and parts[2] == "status.json"
    )


def _raw_entry_is_documentation(
    metadata_field: bytes, path_field: bytes, path_predicate: Callable[[str], bool]
) -> bool:
    """One raw diff entry: a plain-mode A/M/D change of a documentation path."""
    metadata = metadata_field.split()
    if len(metadata) != 5 or not metadata[0].startswith(b":"):
        return False
    modes = {metadata[0][1:], metadata[1]}
    if not modes <= {b"100644", b"000000"} or b"100644" not in modes:
        return False
    if metadata[4] not in {b"A", b"M", b"D"}:
        return False
    try:
        path = path_field.decode("utf-8")
    except UnicodeDecodeError:
        return False
    return path_predicate(path)


def documentation_diff(
    raw: bytes, path_predicate: Callable[[str], bool] = documentation_path
) -> bool:
    """Read --raw -z --no-renames, retaining deletions and executable modes."""
    fields = raw.split(b"\0")
    if fields[-1] != b"" or len(fields) < 3 or len(fields) % 2 != 1:
        return False
    return all(
        _raw_entry_is_documentation(fields[index], fields[index + 1], path_predicate)
        for index in range(0, len(fields) - 1, 2)
    )


def requires_code(event: str, base: str, head: str) -> bool:
    """Unknown refs, unavailable history and empty comparisons run full CI."""
    if event != "pull_request" or not all(
        re.fullmatch(r"[0-9a-f]{40}", ref) for ref in (base, head)
    ):
        return True
    try:
        result = subprocess.run(
            ["git", "diff", "--raw", "--no-renames", "-z", f"{base}...{head}"],
            check=True,
            capture_output=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return True
    return not documentation_diff(result.stdout)


_TOOLCHAIN_PATHS = ("Cargo.lock", "rust-toolchain.toml")
# Any change to CI wiring or to this selector runs every path-gated job.
_ALL_GATES_PATHS = (".github/", "scripts/ci_change_scope.py")
# Pull-request jobs that run only when a path they exercise changed. Push,
# schedule and dispatch events always run them.
PATH_GATES: dict[str, tuple[str, ...]] = {
    "clients": (
        "clients/",
        "contract/",
        "crates/eg-method-codec/",
        "crates/eg-types/",
        "scripts/build_method_codec_wasm.py",
        *_TOOLCHAIN_PATHS,
    ),
    "whisper": (
        "crates/eg-asr-whisper/",
        "crates/eg-audio/",
        "crates/eg-resource/",
        "scripts/fetch_whisper_test_fixture.sh",
        *_TOOLCHAIN_PATHS,
    ),
    "ontology": (
        "crates/eg-core/ontology/",
        "src/server/graph_schema/",
        "src/server/graph_schema.rs",
        *_TOOLCHAIN_PATHS,
    ),
    "tts": (
        "crates/eg-tts-piper/",
        "crates/eg-audio/",
        "scripts/fetch_onnxruntime.sh",
        *_TOOLCHAIN_PATHS,
    ),
    "packaging": (
        "crates/eg-numeric/",
        "crates/eg-pyengine/",
        "packaging/",
        "pyproject.toml",
        "build.rs",
        "build_backend.py",
        "Cargo.toml",
        "scripts/inject_numeric_kernel.py",
        "scripts/inject_ontology.py",
        "scripts/strip_wheel_executables.py",
        "scripts/normalize_wheel_sbom.py",
        "scripts/normalize_wheel_build_paths.py",
        "scripts/check_wheel_privacy.py",
        "scripts/check_wheel_completeness.py",
        "scripts/configure_rust_path_remap.py",
        *_TOOLCHAIN_PATHS,
    ),
}


def _matches(path: str, prefixes: tuple[str, ...]) -> bool:
    return any(
        path.startswith(prefix) if prefix.endswith("/") else path == prefix
        for prefix in prefixes
    )


def gates_for_paths(paths: list[str]) -> dict[str, bool]:
    """Select path-gated jobs for a known pull-request file list."""
    if any(_matches(path, _ALL_GATES_PATHS) for path in paths):
        return dict.fromkeys(PATH_GATES, True)
    return {
        gate: any(_matches(path, prefixes) for path in paths)
        for gate, prefixes in PATH_GATES.items()
    }


def path_gates(event: str, base: str, head: str) -> dict[str, bool]:
    """Unknown refs, unavailable history and non-PR events run every gate."""
    if event != "pull_request" or not all(
        re.fullmatch(r"[0-9a-f]{40}", ref) for ref in (base, head)
    ):
        return dict.fromkeys(PATH_GATES, True)
    try:
        result = subprocess.run(
            ["git", "diff", "--name-only", "--no-renames", "-z", f"{base}...{head}"],
            check=True,
            capture_output=True,
        )
        paths = [p for p in result.stdout.decode("utf-8").split("\0") if p]
    except (OSError, subprocess.CalledProcessError, UnicodeDecodeError):
        return dict.fromkeys(PATH_GATES, True)
    if not paths:
        return dict.fromkeys(PATH_GATES, True)
    return gates_for_paths(paths)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event", required=True)
    parser.add_argument("--base", default="")
    parser.add_argument("--head", default="")
    parser.add_argument(
        "--path-gates",
        action="store_true",
        help="also print one <gate>=true|false line per pull-request path gate",
    )
    args = parser.parse_args()
    code = requires_code(args.event, args.base, args.head)
    print(f"code={str(code).lower()}")
    if args.path_gates:
        gates = (
            path_gates(args.event, args.base, args.head)
            if code
            else dict.fromkeys(PATH_GATES, False)
        )
        for gate, selected in gates.items():
            print(f"{gate}={str(selected).lower()}")


if __name__ == "__main__":
    main()
