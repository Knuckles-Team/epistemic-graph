"""Shared metadata and staging operations for the release wheel jobs."""

import argparse
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import tomllib


def rust_pin(path: Path) -> str:
    channel = tomllib.loads(path.read_text())["toolchain"]["channel"]
    if not isinstance(channel, str) or not re.fullmatch(
        r"[0-9]+[.][0-9]+[.][0-9]+", channel
    ):
        raise ValueError("release requires an exact Rust version")
    return channel


def stage(source: Path, destination: Path) -> None:
    candidates = sorted(source.glob("epistemic_graph-*.whl"))
    if (
        len(candidates) != 1
        or candidates[0].is_symlink()
        or not candidates[0].is_file()
    ):
        raise ValueError("expected one regular primary release wheel")
    destination.mkdir()
    shutil.copy2(candidates[0], destination / candidates[0].name)


def verify_and_stage(reproduction: bool) -> None:
    if reproduction:
        subprocess.run(
            [
                sys.executable,
                "scripts/compare_wheel_reproducibility.py",
                "dist-primary",
                "dist-reproduction",
            ],
            check=True,
        )
    stage(Path("dist-primary"), Path("dist"))


def source_time() -> str:
    timestamp = subprocess.check_output(
        ["git", "log", "-1", "--format=%ct"], text=True
    ).strip()
    if not timestamp.isascii() or not timestamp.isdigit():
        raise ValueError("invalid source timestamp")
    return timestamp


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("rust-pin", "source-time", "stage"))
    parser.add_argument("--reproduction", choices=("true", "false"), default="false")
    args = parser.parse_args()
    if args.operation == "stage":
        verify_and_stage(args.reproduction == "true")
        return
    if args.operation == "rust-pin":
        output, key, value = (
            "GITHUB_OUTPUT",
            "channel",
            rust_pin(Path("rust-toolchain.toml")),
        )
    else:
        output, key, value = "GITHUB_ENV", "SOURCE_DATE_EPOCH", source_time()
    with Path(os.environ[output]).open("a", encoding="utf-8") as stream:
        stream.write(f"{key}={value}\n")


if __name__ == "__main__":
    main()
