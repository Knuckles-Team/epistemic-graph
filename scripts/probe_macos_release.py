#!/usr/bin/env python3
"""Bounded native linker/runtime and five-binary packaging measurements."""

from __future__ import annotations

import ctypes
import hashlib
import json
import os
import subprocess
import tempfile
import time
import zipfile
from pathlib import Path


def run(args: list[str], cwd: Path) -> float:
    started = time.monotonic()
    subprocess.run(args, cwd=cwd, check=True, timeout=180)
    return round(time.monotonic() - started, 3)


def linker_probe(root: Path) -> None:
    source = root / "runtime.rs"
    source.write_text(
        '#[no_mangle] pub extern "C" fn probe() -> i32 { '
        "let t = std::thread::spawn(|| 42); "
        'assert!(std::panic::catch_unwind(|| panic!("probe")).is_err()); '
        "t.join().unwrap() }\nfn main() { assert_eq!(probe(), 42); }\n"
    )
    flags = os.environ["CARGO_ENCODED_RUSTFLAGS"].split("\x1f")
    common = [
        "rustc",
        str(source),
        "-Copt-level=3",
        "-Clto=thin",
        "-Ccodegen-units=1",
        *flags,
    ]
    run([*common, "-o", str(root / "runtime")], root)
    run([str(root / "runtime")], root)
    library = root / "runtime.dylib"
    run([*common, "--crate-type=cdylib", "-o", str(library)], root)
    assert ctypes.CDLL(str(library)).probe() == 42
    run(["otool", "-L", str(library)], root)
    if os.uname().machine == "arm64":
        branch_probe(root)


def branch_probe(root: Path) -> None:
    # Separate 64MiB atoms allow the linker to insert thunks across >128MiB.
    # No dead stripping: all padding must survive for a real branch-range test.
    assembly = root / "branches.s"
    assembly.write_text(
        ".text\n.p2align 2\n.globl _main\n_main:\n"
        "stp x29, x30, [sp, #-16]!\nbl _far\n"
        "ldp x29, x30, [sp], #16\nret\n"
        ".globl _near\n_near:\nmov w0, #0\nret\n"
        ".globl _pad1\n_pad1:\n.space 67108864\n"
        ".globl _pad2\n_pad2:\n.space 67108864\n"
        ".globl _pad3\n_pad3:\n.space 33554432\n"
        ".globl _far\n_far:\nstp x29, x30, [sp, #-16]!\nbl _near\n"
        "ldp x29, x30, [sp], #16\nret\n.subsections_via_symbols\n"
    )
    binary = root / "branches"
    run(
        [
            "xcrun",
            "clang",
            f"-fuse-ld={os.environ['EG_MACHO_LINKER']}",
            str(assembly),
            "-Wl,-map," + str(root / "branches.map"),
            "-o",
            str(binary),
        ],
        root,
    )
    assert binary.stat().st_size > 160 * 1024 * 1024
    assert "thunk" in (root / "branches.map").read_text()
    run([str(binary)], root)


def packaging_fixture(root: Path) -> tuple[Path, Path]:
    project = root / "package"
    (project / "src/bin").mkdir(parents=True)
    (project / "Cargo.toml").write_text(
        '[package]\nname="eg-mac-probe"\nversion="0.1.0"\nedition="2021"\n'
        '[profile.release]\nopt-level=3\nlto="thin"\ncodegen-units=1\n'
        'panic="unwind"\nstrip=true\n'
    )
    (project / "pyproject.toml").write_text(
        '[build-system]\nrequires=["maturin==1.15.0"]\nbuild-backend="maturin"\n'
        '[project]\nname="eg-mac-probe"\nversion="0.1.0"\n'
        '[tool.maturin]\nbindings="bin"\nstrip=true\n'
    )
    (project / "src/lib.rs").write_text("pub fn value() -> u32 { 42 }\n")
    for number in range(5):
        (project / f"src/bin/tool{number}.rs").write_text(
            'fn main() { assert_eq!(eg_mac_probe::value(), 42); println!("42"); }\n'
        )
    wrapper = root / "rustc-wrapper.py"
    wrapper.write_text(
        "#!/usr/bin/env python3\nimport json,os,sys\n"
        'with open(os.environ["EG_PROBE_RUSTC_LOG"],"a") as log:\n'
        ' log.write(json.dumps(sys.argv[2:])+"\\n")\n'
        "os.execv(sys.argv[1], sys.argv[1:])\n"
    )
    wrapper.chmod(0o755)
    os.environ["RUSTC_WRAPPER"] = str(wrapper)
    log = root / "rustc.jsonl"
    os.environ["EG_PROBE_RUSTC_LOG"] = str(log)
    os.environ["CARGO_TARGET_DIR"] = str(root / "target")
    return project, log


def packaging_probe(root: Path) -> None:
    project, log = packaging_fixture(root)
    durations = {}
    for mode in ("cold", "warm", "batched"):
        if mode == "batched":
            os.environ["CARGO_TARGET_DIR"] = str(root / "batch-target")
            run(["cargo", "generate-lockfile", "--offline"], project)
            durations["prebuild"] = run(
                [
                    "/usr/bin/time",
                    "-l",
                    "cargo",
                    "build",
                    "--release",
                    "--locked",
                    "--bins",
                    "--jobs",
                    "2",
                    "--target",
                    os.environ["EG_WHEEL_TARGET"],
                ],
                project,
            )
        log.write_text("")
        durations[mode] = run(
            [
                "maturin",
                "build",
                "--release",
                "--jobs",
                "2",
                "--target",
                os.environ["EG_WHEEL_TARGET"],
                "--out",
                str(root / mode),
                *(["--strip=false"] if mode == "batched" else []),
            ],
            project,
        )
        calls = [json.loads(line) for line in log.read_text().splitlines()]
        print(
            json.dumps(
                {"mode": mode, "seconds": durations[mode], "rustc_calls": calls}
            ),
            flush=True,
        )
        if mode == "batched":
            assert not [call for call in calls if "--crate-name" in call], calls
        (wheel,) = (root / mode).glob("*.whl")
        print("packaged_wheel=" + wheel.name, flush=True)
        with zipfile.ZipFile(wheel) as archive:
            tools = [
                name for name in archive.namelist() if ".data/scripts/tool" in name
            ]
            assert len(tools) == 5, tools
            for name in tools:
                dest = root / mode / Path(name).name
                payload = archive.read(name)
                if mode == "batched":
                    original = (
                        root
                        / "batch-target"
                        / os.environ["EG_WHEEL_TARGET"]
                        / "release"
                        / Path(name).name
                    ).read_bytes()
                    assert payload == original
                    print(
                        json.dumps(
                            {
                                "binary": Path(name).name,
                                "sha256": hashlib.sha256(payload).hexdigest(),
                                "matches_stripped_cargo_output": True,
                            }
                        ),
                        flush=True,
                    )
                dest.write_bytes(payload)
                dest.chmod(0o755)
                run([str(dest)], root)
    print(json.dumps({"five_binary_packaging_seconds": durations}), flush=True)


def abi3_probe(root: Path) -> None:
    project = root / "abi3"
    (project / "src").mkdir(parents=True)
    (project / "Cargo.toml").write_text(
        '[package]\nname="eg-mac-abi3"\nversion="0.1.0"\nedition="2021"\n'
        '[lib]\nname="eg_mac_abi3"\ncrate-type=["cdylib"]\n'
        '[dependencies]\npyo3={version="=0.29.2",'
        'features=["extension-module","abi3-py39"]}\n'
        '[profile.release]\nopt-level=3\nlto="thin"\ncodegen-units=1\n'
        'panic="unwind"\nstrip=true\n'
    )
    (project / "src/lib.rs").write_text(
        "use pyo3::prelude::*;\n#[pymodule]\n"
        "fn eg_mac_abi3(m: &Bound<'_, PyModule>) -> PyResult<()> {"
        'm.add("answer", 42)?; Ok(())}\n'
    )
    run(["maturin", "build", "--release", "--jobs", "2", "--out", "dist"], project)
    (wheel,) = (project / "dist").glob("*.whl")
    assert "abi3" in wheel.name
    run(
        [
            "python",
            "-m",
            "pip",
            "install",
            "--no-deps",
            "--target",
            str(project / "site"),
            str(wheel),
        ],
        project,
    )
    run(
        [
            "python",
            "-c",
            "import sys;sys.path.insert(0,"
            + repr(str(project / "site"))
            + ");import eg_mac_abi3;assert eg_mac_abi3.answer==42",
        ],
        project,
    )
    print("abi3_import_passed=" + wheel.name, flush=True)


def main() -> None:
    assert os.uname().sysname == "Darwin"
    with tempfile.TemporaryDirectory(
        prefix="eg-macos-probe-", dir=os.environ["RUNNER_TEMP"]
    ) as path:
        root = Path(path)
        linker_probe(root)
        packaging_probe(root)
        abi3_probe(root)


if __name__ == "__main__":
    main()
