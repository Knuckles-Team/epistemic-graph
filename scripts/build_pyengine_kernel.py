#!/usr/bin/env python3
"""Build the eg-pyengine kernel into a mounted or editable working tree.

Use `--check` for a no-build presence check. The shared build path lives in
`scripts/_kernel_build.py`; the wheel path uses `inject_pyengine.py`.
"""

from __future__ import annotations

from _kernel_build import KernelSpec, main

if __name__ == "__main__":
    raise SystemExit(
        main(
            KernelSpec(
                module="engine",
                crate="eg-pyengine",
                injector="inject_pyengine",
                marker="__engine__",
                expected_stamp="eg-pyengine",
                script="build_pyengine_kernel.py",
            )
        )
    )
