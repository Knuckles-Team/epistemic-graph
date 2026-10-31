#!/usr/bin/env python3
"""Build the medulla-numeric kernel into a mounted or editable working tree.

Use `--check` for a no-build presence check. The shared build path lives in
`scripts/_kernel_build.py`; the wheel path uses `inject_numeric_kernel.py`.
"""

from __future__ import annotations

from _kernel_build import KernelSpec, main

if __name__ == "__main__":
    raise SystemExit(
        main(
            KernelSpec(
                module="numeric",
                crate="medulla-numeric",
                injector="inject_numeric_kernel",
                marker="__kernel__",
                expected_stamp="medulla-numeric",
                script="build_numeric_kernel.py",
            )
        )
    )
