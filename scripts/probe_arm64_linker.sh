#!/usr/bin/env bash
# Small native-container probes, not a substitute for the complete wheel gate.
set -euo pipefail
source scripts/configure_arm64_linker.sh aarch64-unknown-linux-gnu
probe_dir="$(mktemp -d /tmp/eg-arm64-link-probe.XXXXXX)"
trap 'rm -rf "$probe_dir"' EXIT
IFS=$'\x1f' read -r -a flags <<< "$CARGO_ENCODED_RUSTFLAGS"
cat > "$probe_dir/runtime.rs" <<'RS'
use std::sync::atomic::{AtomicU64, Ordering};
static VALUE: AtomicU64 = AtomicU64::new(40);
#[no_mangle]
pub extern "C" fn probe_answer() -> u64 {
    VALUE.fetch_add(2, Ordering::SeqCst) + 2
}
fn main() {
    assert_eq!(std::thread::spawn(|| probe_answer()).join().unwrap(), 42);
    assert!(std::panic::catch_unwind(|| panic!("expected probe unwind")).is_err());
    println!("ARM64 PIE atomic/thread/unwind probe passed");
}
RS
rustc --edition 2021 -Copt-level=3 -Clto=thin -Ccodegen-units=1 -Cpanic=unwind "${flags[@]}" "$probe_dir/runtime.rs" -o "$probe_dir/runtime"
"$probe_dir/runtime"
rustc --edition 2021 --crate-type cdylib -Copt-level=3 -Clto=thin -Ccodegen-units=1 -Cpanic=unwind "${flags[@]}" "$probe_dir/runtime.rs" -o "$probe_dir/libprobe.so"
/opt/python/cp313-cp313/bin/python - "$probe_dir/libprobe.so" <<'PYTHON'
import ctypes
import sys
library = ctypes.CDLL(sys.argv[1])
library.probe_answer.restype = ctypes.c_uint64
assert library.probe_answer() == 42
print('ARM64 cdylib load and atomic probe passed')
PYTHON
# Two input objects, separate output sections >128 MiB apart. Both calls
# need PIC range-extension thunks; running the PIE also checks relocation.
cat > "$probe_dir/start.s" <<'ASM'
.section .text.start,"ax",@progbits
.global _start
_start:
    bl far_target
    bl other_target
    mov x0, #0
    mov x8, #93
    svc #0
ASM
cat > "$probe_dir/far.s" <<'ASM'
.section .text.far,"ax",@progbits
.global far_target
far_target:
    ret
.section .text.other,"ax",@progbits
.global other_target
other_target:
    ret
ASM
cat > "$probe_dir/layout.ld" <<'LD'
ENTRY(_start)
SECTIONS {
  . = 0x10000;
  .text.start ALIGN(0x10000) : { *(.text.start) }
  . = 0x10010000;
  .text.far : { *(.text.far) }
  . = 0x20010000;
  .text.other : { *(.text.other) }
  .dynamic ALIGN(0x10000) : { *(.dynamic) }
}
LD
cc -c "$probe_dir/start.s" -o "$probe_dir/start.o"
cc -c "$probe_dir/far.s" -o "$probe_dir/far.o"
linker="$(rustc --print sysroot)/lib/rustlib/aarch64-unknown-linux-gnu/bin/gcc-ld/ld.lld"
"$linker" -pie --no-dynamic-linker -T "$probe_dir/layout.ld" "$probe_dir/start.o" "$probe_dir/far.o" -o "$probe_dir/far-call"
# Keep writable dynamic metadata off executable pages, including 64 KiB ARM
# pages; otherwise a valid thunk can jump into a page remapped without execute.
readelf -l "$probe_dir/far-call"
readelf -h "$probe_dir/far-call"
readelf -sW "$probe_dir/far-call" | grep __AArch64ADRPThunk_
/opt/python/cp313-cp313/bin/python - "$probe_dir/far-call" <<'PYTHON'
import struct
import sys
with open(sys.argv[1], 'rb') as elf:
    header = elf.read(64)
    offset = struct.unpack_from('<Q', header, 32)[0]
    size, count = struct.unpack_from('<HH', header, 54)
    loads = []
    for index in range(count):
        elf.seek(offset + index * size)
        kind, flags, _, address, _, _, memory, _ = struct.unpack('<IIQQQQQQ', elf.read(56))
        if kind == 1:
            loads.append((address // 65536, (address + memory + 65535) // 65536, flags))
    for left, right in zip(loads, loads[1:]):
        assert left[1] <= right[0], ('overlapping 64 KiB LOAD pages', left, right)
print('Sparse PIE segment permissions occupy separate pages')
PYTHON
"$probe_dir/far-call"
# Also exercise real large input sections, not just linker-script address gaps.
# Based on the section placement exercised by LLVM 22.1.2's
# lld/test/ELF/aarch64-thunk-section-location.s (Apache-2.0 WITH LLVM-exception).
for index in 1 2 3 4 5; do
    printf '.section .text.padding%s,"ax",@progbits\n.space 0x2000000\n' "$index"
done > "$probe_dir/padding.s"
cc -c "$probe_dir/padding.s" -o "$probe_dir/padding.o"
"$linker" -pie --no-dynamic-linker "$probe_dir/start.o" "$probe_dir/padding.o" "$probe_dir/far.o" -o "$probe_dir/large-input"
readelf -sW "$probe_dir/large-input" | grep __AArch64ADRPThunk_
"$probe_dir/large-input"
readelf -l "$probe_dir/runtime"
readelf --version-info "$probe_dir/libprobe.so"
/opt/python/cp313-cp313/bin/python - "$probe_dir/runtime" "$probe_dir/libprobe.so" <<'PYTHON'
import re
import subprocess
import sys
for path in sys.argv[1:]:
    header = subprocess.check_output(['readelf', '-h', path], text=True)
    assert 'AArch64' in header and 'DYN' in header, header
    versions = subprocess.check_output(['readelf', '--version-info', path], text=True)
    glibc = [tuple(map(int, value.split('.'))) for value in re.findall(r'GLIBC_(\d+\.\d+)', versions)]
    assert glibc and max(glibc) <= (2, 28), glibc
interpreter = subprocess.check_output(['readelf', '-l', sys.argv[1]], text=True)
assert '/lib/ld-linux-aarch64.so.1' in interpreter, interpreter
print('ELF architecture, PIE/shared type, interpreter and glibc baseline passed')
PYTHON
printf 'Native ARM64 bundled-LLD probes passed\n'
# The same pinned maturin action now builds a tiny binary wheel with the
# exported flags. No repository crate or release artifact is built here.
mkdir -p .ci-arm64-link-probe/src
cp "$probe_dir/runtime.rs" .ci-arm64-link-probe/src/main.rs
cat > .ci-arm64-link-probe/Cargo.toml <<'TOML'
[package]
name = "eg-arm64-link-probe"
version = "0.0.0"
edition = "2021"
[workspace]
[profile.release]
opt-level = 3
lto = "thin"
codegen-units = 1
panic = "unwind"
TOML
cat > .ci-arm64-link-probe/pyproject.toml <<'TOML'
[build-system]
requires = ["maturin==1.15.0"]
build-backend = "maturin"
[project]
name = "eg-arm64-link-probe"
version = "0.0.0"
[tool.maturin]
bindings = "bin"
TOML
# The action's build follows this sourced hook in the same shell. Isolate
# both Cargo and Python metadata from the real release project.
cd .ci-arm64-link-probe
