#!/usr/bin/env bash
# Source inside each maturin Linux container, after its Rust toolchain setup.
# Keep GCC's startup objects and glibc selection; replace only the ELF linker.
eg_configure_arm64_linker() {
    [ "${1:-}" = aarch64-unknown-linux-gnu ] || return 0
    [ "$(uname -m)" = aarch64 ] || { echo 'ARM64 linker requires a native ARM64 container' >&2; return 1; }
    [ "$(rustc --version | cut -d ' ' -f 2)" = 1.96.0 ] || { echo 'Expected release Rust 1.96.0' >&2; return 1; }
    [ -n "${CARGO_ENCODED_RUSTFLAGS:-}" ] || { echo 'Missing encoded release path remaps' >&2; return 1; }
    local linker_dir version driver
    linker_dir="$(rustc --print sysroot)/lib/rustlib/aarch64-unknown-linux-gnu/bin/gcc-ld"
    [ -x "$linker_dir/ld.lld" ] || { echo 'Missing bundled LLD driver shim' >&2; return 1; }
    version="$("$linker_dir/ld.lld" --version)" || return
    case "$version" in 'LLD 22.1.2'*) ;; *) echo "Unexpected bundled linker: $version" >&2; return 1 ;; esac
    # This is the same cc driver rustc uses on this native target. --version
    # reaches the selected linker without requiring an input object.
    driver="$(cc -B"$linker_dir" -fuse-ld=lld -Wl,--version 2>&1)" || return
    case "$driver" in *"$version"*) ;; *) echo 'GCC did not select the bundled LLD' >&2; return 1 ;; esac
    rustc --version --verbose
    cc --version
    ld --version
    printf '%s\n' "$version" "$driver"
    export CARGO_ENCODED_RUSTFLAGS="${CARGO_ENCODED_RUSTFLAGS}"$'\x1f'"-Clink-arg=-B$linker_dir"$'\x1f''-Clink-arg=-fuse-ld=lld'
}
eg_configure_arm64_linker "${1:-}"
