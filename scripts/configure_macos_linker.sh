#!/usr/bin/env bash
# Source on a native Mac after the pinned Rust toolchain and path remaps exist.
eg_configure_macos_linker() {
    local expected linker version
    case "${1:-}" in
        aarch64-apple-darwin) expected=arm64 ;;
        x86_64-apple-darwin) expected=x86_64 ;;
        *) return 0 ;;
    esac
    [ "$(uname -s)" = Darwin ] && [ "$(uname -m)" = "$expected" ] || return 1
    [ "$(rustc --version | cut -d ' ' -f 2)" = 1.96.0 ] || return 1
    [ -n "${CARGO_ENCODED_RUSTFLAGS:-}" ] || return 1
    linker="$(rustc --print sysroot)/lib/rustlib/$1/bin/gcc-ld/ld64.lld"
    version="$("$linker" --version)" || return
    case "$version" in 'LLD 22.1.2'*) ;; *) echo "Unexpected Mach-O linker: $version" >&2; return 1 ;; esac
    export SDKROOT
    SDKROOT="$(xcrun --sdk macosx --show-sdk-path)" || return
    export CARGO_ENCODED_RUSTFLAGS="${CARGO_ENCODED_RUSTFLAGS}"$'\x1f'"-Clink-arg=-fuse-ld=$linker"
    export EG_MACHO_LINKER="$linker"
    rustc --version --verbose
    xcodebuild -version
    xcrun --sdk macosx --show-sdk-version
    xcrun clang --version
    printf '%s\n' "$version"
}
eg_configure_macos_linker "${1:-}"
