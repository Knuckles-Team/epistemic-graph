fn main() {
    // No native BLAS linkage: nalgebra (features = ["serde"]) is pure-Rust and
    // needs no system openblas. The previous `cargo:rustc-link-lib=dylib=openblas`
    // forced a libopenblas link that broke the manylinux/macOS/Windows wheel
    // builds ("unable to find library -lopenblas"). Nothing in the engine
    // requires an external BLAS, so the build is now self-contained.
}
