//! # eg-image — native governed image serving
//!
//! The current pure-Rust runtime validates PNG chunks and CRCs, performs bounded
//! zlib inflate and PNG row reconstruction, converts supported 8-bit color layouts
//! to request-local RGBA pixels, and derives a 64-bit difference hash. Durable
//! [`ImageData`] contains SHA-256 content identity, decoded format facts, perceptual
//! hash, and governed regions; decoded pixels never enter a snapshot. The lightweight
//! header API can also inspect JPEG dimensions, while the served codec is PNG.
//!
//! ## `ModalityContract`
//!
//! Behind the crate's `contract` feature, `ImageData::evidence_address()` returns a real
//! `eg_modality::EvidenceAddress::ImageRegion` from the first stored region (or `None`
//! when the image carries no region index yet — never fabricated). See
//! `src/contract.rs`.
//!
//! ## `runtime` (CONCEPT:EG-P1-3, OPT-IN, default OFF)
//!
//! `src/runtime.rs`, behind `runtime`, provides the decoder, perceptual feature
//! extraction, spatial-grid postings, and exact pHash/region predicates. Governed
//! persistence is exposed through [`ImageServingRuntime`].

mod header;
mod image;

// Governed contract implementation.
#[cfg(feature = "contract")]
mod contract;

// Pure-Rust native runtime.
#[cfg(feature = "runtime")]
pub mod runtime;

pub use header::{content_hash, read_jpeg_dimensions, read_png_dimensions};
pub use image::{ImageColorSpace, ImageData, ImageFormat, ImageRegion};

#[cfg(feature = "serving")]
pub type ImageServingRuntime = eg_modality::ServedModalityRuntime<ImageData>;

// The leaf default stays value-only; the main facade enables the native runtime.
#[cfg(all(test, not(feature = "runtime")))]
mod default_build_guardrail {
    #[test]
    fn default_build_has_no_runtime() {
        assert!(
            !cfg!(feature = "runtime"),
            "the `runtime` feature must stay opt-in, never part of `default`"
        );
    }
}
