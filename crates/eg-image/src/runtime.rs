//! OPT-IN image runtime seam (CONCEPT:EG-P1-3), behind this crate's own `runtime`
//! feature (default OFF, no new dependency).
//!
//! `eg-image`'s default build is deliberately metadata/region-level (see the
//! crate's module docs) — no pixel decode, ever, in the default build. Real
//! image-runtime work (full pixel decode, color-space conversion, EXIF parsing,
//! multi-resolution pyramids, thumbnail generation, segmentation masks, an
//! image-embedding model) needs exactly the heavy C/native dependencies
//! (`image`/`libjpeg-turbo`/`exif-rs`/an ONNX/torch runtime, …) this workspace's Pi
//! contract keeps out of the default build. So this module, gated behind
//! `runtime`, defines ONLY the typed artifacts + the plugin TRAITS a real decoder/
//! embedding model would implement — it adds no dependency at all, even with
//! `runtime` on, because it contains no implementation to depend on anything with.
//!
//! * [`DecodedImage`] — the typed decode artifact: [`ColorInfo`], optional
//!   [`ExifData`], a resolution [`PyramidLevel`] list, [`Thumbnail`]s, and
//!   segmentation [`MaskArtifact`]s.
//! * [`ImageDecoderPlugin`] — decode raw bytes (+ the existing [`crate::ImageData`]
//!   metadata) into a [`DecodedImage`]. A real plugin lives in its own crate/
//!   service; this module ships only [`NoopImageDecoder`], a trivial placeholder
//!   that returns an EMPTY `DecodedImage` (never fabricated color/EXIF/pyramid
//!   data) — enough to prove the trait seam end-to-end.
//! * [`ImageEmbeddingPlugin`] — the image-embedding hook. [`NoopImageEmbedder`] is
//!   the same kind of trivial, always-`None` placeholder.
//!
//! Real decode/embedding pipelines (a `image`-crate-backed decoder, EXIF
//! extraction, a CLIP/SigLIP-style embedding model, …) are explicitly OUT of scope
//! — documented follow-ups, implemented as external plugins.

use crate::ImageData;

/// Color-space + bit-depth info a real pixel decode would report. Placeholder
/// fields only — never fabricated by this crate (see [`NoopImageDecoder`]).
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ColorInfo {
    pub color_space: String,
    pub bit_depth: u8,
}

/// EXIF metadata a real decoder would extract from the source bytes.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct ExifData {
    pub camera_make: Option<String>,
    pub camera_model: Option<String>,
    pub captured_at: Option<String>,
    pub gps: Option<(f64, f64)>,
}

/// One level of a multi-resolution image pyramid, each level's downsampled bytes
/// stored as their own blob.
#[derive(Clone, Debug, PartialEq)]
pub struct PyramidLevel {
    pub level: u32,
    pub width: u32,
    pub height: u32,
    pub blob_ref: String,
}

/// A generated thumbnail, stored as its own blob.
#[derive(Clone, Debug, PartialEq)]
pub struct Thumbnail {
    pub width: u32,
    pub height: u32,
    pub blob_ref: String,
}

/// A segmentation mask (e.g. from an object/semantic segmentation model), stored
/// as its own blob.
#[derive(Clone, Debug, PartialEq)]
pub struct MaskArtifact {
    pub label: String,
    pub blob_ref: String,
}

/// The typed decode artifact a real [`ImageDecoderPlugin`] would produce.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct DecodedImage {
    pub color: ColorInfo,
    pub exif: Option<ExifData>,
    pub pyramid: Vec<PyramidLevel>,
    pub thumbnails: Vec<Thumbnail>,
    pub masks: Vec<MaskArtifact>,
}

/// The plugin seam: decode `bytes` (the image's original bytes, resolvable via
/// `image.blob_ref`) into a [`DecodedImage`]. `None` means "cannot decode these
/// bytes". Implement this trait in a separate crate/service backed by a real
/// codec — this crate ships no such implementation.
pub trait ImageDecoderPlugin {
    fn decode(&self, image: &ImageData, bytes: &[u8]) -> Option<DecodedImage>;
}

/// The image-embedding hook: produce a dense vector embedding for an image. A
/// real implementation would call out to a vision-embedding model (CLIP/SigLIP/…);
/// this crate defines only the seam.
pub trait ImageEmbeddingPlugin {
    fn embed(&self, image: &ImageData) -> Option<Vec<f32>>;
}

/// A trivial, dependency-free [`ImageDecoderPlugin`] placeholder: always returns
/// an EMPTY `DecodedImage` (default color info, no EXIF/pyramid/thumbnails/masks)
/// for any recognized (non-empty) byte slice — proving the trait wiring compiles
/// and runs end-to-end without claiming any real decode occurred. A real decoder
/// is a documented, out-of-scope follow-up.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopImageDecoder;

impl ImageDecoderPlugin for NoopImageDecoder {
    fn decode(&self, _image: &ImageData, bytes: &[u8]) -> Option<DecodedImage> {
        if bytes.is_empty() {
            return None;
        }
        Some(DecodedImage::default())
    }
}

/// A trivial, dependency-free [`ImageEmbeddingPlugin`] placeholder: always
/// returns `None` — no embedding model is bundled. A real embedder is a
/// documented, out-of-scope follow-up.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopImageEmbedder;

impl ImageEmbeddingPlugin for NoopImageEmbedder {
    fn embed(&self, _image: &ImageData) -> Option<Vec<f32>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_decoder_returns_an_empty_decoded_image_for_nonempty_bytes() {
        let image = ImageData::new(10, 10, "blob-1");
        let decoded = NoopImageDecoder.decode(&image, b"some bytes").unwrap();
        assert_eq!(decoded, DecodedImage::default());
    }

    #[test]
    fn noop_decoder_returns_none_for_empty_bytes() {
        let image = ImageData::new(10, 10, "blob-1");
        assert_eq!(NoopImageDecoder.decode(&image, &[]), None);
    }

    #[test]
    fn noop_embedder_never_fabricates_a_vector() {
        let image = ImageData::new(10, 10, "blob-1");
        assert_eq!(NoopImageEmbedder.embed(&image), None);
    }
}
