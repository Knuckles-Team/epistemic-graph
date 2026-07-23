//! Source-free decoded image facts, perceptual identity, and governed region ranges.
//! The SHA-256 `blob_ref` binds them to request-local source pixels.

use serde::{Deserialize, Serialize};

/// Which header this crate recognized when building an [`ImageData`] from raw bytes
/// (`ImageData::from_bytes`) — purely informational (capability discovery / logging),
/// never gates any behavior.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ImageFormat {
    Png,
    Jpeg,
    /// Built directly (`ImageData::new`) rather than parsed from real bytes — the
    /// caller supplied `width`/`height` from elsewhere (e.g. it already had them from
    /// an upstream extractor).
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ImageColorSpace {
    Gray,
    GrayAlpha,
    Rgb,
    Rgba,
    Indexed,
    Unknown,
}

/// One named rectangular region inside an image, in pixel space — as supplied by an
/// external extractor. This crate never fabricates one; a region only exists here
/// because a caller passed it in.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ImageRegion {
    /// Optional opaque label reference. Governed serving rejects display text.
    #[serde(default)]
    pub label: Option<String>,
    pub x: f64,
    pub y: f64,
    pub width: f64,
    pub height: f64,
}

impl ImageRegion {
    pub fn new(x: f64, y: f64, width: f64, height: f64) -> Self {
        Self {
            label: None,
            x,
            y,
            width,
            height,
        }
    }

    pub fn labeled(label: impl Into<String>, x: f64, y: f64, width: f64, height: f64) -> Self {
        Self {
            label: Some(label.into()),
            x,
            y,
            width,
            height,
        }
    }
}

/// The image modality's stored value: real dimensions + a content-addressed blob
/// reference + an optional, extractor-supplied region index. No pixel buffer — see
/// module docs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ImageData {
    pub width: u32,
    pub height: u32,
    pub format: ImageFormat,
    /// Content address of the ORIGINAL image bytes (e.g. `crate::header::content_hash`
    /// output), resolvable through the engine's blob CAS. Opaque to this crate.
    pub blob_ref: String,
    pub color_space: ImageColorSpace,
    pub bit_depth: u8,
    /// Native 9x8 luminance difference hash over decoded pixels.
    pub perceptual_hash: u64,
    /// Named bounding-box regions inside the image, in pixel space. May be empty (an
    /// image with no detected/known regions yet).
    #[serde(default)]
    pub regions: Vec<ImageRegion>,
}

impl ImageData {
    /// Construct directly from already-known dimensions (e.g. supplied by an upstream
    /// extractor/pipeline that already parsed or was told the size).
    pub fn new(width: u32, height: u32, blob_ref: impl Into<String>) -> Self {
        Self {
            width,
            height,
            format: ImageFormat::Unknown,
            blob_ref: blob_ref.into(),
            color_space: ImageColorSpace::Unknown,
            bit_depth: 0,
            perceptual_hash: 0,
            regions: Vec::new(),
        }
    }

    pub fn with_regions(mut self, regions: Vec<ImageRegion>) -> Self {
        self.regions = regions;
        self
    }

    pub fn with_format(mut self, format: ImageFormat) -> Self {
        self.format = format;
        self
    }

    pub fn with_native_features(
        mut self,
        color_space: ImageColorSpace,
        bit_depth: u8,
        perceptual_hash: u64,
    ) -> Self {
        self.color_space = color_space;
        self.bit_depth = bit_depth;
        self.perceptual_hash = perceptual_hash;
        self
    }

    /// Build a header-only `ImageData`: parses the PNG/JPEG header for
    /// `width`/`height` (never fabricated), content-addresses the bytes for
    /// `blob_ref`, and attaches the given (externally supplied, possibly empty)
    /// region index verbatim. Returns `None` if the header is neither a recognized
    /// PNG nor JPEG — a caller that already knows the dimensions from elsewhere can
    /// still build one directly via [`ImageData::new`]. Production serving uses
    /// `NativeImageRuntime` so decoded color facts and perceptual identity are
    /// mandatory.
    pub fn from_bytes(bytes: &[u8], regions: Vec<ImageRegion>) -> Option<Self> {
        let (format, width, height) =
            if let Some((w, h)) = crate::header::read_png_dimensions(bytes) {
                (ImageFormat::Png, w, h)
            } else if let Some((w, h)) = crate::header::read_jpeg_dimensions(bytes) {
                (ImageFormat::Jpeg, w, h)
            } else {
                return None;
            };
        Some(Self {
            width,
            height,
            format,
            blob_ref: crate::header::content_hash(bytes),
            color_space: ImageColorSpace::Unknown,
            bit_depth: 0,
            perceptual_hash: 0,
            regions,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_defaults_to_unknown_format_and_no_regions() {
        let img = ImageData::new(100, 200, "abc");
        assert_eq!(img.format, ImageFormat::Unknown);
        assert!(img.regions.is_empty());
    }

    #[test]
    fn serde_round_trips() {
        let img = ImageData::new(10, 20, "hash1").with_regions(vec![ImageRegion::labeled(
            "eg:label:0000000000000001",
            1.0,
            2.0,
            3.0,
            4.0,
        )]);
        let json = serde_json::to_string(&img).unwrap();
        let back: ImageData = serde_json::from_str(&json).unwrap();
        assert_eq!(img, back);
    }

    #[test]
    fn from_bytes_never_fabricates_dimensions_for_unrecognized_bytes() {
        assert_eq!(ImageData::from_bytes(b"not an image", Vec::new()), None);
    }
}
