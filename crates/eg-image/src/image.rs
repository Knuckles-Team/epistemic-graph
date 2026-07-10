//! The image modality's stored value model (CONCEPT:E4-follow-up — image/audio/video
//! evidence) — metadata + a content-addressed blob reference + an optional region
//! index, deliberately NOT a decoded pixel buffer (see the crate docs' Pi-contract
//! rationale).
//!
//! [`ImageData`] mirrors `eg-tensor`'s `Tensor` / `eg-geo`'s `Geometry` shape: a small,
//! serde-serializable value that persists as a typed property in the engine's redb
//! per-graph store. Unlike those two, an image's raw content lives OUTSIDE this value
//! (in the engine's content-addressed blob CAS) — `blob_ref` is that content address
//! (see [`crate::header::content_hash`]). `width`/`height` are read from the REAL file
//! header (`crate::header::read_png_dimensions` / `read_jpeg_dimensions`) — never
//! fabricated — and `regions` is an OPTIONAL index of named sub-image bounding boxes
//! supplied by an external extractor (an object detector, an OCR box finder, a
//! human annotation, …); this crate never invents one.

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

/// One named rectangular region inside an image, in pixel space — as supplied by an
/// external extractor. This crate never fabricates one; a region only exists here
/// because a caller passed it in.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ImageRegion {
    /// A caller-supplied label ("face", "license-plate", a detector's class name, …).
    /// Optional — a region need not be classified.
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
    #[serde(default = "default_format")]
    pub format: ImageFormat,
    /// Content address of the ORIGINAL image bytes (e.g. `crate::header::content_hash`
    /// output), resolvable through the engine's blob CAS. Opaque to this crate.
    pub blob_ref: String,
    /// Named bounding-box regions inside the image, in pixel space. May be empty (an
    /// image with no detected/known regions yet).
    #[serde(default)]
    pub regions: Vec<ImageRegion>,
}

fn default_format() -> ImageFormat {
    ImageFormat::Unknown
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

    /// Build an `ImageData` from REAL image bytes: parses the PNG/JPEG header for
    /// `width`/`height` (never fabricated), content-addresses the bytes for
    /// `blob_ref`, and attaches the given (externally supplied, possibly empty)
    /// region index verbatim. Returns `None` if the header is neither a recognized
    /// PNG nor JPEG — a caller that already knows the dimensions from elsewhere can
    /// still build one directly via [`ImageData::new`].
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
        let img = ImageData::new(10, 20, "hash1")
            .with_regions(vec![ImageRegion::labeled("face", 1.0, 2.0, 3.0, 4.0)]);
        let json = serde_json::to_string(&img).unwrap();
        let back: ImageData = serde_json::from_str(&json).unwrap();
        assert_eq!(img, back);
    }

    #[test]
    fn from_bytes_never_fabricates_dimensions_for_unrecognized_bytes() {
        assert_eq!(ImageData::from_bytes(b"not an image", Vec::new()), None);
    }
}
