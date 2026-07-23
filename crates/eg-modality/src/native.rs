//! Shared native secondary-index and predicate vocabulary for served modalities.
//!
//! Keys contain only irreversible references or bounded numeric coordinates.  The
//! runtime persists no query text, source location, display label, or decoded media.

use serde::{Deserialize, Serialize};

use crate::OpaqueRef;

pub const SPATIAL_GRID_WIDTH: u8 = 16;
pub const TEMPORAL_BUCKET_MS: u64 = 1_000;
pub const MAX_TEMPORAL_QUERY_BUCKETS: usize = 4_096;
pub const MAX_PERCEPTUAL_HASH_DISTANCE: u8 = 15;

/// One exact key in the rebuilt native secondary index.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum NativeIndexKey {
    Lexeme(OpaqueRef),
    SpatialCell { x: u8, y: u8 },
    TemporalBucket(u64),
    SignatureBand { band: u8, value: u16 },
}

/// Typed predicates accepted by the common served runtime.  Text has already been
/// converted to an authority-keyed opaque lexeme before this value is constructed.
#[derive(Clone, Debug, PartialEq)]
pub enum NativePredicate {
    DocumentLexeme {
        lexeme_ref: OpaqueRef,
        page: Option<u32>,
    },
    ImageRegion {
        x: f64,
        y: f64,
        width: f64,
        height: f64,
    },
    ImagePerceptualHash {
        hash: u64,
        maximum_distance: u8,
    },
    AudioWindow {
        start_ms: u64,
        end_ms: u64,
        minimum_rms: f32,
    },
    VideoWindow {
        start_ms: u64,
        end_ms: u64,
        keyframes_only: bool,
    },
}

impl NativePredicate {
    /// Return the bounded set of keys whose posting lists can contain a match.
    pub fn candidate_keys(&self) -> Result<Vec<NativeIndexKey>, NativePredicateError> {
        match self {
            Self::DocumentLexeme { lexeme_ref, page } => {
                if page == &Some(0) {
                    return Err(NativePredicateError::Invalid);
                }
                Ok(vec![NativeIndexKey::Lexeme(lexeme_ref.clone())])
            }
            Self::ImageRegion {
                x,
                y,
                width,
                height,
            } => spatial_cells(*x, *y, *width, *height),
            Self::ImagePerceptualHash {
                hash,
                maximum_distance,
            } => {
                if *maximum_distance > MAX_PERCEPTUAL_HASH_DISTANCE {
                    return Err(NativePredicateError::TooBroad);
                }
                Ok(signature_candidate_bands(*hash, *maximum_distance))
            }
            Self::AudioWindow {
                start_ms,
                end_ms,
                minimum_rms,
            } => {
                if !minimum_rms.is_finite() || !(0.0..=1.0).contains(minimum_rms) {
                    return Err(NativePredicateError::Invalid);
                }
                temporal_buckets(*start_ms, *end_ms)
            }
            Self::VideoWindow {
                start_ms, end_ms, ..
            } => temporal_buckets(*start_ms, *end_ms),
        }
    }
}

pub fn spatial_cells(
    x: f64,
    y: f64,
    width: f64,
    height: f64,
) -> Result<Vec<NativeIndexKey>, NativePredicateError> {
    if ![x, y, width, height].iter().all(|value| value.is_finite())
        || x < 0.0
        || y < 0.0
        || width <= 0.0
        || height <= 0.0
        || x + width > 1.0
        || y + height > 1.0
    {
        return Err(NativePredicateError::Invalid);
    }
    let scale = f64::from(SPATIAL_GRID_WIDTH);
    let start_x = (x * scale).floor().min(scale - 1.0) as u8;
    let start_y = (y * scale).floor().min(scale - 1.0) as u8;
    let end_x = (((x + width) * scale).ceil() - 1.0).clamp(0.0, scale - 1.0) as u8;
    let end_y = (((y + height) * scale).ceil() - 1.0).clamp(0.0, scale - 1.0) as u8;
    let mut keys =
        Vec::with_capacity(usize::from(end_x - start_x + 1) * usize::from(end_y - start_y + 1));
    for cell_y in start_y..=end_y {
        for cell_x in start_x..=end_x {
            keys.push(NativeIndexKey::SpatialCell {
                x: cell_x,
                y: cell_y,
            });
        }
    }
    Ok(keys)
}

pub fn temporal_buckets(
    start_ms: u64,
    end_ms: u64,
) -> Result<Vec<NativeIndexKey>, NativePredicateError> {
    if end_ms <= start_ms {
        return Err(NativePredicateError::Invalid);
    }
    let first = start_ms / TEMPORAL_BUCKET_MS;
    let last = (end_ms - 1) / TEMPORAL_BUCKET_MS;
    let count = last
        .checked_sub(first)
        .and_then(|value| value.checked_add(1))
        .and_then(|value| usize::try_from(value).ok())
        .ok_or(NativePredicateError::TooBroad)?;
    if count > MAX_TEMPORAL_QUERY_BUCKETS {
        return Err(NativePredicateError::TooBroad);
    }
    Ok((first..=last).map(NativeIndexKey::TemporalBucket).collect())
}

pub fn signature_bands(hash: u64) -> Vec<NativeIndexKey> {
    (0..4)
        .map(|band| NativeIndexKey::SignatureBand {
            band,
            value: ((hash >> (u32::from(band) * 16)) & 0xffff) as u16,
        })
        .collect()
}

/// Multi-probe the exact 16-bit postings without losing any hash within the
/// requested 64-bit Hamming radius. By the pigeonhole principle, a hash at total
/// distance `d` has at least one of four bands within `floor(d / 4)` bits. The
/// production maximum therefore expands to at most 2,788 exact posting keys.
fn signature_candidate_bands(hash: u64, maximum_distance: u8) -> Vec<NativeIndexKey> {
    let radius = maximum_distance / 4;
    let mut keys = Vec::new();
    for band in 0..4u8 {
        let value = ((hash >> (u32::from(band) * 16)) & 0xffff) as u16;
        keys.push(NativeIndexKey::SignatureBand { band, value });
        if radius >= 1 {
            for first in 0..16 {
                keys.push(NativeIndexKey::SignatureBand {
                    band,
                    value: value ^ (1u16 << first),
                });
            }
        }
        if radius >= 2 {
            for first in 0..16 {
                for second in first + 1..16 {
                    keys.push(NativeIndexKey::SignatureBand {
                        band,
                        value: value ^ (1u16 << first) ^ (1u16 << second),
                    });
                }
            }
        }
        if radius >= 3 {
            for first in 0..16 {
                for second in first + 1..16 {
                    for third in second + 1..16 {
                        keys.push(NativeIndexKey::SignatureBand {
                            band,
                            value: value ^ (1u16 << first) ^ (1u16 << second) ^ (1u16 << third),
                        });
                    }
                }
            }
        }
    }
    keys
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativePredicateError {
    Invalid,
    TooBroad,
}

/// Work evidence returned with a native query. Tests assert that selective queries
/// examine posting-list candidates rather than the complete served population.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeQueryStats {
    pub index_lookups: usize,
    pub candidates: usize,
    pub examined: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn perceptual_multi_probe_has_no_false_negative_at_supported_radius() {
        let query = 0u64;
        let match_at_distance_fifteen = 0x0007_000f_000f_000f;
        assert_eq!((query ^ match_at_distance_fifteen).count_ones(), 15);
        let candidates: std::collections::BTreeSet<_> =
            signature_candidate_bands(query, MAX_PERCEPTUAL_HASH_DISTANCE)
                .into_iter()
                .collect();
        assert_eq!(candidates.len(), 2_788);
        assert!(signature_bands(match_at_distance_fifteen)
            .into_iter()
            .any(|key| candidates.contains(&key)));
    }
}
