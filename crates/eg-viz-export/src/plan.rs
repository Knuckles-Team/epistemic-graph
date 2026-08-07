//! `RenderPlan` — the screen-space draw list every V3a backend (PNG/SVG/PDF)
//! rasterizes/serializes from. One resolution pass ([`crate::render::resolve`])
//! builds this once; every output format reads the SAME plan, so a PNG and an
//! SVG of the same [`eg_viz_core::ViewResult`] are pixel-for-pixel the same
//! picture by construction, never three independently-drifting renderers.
//!
//! ★ The property that makes this the tier-honoring boundary: a [`RenderPlan`]'s
//! `ops.len()` is bounded by the tier that produced it (`width_px` for a
//! [`eg_viz_core::LodTier::Decimate`] plan, a fixed grid cell count for
//! [`eg_viz_core::LodTier::Density`]) — never by the source row count. See
//! [`crate::reduce`] for the functions that build each tier's ops.

/// One screen-space draw primitive. Colors are plain `[u8; 4]` RGBA — no theme
/// resolution happens below this layer.
#[derive(Clone, Debug, PartialEq)]
pub enum DrawOp {
    /// A filled circular marker (Scatter, Direct tier).
    Point {
        x: f32,
        y: f32,
        radius: f32,
        color: [u8; 4],
    },
    /// A straight stroke segment (Line/Area, Direct tier; also the per-pixel-
    /// column vertical range a Decimate-tier reduction emits).
    Segment {
        x0: f32,
        y0: f32,
        x1: f32,
        y1: f32,
        color: [u8; 4],
    },
    /// A filled, optionally alpha-blended rectangle (Bar, Direct tier; a
    /// Density-tier grid cell, alpha carrying its normalized count).
    Rect {
        x: f32,
        y: f32,
        w: f32,
        h: f32,
        color: [u8; 4],
    },
}

/// The bounded, resolved draw list for one render. `ops.len()` is the property
/// under test in the large-N proof (`crates/eg-viz-export/examples/*`).
#[derive(Clone, Debug, PartialEq)]
pub struct RenderPlan {
    pub width_px: u32,
    pub height_px: u32,
    pub background: [u8; 4],
    pub ops: Vec<DrawOp>,
}

impl RenderPlan {
    pub fn new(width_px: u32, height_px: u32, background: [u8; 4]) -> Self {
        Self {
            width_px,
            height_px,
            background,
            ops: Vec::new(),
        }
    }
}

/// A linear `[min, max] -> [0, extent]` mapping, y-flipped when `flip` is set
/// (screen-space y grows downward; data-space y conventionally grows upward).
/// The only scale kind V3a resolves in this lane — [`eg_viz_core::ScaleKind`]
/// variants beyond `Linear` fall back to this (documented gap; see
/// `crate::render` module doc).
#[derive(Clone, Copy, Debug)]
pub struct LinearMap {
    pub domain_min: f64,
    pub domain_max: f64,
    pub extent: f32,
    pub flip: bool,
}

impl LinearMap {
    pub fn new(domain_min: f64, domain_max: f64, extent: f32, flip: bool) -> Self {
        // A degenerate (zero-width) domain still maps everything to the
        // midpoint rather than dividing by zero.
        let (domain_min, domain_max) = if (domain_max - domain_min).abs() < f64::EPSILON {
            (domain_min - 0.5, domain_max + 0.5)
        } else {
            (domain_min, domain_max)
        };
        Self {
            domain_min,
            domain_max,
            extent,
            flip,
        }
    }

    pub fn map(&self, value: f64) -> f32 {
        let t = ((value - self.domain_min) / (self.domain_max - self.domain_min)).clamp(0.0, 1.0);
        let px = t as f32 * self.extent;
        if self.flip {
            self.extent - px
        } else {
            px
        }
    }
}
