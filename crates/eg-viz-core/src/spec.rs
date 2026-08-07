//! `ViewSpec` — the declarative chart IR (D-VZ-1 lane V0).
//!
//! A `ViewSpec` describes WHAT to draw, never HOW: marks (what geometry), encodings
//! (which columns drive which visual channel), scales (how a data value maps to a
//! screen coordinate), facets (small multiples), theme tokens, and interaction
//! flags. It carries no rows and no pixels — a `ColumnStoreIngest` (V1) resolves
//! `MarkSpec::data_ref` against real data, an `LodKernel` (V2) picks a tier and
//! reduces it, and a render backend (V3a/V3b) turns the reduced form into pixels or
//! a wire payload. This mirrors `open-source-libraries/xy`'s chart-grammar
//! (`spec/design/chart-grammar.md`) and wire-protocol invariants (§28: every
//! reduction decision is recorded, never silent — see [`crate::result::ViewResult`])
//! adapted to a Rust-native IR instead of `xy`'s Python component tree.
//!
//! **Versioned.** [`ViewSpec::CURRENT_VERSION`] rides every spec; a consumer that
//! cannot handle a spec's version must reject it rather than silently misrender it
//! (the same handshake discipline `xy`'s `PROTOCOL_VERSION` enforces — see
//! `spec/design/wire-protocol.md` §7 in that repo for the rationale this crate
//! follows without adopting `xy`'s wire format itself).

use serde::{Deserialize, Serialize};

/// A chart mark (geometry) kind. Closed set for V0 — matches the mark family the
/// program charter names explicitly (`plans/au-eg-program/PROGRAM.md` "Native
/// visualization" section): line, scatter, bar, area, heatmap, and graph
/// (force-directed node-link, V6). Adding a kind is a version bump, not a silent
/// extension — an unrecognized kind must fail a deserialize rather than degrade to
/// a default rendering (the `xy` v11/v12 handshake precedent: a cached old client
/// must never draw a compatible-looking wrong picture).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarkKind {
    Line,
    Scatter,
    Bar,
    Area,
    Heatmap,
    /// Force-directed node-link rendering (V6, "graph-native marks").
    Graph,
}

impl MarkKind {
    /// Every mark kind this crate knows about, in a stable order — used to build
    /// the default capability matrix and to iterate tier-selection tests.
    pub const ALL: [MarkKind; 6] = [
        MarkKind::Line,
        MarkKind::Scatter,
        MarkKind::Bar,
        MarkKind::Area,
        MarkKind::Heatmap,
        MarkKind::Graph,
    ];
}

/// How a channel value is reduced across rows sharing a bin/category, when the
/// channel encoding needs one. Mirrors the aggregate vocabulary common to
/// declarative chart grammars (Vega-Lite, `xy`'s LOD density path §2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Aggregate {
    Mean,
    Sum,
    Count,
    Min,
    Max,
    Median,
}

/// One channel's binding to a column, plus how it aggregates and which named scale
/// (see [`ScaleSpec::id`]) maps its values to screen space.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EncodingSpec {
    /// Column name in the resolved dataset (V1 `ColumnStoreIngest` resolves this
    /// against the `MarkSpec::data_ref` dataset).
    pub field: String,
    /// The named [`ScaleSpec::id`] this channel maps through. `None` for a channel
    /// with no natural scale (e.g. `shape` mapping directly to a fixed palette of
    /// marker glyphs).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scale: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregate: Option<Aggregate>,
}

impl EncodingSpec {
    pub fn field(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            scale: None,
            aggregate: None,
        }
    }

    pub fn with_scale(mut self, scale: impl Into<String>) -> Self {
        self.scale = Some(scale.into());
        self
    }

    pub fn with_aggregate(mut self, aggregate: Aggregate) -> Self {
        self.aggregate = Some(aggregate);
        self
    }
}

/// The channel bindings a mark may carry: x/y/color/size/shape (the charter's
/// explicit encoding list). `x`/`y` are the position channels every mark kind but
/// `heatmap` (which binds a `z` matrix, carried on [`MarkSpec::data_ref`] resolution
/// rather than as a scalar encoding here) needs to be meaningfully drawn; color,
/// size, and shape are optional visual channels layered on top.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Encodings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub x: Option<EncodingSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub y: Option<EncodingSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub color: Option<EncodingSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<EncodingSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shape: Option<EncodingSpec>,
}

/// A named axis/scale a panel exposes. Mirrors `xy`'s rule G2/G3
/// (`spec/design/chart-grammar.md`): scale type is a panel decision, and a mark
/// referencing an undeclared scale id is a build-time error, never a silent
/// coercion — see [`ViewSpec::validate`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScaleKind {
    Linear,
    Log,
    Time,
    Category,
    /// Zero-inclusive long-tail scale (`xy` dossier §16): linear near zero,
    /// logarithmic in the tails.
    Symlog,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScaleSpec {
    pub id: String,
    pub kind: ScaleKind,
    /// Explicit `(min, max)` domain; `None` means "autorange from the data",
    /// resolved once the referencing marks' data is available (V1+).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<(f64, f64)>,
}

impl ScaleSpec {
    pub fn new(id: impl Into<String>, kind: ScaleKind) -> Self {
        Self {
            id: id.into(),
            kind,
            domain: None,
        }
    }

    pub fn with_domain(mut self, min: f64, max: f64) -> Self {
        self.domain = Some((min, max));
        self
    }
}

/// Small-multiples faceting (`xy`'s `facet_chart`, `spec/design/chart-grammar.md`
/// §5.2): repeat the same mark composition once per distinct value of `by`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FacetSpec {
    pub by: String,
    #[serde(default = "FacetSpec::default_columns")]
    pub columns: u32,
    #[serde(default = "FacetSpec::default_true")]
    pub share_x: bool,
    #[serde(default = "FacetSpec::default_true")]
    pub share_y: bool,
}

impl FacetSpec {
    fn default_columns() -> u32 {
        3
    }
    fn default_true() -> bool {
        true
    }

    pub fn new(by: impl Into<String>) -> Self {
        Self {
            by: by.into(),
            columns: Self::default_columns(),
            share_x: true,
            share_y: true,
        }
    }
}

/// Theme tokens a render backend resolves against — palette + chrome colors +
/// typography, so the same `ViewSpec` renders consistently across static export,
/// the interactive client, and agent thumbnails (the "one system, light and dark"
/// requirement every render surface shares).
// `derive(Default)` is exact here — every field's derived default (`Vec::new()`,
// `None`, `false`) is byte-for-byte what a hand-written impl would produce, so
// there is no diverging field to justify a manual impl (clippy::derivable_impls).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ThemeTokens {
    /// Categorical palette, hex colors in series order.
    #[serde(default)]
    pub palette: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub background: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub foreground: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub font_family: Option<String>,
    #[serde(default)]
    pub dark_mode: bool,
}

/// Interaction switches (`xy`'s `interaction_config`,
/// `spec/design/chart-grammar.md` §5.1) — what the client is allowed to do, not
/// how it does it. A static-export consumer ignores every field here (see
/// [`crate::capability::Surface::StaticExport`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct InteractionFlags {
    #[serde(default)]
    pub hover: bool,
    #[serde(default)]
    pub click: bool,
    #[serde(default)]
    pub pan: bool,
    #[serde(default)]
    pub zoom: bool,
    #[serde(default)]
    pub brush: bool,
    #[serde(default)]
    pub select: bool,
}

/// Minimal mark-local style props (opacity/stroke). Deliberately small for V0 —
/// `xy`'s full style vocabulary (`spec/api/capability-matrix.md`) is a later-lane
/// concern once a real renderer exists to honor it.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct MarkStyle {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opacity: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stroke_width: Option<f32>,
}

/// One mark in the panel's z-ordered layer list (`xy` rule G1: marks layer by
/// listed order).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MarkSpec {
    pub kind: MarkKind,
    /// Opaque reference to the dataset this mark draws — a query hash, RowSet
    /// handle, or content-addressed dataset ref (V1 resolves it; this crate never
    /// interprets it).
    pub data_ref: String,
    #[serde(default)]
    pub encodings: Encodings,
    #[serde(default)]
    pub style: MarkStyle,
}

impl MarkSpec {
    pub fn new(kind: MarkKind, data_ref: impl Into<String>) -> Self {
        Self {
            kind,
            data_ref: data_ref.into(),
            encodings: Encodings::default(),
            style: MarkStyle::default(),
        }
    }

    pub fn with_encodings(mut self, encodings: Encodings) -> Self {
        self.encodings = encodings;
        self
    }
}

/// A single-panel error from [`ViewSpec::validate`]. Kept as a plain string list
/// rather than a typed enum — V0 has one caller (a human/agent reading a rejected
/// spec), and a typed enum bakes in a shape before there is a real planner to
/// consume it programmatically.
pub type SpecErrors = Vec<String>;

/// The declarative chart IR: marks, scales, an optional facet, theme tokens, and
/// interaction flags. One panel today (`xy`'s `figure`/`panel` grid is still a
/// pending item even in that project — see its chart-grammar §7 migration notes);
/// multi-panel is an additive version bump, not a breaking one, when it lands.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ViewSpec {
    pub version: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub marks: Vec<MarkSpec>,
    #[serde(default)]
    pub scales: Vec<ScaleSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub facet: Option<FacetSpec>,
    #[serde(default)]
    pub theme: ThemeTokens,
    #[serde(default)]
    pub interaction: InteractionFlags,
}

impl ViewSpec {
    /// The IR version this crate emits and accepts. Bump on any change an older
    /// consumer would silently misrender (the `xy` `PROTOCOL_VERSION` discipline —
    /// `docs/design/wire-protocol.md` §7 in that repo).
    pub const CURRENT_VERSION: u16 = 1;

    pub fn new(marks: Vec<MarkSpec>) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            title: None,
            marks,
            scales: Vec::new(),
            facet: None,
            theme: ThemeTokens::default(),
            interaction: InteractionFlags::default(),
        }
    }

    /// Structural validation independent of any resolved dataset: version
    /// currency, at least one mark, every encoding's `scale` id resolves to a
    /// declared [`ScaleSpec`] (`xy` rule G2: a mark never gets a private scale —
    /// referencing an undeclared id is a build error, not a coercion), and every
    /// facet `by` is non-empty.
    pub fn validate(&self) -> Result<(), SpecErrors> {
        let mut errors = Vec::new();

        if self.version != Self::CURRENT_VERSION {
            errors.push(format!(
                "unsupported ViewSpec version {} (expected {})",
                self.version,
                Self::CURRENT_VERSION
            ));
        }
        if self.marks.is_empty() {
            errors.push("ViewSpec must declare at least one mark".to_string());
        }

        let declared_scales: std::collections::HashSet<&str> =
            self.scales.iter().map(|s| s.id.as_str()).collect();
        for (i, mark) in self.marks.iter().enumerate() {
            if mark.data_ref.trim().is_empty() {
                errors.push(format!("mark[{i}] has an empty data_ref"));
            }
            for (channel, encoding) in [
                ("x", &mark.encodings.x),
                ("y", &mark.encodings.y),
                ("color", &mark.encodings.color),
                ("size", &mark.encodings.size),
                ("shape", &mark.encodings.shape),
            ] {
                if let Some(encoding) = encoding {
                    if let Some(scale_id) = &encoding.scale {
                        if !declared_scales.contains(scale_id.as_str()) {
                            errors.push(format!(
                                "mark[{i}] channel `{channel}` references undeclared scale `{scale_id}`"
                            ));
                        }
                    }
                }
            }
        }

        if let Some(facet) = &self.facet {
            if facet.by.trim().is_empty() {
                errors.push("facet.by must be non-empty".to_string());
            }
            if facet.columns == 0 {
                errors.push("facet.columns must be positive".to_string());
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> ViewSpec {
        let mut spec = ViewSpec::new(vec![MarkSpec::new(MarkKind::Scatter, "ds:query-hash-1")
            .with_encodings(Encodings {
                x: Some(EncodingSpec::field("gdp").with_scale("x")),
                y: Some(EncodingSpec::field("life_expectancy").with_scale("y")),
                color: Some(EncodingSpec::field("continent")),
                size: None,
                shape: None,
            })]);
        spec.scales = vec![
            ScaleSpec::new("x", ScaleKind::Linear),
            ScaleSpec::new("y", ScaleKind::Log).with_domain(1.0, 100.0),
        ];
        spec.facet = Some(FacetSpec::new("region"));
        spec.theme.palette = vec!["#4C78A8".to_string(), "#F58518".to_string()];
        spec.interaction.pan = true;
        spec.interaction.zoom = true;
        spec
    }

    #[test]
    fn round_trips_through_json() {
        let spec = sample();
        let json = serde_json::to_string(&spec).expect("serialize");
        let restored: ViewSpec = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(spec, restored);
    }

    #[test]
    fn round_trips_through_msgpack() {
        // The wire-adjacent encoding the rest of the engine standardizes on
        // (`eg-jobs` uses `rmp_serde` for its own durable records) — proving
        // round-trip holds under a binary codec too, not just JSON.
        let spec = sample();
        let bytes = rmp_serde::to_vec_named(&spec).expect("serialize");
        let restored: ViewSpec = rmp_serde::from_slice(&bytes).expect("deserialize");
        assert_eq!(spec, restored);
    }

    #[test]
    fn valid_spec_passes_validation() {
        assert!(sample().validate().is_ok());
    }

    #[test]
    fn empty_marks_are_rejected() {
        let spec = ViewSpec::new(Vec::new());
        let errors = spec.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("at least one mark")));
    }

    #[test]
    fn undeclared_scale_reference_is_rejected() {
        let mut spec = sample();
        spec.scales.clear();
        let errors = spec.validate().unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.contains("undeclared scale") && e.contains('x')));
    }

    #[test]
    fn stale_version_is_rejected_not_silently_misread() {
        let mut spec = sample();
        spec.version = ViewSpec::CURRENT_VERSION + 1;
        let errors = spec.validate().unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.contains("unsupported ViewSpec version")));
    }

    #[test]
    fn unknown_mark_kind_fails_deserialize_rather_than_defaulting() {
        let bad = r#"{"version":1,"marks":[{"kind":"pie","data_ref":"x"}]}"#;
        assert!(serde_json::from_str::<ViewSpec>(bad).is_err());
    }
}
