use super::*;

/// Materialized result of a fenced `Method::RecomputeMaterialization` writeback.
/// All identifiers are domain-separated opaque projection references.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RecomputeMaterializationResult {
    pub id: String,
    pub depends_on: Vec<String>,
    pub generating_activity: Option<String>,
    pub status: String,
    pub source_graph_version: u64,
    pub fence_epoch: u64,
    /// `true` when the authoritative recompute intent is committed but its local
    /// durable projection image has not yet been acknowledged by the outbox worker.
    pub projection_pending: bool,
}

/// Materialized result of a `Method::MaterializationStatus` run (Seam 3). `status`
/// is one of `"Fresh"`/`"Stale"`/`"Retracted"`, or `None` when `id` was never
/// registered (or a build without `epistemic-tms` never populates the index).
/// Returned via `ResultPayload::raw`.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct MaterializationStatusResult {
    pub status: Option<String>,
    pub source_graph_version: u64,
}

/// Materialized result of a `Method::StaleMaterializations` run (Seam 3 follow-up).
/// `ids` is every opaque materialization reference currently `Stale` in the
/// durable per-graph projection, sorted by its persisted `BTreeSet`. Empty means
/// nothing is stale; missing or corrupt projection authority is an error.
/// Returned via `ResultPayload::raw`.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct StaleMaterializationsResult {
    pub ids: Vec<String>,
    pub source_graph_version: u64,
}

/// Materialized result of a `Method::ResolveConflict` run (EPI-P3-7). `semantics`
/// echoes the request. Every id in the request's `node_ids` appears in EXACTLY ONE
/// of `surviving`/`defeated`/`undecided`:
///
/// * `grounded`: `surviving` = the unique grounded extension's members (IN);
///   `defeated` = attacked by an IN argument (OUT); `undecided` = neither (caught in
///   an unresolved/paraconsistent conflict, e.g. an odd attack cycle).
/// * `preferred`/`stable`: `surviving` = in EVERY computed extension (unanimous
///   across every admissible "side"); `defeated` = in NO extension (never
///   credulously acceptable); `undecided` = in SOME but not all (contested), or
///   every requested id when NO extension exists at all (a legitimate `stable`
///   result, or the crate's own NP-hardness cap firing on a large graph — see
///   `eg_epistemic::tms` module docs; never a fabricated verdict either way).
///
/// `extension_sets` is the raw extension(s) the verdict was computed from, over the
/// WHOLE graph (not filtered to `node_ids`): exactly one entry for `grounded`,
/// zero-or-more for `preferred`/`stable`. Returned via `ResultPayload::raw`.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ResolveConflictResult {
    pub semantics: String,
    pub surviving: Vec<String>,
    pub defeated: Vec<String>,
    pub undecided: Vec<String>,
    pub extension_sets: Vec<Vec<String>>,
}

// ── X-1 multimodal-evidence citation wiring (CONCEPT:EG-X1, facade feature
// `evidence-graph`) ─────────────────────────────────────────────────────────────

/// Wire mirror of `eg_epistemic::EvidenceCitation` — one node bearing on a claim
/// (support/contradiction/attack), together with its complete governed locus.
/// `kind` is the `Debug`-rendered `eg_epistemic::EdgeKind`
/// (`"Supports"`/`"Contradicts"`/`"Attacks"`), the SAME flat-string convention
/// `JustificationNodeWire::rule` uses for its `JustRule`. `locus` reuses
/// [`EvidenceLocusWire`] (the governed evidence-locus wire mirror —
/// `epistemic` implies `query`, so it is always nameable here).
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EvidenceCitationWire {
    pub evidence_id: String,
    /// One of `"Supports"`, `"Contradicts"`, `"Attacks"`.
    pub kind: String,
    pub locus: EvidenceLocusWire,
    /// SURPASS gap-closure ("unify the two evidence resolvers"): the REAL content
    /// this citation's `locus` resolves to, via `eg_alignment::EvidenceResolver`
    /// (`src/server/blob/cas_resolver.rs`'s `CasEvidenceResolver`, the SAME
    /// engine-backed resolver that previously had zero served-RPC call sites — only
    /// its own unit tests exercised it). `None` when the build lacks the `alignment`
    /// feature, no blob store is configured, or the resolver had nothing for the
    /// locus subject (e.g. a dangling `blob_ref`) — degrades to the
    /// pre-existing locus-only behavior, never a fabricated resolution.
    #[serde(default)]
    pub resolved: Option<ResolvedArtifactWire>,
}

/// Wire mirror of `eg_alignment::ResolvedArtifact` (SURPASS gap-closure: "unify the
/// two evidence resolvers", "real crop/slice codecs"). `kind` is `"text"` (a real
/// excerpt — currently `CharacterRange` by character range, `CodeSymbol` by line
/// range), `"blob"` (an address that is itself an intentionally opaque, versioned
/// pointer — e.g. `RowVersion`/`TraceSpan` — where the real CAS digest IS the exact
/// result), or `"unresolved"` (GOC-05 gate 3: an address that promises a region/
/// interval but has no registered decoder yet — the honest typed outcome instead of
/// silently reporting a raw digest as if it were that region; see `reason`).
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ResolvedArtifactWire {
    /// `"text"`, `"blob"`, or `"unresolved"`.
    pub kind: String,
    pub subject_ref: String,
    /// The resolved excerpt, when `kind == "text"`.
    pub excerpt: Option<String>,
    /// The real CAS digest, when `kind == "blob"`.
    pub blob_ref: Option<String>,
    /// A human-readable note on what the `blob` reference represents, when
    /// `kind == "blob"`.
    pub note: Option<String>,
    /// The typed unresolved reason (`missing_rendition` / `codec_unavailable` /
    /// `policy_denied` / `corrupt_bytes` / `out_of_range`), when
    /// `kind == "unresolved"`.
    #[serde(default)]
    pub reason: Option<String>,
}

/// Materialized result of a `Method::ExplainEvidence` run. Returned via
/// `ResultPayload::raw`.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ExplainEvidenceResult {
    pub citations: Vec<EvidenceCitationWire>,
}

// ── EPI-P3-3 causal reasoning + provenance ranking wiring (facade feature
// `epistemic-causal`) ───────────────────────────────────────────────────────────

/// Wire mirror of `eg_epistemic::StructuralEquation` (EPI-P3-3), keyed by the
/// variable `id` it defines — one entry of `Method::CausalEstimate::variables`.
/// `parents` MUST name only ids that appear EARLIER in that same list (parents
/// before children), mirroring `eg_epistemic::CausalGraph::add_variable`'s own
/// topological-order invariant; the handler surfaces a violation as an explicit
/// error, never a panic.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct StructuralEquationWire {
    pub id: String,
    /// `(parent id, weight)` pairs.
    pub parents: Vec<(String, f64)>,
    pub bias: f64,
    /// Variance of this variable's own exogenous noise term (`0.0` = deterministic
    /// given its parents).
    pub noise_var: f64,
}

/// Wire mirror of `eg_epistemic::CausalEstimate` (EPI-P3-3) — a calibrated mean/
/// variance/credible-interval result of one causal query for one variable.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CausalEstimateWire {
    pub mean: f64,
    pub variance: f64,
    pub interval: (f64, f64),
    pub level: f64,
}

/// Materialized result of a `Method::CausalEstimate` run: one estimate per
/// variable, in the SAME order as the request's `variables` list. Returned via
/// `ResultPayload::raw`.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CausalEstimateResult {
    pub estimates: Vec<(String, CausalEstimateWire)>,
}

/// Which of `eg_epistemic::CausalGraph`'s two non-counterfactual queries
/// `Method::CausalEstimate::do_values` feeds (EPI-P3-6).
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum CausalQueryModeWire {
    Intervene,
    Observe,
}

/// Materialized result of a `Method::CausalCounterfactual` run (EPI-P3-6): one
/// POINT value per variable — not a calibrated distribution, since Pearl's
/// point-counterfactual is deterministic given the abduced exogenous noise — in
/// the SAME order as the request's `variables` list. Returned via
/// `ResultPayload::raw`.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CausalCounterfactualResult {
    pub values: Vec<(String, f64)>,
}

/// Wire mirror of `eg_epistemic::Calibration` (EPI-P3-3) — the calibrated interval
/// backing a `RetrievalCandidateWire`'s evidence-quality score, when the candidate
/// has one.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CalibrationWire {
    pub interval: (f64, f64),
    pub level: f64,
    pub evidence_count: usize,
}

/// Wire mirror of `eg_epistemic::RetrievalCandidate` (EPI-P3-3) — one candidate in
/// a `Method::RankByProvenance` request, before ranking.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RetrievalCandidateWire {
    pub id: String,
    pub similarity: f64,
    pub source_reliability: f64,
    pub freshness: f64,
    /// `None` for a candidate with no evidence-graph backing (ranks on
    /// similarity/reliability/freshness alone — an honest "unknown", never a
    /// fabricated middling score).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub calibration: Option<CalibrationWire>,
}

/// Wire mirror of `eg_epistemic::RankWeights` (EPI-P3-3) — weights combining
/// similarity with the evidence-quality/provenance signal for `RankByProvenance`.
/// Defaults to `{ similarity: 0.5, evidence_quality: 0.5 }`, the SAME
/// equal-weighting default `eg_epistemic::RankWeights::default()` uses.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RankWeightsWire {
    pub similarity: f64,
    pub evidence_quality: f64,
}

#[cfg(feature = "epistemic")]
impl Default for RankWeightsWire {
    fn default() -> Self {
        RankWeightsWire {
            similarity: 0.5,
            evidence_quality: 0.5,
        }
    }
}
