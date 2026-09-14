use super::*;

/// The sole located-evidence wire representation. Its custom deserializer rejects
/// unsafe references and malformed coordinates before a request reaches a handler.
#[cfg(feature = "query")]
#[derive(Debug, Clone, PartialEq, Serialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EvidenceLocusWire {
    pub id: String,
    pub subject: EvidenceResourceWire,
    pub address: EvidenceAddressWire,
    pub policy_ref: String,
    pub derivation_ref: String,
}

#[cfg(feature = "query")]
impl EvidenceLocusWire {
    fn valid_opaque(value: &str, namespace: Option<&str>) -> bool {
        let parts: Vec<&str> = value.split(':').collect();
        (3..=6).contains(&parts.len())
            && parts.first() == Some(&"eg")
            && namespace.is_none_or(|expected| parts.get(1) == Some(&expected))
            && parts[1..parts.len() - 1].iter().all(|part| {
                !part.is_empty()
                    && part.len() <= 32
                    && part.bytes().all(|byte| {
                        byte.is_ascii_lowercase()
                            || byte.is_ascii_digit()
                            || matches!(byte, b'_' | b'-')
                    })
            })
            && parts.last().is_some_and(|token| {
                (16..=128).contains(&token.len())
                    && token
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
            })
    }

    fn valid_subject(subject: &EvidenceResourceWire) -> bool {
        match subject {
            EvidenceResourceWire::Artifact(value) => Self::valid_opaque(value, Some("artifact")),
            EvidenceResourceWire::Occurrence(value) => {
                Self::valid_opaque(value, Some("occurrence"))
            }
            EvidenceResourceWire::Rendition(value) => Self::valid_opaque(value, Some("rendition")),
            EvidenceResourceWire::Segment(value) => Self::valid_opaque(value, Some("segment")),
            EvidenceResourceWire::Feature(value) => Self::valid_opaque(value, Some("feature")),
            EvidenceResourceWire::EvidenceLocus(value) => Self::valid_opaque(value, Some("locus")),
        }
    }

    fn valid_region(x: f64, y: f64, width: f64, height: f64) -> bool {
        x.is_finite()
            && y.is_finite()
            && width.is_finite()
            && height.is_finite()
            && width > 0.0
            && height > 0.0
    }

    fn valid_ordered_range(address: &EvidenceAddressWire) -> bool {
        match address {
            EvidenceAddressWire::CharacterRange { start, end }
            | EvidenceAddressWire::AudioRange {
                start_ms: start,
                end_ms: end,
            }
            | EvidenceAddressWire::VideoTimeRange {
                start_ms: start,
                end_ms: end,
            }
            | EvidenceAddressWire::MetricWindow {
                start_ms: start,
                end_ms: end,
            } => end > start,
            _ => false,
        }
    }

    fn valid_table_range(row_start: u64, row_end: u64, col_start: u64, col_end: u64) -> bool {
        row_end >= row_start && col_end >= col_start
    }

    fn valid_code_symbol(
        revision_ref: &str,
        symbol_ref: &str,
        start_line: u32,
        end_line: u32,
    ) -> bool {
        Self::valid_opaque(revision_ref, None)
            && Self::valid_opaque(symbol_ref, None)
            && end_line >= start_line
    }

    fn valid_trace_span(trace_ref: &str, span_ref: &str) -> bool {
        Self::valid_opaque(trace_ref, None) && Self::valid_opaque(span_ref, None)
    }

    fn valid_address(address: &EvidenceAddressWire) -> bool {
        if matches!(
            address,
            EvidenceAddressWire::CharacterRange { .. }
                | EvidenceAddressWire::AudioRange { .. }
                | EvidenceAddressWire::VideoTimeRange { .. }
                | EvidenceAddressWire::MetricWindow { .. }
        ) {
            return Self::valid_ordered_range(address);
        }
        if let EvidenceAddressWire::FrameRange {
            start_frame,
            end_frame,
        } = address
        {
            return end_frame >= start_frame;
        }
        if let EvidenceAddressWire::TableCellRange {
            row_start,
            row_end,
            col_start,
            col_end,
        } = address
        {
            return Self::valid_table_range(*row_start, *row_end, *col_start, *col_end);
        }
        if let EvidenceAddressWire::ImageRegion {
            x,
            y,
            width,
            height,
        }
        | EvidenceAddressWire::PageRegion {
            x,
            y,
            width,
            height,
            ..
        } = address
        {
            return Self::valid_region(*x, *y, *width, *height);
        }
        if let EvidenceAddressWire::Point { x, y } = address {
            return x.is_finite() && y.is_finite();
        }
        match address {
            EvidenceAddressWire::RowVersion { row_ref, .. } => Self::valid_opaque(row_ref, None),
            EvidenceAddressWire::CodeSymbol {
                revision_ref,
                symbol_ref,
                start_line,
                end_line,
            } => Self::valid_code_symbol(revision_ref, symbol_ref, *start_line, *end_line),
            EvidenceAddressWire::TraceSpan {
                trace_ref,
                span_ref,
            } => Self::valid_trace_span(trace_ref, span_ref),
            _ => false,
        }
    }

    fn valid(&self) -> bool {
        Self::valid_opaque(&self.id, Some("locus"))
            && Self::valid_subject(&self.subject)
            && Self::valid_address(&self.address)
            && Self::valid_opaque(&self.policy_ref, None)
            && Self::valid_opaque(&self.derivation_ref, Some("derivation"))
    }
}

#[cfg(feature = "query")]
impl<'de> Deserialize<'de> for EvidenceLocusWire {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Unchecked {
            id: String,
            subject: EvidenceResourceWire,
            address: EvidenceAddressWire,
            policy_ref: String,
            derivation_ref: String,
        }

        let value = Unchecked::deserialize(deserializer)?;
        let locus = Self {
            id: value.id,
            subject: value.subject,
            address: value.address,
            policy_ref: value.policy_ref,
            derivation_ref: value.derivation_ref,
        };
        if locus.valid() {
            Ok(locus)
        } else {
            Err(<D::Error as serde::de::Error>::custom(
                "invalid governed evidence locus",
            ))
        }
    }
}

/// Materialized result of a `Method::ExplainPolicy` run (CONCEPT:EG-KG.sharding.row-level-security).
/// Returned via `ResultPayload::raw`.
#[cfg(feature = "query")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ExplainPolicyResult {
    /// Ids the caller's RLS-filtered view actually returns.
    pub visible_ids: Vec<String>,
    /// Ids present in the UNFILTERED result but absent from `visible_ids` — what the
    /// policy denied. Always empty when no RLS filtering applied (no `security` feature,
    /// or no caller/RLS on this connection).
    pub policy_denied_ids: Vec<String>,
}

/// One node of an `EXPLAIN BELIEF` justification tree — the wire projection of
/// `eg_epistemic::ProofNode`. `rule` is the `Debug`-rendered `eg_epistemic::JustRule`
/// (`"Asserted"`, `"DerivedSupport"`, `"DerivedContradiction"`, `"BayesianUpdate"`).
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct JustificationNodeWire {
    pub claim: String,
    pub rule: String,
    pub confidence: f64,
    pub premises: Vec<JustificationNodeWire>,
}

/// Materialized result of a `Method::ExplainBelief` run. Returned via `ResultPayload::raw`.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ExplainBeliefResult {
    pub root: JustificationNodeWire,
}

/// Wire mirror of `eg_epistemic::redact::DisclosureLevel` (EPI-P3-4, L51) — the
/// `Method::ExplainBelief::disclosure_level` request field AND the
/// `ExplainBeliefRedactedResult::level` response field share this one type. Plain
/// serde/enum, no `eg-epistemic` dependency needed here (`eg-types` sits BELOW
/// `eg-epistemic` in the crate DAG).
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DisclosureLevelWire {
    Full,
    Skeleton,
    ExistenceOnly,
}

/// Wire mirror of `eg_epistemic::redact::ExistenceSignal`.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ExistenceSignalWire {
    Supported,
    Contradicted,
    Uncertain,
}

/// Wire mirror of `eg_epistemic::redact::RedactedProofNode` — structurally parallel to
/// [`JustificationNodeWire`], except `claim` is `None` (with `redaction_label` set)
/// when the requesting actor's RLS access does not extend to that proof-tree node.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RedactedJustificationNodeWire {
    pub claim: Option<String>,
    pub redaction_label: Option<String>,
    pub rule: String,
    pub confidence: f64,
    pub premises: Vec<RedactedJustificationNodeWire>,
}

/// Result of a `Method::ExplainBelief` call that set `disclosure_level` (feature
/// `epistemic-redaction`) — returned via `ResultPayload::raw` INSTEAD OF
/// `ExplainBeliefResult` for that same call (the caller who set `disclosure_level`
/// already knows to decode this type). Wire mirror of
/// `eg_epistemic::redact::RedactedJustificationGraph`.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ExplainBeliefRedactedResult {
    pub level: DisclosureLevelWire,
    pub existence: ExistenceSignalWire,
    /// `Some` at `Full`/`Skeleton`; `None` at `ExistenceOnly` (no structure rendered).
    pub root: Option<RedactedJustificationNodeWire>,
}

/// Unified response body for `Method::ExplainBelief`.
///
/// The classic and disclosure-aware forms remain distinct on the wire while the
/// method has one stable result type for callers that support both modes.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ExplainBeliefResponse {
    Redacted(ExplainBeliefRedactedResult),
    Classic(ExplainBeliefResult),
}

/// Wire mirror of `eg_epistemic::AuthorityPolicy` — the confidence-weighting policy an
/// `EpistemicStatus` was computed under ("under whose authority").
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AuthorityPolicyWire {
    pub source_reliability: f64,
    pub attack_multiplier: f64,
    pub prior_strength: f64,
}

/// Wire mirror of `eg_epistemic::query::WhyNot` — flattens `WhyNotReason`'s per-variant
/// payload (`Contradicted { blockers }` / `Undecided { competing }`) into two plain
/// `Vec<String>` fields, each empty unless the matching reason tag applies (a pure-serde
/// enum-with-data mirror would work too, but this keeps the wire shape flat like every
/// other `*Wire` type here).
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct WhyNotWire {
    pub claim: String,
    /// One of `"Unknown"`, `"InsufficientConfidence"`, `"Contradicted"`, `"Undecided"`.
    pub reason: String,
    /// Populated iff `reason == "Contradicted"`.
    pub blockers: Vec<String>,
    /// Populated iff `reason == "Undecided"`.
    pub competing: Vec<String>,
    pub confidence: f64,
}

/// Wire mirror of `eg_epistemic::query::MinimalFlipSet` — "what would invalidate it".
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct MinimalFlipSetWire {
    pub claim: String,
    pub believed_now: bool,
    pub evidence_ids: Vec<String>,
    pub believed_after: bool,
}

/// Wire mirror of `eg_epistemic::query::EpistemicStatus` — the Phase-3 acceptance
/// capstone (see `Method::EpistemicStatus` docs).
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EpistemicStatusWire {
    pub claim: String,
    pub believed: bool,
    pub confidence: f64,
    pub uncertainty: f64,
    pub proof: JustificationNodeWire,
    pub why_not: Option<WhyNotWire>,
    pub evidence: Vec<String>,
    pub contradicting: Vec<String>,
    pub attacking: Vec<String>,
    pub authority: AuthorityPolicyWire,
    pub valid_time: Option<(Option<u64>, Option<u64>)>,
    pub tx_time: Option<(Option<u64>, Option<u64>)>,
    pub what_would_invalidate: Option<MinimalFlipSetWire>,
}

/// Materialized result of a `Method::EpistemicStatus` run. Returned via
/// `ResultPayload::raw`.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EpistemicStatusResult {
    pub status: EpistemicStatusWire,
}

/// Wire mirror of `eg_epistemic::query::ChangedBelief` — one entry of a
/// `Method::WhatChanged` result.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ChangedBeliefWire {
    pub id: String,
    pub believed_before: bool,
    pub believed_after: bool,
    pub confidence_before: f64,
    pub confidence_after: f64,
    pub evidence_added: Vec<String>,
    pub evidence_removed: Vec<String>,
    pub reason: String,
}

/// Materialized result of a `Method::WhatChanged` run. Returned via
/// `ResultPayload::raw`.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct WhatChangedResult {
    pub changed: Vec<ChangedBeliefWire>,
}
