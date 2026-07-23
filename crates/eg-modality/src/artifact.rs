//! Universal governed artifact and evidence protocol.
//!
//! This module is the storage-neutral identity layer shared by every modality.
//! Raw source locations, file names, user names, endpoints, and payloads are never
//! represented here.  Every durable reference is an [`OpaqueRef`] issued by a
//! trusted boundary after sanitisation, and a [`PrivacyAttestation`] is required on
//! each committed bundle.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{de::Error as _, Deserialize, Deserializer, Serialize, Serializer};

/// Current wire/storage version of [`ArtifactBundle`].
pub const ARTIFACT_PROTOCOL_VERSION: u16 = 1;

/// A privacy-safe, non-reversible durable reference.
///
/// The lexical form is `eg:<namespace>[:<subnamespace>]:<opaque-token>`.  The final
/// token is 16–128 lowercase hexadecimal characters. Namespaces contain only
/// lowercase ASCII letters, digits, `_`, or `-`. Consequently a URL, email address,
/// Windows path, POSIX path, or ordinary display name cannot accidentally enter this
/// protocol; issuers must still attest that the token is random or irreversibly keyed.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OpaqueRef(String);

impl OpaqueRef {
    pub fn new(value: impl Into<String>) -> Result<Self, ProtocolError> {
        let value = value.into();
        let parts: Vec<&str> = value.split(':').collect();
        let valid = (3..=6).contains(&parts.len())
            && parts.first() == Some(&"eg")
            && parts[1..parts.len() - 1].iter().all(|part| {
                !part.is_empty()
                    && part.len() <= 32
                    && part.bytes().all(|b| {
                        b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-'
                    })
            })
            && parts.last().is_some_and(|token| {
                (16..=128).contains(&token.len())
                    && token
                        .bytes()
                        .all(|b| b.is_ascii_digit() || matches!(b, b'a'..=b'f'))
            });
        if !valid {
            return Err(ProtocolError::InvalidOpaqueReference);
        }
        Ok(Self(value))
    }

    /// Construct an opaque reference from a namespace and an already irreversible
    /// lowercase-hex token (normally a keyed digest or random identifier).
    pub fn scoped(namespace: &str, token: &str) -> Result<Self, ProtocolError> {
        Self::new(format!("eg:{namespace}:{token}"))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn namespace(&self) -> &str {
        self.0.split(':').nth(1).unwrap_or_default()
    }
}

impl fmt::Debug for OpaqueRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("OpaqueRef").field(&self.0).finish()
    }
}

impl fmt::Display for OpaqueRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Serialize for OpaqueRef {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for OpaqueRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(|_| D::Error::custom("invalid opaque reference"))
    }
}

macro_rules! opaque_id {
    ($name:ident, $namespace:literal) => {
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(OpaqueRef);

        impl $name {
            pub fn from_token(token: &str) -> Result<Self, ProtocolError> {
                Ok(Self(OpaqueRef::scoped($namespace, token)?))
            }

            pub fn from_opaque(reference: OpaqueRef) -> Result<Self, ProtocolError> {
                if reference.namespace() != $namespace {
                    return Err(ProtocolError::InvalidOpaqueReference);
                }
                Ok(Self(reference))
            }

            pub fn as_ref(&self) -> &OpaqueRef {
                &self.0
            }

            pub fn into_opaque(self) -> OpaqueRef {
                self.0
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                self.0.serialize(serializer)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let reference = OpaqueRef::deserialize(deserializer)?;
                Self::from_opaque(reference)
                    .map_err(|_| D::Error::custom("invalid typed opaque identity"))
            }
        }
    };
}

opaque_id!(ArtifactId, "artifact");
opaque_id!(OccurrenceId, "occurrence");
opaque_id!(RenditionId, "rendition");
opaque_id!(SegmentId, "segment");
opaque_id!(FeatureId, "feature");
opaque_id!(EvidenceLocusId, "locus");
opaque_id!(DerivationId, "derivation");

/// A typed resource reference usable as a feature/evidence/derivation subject.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum ResourceId {
    Artifact(ArtifactId),
    Occurrence(OccurrenceId),
    Rendition(RenditionId),
    Segment(SegmentId),
    Feature(FeatureId),
    EvidenceLocus(EvidenceLocusId),
}

impl ResourceId {
    pub fn opaque(&self) -> &OpaqueRef {
        match self {
            Self::Artifact(id) => id.as_ref(),
            Self::Occurrence(id) => id.as_ref(),
            Self::Rendition(id) => id.as_ref(),
            Self::Segment(id) => id.as_ref(),
            Self::Feature(id) => id.as_ref(),
            Self::EvidenceLocus(id) => id.as_ref(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModalityKind {
    Document,
    Image,
    Audio,
    Video,
    Table,
    TimeSeries,
    Code,
    Trace,
    Vector,
    Binary,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Classification {
    Public,
    Internal,
    Confidential,
    Restricted,
}

/// Governance that follows an occurrence and every object derived from it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyEnvelope {
    pub tenant_ref: OpaqueRef,
    pub access_policy_ref: OpaqueRef,
    pub classification: Classification,
    pub retention_policy_ref: OpaqueRef,
    pub deletion_policy_ref: OpaqueRef,
    #[serde(default)]
    pub legal_hold_ref: Option<OpaqueRef>,
    #[serde(default)]
    pub purpose_refs: Vec<OpaqueRef>,
}

/// Proof that privacy controls ran before a bundle reached durable storage.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrivacyAttestation {
    pub scanner_ref: OpaqueRef,
    pub policy_version_ref: OpaqueRef,
    /// Must be false. Raw sensitive content belongs in an approved encrypted CAS,
    /// addressed here only through an opaque content reference.
    pub raw_pii_persisted: bool,
    /// Must be false. Local paths, host/user names, and source endpoints are not
    /// protocol data.
    pub local_identifiers_persisted: bool,
}

impl PrivacyAttestation {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.raw_pii_persisted || self.local_identifiers_persisted {
            return Err(ProtocolError::PrivacyAttestationFailed);
        }
        Ok(())
    }
}

/// Reproducible derivation lineage for a rendition or feature.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Derivation {
    pub id: DerivationId,
    pub transform_ref: OpaqueRef,
    pub implementation_ref: OpaqueRef,
    pub version_ref: OpaqueRef,
    #[serde(default)]
    pub model_ref: Option<OpaqueRef>,
    pub inputs: Vec<ResourceId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Artifact {
    pub id: ArtifactId,
    pub content_ref: OpaqueRef,
    pub modality: ModalityKind,
    pub schema_ref: OpaqueRef,
    pub content_version: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Occurrence {
    pub id: OccurrenceId,
    pub artifact_id: ArtifactId,
    /// Opaque identifier for the authoritative source occurrence. Never a URL,
    /// path, mailbox, user name, or upstream object title.
    pub source_ref: OpaqueRef,
    pub observation_version: u64,
    pub policy: PolicyEnvelope,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rendition {
    pub id: RenditionId,
    pub occurrence_id: OccurrenceId,
    pub content_ref: OpaqueRef,
    pub modality: ModalityKind,
    pub schema_ref: OpaqueRef,
    pub derivation: Derivation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SegmentKind {
    Page,
    Paragraph,
    Table,
    Row,
    Region,
    AudioRange,
    VideoShot,
    FrameRange,
    TimeWindow,
    CodeSymbol,
    TraceSpan,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Segment {
    pub id: SegmentId,
    pub rendition_id: RenditionId,
    #[serde(default)]
    pub parent_segment_id: Option<SegmentId>,
    pub kind: SegmentKind,
    pub ordinal: u64,
    pub schema_ref: OpaqueRef,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeatureKind {
    Embedding,
    Statistic,
    Label,
    Signature,
    PerceptualHash,
    QualityMetric,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Feature {
    pub id: FeatureId,
    pub subject: ResourceId,
    pub kind: FeatureKind,
    /// Opaque pointer to the typed feature value; large vectors and values are not
    /// duplicated in the semantic protocol.
    pub value_ref: OpaqueRef,
    pub schema_ref: OpaqueRef,
    pub derivation: Derivation,
}

/// Exact, modality-neutral address inside a governed resource. Every symbolic
/// component is opaque; only non-identifying numeric coordinates are inline.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EvidenceAddress {
    CharacterRange {
        start: u64,
        end: u64,
    },
    TableCellRange {
        row_start: u64,
        row_end: u64,
        col_start: u64,
        col_end: u64,
    },
    ImageRegion {
        x: f64,
        y: f64,
        width: f64,
        height: f64,
    },
    PageRegion {
        page: u32,
        x: f64,
        y: f64,
        width: f64,
        height: f64,
    },
    AudioRange {
        start_ms: u64,
        end_ms: u64,
    },
    VideoTimeRange {
        start_ms: u64,
        end_ms: u64,
    },
    FrameRange {
        start_frame: u64,
        end_frame: u64,
    },
    MetricWindow {
        start_ms: u64,
        end_ms: u64,
    },
    Point {
        x: f64,
        y: f64,
    },
    RowVersion {
        row_ref: OpaqueRef,
        version: u64,
    },
    CodeSymbol {
        revision_ref: OpaqueRef,
        symbol_ref: OpaqueRef,
        start_line: u32,
        end_line: u32,
    },
    TraceSpan {
        trace_ref: OpaqueRef,
        span_ref: OpaqueRef,
    },
}

impl EvidenceAddress {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        let valid = match self {
            Self::CharacterRange { start, end }
            | Self::AudioRange {
                start_ms: start,
                end_ms: end,
            }
            | Self::VideoTimeRange {
                start_ms: start,
                end_ms: end,
            }
            | Self::MetricWindow {
                start_ms: start,
                end_ms: end,
            } => end > start,
            Self::FrameRange {
                start_frame,
                end_frame,
            } => end_frame >= start_frame,
            Self::TableCellRange {
                row_start,
                row_end,
                col_start,
                col_end,
            } => row_end >= row_start && col_end >= col_start,
            Self::ImageRegion {
                x,
                y,
                width,
                height,
            }
            | Self::PageRegion {
                x,
                y,
                width,
                height,
                ..
            } => {
                x.is_finite()
                    && y.is_finite()
                    && width.is_finite()
                    && height.is_finite()
                    && *width > 0.0
                    && *height > 0.0
            }
            Self::Point { x, y } => x.is_finite() && y.is_finite(),
            Self::RowVersion { .. } | Self::TraceSpan { .. } => true,
            Self::CodeSymbol {
                start_line,
                end_line,
                ..
            } => end_line >= start_line,
        };
        valid
            .then_some(())
            .ok_or(ProtocolError::InvalidEvidenceAddress)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct EvidenceLocus {
    pub id: EvidenceLocusId,
    pub subject: ResourceId,
    pub address: EvidenceAddress,
    pub policy_ref: OpaqueRef,
    pub derivation_ref: DerivationId,
}

impl EvidenceLocus {
    /// Validate the numeric address carried by this governed identity. Opaque and
    /// typed identifiers validate while deserializing and cannot contain paths,
    /// endpoints, account names, or display text.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.address.validate()
    }
}

impl<'de> Deserialize<'de> for EvidenceLocus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Unchecked {
            id: EvidenceLocusId,
            subject: ResourceId,
            address: EvidenceAddress,
            policy_ref: OpaqueRef,
            derivation_ref: DerivationId,
        }

        let value = Unchecked::deserialize(deserializer)?;
        let locus = Self {
            id: value.id,
            subject: value.subject,
            address: value.address,
            policy_ref: value.policy_ref,
            derivation_ref: value.derivation_ref,
        };
        locus
            .validate()
            .map(|()| locus)
            .map_err(|_| D::Error::custom("invalid evidence locus"))
    }
}

/// One atomic, governed cross-modal commit payload.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ArtifactBundle {
    pub protocol_version: u16,
    pub privacy: PrivacyAttestation,
    pub artifacts: Vec<Artifact>,
    pub occurrences: Vec<Occurrence>,
    pub renditions: Vec<Rendition>,
    pub segments: Vec<Segment>,
    pub features: Vec<Feature>,
    pub evidence_loci: Vec<EvidenceLocus>,
}

impl ArtifactBundle {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.protocol_version != ARTIFACT_PROTOCOL_VERSION {
            return Err(ProtocolError::UnsupportedVersion);
        }
        self.privacy.validate()?;
        if self.artifacts.is_empty() || self.occurrences.is_empty() {
            return Err(ProtocolError::IncompleteBundle);
        }

        let artifacts = unique(self.artifacts.iter().map(|v| v.id.as_ref()))?;
        let occurrences = unique(self.occurrences.iter().map(|v| v.id.as_ref()))?;
        let renditions = unique(self.renditions.iter().map(|v| v.id.as_ref()))?;
        let segments = unique(self.segments.iter().map(|v| v.id.as_ref()))?;
        let features = unique(self.features.iter().map(|v| v.id.as_ref()))?;
        let loci = unique(self.evidence_loci.iter().map(|v| v.id.as_ref()))?;
        let derivation_values: Vec<&Derivation> = self
            .renditions
            .iter()
            .map(|value| &value.derivation)
            .chain(self.features.iter().map(|value| &value.derivation))
            .collect();
        let derivations: BTreeSet<&str> = derivation_values
            .iter()
            .map(|value| value.id.as_ref().as_str())
            .collect();
        for (index, left) in derivation_values.iter().enumerate() {
            if derivation_values[index + 1..]
                .iter()
                .any(|right| left.id == right.id && left != right)
            {
                return Err(ProtocolError::DuplicateIdentity);
            }
        }
        let policy_refs: BTreeSet<&str> = self
            .occurrences
            .iter()
            .map(|value| value.policy.access_policy_ref.as_str())
            .collect();

        if self
            .artifacts
            .iter()
            .any(|value| value.content_version == 0)
        {
            return Err(ProtocolError::IncompleteBundle);
        }

        for occurrence in &self.occurrences {
            if !artifacts.contains(occurrence.artifact_id.as_ref().as_str()) {
                return Err(ProtocolError::DanglingReference);
            }
            if occurrence.observation_version == 0 || occurrence.policy.purpose_refs.is_empty() {
                return Err(ProtocolError::IncompleteBundle);
            }
        }
        for rendition in &self.renditions {
            if !occurrences.contains(rendition.occurrence_id.as_ref().as_str()) {
                return Err(ProtocolError::DanglingReference);
            }
            self.validate_derivation(
                &rendition.derivation,
                &artifacts,
                &occurrences,
                &renditions,
                &segments,
                &features,
                &loci,
            )?;
        }
        for segment in &self.segments {
            if !renditions.contains(segment.rendition_id.as_ref().as_str()) {
                return Err(ProtocolError::DanglingReference);
            }
            if let Some(parent) = &segment.parent_segment_id {
                if !segments.contains(parent.as_ref().as_str()) || parent == &segment.id {
                    return Err(ProtocolError::DanglingReference);
                }
            }
        }
        for feature in &self.features {
            validate_resource(
                &feature.subject,
                &artifacts,
                &occurrences,
                &renditions,
                &segments,
                &features,
                &loci,
            )?;
            self.validate_derivation(
                &feature.derivation,
                &artifacts,
                &occurrences,
                &renditions,
                &segments,
                &features,
                &loci,
            )?;
        }
        validate_acyclic_derivations(&self.renditions, &self.features)?;
        for locus in &self.evidence_loci {
            validate_resource(
                &locus.subject,
                &artifacts,
                &occurrences,
                &renditions,
                &segments,
                &features,
                &loci,
            )?;
            if !derivations.contains(locus.derivation_ref.as_ref().as_str())
                || !policy_refs.contains(locus.policy_ref.as_str())
            {
                return Err(ProtocolError::DanglingReference);
            }
            locus.address.validate()?;
        }
        Ok(())
    }

    /// Production certification requires all six identity tiers and at least one
    /// exact evidence locus. This is stricter than structural [`Self::validate`],
    /// which also accepts raw-only artifacts before extraction has completed.
    pub fn validate_certified(&self) -> Result<(), ProtocolError> {
        self.validate()?;
        if self.renditions.is_empty()
            || self.segments.is_empty()
            || self.features.is_empty()
            || self.evidence_loci.is_empty()
        {
            return Err(ProtocolError::IncompleteBundle);
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn validate_derivation(
        &self,
        derivation: &Derivation,
        artifacts: &BTreeSet<&str>,
        occurrences: &BTreeSet<&str>,
        renditions: &BTreeSet<&str>,
        segments: &BTreeSet<&str>,
        features: &BTreeSet<&str>,
        loci: &BTreeSet<&str>,
    ) -> Result<(), ProtocolError> {
        if derivation.inputs.is_empty() {
            return Err(ProtocolError::IncompleteBundle);
        }
        for input in &derivation.inputs {
            validate_resource(
                input,
                artifacts,
                occurrences,
                renditions,
                segments,
                features,
                loci,
            )?;
        }
        Ok(())
    }
}

fn unique<'a>(
    values: impl Iterator<Item = &'a OpaqueRef>,
) -> Result<BTreeSet<&'a str>, ProtocolError> {
    let mut out = BTreeSet::new();
    for value in values {
        if !out.insert(value.as_str()) {
            return Err(ProtocolError::DuplicateIdentity);
        }
    }
    Ok(out)
}

fn validate_acyclic_derivations(
    renditions: &[Rendition],
    features: &[Feature],
) -> Result<(), ProtocolError> {
    let dependencies: BTreeMap<ResourceId, Vec<ResourceId>> = renditions
        .iter()
        .map(|value| {
            (
                ResourceId::Rendition(value.id.clone()),
                value.derivation.inputs.clone(),
            )
        })
        .chain(features.iter().map(|value| {
            (
                ResourceId::Feature(value.id.clone()),
                value.derivation.inputs.clone(),
            )
        }))
        .collect();
    let mut visiting = BTreeSet::new();
    let mut complete = BTreeSet::new();
    for resource in dependencies.keys() {
        visit_derivation(resource, &dependencies, &mut visiting, &mut complete)?;
    }
    Ok(())
}

fn visit_derivation(
    resource: &ResourceId,
    dependencies: &BTreeMap<ResourceId, Vec<ResourceId>>,
    visiting: &mut BTreeSet<ResourceId>,
    complete: &mut BTreeSet<ResourceId>,
) -> Result<(), ProtocolError> {
    if complete.contains(resource) {
        return Ok(());
    }
    if !visiting.insert(resource.clone()) {
        return Err(ProtocolError::DerivationCycle);
    }
    if let Some(inputs) = dependencies.get(resource) {
        for input in inputs {
            if dependencies.contains_key(input) {
                visit_derivation(input, dependencies, visiting, complete)?;
            }
        }
    }
    visiting.remove(resource);
    complete.insert(resource.clone());
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_resource<'a>(
    resource: &ResourceId,
    artifacts: &BTreeSet<&'a str>,
    occurrences: &BTreeSet<&'a str>,
    renditions: &BTreeSet<&'a str>,
    segments: &BTreeSet<&'a str>,
    features: &BTreeSet<&'a str>,
    loci: &BTreeSet<&'a str>,
) -> Result<(), ProtocolError> {
    let found = match resource {
        ResourceId::Artifact(id) => artifacts.contains(id.as_ref().as_str()),
        ResourceId::Occurrence(id) => occurrences.contains(id.as_ref().as_str()),
        ResourceId::Rendition(id) => renditions.contains(id.as_ref().as_str()),
        ResourceId::Segment(id) => segments.contains(id.as_ref().as_str()),
        ResourceId::Feature(id) => features.contains(id.as_ref().as_str()),
        ResourceId::EvidenceLocus(id) => loci.contains(id.as_ref().as_str()),
    };
    found.then_some(()).ok_or(ProtocolError::DanglingReference)
}

/// Privacy-safe protocol errors: no rejected input or source detail is echoed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtocolError {
    InvalidOpaqueReference,
    UnsupportedVersion,
    PrivacyAttestationFailed,
    DuplicateIdentity,
    DanglingReference,
    InvalidEvidenceAddress,
    DerivationCycle,
    IncompleteBundle,
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidOpaqueReference => "invalid opaque reference",
            Self::UnsupportedVersion => "unsupported artifact protocol version",
            Self::PrivacyAttestationFailed => "privacy attestation failed",
            Self::DuplicateIdentity => "duplicate artifact protocol identity",
            Self::DanglingReference => "dangling artifact protocol reference",
            Self::InvalidEvidenceAddress => "invalid evidence address",
            Self::DerivationCycle => "cyclic artifact derivation",
            Self::IncompleteBundle => "incomplete artifact bundle",
        })
    }
}

impl std::error::Error for ProtocolError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(namespace: &str, suffix: u8) -> OpaqueRef {
        OpaqueRef::scoped(namespace, &format!("00000000000000{suffix:02x}")).unwrap()
    }

    fn certified_bundle() -> ArtifactBundle {
        let artifact = ArtifactId(r("artifact", 1));
        let occurrence = OccurrenceId(r("occurrence", 2));
        let rendition = RenditionId(r("rendition", 3));
        let segment = SegmentId(r("segment", 4));
        let feature = FeatureId(r("feature", 5));
        let derivation = Derivation {
            id: DerivationId(r("derivation", 6)),
            transform_ref: r("transform", 7),
            implementation_ref: r("implementation", 8),
            version_ref: r("version", 9),
            model_ref: None,
            inputs: vec![ResourceId::Occurrence(occurrence.clone())],
        };
        ArtifactBundle {
            protocol_version: ARTIFACT_PROTOCOL_VERSION,
            privacy: PrivacyAttestation {
                scanner_ref: r("scanner", 10),
                policy_version_ref: r("policyversion", 11),
                raw_pii_persisted: false,
                local_identifiers_persisted: false,
            },
            artifacts: vec![Artifact {
                id: artifact.clone(),
                content_ref: r("content", 12),
                modality: ModalityKind::Document,
                schema_ref: r("schema", 13),
                content_version: 1,
            }],
            occurrences: vec![Occurrence {
                id: occurrence.clone(),
                artifact_id: artifact,
                source_ref: r("source", 14),
                observation_version: 1,
                policy: PolicyEnvelope {
                    tenant_ref: r("tenant", 15),
                    access_policy_ref: r("policy", 16),
                    classification: Classification::Internal,
                    retention_policy_ref: r("retention", 17),
                    deletion_policy_ref: r("deletion", 18),
                    legal_hold_ref: None,
                    purpose_refs: vec![r("purpose", 19)],
                },
            }],
            renditions: vec![Rendition {
                id: rendition.clone(),
                occurrence_id: occurrence,
                content_ref: r("content", 20),
                modality: ModalityKind::Document,
                schema_ref: r("schema", 21),
                derivation: derivation.clone(),
            }],
            segments: vec![Segment {
                id: segment.clone(),
                rendition_id: rendition,
                parent_segment_id: None,
                kind: SegmentKind::Paragraph,
                ordinal: 0,
                schema_ref: r("schema", 22),
            }],
            features: vec![Feature {
                id: feature,
                subject: ResourceId::Segment(segment.clone()),
                kind: FeatureKind::Embedding,
                value_ref: r("value", 23),
                schema_ref: r("schema", 24),
                derivation: derivation.clone(),
            }],
            evidence_loci: vec![EvidenceLocus {
                id: EvidenceLocusId(r("locus", 25)),
                subject: ResourceId::Segment(segment),
                address: EvidenceAddress::CharacterRange { start: 0, end: 8 },
                policy_ref: r("policy", 16),
                derivation_ref: derivation.id,
            }],
        }
    }

    #[test]
    fn certified_bundle_has_all_six_stable_identity_tiers() {
        certified_bundle().validate_certified().unwrap();
    }

    #[test]
    fn opaque_refs_reject_locations_and_personal_identifiers() {
        for unsafe_value in [
            "https://example.invalid/object",
            "file:///private/location",
            "C:\\private\\location",
            "/private/location",
            "person@example.invalid",
            "display name",
            "eg:user:christophersmith",
        ] {
            assert_eq!(
                OpaqueRef::new(unsafe_value),
                Err(ProtocolError::InvalidOpaqueReference)
            );
        }
    }

    #[test]
    fn deserialization_cannot_bypass_opaque_reference_validation() {
        let decoded = serde_json::from_str::<OpaqueRef>("\"/private/location\"");
        assert!(decoded.is_err());
        let wrong_typed_id = serde_json::from_str::<ArtifactId>("\"eg:segment:0000000000000001\"");
        assert!(wrong_typed_id.is_err());
    }

    #[test]
    fn privacy_attestation_is_fail_closed() {
        let mut bundle = certified_bundle();
        bundle.privacy.local_identifiers_persisted = true;
        assert_eq!(
            bundle.validate(),
            Err(ProtocolError::PrivacyAttestationFailed)
        );
    }

    #[test]
    fn dangling_references_and_invalid_loci_are_rejected() {
        let mut dangling = certified_bundle();
        dangling.segments[0].rendition_id = RenditionId(r("rendition", 99));
        assert_eq!(dangling.validate(), Err(ProtocolError::DanglingReference));

        let mut invalid_locus = certified_bundle();
        invalid_locus.evidence_loci[0].address =
            EvidenceAddress::CharacterRange { start: 9, end: 9 };
        assert_eq!(
            invalid_locus.validate(),
            Err(ProtocolError::InvalidEvidenceAddress)
        );

        let mut missing_derivation = certified_bundle();
        missing_derivation.evidence_loci[0].derivation_ref = DerivationId(r("derivation", 99));
        assert_eq!(
            missing_derivation.validate(),
            Err(ProtocolError::DanglingReference)
        );
    }

    #[test]
    fn derivation_cycles_and_incomplete_governance_are_rejected() {
        let mut cyclic = certified_bundle();
        let feature_id = cyclic.features[0].id.clone();
        cyclic.features[0].derivation.id = DerivationId(r("derivation", 26));
        cyclic.features[0].derivation.inputs = vec![ResourceId::Feature(feature_id)];
        assert_eq!(cyclic.validate(), Err(ProtocolError::DerivationCycle));

        let mut missing_purpose = certified_bundle();
        missing_purpose.occurrences[0].policy.purpose_refs.clear();
        assert_eq!(
            missing_purpose.validate(),
            Err(ProtocolError::IncompleteBundle)
        );
    }
}
