//! Typed `Claim` and `Evidence` graph nodes (EH-194).
//!
//! A claim and its evidence are stored as ordinary property-graph nodes whose `type`
//! is `"Claim"` or `"Evidence"`. Until this module they were a naming convention: every
//! writer (`eg-jobs`, the mining writeback, `eg-quantum-workloads`) assembled the
//! property object by hand and every reader probed it key by key. These structs are
//! the one owner of that shape. Writers build a [`Claim`]/[`Evidence`] and call
//! [`EpistemicNode::to_properties`]; readers call [`EpistemicNode::from_properties`]
//! and get either a validated node or a typed [`EpistemicNodeError`] naming exactly
//! what is wrong, instead of a silently skipped key.
//!
//! The stored bytes do not change: the typed core fields serialize under the same keys
//! the convention used, and every other key a writer attaches (lineage, domain
//! payload) rides in `attributes`, so an existing store decodes unchanged.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// The node `type` label of a claim.
pub const CLAIM_LABEL: &str = "Claim";
/// The node `type` label of a piece of evidence.
pub const EVIDENCE_LABEL: &str = "Evidence";
/// The property key that carries the node label.
const TYPE_KEY: &str = "type";

/// Which epistemic node a property object is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EpistemicNodeKind {
    Claim,
    Evidence,
}

/// Every epistemic kind with its stored label: the single mapping both directions use.
const KIND_LABELS: [(EpistemicNodeKind, &str); 2] = [
    (EpistemicNodeKind::Claim, CLAIM_LABEL),
    (EpistemicNodeKind::Evidence, EVIDENCE_LABEL),
];

impl EpistemicNodeKind {
    /// The stored `type` label.
    pub fn label(self) -> &'static str {
        match self {
            Self::Claim => CLAIM_LABEL,
            Self::Evidence => EVIDENCE_LABEL,
        }
    }

    /// The kind a stored `type` label names, or `None` for a non-epistemic node.
    pub fn from_label(label: &str) -> Option<Self> {
        KIND_LABELS
            .iter()
            .find(|(_, known)| *known == label)
            .map(|(kind, _)| *kind)
    }

    /// The kind of a decoded property object, or `None` when it is not a claim or
    /// evidence node (including when it carries no string `type` at all).
    pub fn of(properties: &Value) -> Option<Self> {
        properties
            .get(TYPE_KEY)
            .and_then(Value::as_str)
            .and_then(Self::from_label)
    }
}

/// Why a claim or evidence property object is not a valid typed node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "snake_case")]
pub enum EpistemicNodeError {
    /// The object is not a JSON object.
    NotAnObject,
    /// The `type` label is absent or names something else.
    WrongKind {
        expected: EpistemicNodeKind,
        found: Option<String>,
    },
    /// A required core field is absent or has the wrong JSON type.
    Malformed { detail: String },
    /// A required text field is empty.
    EmptyField { field: String },
    /// `confidence` is not a finite number in `[0, 1]`.
    ConfidenceOutOfRange,
    /// The typed node could not be serialized to a property object.
    Unencodable { detail: String },
}

impl std::fmt::Display for EpistemicNodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotAnObject => f.write_str("epistemic node properties are not an object"),
            Self::WrongKind { expected, found } => write!(
                f,
                "expected a `{}` node, found type {found:?}",
                expected.label()
            ),
            Self::Malformed { detail } => write!(f, "malformed epistemic node: {detail}"),
            Self::EmptyField { field } => write!(f, "epistemic node field `{field}` is empty"),
            Self::ConfidenceOutOfRange => {
                f.write_str("epistemic node confidence must be a finite value in [0, 1]")
            }
            Self::Unencodable { detail } => write!(f, "epistemic node is unencodable: {detail}"),
        }
    }
}

impl std::error::Error for EpistemicNodeError {}

/// The calibrated interval a claim may carry (the stored twin of
/// `eg_epistemic::Calibration`): a central credible interval, the probability mass it
/// covers, and how many pieces of evidence fed it.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimCalibration {
    pub interval: (f64, f64),
    pub level: f64,
    pub evidence_count: usize,
}

/// A stored claim: an assertion `about` one subject, produced by a `family` of
/// derivation, carrying a prior `confidence` and a `validation_state`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Claim {
    /// The derivation family that produced the claim (e.g. `association_rules`).
    pub family: String,
    /// The subject the claim is about (a node id or result reference).
    pub about: String,
    /// The stored prior confidence in `[0, 1]`.
    pub confidence: f64,
    /// Where the claim is in validation (`unvalidated` on a fresh write).
    pub validation_state: String,
    /// The calibrated interval, or `None` when no calibration signal was computed —
    /// stored as an explicit `null` so the absence is recorded, not implied.
    #[serde(default)]
    pub calibration: Option<ClaimCalibration>,
    /// Ids whose change or removal invalidates this claim.
    #[serde(default)]
    pub invalidation_deps: Vec<String>,
    /// Every other stored key (lineage, domain payload), preserved verbatim.
    #[serde(flatten)]
    pub attributes: BTreeMap<String, Value>,
}

/// A stored piece of evidence: an observation `about` one subject, attributed to a
/// `provenance`, carrying its own `confidence`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Evidence {
    /// The derivation family the evidence belongs to.
    pub family: String,
    /// The subject the evidence is about.
    pub about: String,
    /// Where the evidence came from (a source label, `job:<id>`, a backend digest).
    pub provenance: String,
    /// The evidence's own confidence in `[0, 1]`.
    pub confidence: f64,
    /// Where the evidence is in validation.
    pub validation_state: String,
    /// Every other stored key, preserved verbatim.
    #[serde(flatten)]
    pub attributes: BTreeMap<String, Value>,
}

/// A decoded epistemic node.
#[derive(Clone, Debug, PartialEq)]
pub enum EpistemicNode {
    Claim(Claim),
    Evidence(Evidence),
}

impl EpistemicNode {
    /// The node's kind.
    pub fn kind(&self) -> EpistemicNodeKind {
        match self {
            Self::Claim(_) => EpistemicNodeKind::Claim,
            Self::Evidence(_) => EpistemicNodeKind::Evidence,
        }
    }

    /// Decode a node's property object. `Ok(None)` for a node that is neither a claim
    /// nor evidence; `Err` for one that says it is but does not have the shape.
    pub fn from_properties(properties: &Value) -> Result<Option<Self>, EpistemicNodeError> {
        match EpistemicNodeKind::of(properties) {
            None => Ok(None),
            Some(EpistemicNodeKind::Claim) => Claim::from_properties(properties)
                .map(Self::Claim)
                .map(Some),
            Some(EpistemicNodeKind::Evidence) => Evidence::from_properties(properties)
                .map(Self::Evidence)
                .map(Some),
        }
    }

    /// The stored property object, `type` label included.
    pub fn to_properties(&self) -> Result<Value, EpistemicNodeError> {
        match self {
            Self::Claim(claim) => claim.to_properties(),
            Self::Evidence(evidence) => evidence.to_properties(),
        }
    }
}

/// The stored keys a claim's typed fields own; no attribute may reuse one.
const CLAIM_KEYS: [&str; 7] = [
    TYPE_KEY,
    "family",
    "about",
    "confidence",
    "validation_state",
    "calibration",
    "invalidation_deps",
];
/// The stored keys an evidence node's typed fields own.
const EVIDENCE_KEYS: [&str; 6] = [
    TYPE_KEY,
    "family",
    "about",
    "provenance",
    "confidence",
    "validation_state",
];

impl Claim {
    /// A fresh claim with no calibration, no invalidation dependencies and no
    /// attributes.
    pub fn new(
        family: impl Into<String>,
        about: impl Into<String>,
        confidence: f64,
        validation_state: impl Into<String>,
    ) -> Self {
        Self {
            family: family.into(),
            about: about.into(),
            confidence,
            validation_state: validation_state.into(),
            calibration: None,
            invalidation_deps: Vec::new(),
            attributes: BTreeMap::new(),
        }
    }

    /// Attach the calibrated interval (`None` records an explicit absence).
    pub fn with_calibration(mut self, calibration: Option<ClaimCalibration>) -> Self {
        self.calibration = calibration;
        self
    }

    /// Attach the ids whose change invalidates this claim.
    pub fn with_invalidation_deps<I, S>(mut self, deps: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.invalidation_deps = deps.into_iter().map(Into::into).collect();
        self
    }

    /// Merge a JSON object of extra attributes (lineage, domain payload).
    pub fn with_attributes(mut self, attributes: Value) -> Result<Self, EpistemicNodeError> {
        merge_attributes(&mut self.attributes, attributes)?;
        Ok(self)
    }

    /// Decode and validate a `"Claim"` property object.
    pub fn from_properties(properties: &Value) -> Result<Self, EpistemicNodeError> {
        let claim: Self = decode_kind(properties, EpistemicNodeKind::Claim)?;
        claim.validate()?;
        Ok(claim)
    }

    /// Validate, then produce the stored property object, `type` label included.
    pub fn to_properties(&self) -> Result<Value, EpistemicNodeError> {
        self.validate()?;
        encode_kind(self, EpistemicNodeKind::Claim)
    }

    /// Check the core fields a reader relies on, and that no attribute shadows one.
    pub fn validate(&self) -> Result<(), EpistemicNodeError> {
        require_text("family", &self.family)?;
        require_text("about", &self.about)?;
        require_text("validation_state", &self.validation_state)?;
        require_confidence(self.confidence)?;
        reject_shadowing(&self.attributes, &CLAIM_KEYS)
    }
}

impl Evidence {
    /// A fresh evidence node with no attributes.
    pub fn new(
        family: impl Into<String>,
        about: impl Into<String>,
        provenance: impl Into<String>,
        confidence: f64,
        validation_state: impl Into<String>,
    ) -> Self {
        Self {
            family: family.into(),
            about: about.into(),
            provenance: provenance.into(),
            confidence,
            validation_state: validation_state.into(),
            attributes: BTreeMap::new(),
        }
    }

    /// Merge a JSON object of extra attributes.
    pub fn with_attributes(mut self, attributes: Value) -> Result<Self, EpistemicNodeError> {
        merge_attributes(&mut self.attributes, attributes)?;
        Ok(self)
    }

    /// Decode and validate an `"Evidence"` property object.
    pub fn from_properties(properties: &Value) -> Result<Self, EpistemicNodeError> {
        let evidence: Self = decode_kind(properties, EpistemicNodeKind::Evidence)?;
        evidence.validate()?;
        Ok(evidence)
    }

    /// Validate, then produce the stored property object, `type` label included.
    pub fn to_properties(&self) -> Result<Value, EpistemicNodeError> {
        self.validate()?;
        encode_kind(self, EpistemicNodeKind::Evidence)
    }

    /// Check the core fields a reader relies on, and that no attribute shadows one.
    pub fn validate(&self) -> Result<(), EpistemicNodeError> {
        require_text("family", &self.family)?;
        require_text("about", &self.about)?;
        require_text("provenance", &self.provenance)?;
        require_text("validation_state", &self.validation_state)?;
        require_confidence(self.confidence)?;
        reject_shadowing(&self.attributes, &EVIDENCE_KEYS)
    }
}

/// Check the `type` label, strip it, and deserialize the remaining object.
fn decode_kind<T: serde::de::DeserializeOwned>(
    properties: &Value,
    expected: EpistemicNodeKind,
) -> Result<T, EpistemicNodeError> {
    let object = properties
        .as_object()
        .ok_or(EpistemicNodeError::NotAnObject)?;
    let found = object.get(TYPE_KEY).and_then(Value::as_str);
    if found != Some(expected.label()) {
        return Err(EpistemicNodeError::WrongKind {
            expected,
            found: found.map(str::to_string),
        });
    }
    let mut body: Map<String, Value> = object.clone();
    body.remove(TYPE_KEY);
    serde_json::from_value(Value::Object(body)).map_err(|error| EpistemicNodeError::Malformed {
        detail: error.to_string(),
    })
}

/// Serialize a validated typed node and stamp its `type` label.
fn encode_kind<T: Serialize>(
    node: &T,
    kind: EpistemicNodeKind,
) -> Result<Value, EpistemicNodeError> {
    let unencodable = |detail: String| EpistemicNodeError::Unencodable { detail };
    let Value::Object(mut object) =
        serde_json::to_value(node).map_err(|error| unencodable(error.to_string()))?
    else {
        return Err(unencodable(
            "a typed node did not serialize to an object".into(),
        ));
    };
    object.insert(
        TYPE_KEY.to_string(),
        Value::String(kind.label().to_string()),
    );
    Ok(Value::Object(object))
}

fn merge_attributes(
    into: &mut BTreeMap<String, Value>,
    attributes: Value,
) -> Result<(), EpistemicNodeError> {
    let Value::Object(object) = attributes else {
        return Err(EpistemicNodeError::Malformed {
            detail: "attributes must be a JSON object".into(),
        });
    };
    into.extend(object);
    Ok(())
}

/// An attribute under a typed field's key would silently overwrite that field in the
/// stored object (serde flattens the map after the fields), so it is refused.
fn reject_shadowing(
    attributes: &BTreeMap<String, Value>,
    owned: &[&str],
) -> Result<(), EpistemicNodeError> {
    match owned.iter().find(|key| attributes.contains_key(**key)) {
        Some(key) => Err(EpistemicNodeError::Unencodable {
            detail: format!("attribute `{key}` shadows a typed field"),
        }),
        None => Ok(()),
    }
}

fn require_text(field: &str, value: &str) -> Result<(), EpistemicNodeError> {
    if value.trim().is_empty() {
        return Err(EpistemicNodeError::EmptyField {
            field: field.to_string(),
        });
    }
    Ok(())
}

fn require_confidence(confidence: f64) -> Result<(), EpistemicNodeError> {
    if confidence.is_finite() && (0.0..=1.0).contains(&confidence) {
        return Ok(());
    }
    Err(EpistemicNodeError::ConfidenceOutOfRange)
}

#[cfg(test)]
mod tests;
