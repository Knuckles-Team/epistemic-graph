//! Schema drift against an approved record contract: classification,
//! containment, repair proposal and activation for one delivery.
//!
//! A source's approved [`super::repair::RecordContract`] bounds what its
//! deliveries may contain. For each delivery the engine infers the record
//! shape from the sampled records ([`InferredRecordShape::infer`]) and takes
//! its per-delivery digest under the canonical
//! [`crate::contract::Digest256`] framing
//! ([`InferredRecordShape::shape_digest`]). [`classify`] then places the
//! delivery against the approved contract in the closed five-way
//! [`DriftClass`]:
//!
//! * [`DriftClass::Compatible`] — the shape fits the contract;
//! * [`DriftClass::Widening`] — the shape strictly adds (a type, or an
//!   optional field);
//! * [`DriftClass::Narrowing`] — the shape strictly drops (a type, or an
//!   optional field) or gains a mandatory one;
//! * [`DriftClass::Rename`] — exactly one field left and exactly one arrived
//!   with the same type signature;
//! * [`DriftClass::Breaking`] — a required field or value is gone, or a type
//!   set changed neither as a superset nor a subset.
//!
//! The operator-declared [`EvolutionPolicy`] decides the action: every policy
//! quarantines `rename` and `breaking` drift, and the default
//! ([`EvolutionPolicy::Quarantine`]) quarantines everything that is not
//! compatible. A quarantined drift never applies: it becomes a
//! [`RepairProposal`] — the inferred shape as the repair's typed contract,
//! with the SHACL [`super::repair::render_repair_shapes`] renders and the
//! candidate digest
//! [`super::approval::approved_candidate_digest`] binds — and activation of
//! that candidate is ONLY the existing gate: the shadow-branch step order of
//! [`super::shadow_branch::SchemaRepairActivation`] plus the approval check
//! of [`super::approval::verify_schema_approval`] (two-person, DK R027). This
//! module adds no activation path of its own.
//!
//! The [`DriftReport`] is observation-class evidence: the two digests, the
//! class, the policy decision and the per-field detail. It mutates nothing.
//!
//! Complexity: one merge pass over the two already-sorted field sets plus one
//! SHA-256 over the shape's framed bytes — O(fields), allocation-light.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::approval::{approved_candidate_digest, validate_approved_source_id};
use super::repair::{
    render_repair_shapes, FieldContract, JsonType, RecordContract, MAX_CONTRACT_FIELD_BYTES,
};
use super::shadow_branch::{SchemaRepairActivation, SchemaRepairStep};
use crate::contract::Digest256;

/// Digest domain of the per-delivery inferred shape.
pub const SHAPE_DIGEST_DOMAIN: &str = "eg/schema-drift-shape/v1";

/// The closed five-way drift class of one delivery against its approved
/// contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DriftClass {
    /// The shape fits the approved contract.
    Compatible,
    /// The shape strictly adds: types the contract does not name, or a field
    /// the contract does not declare that is not present in every record.
    Widening,
    /// The shape strictly drops: types the contract names that no longer
    /// arrive, an optional field that no longer arrives, or a field that the
    /// contract does not declare but every record carries.
    Narrowing,
    /// Exactly one declared field left and exactly one undeclared field
    /// arrived, with the same type signature: the field was renamed.
    Rename,
    /// A required field or required value is gone, or a type set changed
    /// neither as a superset nor a subset of the approved one.
    Breaking,
}

impl DriftClass {
    /// Most severe last.
    fn severity(self) -> u8 {
        match self {
            Self::Compatible => 0,
            Self::Widening => 1,
            Self::Narrowing => 2,
            Self::Rename => 3,
            Self::Breaking => 4,
        }
    }
}

/// The operator-declared evolution policy. Quarantine is the default: an
/// undeclared policy quarantines every drifted delivery, so drift never
/// applies itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum EvolutionPolicy {
    /// Every drifted delivery is quarantined; only an approved repair may
    /// activate a new contract.
    #[default]
    Quarantine,
    /// Compatible and widening drifts update the contract in place;
    /// narrowing, rename and breaking drifts are quarantined.
    ApplyWidening,
    /// Everything except rename and breaking drift updates the contract in
    /// place; rename and breaking drifts are quarantined.
    ApplyMonotonic,
}

/// What the policy does with one classified delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DriftAction {
    /// The shape fits the approved contract; the delivery is accepted as-is.
    Accepted,
    /// The policy applies the drift to the contract in place; no approval
    /// needed.
    Applied,
    /// The drift is held back: the delivery is quarantined, a repair is
    /// proposed, and activation requires the approval gate.
    Quarantined,
}

/// One field's contribution to the classification, over the union of the two
/// field sets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum FieldDrift {
    /// Declared and observed with the same type set; at most an optional
    /// presence change.
    Unchanged,
    /// The observed types strictly contain the approved types.
    Widened,
    /// The approved types strictly contain the observed types.
    Narrowed,
    /// The type sets changed neither as a superset nor a subset.
    Retyped,
    /// A required field no longer arrives.
    RequiredMissing,
    /// An optional field no longer arrives.
    OptionalMissing,
    /// An undeclared field arrives in every record.
    NewRequired,
    /// An undeclared field arrives in some records.
    NewOptional,
    /// The declared field a rename leaves behind.
    RenamedOut,
    /// The undeclared field a rename brings in.
    RenamedIn,
}

/// One field as the delivery actually showed it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct InferredField {
    /// The JSON types observed, in canonical (sorted) order, unique.
    pub types: Vec<JsonType>,
    /// Present in every sampled record.
    pub always_present: bool,
}

/// The record shape one delivery inferred from its sampled records.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct InferredRecordShape {
    pub fields: BTreeMap<String, InferredField>,
}

impl InferredRecordShape {
    /// Infer the shape from the delivery's sampled records (at least one).
    ///
    /// `always_present` means present in EVERY sample; a field seen in only
    /// some samples is optional from the delivery's side.
    pub fn infer(samples: &[serde_json::Value]) -> Result<Self, String> {
        if samples.is_empty() {
            return Err("shape inference needs at least one sampled record".to_string());
        }
        let mut acc: BTreeMap<String, FieldAccumulator> = BTreeMap::new();
        for (index, sample) in samples.iter().enumerate() {
            let Some(records) = sample.as_object() else {
                return Err(format!("sample {index} is not a JSON object record"));
            };
            for (name, value) in records {
                validate_field_name(name)?;
                let field = acc.entry(name.clone()).or_default();
                field.present += 1;
                field.types.insert(json_type_of(value));
            }
        }
        let total = samples.len();
        Ok(Self {
            fields: acc
                .into_iter()
                .map(|(name, field)| {
                    (
                        name,
                        InferredField {
                            types: field.types.into_iter().collect(),
                            always_present: field.present == total,
                        },
                    )
                })
                .collect(),
        })
    }

    /// The per-delivery shape digest under the [`Digest256`] framing.
    ///
    /// The framing field is a compact, deterministic encoding of the shape —
    /// per field (sorted by name): the name (length-prefixed), the type
    /// count, the type discriminants, and the presence bit — so the digest
    /// costs one bounded buffer and one SHA-256, not a JSON serialization.
    pub fn shape_digest(&self) -> String {
        let mut shape = Vec::with_capacity(self.fields.len() * 24);
        for (name, field) in &self.fields {
            shape.extend_from_slice(&(name.len() as u16).to_be_bytes());
            shape.extend_from_slice(name.as_bytes());
            shape.push(field.types.len() as u8);
            for kind in &field.types {
                shape.push(json_type_discriminant(kind));
            }
            shape.push(u8::from(field.always_present));
        }
        Digest256::framed(SHAPE_DIGEST_DOMAIN.as_bytes(), &[&shape])
            .expect("shape digest framing bounds are unreachable for a bounded shape")
            .to_hex()
    }

    /// The inferred shape as the repair's typed record contract: the observed
    /// types, required when present in every sampled record.
    pub fn to_record_contract(&self) -> RecordContract {
        RecordContract {
            fields: self
                .fields
                .iter()
                .map(|(name, field)| {
                    (
                        name.clone(),
                        FieldContract {
                            required: field.always_present,
                            types: field.types.clone(),
                        },
                    )
                })
                .collect(),
        }
    }
}

#[derive(Default)]
struct FieldAccumulator {
    types: BTreeSet<JsonType>,
    present: usize,
}

fn validate_field_name(name: &str) -> Result<(), String> {
    let printable = !name.is_empty() && !name.chars().any(char::is_control);
    if !printable || name.len() > MAX_CONTRACT_FIELD_BYTES {
        return Err(format!(
            "record shape field names are printable and 1..={MAX_CONTRACT_FIELD_BYTES} bytes"
        ));
    }
    Ok(())
}

/// The shape-digest discriminant of a JSON type (declaration order).
fn json_type_discriminant(kind: &JsonType) -> u8 {
    match kind {
        JsonType::Null => 0,
        JsonType::Boolean => 1,
        JsonType::Integer => 2,
        JsonType::Number => 3,
        JsonType::String => 4,
        JsonType::Array => 5,
        JsonType::Object => 6,
    }
}

fn json_type_of(value: &serde_json::Value) -> JsonType {
    match value {
        serde_json::Value::Null => JsonType::Null,
        serde_json::Value::Bool(_) => JsonType::Boolean,
        serde_json::Value::Number(number) => {
            if number.is_i64() || number.is_u64() {
                JsonType::Integer
            } else {
                JsonType::Number
            }
        }
        serde_json::Value::String(_) => JsonType::String,
        serde_json::Value::Array(_) => JsonType::Array,
        serde_json::Value::Object(_) => JsonType::Object,
    }
}

/// Whether `candidate` contains every type in `of` (of ⊆ candidate).
fn contains_all(candidate: &[JsonType], of: &[JsonType]) -> bool {
    of.iter().all(|kind| candidate.contains(kind))
}

/// Strict superset: `candidate` ⊇ `of` and `candidate` ≠ `of`.
fn is_strict_superset(candidate: &[JsonType], of: &[JsonType]) -> bool {
    contains_all(candidate, of) && candidate != of
}

fn worst(current: DriftClass, candidate: DriftClass) -> DriftClass {
    if candidate.severity() > current.severity() {
        candidate
    } else {
        current
    }
}

/// Observation-class evidence of one delivery's drift against its approved
/// contract: the two digests, the class, the policy decision and the
/// per-field detail. It mutates nothing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DriftReport {
    /// The approved contract's identity: the digest an approval grant binds
    /// for `source_id` and this contract.
    pub approved_digest: String,
    /// The delivery's inferred shape digest.
    pub shape_digest: String,
    pub class: DriftClass,
    pub policy: EvolutionPolicy,
    pub action: DriftAction,
    /// Per-field detail over the union of the two field sets.
    pub fields: BTreeMap<String, FieldDrift>,
}

/// One union field in name order: the shared fields' drift is decided on the
/// spot, the removed/added fields' drift waits for the rename decision.
enum UnionField<'a> {
    /// Declared and observed.
    Shared { name: &'a str, drift: FieldDrift },
    /// Declared but not observed.
    Removed {
        name: &'a str,
        spec: &'a FieldContract,
    },
    /// Observed but not declared.
    Added {
        name: &'a str,
        observed: &'a InferredField,
    },
}

/// Which side of the merge walk supplies the next union field.
enum MergeStep {
    Shared,
    Removed,
    Added,
}

/// Classify `shape` (one delivery) against `approved` under `policy`.
///
/// `approved_digest` is the contract in force's identity —
/// [`super::approval::approved_candidate_digest`] of the `approved:<name>`
/// key and this contract — supplied pre-computed: it is stable per contract,
/// not per delivery.
///
/// O(fields): one merge pass over the two already-sorted field sets (one
/// name comparison per union field, no tree lookups), one bounded buffer and
/// one SHA-256 for the per-delivery shape digest.
pub fn classify(
    approved: &RecordContract,
    approved_digest: &str,
    shape: &InferredRecordShape,
    policy: EvolutionPolicy,
) -> DriftReport {
    // Both field sets are BTreeMaps, hence already in name order: a merge
    // pass over the two iterators decides every union field in one pass.
    let mut a = approved.fields.iter();
    let mut s = shape.fields.iter();
    let mut a_ent = a.next();
    let mut s_ent = s.next();

    let mut union: Vec<UnionField<'_>> =
        Vec::with_capacity(approved.fields.len() + shape.fields.len());
    let mut removed: Vec<(&str, &FieldContract)> = Vec::new();
    let mut added: Vec<(&str, &InferredField)> = Vec::new();
    let mut class = DriftClass::Compatible;

    while a_ent.is_some() || s_ent.is_some() {
        let step = match (&a_ent, &s_ent) {
            (Some((name, _)), Some((sname, _))) if name == sname => MergeStep::Shared,
            (Some((name, _)), Some((sname, _))) if name.as_str() < sname.as_str() => {
                MergeStep::Removed
            }
            (Some(_), Some(_)) => MergeStep::Added,
            (Some(_), None) => MergeStep::Removed,
            (None, Some(_)) => MergeStep::Added,
            (None, None) => unreachable!("the pass ends when both sets are done"),
        };
        match step {
            MergeStep::Shared => {
                let (name, spec) = a_ent.take().expect("the step decided shared");
                let (_, observed) = s_ent.take().expect("the step decided shared");
                let field_class = classify_field(spec, observed);
                union.push(UnionField::Shared {
                    name: name.as_str(),
                    drift: match field_class {
                        DriftClass::Compatible => FieldDrift::Unchanged,
                        DriftClass::Widening => FieldDrift::Widened,
                        DriftClass::Narrowing => FieldDrift::Narrowed,
                        DriftClass::Breaking => FieldDrift::Retyped,
                        _ => unreachable!("shared-field drift is never rename"),
                    },
                });
                class = worst(class, field_class);
                a_ent = a.next();
                s_ent = s.next();
            }
            MergeStep::Removed => {
                let (name, spec) = a_ent.take().expect("the step decided removed");
                union.push(UnionField::Removed {
                    name: name.as_str(),
                    spec,
                });
                removed.push((name.as_str(), spec));
                a_ent = a.next();
            }
            MergeStep::Added => {
                let (name, observed) = s_ent.take().expect("the step decided added");
                union.push(UnionField::Added {
                    name: name.as_str(),
                    observed,
                });
                added.push((name.as_str(), observed));
                s_ent = s.next();
            }
        }
    }

    // The removed/added drift depends on the rename decision, which needs
    // the FULL removed/added pair: finalize every union field in name order
    // and build the per-field detail map in one bulk pass.
    let rename = removed.len() == 1 && added.len() == 1 && same_signature(removed[0].1, added[0].1);
    let fields: BTreeMap<String, FieldDrift> = union
        .into_iter()
        .map(|entry| match entry {
            UnionField::Shared { name, drift } => (name.to_string(), drift),
            UnionField::Removed { name, spec } => {
                let drift = if rename {
                    FieldDrift::RenamedOut
                } else if spec.required {
                    class = worst(class, DriftClass::Breaking);
                    FieldDrift::RequiredMissing
                } else {
                    class = worst(class, DriftClass::Narrowing);
                    FieldDrift::OptionalMissing
                };
                (name.to_string(), drift)
            }
            UnionField::Added { name, observed } => {
                let drift = if rename {
                    FieldDrift::RenamedIn
                } else if observed.always_present {
                    class = worst(class, DriftClass::Narrowing);
                    FieldDrift::NewRequired
                } else {
                    class = worst(class, DriftClass::Widening);
                    FieldDrift::NewOptional
                };
                (name.to_string(), drift)
            }
        })
        .collect();
    if rename {
        class = worst(class, DriftClass::Rename);
    }

    DriftReport {
        approved_digest: approved_digest.to_string(),
        shape_digest: shape.shape_digest(),
        class,
        policy,
        action: decide(class, policy),
        fields,
    }
}

/// Classify one delivery under the `approved:<name>` key `source_id`,
/// computing the contract in force's identity from the key and the contract.
/// The hot path is [`classify`], which takes the identity pre-computed.
pub fn classify_delivery(
    source_id: &str,
    approved: &RecordContract,
    shape: &InferredRecordShape,
    policy: EvolutionPolicy,
) -> DriftReport {
    classify(
        approved,
        &approved_candidate_digest(source_id, approved),
        shape,
        policy,
    )
}

/// One shared field: presence, then the type set.
fn classify_field(spec: &FieldContract, observed: &InferredField) -> DriftClass {
    if spec.types == observed.types {
        if spec.required && !observed.always_present {
            // A value the contract requires sometimes does not arrive.
            return DriftClass::Breaking;
        }
        return DriftClass::Compatible;
    }
    match (
        is_strict_superset(&observed.types, &spec.types),
        is_strict_superset(&spec.types, &observed.types),
    ) {
        (true, false) => DriftClass::Widening,
        (false, true) => DriftClass::Narrowing,
        (_, _) => DriftClass::Breaking,
    }
}

/// The rename signature: the approved type set, ignoring presence — a rename
/// is a name change, and presence is re-decided by the repair proposal.
fn same_signature(spec: &FieldContract, observed: &InferredField) -> bool {
    spec.types == observed.types
}

/// The policy's decision for one class.
fn decide(class: DriftClass, policy: EvolutionPolicy) -> DriftAction {
    match class {
        DriftClass::Compatible => DriftAction::Accepted,
        DriftClass::Widening => match policy {
            EvolutionPolicy::Quarantine => DriftAction::Quarantined,
            EvolutionPolicy::ApplyWidening | EvolutionPolicy::ApplyMonotonic => {
                DriftAction::Applied
            }
        },
        DriftClass::Narrowing => match policy {
            EvolutionPolicy::Quarantine | EvolutionPolicy::ApplyWidening => {
                DriftAction::Quarantined
            }
            EvolutionPolicy::ApplyMonotonic => DriftAction::Applied,
        },
        // A rename breaks every reference to the old name, and a breaking
        // drift never applies itself: only the approval gate activates them.
        DriftClass::Rename | DriftClass::Breaking => DriftAction::Quarantined,
    }
}

/// A repair candidate for a quarantined delivery: the inferred shape as the
/// repair's typed contract, the SHACL EG renders for it (AUD-27: callers
/// never send RDF) and the candidate digest the approval grant must bind.
/// Activation of this candidate is only the existing gate — the
/// shadow-branch step order plus the approval check — nothing new here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RepairProposal {
    /// The `approved:<name>` key the candidate attaches under.
    pub source_id: String,
    pub candidate: RecordContract,
    /// The SHACL document EG renders for `candidate`.
    pub shapes_ttl: String,
    /// The digest an approval grant must bind to activate `candidate`.
    pub candidate_digest: String,
}

impl RepairProposal {
    /// Build the proposal from a quarantined delivery's inferred shape.
    /// Refused unless `source_id` is an `approved:<name>` key and the inferred
    /// shape converts to a valid record contract.
    pub fn new(source_id: &str, shape: &InferredRecordShape) -> Result<Self, String> {
        validate_approved_source_id(source_id)?;
        let candidate = shape.to_record_contract();
        candidate.validate()?;
        let name = source_id
            .strip_prefix(super::approval::APPROVED_SOURCE_PREFIX)
            .expect("validated as the approved:<name> namespace");
        Ok(Self {
            source_id: source_id.to_string(),
            shapes_ttl: render_repair_shapes(name, &candidate),
            candidate_digest: approved_candidate_digest(source_id, &candidate),
            candidate,
        })
    }
}

/// Seed the existing activation gate for a proposal: the mapping decision is
/// done (this candidate exists). The ABox check, shadow-branch ingest and
/// approval steps must still be recorded — in
/// [`SchemaRepairActivation::REQUIRED_ORDER`] — before
/// [`SchemaRepairActivation::checked_for_activation`] admits activation, and
/// the approval itself must pass
/// [`super::approval::verify_schema_approval`].
pub fn seed_activation(proposal: &RepairProposal) -> SchemaRepairActivation {
    SchemaRepairActivation {
        candidate_digest: proposal.candidate_digest.clone(),
        completed: vec![SchemaRepairStep::MappingDecided],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn contract(pairs: &[(&str, bool, &[JsonType])]) -> RecordContract {
        RecordContract {
            fields: pairs
                .iter()
                .map(|(name, required, types)| {
                    (
                        (*name).to_string(),
                        FieldContract {
                            required: *required,
                            types: types.to_vec(),
                        },
                    )
                })
                .collect(),
        }
    }

    fn shape(fields: &[(&str, &[JsonType], bool)]) -> InferredRecordShape {
        InferredRecordShape {
            fields: fields
                .iter()
                .map(|(name, types, always_present)| {
                    (
                        (*name).to_string(),
                        InferredField {
                            types: types.to_vec(),
                            always_present: *always_present,
                        },
                    )
                })
                .collect(),
        }
    }

    // spec: EG-TYPED-PACKS-R710
    #[test]
    fn a_shape_that_fits_the_contract_is_compatible_and_accepted() {
        let approved = contract(&[
            ("name", true, &[JsonType::String]),
            ("note", false, &[JsonType::Null, JsonType::Integer]),
        ]);
        // Every observed type is an approved type and every required value
        // arrives: the shape fits.
        let drifted = shape(&[
            ("name", &[JsonType::String], true),
            ("note", &[JsonType::Null, JsonType::Integer], true),
        ]);
        let report = classify_delivery(
            "approved:src",
            &approved,
            &drifted,
            EvolutionPolicy::default(),
        );
        assert_eq!(report.class, DriftClass::Compatible);
        assert_eq!(report.action, DriftAction::Accepted);
        assert_eq!(report.fields.get("name"), Some(&FieldDrift::Unchanged));
        assert_eq!(report.fields.get("note"), Some(&FieldDrift::Unchanged));
        // A presence-only change on an optional field still fits.
        let approved = contract(&[("tag", false, &[JsonType::String])]);
        let drifted = shape(&[("tag", &[JsonType::String], true)]);
        let report = classify_delivery(
            "approved:src",
            &approved,
            &drifted,
            EvolutionPolicy::default(),
        );
        assert_eq!(report.class, DriftClass::Compatible);
        assert_eq!(report.action, DriftAction::Accepted);
    }

    // spec: EG-TYPED-PACKS-R710
    #[test]
    fn a_new_type_or_an_optional_field_is_widening() {
        let approved = contract(&[("name", true, &[JsonType::String])]);
        // Observed types strictly contain the approved ones.
        let wider_types = shape(&[("name", &[JsonType::Integer, JsonType::String], true)]);
        let report = classify_delivery(
            "approved:src",
            &approved,
            &wider_types,
            EvolutionPolicy::default(),
        );
        assert_eq!(report.class, DriftClass::Widening);
        assert_eq!(report.fields.get("name"), Some(&FieldDrift::Widened));
        // An undeclared field seen in only SOME records is optional from the
        // delivery's side: additive.
        let wider_fields = shape(&[
            ("name", &[JsonType::String], true),
            ("tag", &[JsonType::String], false),
        ]);
        let report = classify_delivery(
            "approved:src",
            &approved,
            &wider_fields,
            EvolutionPolicy::default(),
        );
        assert_eq!(report.class, DriftClass::Widening);
        assert_eq!(report.fields.get("tag"), Some(&FieldDrift::NewOptional));
        assert_eq!(report.action, DriftAction::Quarantined);
    }

    // spec: EG-TYPED-PACKS-R710
    #[test]
    fn a_missing_type_or_an_optional_field_is_narrowing() {
        let approved = contract(&[
            ("name", true, &[JsonType::String]),
            (
                "note",
                false,
                &[JsonType::Integer, JsonType::Null, JsonType::String],
            ),
        ]);
        // The delivery no longer sends nulls or strings for `note`.
        let narrower_types = shape(&[
            ("name", &[JsonType::String], true),
            ("note", &[JsonType::Integer], false),
        ]);
        let report = classify_delivery(
            "approved:src",
            &approved,
            &narrower_types,
            EvolutionPolicy::default(),
        );
        assert_eq!(report.class, DriftClass::Narrowing);
        assert_eq!(report.fields.get("note"), Some(&FieldDrift::Narrowed));
        // The optional `note` field is gone entirely.
        let narrower_fields = shape(&[("name", &[JsonType::String], true)]);
        let report = classify_delivery(
            "approved:src",
            &approved,
            &narrower_fields,
            EvolutionPolicy::default(),
        );
        assert_eq!(report.class, DriftClass::Narrowing);
        assert_eq!(
            report.fields.get("note"),
            Some(&FieldDrift::OptionalMissing)
        );
        assert_eq!(report.action, DriftAction::Quarantined);
    }

    // spec: EG-TYPED-PACKS-R710
    #[test]
    fn a_one_for_one_replacement_with_the_same_signature_is_rename() {
        let approved = contract(&[("name", true, &[JsonType::String])]);
        let renamed = shape(&[("title", &[JsonType::String], true)]);
        let report = classify_delivery(
            "approved:src",
            &approved,
            &renamed,
            EvolutionPolicy::default(),
        );
        assert_eq!(report.class, DriftClass::Rename);
        assert_eq!(report.fields.get("name"), Some(&FieldDrift::RenamedOut));
        assert_eq!(report.fields.get("title"), Some(&FieldDrift::RenamedIn));
        // A rename never applies itself, under any policy.
        let report = classify_delivery(
            "approved:src",
            &approved,
            &renamed,
            EvolutionPolicy::ApplyMonotonic,
        );
        assert_eq!(report.action, DriftAction::Quarantined);
    }

    // spec: EG-TYPED-PACKS-R710
    #[test]
    fn a_missing_required_field_or_a_retyped_field_is_breaking() {
        // A required field no longer arrives.
        let approved = contract(&[
            ("name", true, &[JsonType::String]),
            ("id", true, &[JsonType::Integer]),
        ]);
        let missing_required = shape(&[("name", &[JsonType::String], true)]);
        let report = classify_delivery(
            "approved:src",
            &approved,
            &missing_required,
            EvolutionPolicy::default(),
        );
        assert_eq!(report.class, DriftClass::Breaking);
        assert_eq!(report.fields.get("id"), Some(&FieldDrift::RequiredMissing));
        // The type set changed neither way: a retype.
        let approved = contract(&[("name", true, &[JsonType::String])]);
        let retyped = shape(&[("name", &[JsonType::Integer], true)]);
        let report = classify_delivery(
            "approved:src",
            &approved,
            &retyped,
            EvolutionPolicy::default(),
        );
        assert_eq!(report.class, DriftClass::Breaking);
        assert_eq!(report.fields.get("name"), Some(&FieldDrift::Retyped));
        // A required value that sometimes does not arrive.
        let flaky = shape(&[("name", &[JsonType::String], false)]);
        let report = classify_delivery(
            "approved:src",
            &approved,
            &flaky,
            EvolutionPolicy::default(),
        );
        assert_eq!(report.class, DriftClass::Breaking);
        assert_eq!(report.action, DriftAction::Quarantined);
    }

    // spec: EG-TYPED-PACKS-R710
    #[test]
    fn an_undeclared_policy_is_quarantine_and_quarantine_never_applies() {
        // The default the operator gets when the policy is absent.
        #[derive(Deserialize)]
        struct PolicySetting {
            #[serde(default)]
            policy: EvolutionPolicy,
        }
        let setting: PolicySetting = serde_json::from_value(json!({})).unwrap();
        assert_eq!(setting.policy, EvolutionPolicy::Quarantine);
        assert_eq!(EvolutionPolicy::default(), EvolutionPolicy::Quarantine);

        let approved = contract(&[
            ("name", true, &[JsonType::String]),
            ("note", false, &[JsonType::Null, JsonType::String]),
        ]);
        for drifted in [
            // widening
            shape(&[
                ("name", &[JsonType::Boolean, JsonType::String], true),
                ("note", &[JsonType::Null, JsonType::String], false),
            ]),
            // narrowing: the optional `note` is gone
            shape(&[("name", &[JsonType::String], true)]),
            // rename
            shape(&[
                ("title", &[JsonType::String], true),
                ("note", &[JsonType::Null, JsonType::String], false),
            ]),
            // breaking
            shape(&[
                ("name", &[JsonType::Integer], true),
                ("note", &[JsonType::Null, JsonType::String], false),
            ]),
        ] {
            let report = classify_delivery(
                "approved:src",
                &approved,
                &drifted,
                EvolutionPolicy::Quarantine,
            );
            assert_ne!(report.class, DriftClass::Compatible);
            assert_eq!(
                report.action,
                DriftAction::Quarantined,
                "{:?} drift must quarantine",
                report.class
            );
        }
    }

    // spec: EG-TYPED-PACKS-R710
    #[test]
    fn a_declared_policy_applies_only_the_classes_it_names() {
        let approved = contract(&[
            ("name", true, &[JsonType::String]),
            ("note", false, &[JsonType::Null, JsonType::String]),
        ]);
        let widening = shape(&[
            ("name", &[JsonType::Boolean, JsonType::String], true),
            ("note", &[JsonType::Null, JsonType::String], false),
        ]);
        // The optional `note` is gone entirely.
        let narrowing = shape(&[("name", &[JsonType::String], true)]);
        // `name` left, `title` arrived with the same signature.
        let rename = shape(&[
            ("title", &[JsonType::String], true),
            ("note", &[JsonType::Null, JsonType::String], false),
        ]);
        let breaking = shape(&[
            ("name", &[JsonType::Integer], true),
            ("note", &[JsonType::Null, JsonType::String], false),
        ]);
        let cases = [
            (widening, DriftClass::Widening),
            (narrowing, DriftClass::Narrowing),
            (rename, DriftClass::Rename),
            (breaking, DriftClass::Breaking),
        ];
        for (drifted, expected) in &cases {
            let applied = classify_delivery(
                "approved:src",
                &approved,
                drifted,
                EvolutionPolicy::ApplyWidening,
            );
            assert_eq!(applied.class, *expected);
            let monotonic = classify_delivery(
                "approved:src",
                &approved,
                drifted,
                EvolutionPolicy::ApplyMonotonic,
            );
            assert_eq!(monotonic.class, *expected);
            match expected {
                DriftClass::Widening => {
                    assert_eq!(applied.action, DriftAction::Applied);
                    assert_eq!(monotonic.action, DriftAction::Applied);
                }
                DriftClass::Narrowing => {
                    assert_eq!(applied.action, DriftAction::Quarantined);
                    assert_eq!(monotonic.action, DriftAction::Applied);
                }
                DriftClass::Rename | DriftClass::Breaking => {
                    assert_eq!(applied.action, DriftAction::Quarantined);
                    assert_eq!(monotonic.action, DriftAction::Quarantined);
                }
                DriftClass::Compatible => unreachable!("no compatible case here"),
            }
        }
    }

    // spec: EG-TYPED-PACKS-R710
    #[test]
    fn a_quarantined_drift_becomes_the_repair_proposal_the_approval_binds() {
        let approved = contract(&[("name", true, &[JsonType::String])]);
        let drifted = shape(&[("name", &[JsonType::Integer], true)]);
        let report = classify_delivery(
            "approved:src",
            &approved,
            &drifted,
            EvolutionPolicy::default(),
        );
        assert_eq!(report.action, DriftAction::Quarantined);
        assert_ne!(report.shape_digest, report.approved_digest);

        let proposal = RepairProposal::new("approved:src", &drifted).unwrap();
        assert_eq!(proposal.candidate, drifted.to_record_contract());
        assert!(proposal.candidate.validate().is_ok());
        assert_eq!(
            proposal.shapes_ttl,
            render_repair_shapes("src", &proposal.candidate)
        );
        assert_eq!(
            proposal.candidate_digest,
            approved_candidate_digest("approved:src", &proposal.candidate)
        );
        // A key outside the approved namespace is not a repair target.
        assert!(RepairProposal::new("admin:src", &drifted).is_err());
        // An empty shape has no contract to repair with.
        let empty = shape(&[]);
        assert!(RepairProposal::new("approved:src", &empty).is_err());
    }

    // spec: EG-TYPED-PACKS-R710
    #[test]
    fn the_seeded_activation_refuses_until_the_gate_completes_in_order() {
        let drifted = shape(&[("name", &[JsonType::Integer], true)]);
        let proposal = RepairProposal::new("approved:src", &drifted).unwrap();
        let seeded = seed_activation(&proposal);
        assert_eq!(seeded.candidate_digest, proposal.candidate_digest);
        let err = seeded
            .checked_for_activation()
            .expect_err("the gate must refuse a freshly seeded candidate");
        assert!(err.contains("AboxCheckPassed"), "{err}");
        // Approval recorded BEFORE the shadow-branch ingest is out of order.
        let out_of_order = SchemaRepairActivation {
            candidate_digest: proposal.candidate_digest.clone(),
            completed: vec![
                SchemaRepairStep::MappingDecided,
                SchemaRepairStep::AboxCheckPassed,
                SchemaRepairStep::Approved,
                SchemaRepairStep::ShadowBranchIngested,
            ],
        };
        let err = out_of_order
            .checked_for_activation()
            .expect_err("the gate must refuse an out-of-order candidate");
        assert!(err.contains("out of order"), "{err}");
        // The full order activates.
        let in_order = SchemaRepairActivation {
            candidate_digest: proposal.candidate_digest.clone(),
            completed: SchemaRepairActivation::REQUIRED_ORDER.to_vec(),
        };
        assert!(in_order.checked_for_activation().is_ok());
    }

    // spec: EG-TYPED-PACKS-R710
    #[test]
    fn infer_reads_presence_types_and_digests_each_delivery() {
        let samples = vec![
            json!({"name": "a", "id": 1}),
            json!({"name": "b", "id": 2.5, "tag": ["x"]}),
        ];
        let inferred = InferredRecordShape::infer(&samples).unwrap();
        assert_eq!(
            inferred.fields.get("name").unwrap().types,
            vec![JsonType::String]
        );
        assert!(inferred.fields.get("name").unwrap().always_present);
        assert_eq!(
            inferred.fields.get("id").unwrap().types,
            vec![JsonType::Integer, JsonType::Number]
        );
        assert!(inferred.fields.get("id").unwrap().always_present);
        assert_eq!(
            inferred.fields.get("tag").unwrap().types,
            vec![JsonType::Array]
        );
        assert!(!inferred.fields.get("tag").unwrap().always_present);
        // Two deliveries of the same shape have the same digest; a different
        // shape does not.
        let again = InferredRecordShape::infer(&samples).unwrap();
        assert_eq!(inferred.shape_digest(), again.shape_digest());
        let other = shape(&[("name", &[JsonType::String], true)]);
        assert_ne!(inferred.shape_digest(), other.shape_digest());
        assert!(InferredRecordShape::infer(&[]).is_err());
        assert!(InferredRecordShape::infer(&[json!(7)]).is_err());
    }
}
