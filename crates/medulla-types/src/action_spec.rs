//! Governed actions over the data plane (MEDULLA-UNIFIED-DATA-PLANE-R700). An
//! action declares typed parameters, submission criteria checked in order
//! and effects whose text is a template over the parameters. A submission
//! that fails a criterion, names an unknown parameter, mistypes one, or
//! writes back to an unattached source applies no effect.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// The type of one action parameter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParamType {
    String,
    Integer,
    Float,
    Boolean,
}

/// One typed parameter.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionParameter {
    pub name: String,
    pub ty: ParamType,
    pub required: bool,
}

/// A typed parameter value.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ParamValue {
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

impl ParamValue {
    fn type_of(&self) -> ParamType {
        match self {
            Self::Boolean(_) => ParamType::Boolean,
            Self::Integer(_) => ParamType::Integer,
            Self::Float(_) => ParamType::Float,
            Self::String(_) => ParamType::String,
        }
    }

    fn render(&self) -> String {
        match self {
            Self::Boolean(value) => value.to_string(),
            Self::Integer(value) => value.to_string(),
            Self::Float(value) => value.to_string(),
            Self::String(value) => value.clone(),
        }
    }

    fn compare(&self, other: &ParamValue) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::Integer(a), Self::Integer(b)) => Some(a.cmp(b)),
            (Self::Float(a), Self::Float(b)) => a.partial_cmp(b),
            (Self::Integer(a), Self::Float(b)) => (*a as f64).partial_cmp(b),
            (Self::Float(a), Self::Integer(b)) => a.partial_cmp(&(*b as f64)),
            (Self::String(a), Self::String(b)) => Some(a.cmp(b)),
            (Self::Boolean(a), Self::Boolean(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }
}

/// A comparison a criterion applies to one parameter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CriterionOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CriterionOp {
    fn holds(self, ordering: Option<std::cmp::Ordering>) -> bool {
        use std::cmp::Ordering::{Equal, Greater, Less};
        match (self, ordering) {
            (_, None) => false,
            (Self::Eq, Some(o)) => o == Equal,
            (Self::Ne, Some(o)) => o != Equal,
            (Self::Lt, Some(o)) => o == Less,
            (Self::Le, Some(o)) => o != Greater,
            (Self::Gt, Some(o)) => o == Greater,
            (Self::Ge, Some(o)) => o != Less,
        }
    }
}

/// One submission criterion: `param op value`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmissionCriterion {
    pub name: String,
    pub param: String,
    pub op: CriterionOp,
    pub value: ParamValue,
}

/// What an effect does.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum EffectKind {
    SetProperty,
    CreateNode,
    AddEdge,
    WriteBack { source: String },
}

/// One effect; `template` names parameters as `{{name}}`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionEffectSpec {
    pub kind: EffectKind,
    pub template: String,
}

/// A declared action.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionSpec {
    pub name: String,
    pub params: Vec<ActionParameter>,
    pub criteria: Vec<SubmissionCriterion>,
    pub effects: Vec<ActionEffectSpec>,
}

/// Where one action run ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionStatus {
    Applied,
    Refused,
}

/// A refused submission, with its stable code.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "code", content = "detail")]
pub enum ActionRefusal {
    CriterionFailed(String),
    UnknownParameter(String),
    MissingParameter(String),
    ParameterTypeMismatch(String),
    UnattachedWriteBack(String),
    InvalidSpec(String),
}

impl ActionRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::CriterionFailed(_) => "ACTION_CRITERION_FAILED",
            Self::UnknownParameter(_) => "ACTION_UNKNOWN_PARAMETER",
            Self::MissingParameter(_) => "ACTION_MISSING_PARAMETER",
            Self::ParameterTypeMismatch(_) => "ACTION_PARAMETER_TYPE_MISMATCH",
            Self::UnattachedWriteBack(_) => "ACTION_WRITE_BACK_UNATTACHED",
            Self::InvalidSpec(_) => "ACTION_SPEC_INVALID",
        }
    }
}

/// One criterion's verdict, as recorded.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CriterionVerdict {
    pub criterion: String,
    pub passed: bool,
}

/// One resolved effect.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedEffect {
    pub kind: EffectKind,
    pub text: String,
}

/// The stored record of one action run.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActionRun {
    pub action: String,
    pub params: BTreeMap<String, ParamValue>,
    pub verdicts: Vec<CriterionVerdict>,
    pub effects: Vec<ResolvedEffect>,
    pub status: ActionStatus,
    pub refusal: Option<ActionRefusal>,
}

/// Resolve `{{name}}` placeholders in one left-to-right pass.
pub fn resolve_template(
    template: &str,
    params: &BTreeMap<String, ParamValue>,
) -> Result<String, ActionRefusal> {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after
            .find("}}")
            .ok_or_else(|| ActionRefusal::InvalidSpec("unclosed placeholder".to_string()))?;
        let name = after[..end].trim();
        let value = params
            .get(name)
            .ok_or_else(|| ActionRefusal::UnknownParameter(name.to_string()))?;
        out.push_str(&value.render());
        rest = &after[end + 2..];
    }
    out.push_str(rest);
    Ok(out)
}

impl ActionSpec {
    fn check_params(&self, args: &BTreeMap<String, ParamValue>) -> Result<(), ActionRefusal> {
        let declared: BTreeMap<&str, &ActionParameter> =
            self.params.iter().map(|p| (p.name.as_str(), p)).collect();
        for (name, value) in args {
            let param = declared
                .get(name.as_str())
                .ok_or_else(|| ActionRefusal::UnknownParameter(name.clone()))?;
            if param.ty != value.type_of() {
                return Err(ActionRefusal::ParameterTypeMismatch(name.clone()));
            }
        }
        match self
            .params
            .iter()
            .find(|p| p.required && !args.contains_key(&p.name))
        {
            Some(missing) => Err(ActionRefusal::MissingParameter(missing.name.clone())),
            None => Ok(()),
        }
    }

    fn evaluate_criteria(
        &self,
        args: &BTreeMap<String, ParamValue>,
    ) -> (Vec<CriterionVerdict>, Option<ActionRefusal>) {
        let mut verdicts = Vec::with_capacity(self.criteria.len());
        for criterion in &self.criteria {
            let ordering = args
                .get(&criterion.param)
                .and_then(|v| v.compare(&criterion.value));
            let passed = criterion.op.holds(ordering);
            verdicts.push(CriterionVerdict {
                criterion: criterion.name.clone(),
                passed,
            });
            if !passed {
                return (
                    verdicts,
                    Some(ActionRefusal::CriterionFailed(criterion.name.clone())),
                );
            }
        }
        (verdicts, None)
    }

    fn resolve_effects(
        &self,
        args: &BTreeMap<String, ParamValue>,
        attached: &BTreeSet<String>,
    ) -> Result<Vec<ResolvedEffect>, ActionRefusal> {
        self.effects
            .iter()
            .map(|effect| {
                if let EffectKind::WriteBack { source } = &effect.kind {
                    if !attached.contains(source) {
                        return Err(ActionRefusal::UnattachedWriteBack(source.clone()));
                    }
                }
                Ok(ResolvedEffect {
                    kind: effect.kind.clone(),
                    text: resolve_template(&effect.template, args)?,
                })
            })
            .collect()
    }

    /// Submit `args`. Every refusal returns a `Refused` run with no effect;
    /// an `Applied` run carries every resolved effect, to commit together.
    pub fn submit(
        &self,
        args: BTreeMap<String, ParamValue>,
        attached_sources: &BTreeSet<String>,
    ) -> ActionRun {
        let refused = |verdicts, refusal, params| ActionRun {
            action: self.name.clone(),
            params,
            verdicts,
            effects: Vec::new(),
            status: ActionStatus::Refused,
            refusal: Some(refusal),
        };
        if let Err(refusal) = self.check_params(&args) {
            return refused(Vec::new(), refusal, args);
        }
        let (verdicts, failure) = self.evaluate_criteria(&args);
        if let Some(refusal) = failure {
            return refused(verdicts, refusal, args);
        }
        match self.resolve_effects(&args, attached_sources) {
            Ok(effects) => ActionRun {
                action: self.name.clone(),
                params: args,
                verdicts,
                effects,
                status: ActionStatus::Applied,
                refusal: None,
            },
            Err(refusal) => refused(verdicts, refusal, args),
        }
    }
}

/// The per-graph action registry, ordered by action name.
pub type ActionRegistry = BTreeMap<String, ActionSpec>;

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> ActionSpec {
        ActionSpec {
            name: "approve_refund".to_string(),
            params: vec![
                ActionParameter {
                    name: "order".to_string(),
                    ty: ParamType::String,
                    required: true,
                },
                ActionParameter {
                    name: "amount".to_string(),
                    ty: ParamType::Integer,
                    required: true,
                },
            ],
            criteria: vec![
                SubmissionCriterion {
                    name: "positive".to_string(),
                    param: "amount".to_string(),
                    op: CriterionOp::Gt,
                    value: ParamValue::Integer(0),
                },
                SubmissionCriterion {
                    name: "under_limit".to_string(),
                    param: "amount".to_string(),
                    op: CriterionOp::Le,
                    value: ParamValue::Integer(500),
                },
            ],
            effects: vec![
                ActionEffectSpec {
                    kind: EffectKind::SetProperty,
                    template: "order/{{order}} refund={{amount}}".to_string(),
                },
                ActionEffectSpec {
                    kind: EffectKind::WriteBack {
                        source: "erp".to_string(),
                    },
                    template: "UPDATE orders SET refunded={{amount}} WHERE id='{{order}}'"
                        .to_string(),
                },
            ],
        }
    }

    fn args(amount: i64) -> BTreeMap<String, ParamValue> {
        BTreeMap::from([
            ("order".to_string(), ParamValue::String("o-7".to_string())),
            ("amount".to_string(), ParamValue::Integer(amount)),
        ])
    }

    fn attached() -> BTreeSet<String> {
        BTreeSet::from(["erp".to_string()])
    }

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R700.1
    #[test]
    fn action_spec_golden_round_trip() {
        let mut registry = ActionRegistry::new();
        registry.insert(spec().name.clone(), spec());
        let json = serde_json::to_string(&registry).unwrap();
        assert!(
            json.contains(r#""kind":"write_back","source":"erp""#),
            "{json}"
        );
        let back: ActionRegistry = serde_json::from_str(&json).unwrap();
        assert_eq!(back, registry);
        let bad_op = json.replace(r#""op":"gt""#, r#""op":"like""#);
        assert!(serde_json::from_str::<ActionRegistry>(&bad_op).is_err());
        let bad_field = json.replace(r#""required":true"#, r#""required":true,"x":1"#);
        assert!(serde_json::from_str::<ActionRegistry>(&bad_field).is_err());
    }

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R700.4
    #[test]
    fn refusals_apply_no_effect() {
        let run = spec().submit(args(900), &attached());
        assert_eq!(run.status, ActionStatus::Refused);
        assert_eq!(
            run.refusal.as_ref().unwrap().code(),
            "ACTION_CRITERION_FAILED"
        );
        assert_eq!(
            run.refusal,
            Some(ActionRefusal::CriterionFailed("under_limit".to_string()))
        );
        assert!(run.effects.is_empty());

        let mut unknown = args(10);
        unknown.insert("reason".to_string(), ParamValue::String("x".to_string()));
        let run = spec().submit(unknown, &attached());
        assert_eq!(run.refusal.unwrap().code(), "ACTION_UNKNOWN_PARAMETER");
        assert!(run.effects.is_empty());

        let mut mistyped = args(10);
        mistyped.insert("amount".to_string(), ParamValue::String("10".to_string()));
        let run = spec().submit(mistyped, &attached());
        assert_eq!(
            run.refusal.unwrap().code(),
            "ACTION_PARAMETER_TYPE_MISMATCH"
        );

        let run = spec().submit(args(10), &BTreeSet::new());
        assert_eq!(run.refusal.unwrap().code(), "ACTION_WRITE_BACK_UNATTACHED");
        assert!(run.effects.is_empty());
    }

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R700.5
    #[test]
    fn stored_run_reproduces_its_decision() {
        for amount in [10, 0, 900] {
            let run = spec().submit(args(amount), &attached());
            let stored = serde_json::to_string(&run).unwrap();
            let read: ActionRun = serde_json::from_str(&stored).unwrap();
            let replay = spec().submit(read.params.clone(), &attached());
            assert_eq!(replay, read);
        }
        let applied = spec().submit(args(10), &attached());
        assert_eq!(applied.status, ActionStatus::Applied);
        assert_eq!(applied.verdicts.len(), 2);
        assert_eq!(applied.effects[0].text, "order/o-7 refund=10");
    }
}
