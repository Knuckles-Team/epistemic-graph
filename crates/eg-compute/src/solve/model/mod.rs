//! A validated 0-1 integer programme.
//!
//! [`Model`] is built only from a [`ModelSpec`] that passed validation. At
//! construction every typed constraint is lowered to exactly one linear
//! [`Row`] (so a [`RowId`] names both), the lexicographic objective is
//! scalarised to one integer weight per variable, and the spec is digested.
//! The solver and the verifier both read these rows and weights: they are the
//! problem statement, not solver output.

mod error;
mod objective;
mod spec;
mod validate;

use serde::{Deserialize, Serialize};

pub use error::{Location, ModelError};
pub use objective::{LevelValue, ObjectiveValue, MAX_SCALAR_MAGNITUDE};
pub use spec::{
    Coefficient, ConstraintBody, ConstraintSpec, ModelSpec, ObjectiveLevelSpec, ObjectiveTerm,
    Relation, RowId, Term, VarId,
};
pub use validate::{
    MAX_ABS_COEFFICIENT, MAX_ABS_RHS, MAX_CONSTRAINTS, MAX_LABEL_BYTES, MAX_OBJECTIVE_LEVELS,
    MAX_VARIABLES,
};

use crate::solve::scalar::Sha256Digest;

/// Domain tag of the model digest.
const MODEL_DIGEST_DOMAIN: &str = "eg-solve/model/v1";

/// One linear row `Σ coefficient·x relation rhs`, terms sorted by variable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    terms: Vec<Term>,
    relation: Relation,
    rhs: i64,
}

impl Row {
    fn new(mut terms: Vec<Term>, relation: Relation, rhs: i64) -> Self {
        terms.sort_by_key(|term| term.var);
        Self {
            terms,
            relation,
            rhs,
        }
    }

    fn unit(vars: &[VarId], relation: Relation, rhs: i64) -> Self {
        let terms = vars
            .iter()
            .map(|&var| Term {
                var,
                coefficient: 1,
            })
            .collect();
        Self::new(terms, relation, rhs)
    }

    pub fn terms(&self) -> &[Term] {
        &self.terms
    }

    pub fn relation(&self) -> Relation {
        self.relation
    }

    pub fn rhs(&self) -> i64 {
        self.rhs
    }

    /// The coefficient of `var` in this row, if it appears.
    pub fn coefficient_of(&self, var: VarId) -> Option<i64> {
        self.terms
            .binary_search_by_key(&var, |term| term.var)
            .ok()
            .map(|position| self.terms[position].coefficient)
    }
}

/// A validated, lowered and digested 0-1 integer programme.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "ModelSpec", into = "ModelSpec")]
pub struct Model {
    spec: ModelSpec,
    rows: Vec<Row>,
    weights: Vec<i128>,
    digest: Sha256Digest,
}

impl Model {
    pub fn spec(&self) -> &ModelSpec {
        &self.spec
    }

    pub fn variable_count(&self) -> usize {
        self.spec.variables.len()
    }

    /// The lowered rows; `rows()[r]` is constraint `r`.
    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    /// Scalar objective weight per variable (minimised).
    pub fn weights(&self) -> &[i128] {
        &self.weights
    }

    pub fn digest(&self) -> Sha256Digest {
        self.digest
    }

    /// Every level's value and the scalar objective of a full assignment, or
    /// `None` when `selected` does not have one entry per variable.
    pub fn objective_value(&self, selected: &[bool]) -> Option<ObjectiveValue> {
        (selected.len() == self.variable_count())
            .then(|| objective::evaluate(&self.spec.objective, &self.weights, selected))
    }
}

impl TryFrom<ModelSpec> for Model {
    type Error = ModelError;

    fn try_from(spec: ModelSpec) -> Result<Self, Self::Error> {
        validate::validate(&spec)?;
        let weights = objective::scalar_weights(&spec.objective, spec.variables.len())?;
        let rows = spec.constraints.iter().map(|c| lower(&c.body)).collect();
        let digest = Sha256Digest::of_json(MODEL_DIGEST_DOMAIN, &spec);
        Ok(Self {
            spec,
            rows,
            weights,
            digest,
        })
    }
}

impl From<Model> for ModelSpec {
    fn from(model: Model) -> Self {
        model.spec
    }
}

/// Lower one typed constraint to its single linear row.
fn lower(body: &ConstraintBody) -> Row {
    match body {
        ConstraintBody::Linear {
            terms,
            relation,
            rhs,
        } => Row::new(terms.clone(), *relation, *rhs),
        ConstraintBody::Implication {
            antecedent,
            consequent,
        } => lower_implication(*antecedent, std::slice::from_ref(consequent)),
        ConstraintBody::ImpliesAny {
            antecedent,
            consequents,
        } => lower_implication(*antecedent, consequents),
        ConstraintBody::ExactlyOne { vars } => Row::unit(vars, Relation::Equal, 1),
        ConstraintBody::AtMost { vars, k } => Row::unit(vars, Relation::LessEqual, i64::from(*k)),
        ConstraintBody::AtLeast { vars, k } => {
            Row::unit(vars, Relation::GreaterEqual, i64::from(*k))
        }
        ConstraintBody::Fix { var, value } => Row::unit(
            std::slice::from_ref(var),
            Relation::Equal,
            i64::from(*value),
        ),
    }
}

/// `antecedent ⇒ any(consequents)` as `Σ consequents − antecedent ≥ 0`.
fn lower_implication(antecedent: VarId, consequents: &[VarId]) -> Row {
    let mut terms: Vec<Term> = consequents
        .iter()
        .map(|&var| Term {
            var,
            coefficient: 1,
        })
        .collect();
    terms.push(Term {
        var: antecedent,
        coefficient: -1,
    });
    Row::new(terms, Relation::GreaterEqual, 0)
}
