//! Field-level validation of a [`ModelSpec`].

use super::error::{Location, ModelError};
use super::spec::{
    Coefficient, ConstraintBody, ConstraintSpec, ModelSpec, ObjectiveLevelSpec, Term, VarId,
};

/// Most binary variables a model may declare.
pub const MAX_VARIABLES: usize = 4096;
/// Most constraints a model may declare.
pub const MAX_CONSTRAINTS: usize = 65_536;
/// Most lexicographic objective levels.
pub const MAX_OBJECTIVE_LEVELS: usize = 16;
/// Largest absolute value of a row or objective coefficient.
pub const MAX_ABS_COEFFICIENT: i64 = 1 << 40;
/// Largest absolute value of a right-hand side.
pub const MAX_ABS_RHS: i64 = 1 << 52;
/// Longest variable name, constraint label or level label, in bytes.
pub const MAX_LABEL_BYTES: usize = 256;

/// Check every field of `spec` that the lowering and the scalarisation rely on.
pub(super) fn validate(spec: &ModelSpec) -> Result<(), ModelError> {
    check_sizes(spec)?;
    check_names(&spec.variables)?;
    let variables = spec.variables.len();
    for (index, constraint) in spec.constraints.iter().enumerate() {
        check_constraint(index, constraint, variables)?;
    }
    for (index, level) in spec.objective.iter().enumerate() {
        check_level(index, level, variables)?;
    }
    Ok(())
}

fn check_sizes(spec: &ModelSpec) -> Result<(), ModelError> {
    let variables = spec.variables.len();
    if variables == 0 {
        return Err(ModelError::NoVariables);
    }
    if variables > MAX_VARIABLES {
        return Err(ModelError::TooManyVariables {
            count: variables,
            max: MAX_VARIABLES,
        });
    }
    if spec.constraints.len() > MAX_CONSTRAINTS {
        let count = spec.constraints.len();
        return Err(ModelError::TooManyConstraints {
            count,
            max: MAX_CONSTRAINTS,
        });
    }
    if spec.objective.len() > MAX_OBJECTIVE_LEVELS {
        let count = spec.objective.len();
        return Err(ModelError::TooManyObjectiveLevels {
            count,
            max: MAX_OBJECTIVE_LEVELS,
        });
    }
    Ok(())
}

fn check_names(names: &[String]) -> Result<(), ModelError> {
    let mut order: Vec<usize> = (0..names.len()).collect();
    order.sort_by(|&a, &b| names[a].cmp(&names[b]).then(a.cmp(&b)));
    for (position, &index) in order.iter().enumerate() {
        let var = VarId(index as u32);
        let name = &names[index];
        if name.is_empty() {
            return Err(ModelError::EmptyName { var });
        }
        check_label(Location::Variable(index), name)?;
        if position > 0 && names[order[position - 1]] == *name {
            return Err(ModelError::DuplicateName { var });
        }
    }
    Ok(())
}

fn check_label(location: Location, label: &str) -> Result<(), ModelError> {
    if label.len() > MAX_LABEL_BYTES {
        return Err(ModelError::LabelTooLong {
            location,
            bytes: label.len(),
            max: MAX_LABEL_BYTES,
        });
    }
    Ok(())
}

/// Every variable in range and none repeated.
fn check_vars(location: Location, vars: &[VarId], variables: usize) -> Result<(), ModelError> {
    if let Some(&var) = vars.iter().find(|var| var.index() >= variables) {
        return Err(ModelError::UnknownVariable { location, var });
    }
    let mut sorted = vars.to_vec();
    sorted.sort_unstable();
    match sorted.windows(2).find(|pair| pair[0] == pair[1]) {
        Some(pair) => Err(ModelError::DuplicateVariable {
            location,
            var: pair[0],
        }),
        None => Ok(()),
    }
}

fn check_coefficient(location: Location, var: VarId, value: i64) -> Result<(), ModelError> {
    if value.unsigned_abs() > MAX_ABS_COEFFICIENT.unsigned_abs() {
        return Err(ModelError::CoefficientOutOfRange {
            location,
            var,
            value,
        });
    }
    Ok(())
}

fn check_constraint(
    index: usize,
    constraint: &ConstraintSpec,
    variables: usize,
) -> Result<(), ModelError> {
    let location = Location::Constraint(index);
    check_label(location, &constraint.label)?;
    match &constraint.body {
        ConstraintBody::Linear { terms, rhs, .. } => check_linear(index, terms, *rhs, variables),
        ConstraintBody::Implication {
            antecedent,
            consequent,
        } => check_implication(
            index,
            *antecedent,
            std::slice::from_ref(consequent),
            variables,
        ),
        ConstraintBody::ImpliesAny {
            antecedent,
            consequents,
        } => check_implication(index, *antecedent, consequents, variables),
        ConstraintBody::ExactlyOne { vars } => check_group(index, vars, None, variables),
        ConstraintBody::AtMost { vars, k } | ConstraintBody::AtLeast { vars, k } => {
            check_group(index, vars, Some(*k), variables)
        }
        ConstraintBody::Fix { var, .. } => {
            check_vars(location, std::slice::from_ref(var), variables)
        }
    }
}

fn check_linear(
    index: usize,
    terms: &[Term],
    rhs: i64,
    variables: usize,
) -> Result<(), ModelError> {
    let location = Location::Constraint(index);
    if terms.is_empty() {
        return Err(ModelError::EmptyRow { constraint: index });
    }
    let vars: Vec<VarId> = terms.iter().map(|term| term.var).collect();
    check_vars(location, &vars, variables)?;
    for term in terms {
        if term.coefficient == 0 {
            return Err(ModelError::ZeroCoefficient {
                location,
                var: term.var,
            });
        }
        check_coefficient(location, term.var, term.coefficient)?;
    }
    if rhs.unsigned_abs() > MAX_ABS_RHS.unsigned_abs() {
        return Err(ModelError::RhsOutOfRange {
            constraint: index,
            value: rhs,
        });
    }
    Ok(())
}

fn check_implication(
    index: usize,
    antecedent: VarId,
    consequents: &[VarId],
    variables: usize,
) -> Result<(), ModelError> {
    check_group(index, consequents, None, variables)?;
    check_vars(Location::Constraint(index), &[antecedent], variables)?;
    if consequents.contains(&antecedent) {
        return Err(ModelError::SelfImplication {
            constraint: index,
            var: antecedent,
        });
    }
    Ok(())
}

/// A non-empty, duplicate-free group; `k`, when given, is at most its size.
fn check_group(
    index: usize,
    vars: &[VarId],
    k: Option<u32>,
    variables: usize,
) -> Result<(), ModelError> {
    if vars.is_empty() {
        return Err(ModelError::EmptyRow { constraint: index });
    }
    check_vars(Location::Constraint(index), vars, variables)?;
    match k {
        Some(k) if k as usize > vars.len() => Err(ModelError::CardinalityOutOfRange {
            constraint: index,
            k,
            len: vars.len(),
        }),
        _ => Ok(()),
    }
}

fn check_level(
    index: usize,
    level: &ObjectiveLevelSpec,
    variables: usize,
) -> Result<(), ModelError> {
    let location = Location::ObjectiveLevel(index);
    check_label(location, &level.label)?;
    let vars: Vec<VarId> = level.terms.iter().map(|term| term.var).collect();
    check_vars(location, &vars, variables)?;
    for term in &level.terms {
        if let Coefficient::Known(value) = term.coefficient {
            check_coefficient(location, term.var, value)?;
        }
    }
    Ok(())
}
