//! Re-export of the model wire types [`super::Model::try_from`] validates and
//! lowers. The definitions live in `eg_types::solve::spec` -- see that
//! module's own doc for why there is exactly one copy.

pub use eg_types::solve::spec::{
    Coefficient, ConstraintBody, ConstraintSpec, ModelSpec, ObjectiveLevelSpec, ObjectiveTerm,
    Relation, RowId, Term, VarId,
};
