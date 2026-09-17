//! [`super::Cell::to_typed_json`]'s `ColumnType::Numeric` arm
//! (CONCEPT:EG-KG.query.table-schema-constraints).
//!
//! Split out of `schema.rs` as a FREE FUNCTION taking `&Cell` explicitly, not
//! a `Cell` method (a "shared module instead of growing a file/type past the
//! kiss caps" split, not a rename): a `methods_per_class`/`functions_per_file`
//! violation from a new helper is fixed by moving it off the type and out of
//! the file, per this program's standing lesson (see `WRAPUP.md` precedent
//! from `refactor/eg-bd-raft-a-20260916`), not by nesting it back inside
//! `to_typed_json`.

use serde_json::Value;

use super::Cell;

/// Which stored [`Cell`] shapes carry a NUMERIC-typed value, and (for a
/// legacy f64 cell) the same finite-only guard as before. `None` for a cell
/// shape that isn't numeric text/number/legacy-float/int/timestamp —
/// [`super::Cell::to_typed_json`] falls back to `self.to_json()`, exactly as
/// before.
pub(super) fn numeric_typed_json(
    cell: &Cell,
    precision_scale: Option<(u32, u32)>,
) -> Option<Value> {
    match cell {
        Cell::Json(Value::String(value)) => {
            Some(super::typed_numeric_value(value, precision_scale))
        }
        Cell::Json(Value::Number(value)) => Some(super::typed_numeric_value(
            &value.to_string(),
            precision_scale,
        )),
        // Legacy rows written by the WIP implementation used f64. Preserve
        // their readable value while ensuring new writes never take this path.
        Cell::Float(value) if value.is_finite() => Some(super::typed_numeric_value(
            &value.to_string(),
            precision_scale,
        )),
        Cell::Int(value) | Cell::Timestamp(value) => Some(super::typed_numeric_value(
            &value.to_string(),
            precision_scale,
        )),
        Cell::Text(value) => Some(super::typed_numeric_value(value, precision_scale)),
        _ => None,
    }
}
