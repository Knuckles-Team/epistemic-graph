//! SQL window functions over the series kernels (EH-522): `eg_<func>(series…, params…)
//! OVER (PARTITION BY series ORDER BY ts)` for every function of the `DERIVE` table
//! (`eg_types::series_expr::SIGNATURES`) — `eg_zscore(v, 60)`, `eg_ewma(v, 12)`,
//! `eg_rcorr(a, b, 20)`, `eg_wsum(price, volume, 20)`, …
//!
//! ONE kernel: each partition is fed, in the window's `ORDER BY`, to the same
//! `eg_tsdb::derive::Program` a UQL `DERIVE` column runs, so SQL and UQL agree bit for
//! bit. Series arguments are `v0..`, numeric parameters must be constant over the
//! partition (a literal). `NULL` in is a skipped observation, `NULL` out is warm-up.
//! Compose kernels with a sub-query or CTE (window calls do not nest in SQL).

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::{
    PartitionEvaluator, Signature, Volatility, WindowUDF, WindowUDFImpl,
};
use eg_tsdb::derive::Program;
use eg_types::series_expr::{SeriesExpr, SeriesFunc, SIGNATURES};

/// Every `eg_<func>` window function.
pub(super) fn series_window_udfs() -> Vec<WindowUDF> {
    SIGNATURES
        .iter()
        .map(|sig| WindowUDF::from(SeriesWindow::new(sig.func)))
        .collect()
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SeriesWindow {
    func: SeriesFunc,
    name: String,
    signature: Signature,
}

impl SeriesWindow {
    fn new(func: SeriesFunc) -> Self {
        let sig = func.signature();
        Self {
            func,
            name: format!("eg_{}", sig.name),
            signature: Signature::any(sig.series + sig.params, Volatility::Immutable),
        }
    }
}

impl WindowUDFImpl for SeriesWindow {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _args: PartitionEvaluatorArgs,
    ) -> DfResult<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(SeriesEvaluator { func: self.func }))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> DfResult<FieldRef> {
        Ok(Field::new(field_args.name(), DataType::Float64, true).into())
    }
}

#[derive(Debug)]
struct SeriesEvaluator {
    func: SeriesFunc,
}

impl PartitionEvaluator for SeriesEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> DfResult<ArrayRef> {
        let sig = self.func.signature();
        let columns = values
            .iter()
            .map(float_column)
            .collect::<DfResult<Vec<Float64Array>>>()?;
        let params = columns[sig.series..]
            .iter()
            .map(|c| constant(c, sig.name))
            .collect::<DfResult<Vec<f64>>>()?;
        let args = (0..sig.series)
            .map(|i| SeriesExpr::channel(&format!("v{i}")))
            .collect();
        let mut program = Program::compile(&SeriesExpr::call(self.func, args, params))
            .map_err(|e| DataFusionError::Execution(format!("eg_{}: {e}", sig.name)))?;
        let out: Float64Array = (0..num_rows)
            .map(|row| program.step(&|name| channel(&columns, name, row)))
            .collect();
        Ok(Arc::new(out))
    }
}

fn float_column(values: &ArrayRef) -> DfResult<Float64Array> {
    let cast = cast(values, &DataType::Float64)?;
    cast.as_any()
        .downcast_ref::<Float64Array>()
        .cloned()
        .ok_or_else(|| DataFusionError::Execution("a series argument is not numeric".into()))
}

/// The value of a parameter column that must be one constant over the partition.
fn constant(column: &Float64Array, func: &str) -> DfResult<f64> {
    let first = (!column.is_empty() && column.is_valid(0)).then(|| column.value(0));
    let same = first.is_some_and(|v| column.iter().all(|c| c == Some(v)));
    if let (Some(v), true) = (first, same) {
        return Ok(v);
    }
    Err(DataFusionError::Execution(format!(
        "eg_{func}: a numeric parameter must be a non-NULL constant"
    )))
}

/// Row `row`'s value of series argument `v<i>`.
fn channel(columns: &[Float64Array], name: &str, row: usize) -> Option<f64> {
    let i: usize = name.strip_prefix('v')?.parse().ok()?;
    let column = columns.get(i)?;
    column.is_valid(row).then(|| column.value(row))
}
