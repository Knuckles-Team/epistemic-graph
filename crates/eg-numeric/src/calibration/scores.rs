//! Validated score and probability matrices with labels.

use crate::detkernel::validate;
use crate::detkernel::StatResult;

/// A dense `rows x classes` matrix of finite scores (logits or log-probabilities),
/// at least one row and two classes.
#[derive(Debug, Clone, PartialEq)]
pub struct ScoreMatrix {
    classes: usize,
    data: Vec<f64>,
}

impl ScoreMatrix {
    /// Build from rows of equal width.
    pub fn from_rows(rows: &[Vec<f64>]) -> StatResult<Self> {
        validate::non_empty(rows, "score rows")?;
        let classes = rows[0].len();
        validate::parameter(classes >= 2, "classes", "at least two classes")?;
        let mut data = Vec::with_capacity(rows.len() * classes);
        for row in rows {
            validate::same_len(classes, row.len(), "score row")?;
            data.extend_from_slice(row);
        }
        validate::all_finite(&data, "scores")?;
        Ok(Self { classes, data })
    }

    /// Number of classes (columns).
    pub fn classes(&self) -> usize {
        self.classes
    }

    /// Number of rows.
    pub fn row_count(&self) -> usize {
        self.data.len() / self.classes
    }

    /// Row `index`.
    pub fn row(&self, index: usize) -> &[f64] {
        &self.data[index * self.classes..(index + 1) * self.classes]
    }

    /// Rows in order.
    pub fn rows(&self) -> impl Iterator<Item = &[f64]> {
        self.data.chunks_exact(self.classes)
    }
}

/// A [`ScoreMatrix`] whose rows are probability vectors.
#[derive(Debug, Clone, PartialEq)]
pub struct ProbabilityMatrix(ScoreMatrix);

impl ProbabilityMatrix {
    /// Build from rows that each lie on the probability simplex.
    pub fn from_rows(rows: &[Vec<f64>]) -> StatResult<Self> {
        let matrix = ScoreMatrix::from_rows(rows)?;
        for row in matrix.rows() {
            validate::probability_vector(row, "probability row")?;
        }
        Ok(Self(matrix))
    }

    /// The underlying matrix.
    pub fn matrix(&self) -> &ScoreMatrix {
        &self.0
    }

    /// The underlying matrix, after checking one label per row below the
    /// class count.
    pub fn labelled(&self, labels: &[usize]) -> StatResult<&ScoreMatrix> {
        check_labels(&self.0, labels)?;
        Ok(&self.0)
    }
}

/// Scores with one class label per row.
#[derive(Debug, Clone, PartialEq)]
pub struct LabelledScores {
    scores: ScoreMatrix,
    labels: Vec<usize>,
}

impl LabelledScores {
    /// Pair a matrix with labels below its class count.
    pub fn new(scores: ScoreMatrix, labels: Vec<usize>) -> StatResult<Self> {
        check_labels(&scores, &labels)?;
        Ok(Self { scores, labels })
    }

    /// The scores.
    pub fn scores(&self) -> &ScoreMatrix {
        &self.scores
    }

    /// The labels.
    pub fn labels(&self) -> &[usize] {
        &self.labels
    }
}

fn check_labels(matrix: &ScoreMatrix, labels: &[usize]) -> StatResult<()> {
    validate::same_len(matrix.row_count(), labels.len(), "labels")?;
    validate::labels_below(labels, matrix.classes())
}
