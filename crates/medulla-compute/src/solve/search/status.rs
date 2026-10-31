//! Status classification and certificate assembly after the search stops.

use super::record::core_rows;
use super::Search;
use crate::solve::certificate::{Algorithm, BoundProof, Certificate, Incumbent, SolveStatus};
use crate::solve::model::RowId;
use crate::solve::scalar::Scalar;

/// What the search established, reduced to the facts the status depends on.
struct Established {
    exhausted: bool,
    incumbent: Option<i128>,
    lower_bound: Option<i128>,
    /// The infeasibility core when a certificate tree was kept.
    tree_core: Option<Vec<RowId>>,
    nodes_expanded: u64,
    accepted_gap: i128,
}

impl Search<'_> {
    pub(super) fn finish(self) -> Certificate {
        let model = self.state.model();
        let min_bound = self.recorder.min_bound();
        let tree = self.recorder.into_tree();
        let established = Established {
            exhausted: self.exhausted,
            incumbent: self.incumbent.as_ref().map(|(_, value)| *value),
            lower_bound: if tree.is_some() {
                min_bound
            } else {
                Some(self.root.value)
            },
            tree_core: tree.as_deref().map(core_rows),
            nodes_expanded: self.nodes_expanded,
            accepted_gap: self.config.accepted_gap(),
        };
        let (status, lower_bound) = classify(established);
        let incumbent = self.incumbent.map(|(selected, _)| Incumbent {
            objective: model
                .objective_value(&selected)
                .expect("an incumbent has one entry per variable"),
            selected,
        });
        let proof = match tree {
            Some(nodes) => BoundProof::Tree { nodes },
            None => BoundProof::Root {
                dual: self.root.dual.clone(),
            },
        };
        Certificate {
            model_digest: model.digest(),
            algorithm: Algorithm::DepthFirstDualAscent,
            config: self.config,
            status,
            incumbent,
            lower_bound: lower_bound.map(Scalar::new),
            proof,
            nodes_expanded: self.nodes_expanded,
        }
    }
}

/// The status and the certified lower bound it reports.
fn classify(established: Established) -> (SolveStatus, Option<i128>) {
    match (established.incumbent, established.exhausted) {
        (Some(best), false) => completed_feasible(best, &established),
        (None, false) => completed_infeasible(established),
        (Some(best), true) => exhausted_feasible(best, &established),
        (None, true) => (SolveStatus::BudgetExhausted, established.lower_bound),
    }
}

fn completed_feasible(best: i128, established: &Established) -> (SolveStatus, Option<i128>) {
    let root_closes = established.lower_bound.is_some_and(|bound| bound >= best);
    if established.tree_core.is_some() || root_closes {
        return (SolveStatus::Optimal, Some(best));
    }
    let nodes_expanded = established.nodes_expanded;
    (
        SolveStatus::OptimalByDeterministicSearch { nodes_expanded },
        established.lower_bound,
    )
}

fn completed_infeasible(established: Established) -> (SolveStatus, Option<i128>) {
    match established.tree_core {
        Some(core) => (SolveStatus::Infeasible { core }, None),
        None => {
            let nodes_expanded = established.nodes_expanded;
            (
                SolveStatus::InfeasibleByDeterministicSearch { nodes_expanded },
                None,
            )
        }
    }
}

fn exhausted_feasible(best: i128, established: &Established) -> (SolveStatus, Option<i128>) {
    let Some(bound) = established.lower_bound else {
        return (SolveStatus::BudgetExhausted, None);
    };
    let gap = best - bound;
    if gap <= 0 {
        (SolveStatus::Optimal, Some(best))
    } else if gap <= established.accepted_gap {
        (
            SolveStatus::FeasibleWithGap {
                gap: Scalar::new(gap),
            },
            Some(bound),
        )
    } else {
        (SolveStatus::BudgetExhausted, Some(bound))
    }
}
