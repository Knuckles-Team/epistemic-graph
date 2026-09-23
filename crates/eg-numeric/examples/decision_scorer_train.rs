//! Offline training of the resident decision scorer over an FX-SYNTH corpus
//! (EH-302). CPU only; no GPU, no Python runtime, no model server.
//!
//! ```text
//! python -m epistemic_graph.testing.synthetic.decision_export corpus.json 0 1 2 3
//! cargo run -p eg-numeric --features decision --example decision_scorer_train -- corpus.json
//! ```
//!
//! The corpus is a full-label `LabelledDataset` (acceptability sets by
//! construction). Every fifth item (in corpus order) is the frozen evaluation
//! set; the rest train. The fit is the same `DecisionFit` computation the
//! engine serves (`OptionAttention` head, seeded), and the evaluation is the
//! promotion protocol a `DecisionEval` job runs. Prints one JSON object: the
//! head's content digest, its parameter count and the receipt.

use eg_numeric::decision::admission::{admit, AdmissionRules, Regime};
use eg_numeric::decision::evaluate::{evaluate, EvalSpec};
use eg_numeric::decision::fit::{fit, FitSpec};
use eg_types::contract::BoundedVec;
use eg_types::decision::jobs::{OptimiserSpec, RecordWindow};
use eg_types::decision::statistical::body::{canonical_body_bytes, content_digest_of};
use eg_types::decision::statistical::dataset::LabelledDataset;
use eg_types::decision::statistical::head::HeadKind;
use eg_types::decision::{
    QuantScaleTag, QuantisedValue, StatisticalPolicy, TraceFidelityLevel, UnitRationalWire,
};

fn tenth() -> UnitRationalWire {
    UnitRationalWire::new(1, 10).expect("1/10 is a unit rational")
}

fn policy() -> StatisticalPolicy {
    StatisticalPolicy {
        alpha: tenth(),
        epsilon: tenth(),
        delta: tenth(),
        n_min: 20,
        min_support: 5,
        min_ess: QuantisedValue {
            scale: QuantScaleTag::Q32,
            value: 10 << 32,
        },
        min_outcome_fidelity: TraceFidelityLevel::ToolCalls,
        tenant_public_features: false,
        audit_sample: UnitRationalWire::new(1, 20).expect("1/20 is a unit rational"),
        approved_commit_principals: BoundedVec::default(),
        compact_after_ms: None,
        drop_blob_after_ms: None,
    }
}

/// Split the corpus: every fifth item evaluates, the rest train.
fn split(dataset: &LabelledDataset) -> (LabelledDataset, LabelledDataset) {
    let (mut train, mut held) = (Vec::new(), Vec::new());
    for (index, item) in dataset.items.iter().enumerate() {
        if index % 5 == 4 {
            held.push(item.clone());
        } else {
            train.push(item.clone());
        }
    }
    let with = |items: Vec<_>| LabelledDataset {
        items: BoundedVec::new(items).expect("a subset stays inside the bound"),
        ..dataset.clone()
    };
    (with(train), with(held))
}

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: decision_scorer_train <corpus.json>");
    let bytes = std::fs::read(&path).expect("the corpus is readable");
    let corpus: LabelledDataset = serde_json::from_slice(&bytes).expect("a labelled dataset");
    let corpus = corpus.checked().expect("a well-formed dataset");
    let (train, held) = split(&corpus);
    let stat = policy();
    let rules = AdmissionRules {
        regime: Regime::FullLabel,
        window: RecordWindow {
            from_ms: 0,
            to_ms: u64::MAX,
        },
        fidelity_floor: TraceFidelityLevel::ToolCalls,
        approved_principals: &[],
    };
    let admitted = admit(&train, &rules);
    let spec = FitSpec {
        head_kind: HeadKind::OptionAttention,
        regime: Regime::FullLabel,
        optimiser: OptimiserSpec {
            max_iterations: 200,
            tolerance: QuantisedValue {
                scale: QuantScaleTag::Q32,
                value: 1 << 12,
            },
            seed: 0,
        },
        feature_schema_digest: &corpus.feature_schema_digest,
        statistical: &stat,
    };
    let head = fit(&train, &admitted.items, &spec).expect("the scorer fits");
    let head_digest = content_digest_of(&canonical_body_bytes(&head).expect("encodes"));
    let evaluated = admit(&held, &rules);
    let receipt = evaluate(
        &head,
        &held,
        &evaluated.items,
        evaluated.exclusions,
        &EvalSpec {
            regime: Regime::FullLabel,
            statistical: &stat,
            estimators: &[],
            head_digest: &head_digest,
            policy_digest: "sha256:offline-policy",
        },
    )
    .expect("evaluates");
    let parameters = head.weights.len()
        + head.scorer.as_ref().map_or(0, |s| {
            [
                s.embed.len(),
                s.embed_bias.len(),
                s.query.len(),
                s.key.len(),
                s.value.len(),
                s.self_weight.len(),
                s.context_weight.len(),
            ]
            .iter()
            .sum()
        });
    let report = serde_json::json!({
        "corpus_items": corpus.items.len(),
        "trained_on": admitted.items.len(),
        "evaluated_on": evaluated.items.len(),
        "head_digest": head_digest,
        "parameters": parameters,
        "receipt": receipt,
    });
    println!("{report}");
}
