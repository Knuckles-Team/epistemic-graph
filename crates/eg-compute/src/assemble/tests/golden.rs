//! Golden vectors (DECIDE-LAYER-DESIGN §4.4, EH-007): fixed inputs, the exact
//! record and model they produce, committed as JSON and re-derived on every
//! target. The same file is the Python verifier's golden input.
//!
//! `EG_DECIDE_BLESS=1` rewrites the file; otherwise any difference fails.

use std::path::PathBuf;

use eg_types::decision::{CostBudget, DecisionRecord};
use eg_types::solve::ModelSpec;

use super::super::{assemble, replay_check};
use super::fixture::*;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
struct Golden {
    name: String,
    record: DecisionRecord,
    model: Option<ModelSpec>,
}

fn golden_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/decision/assembly_golden_v1.json")
}

fn cases() -> Vec<Golden> {
    let mut budget = request(&["eg:task/research"], &[]);
    budget.requirements.constraints.cost_budget = Some(CostBudget {
        currency: "USD".to_string(),
        max_micros: 25,
        strict: true,
    });
    [
        ("research", request(&["eg:task/research"], &[])),
        ("research-cost-budget", budget),
        (
            "uncovered",
            request(&[], &["eg:capability/action/process-exec"]),
        ),
    ]
    .into_iter()
    .map(|(name, asked)| {
        let assembly = assemble(inputs(asked, research_library()), identity()).expect("assembles");
        replay_check(&assembly.record).expect("replays");
        Golden {
            name: name.to_string(),
            record: assembly.record,
            model: assembly.model,
        }
    })
    .collect()
}

#[test]
fn the_golden_vectors_reproduce_byte_for_byte() {
    let produced = serde_json::to_string_pretty(&cases()).expect("encodes") + "\n";
    let path = golden_path();
    if std::env::var_os("EG_DECIDE_BLESS").is_some() {
        std::fs::create_dir_all(path.parent().expect("dir")).expect("mkdir");
        std::fs::write(&path, &produced).expect("bless");
        return;
    }
    let committed = std::fs::read_to_string(&path).expect("the golden file is committed");
    assert_eq!(
        committed, produced,
        "golden vectors moved; a record version change needs new vectors"
    );
    let decoded: Vec<Golden> = serde_json::from_str(&committed).expect("decodes");
    for golden in decoded {
        replay_check(&golden.record).expect("every committed golden record replays");
    }
}
