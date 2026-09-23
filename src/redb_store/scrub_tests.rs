//! EH-384 scrub tests, storage level: a planted unreadable node row is found
//! and named, a bounded walk skips nothing, and the cursor resumes.

use super::*;
use crate::protocol::Method;
use crate::redb_store::{commit_ops, read_one_node, temp_path, UnsealFailure};

const GRAPH: &str = "scrub_graph";
const NODE_IDS: [&str; 5] = ["n0", "n1", "n2", "n3", "n4"];
const CORRUPT: &str = "n2";

/// A blob with sealed framing (magic byte + nonce room) in a deployment with
/// no data key: exactly what an at-rest-encrypted row looks like after its
/// key is lost. No capability refuses writing it, and every reader must.
fn sealed_without_key() -> Vec<u8> {
    let mut blob = vec![0xE6];
    blob.extend_from_slice(&[0u8; 40]);
    blob
}

fn payload(id: &str) -> Vec<u8> {
    if id == CORRUPT {
        return sealed_without_key();
    }
    rmp_serde::to_vec_named(&serde_json::json!({ "id": id })).expect("encode node")
}

/// A shard holding `GRAPH` with five nodes, one of them unreadable.
fn planted_shard(tag: &str) -> (Shard, std::path::PathBuf) {
    let path = temp_path("eg-scrub", tag);
    let shard = Shard::open(&path).expect("open scrub shard");
    let mut ops: Vec<(String, Method)> = NODE_IDS
        .iter()
        .map(|id| {
            let method = Method::AddNode {
                node_id: (*id).to_string(),
                properties_msgpack: payload(id),
            };
            (GRAPH.to_string(), method)
        })
        .collect();
    commit_ops(
        &shard,
        &mut ops,
        &mut Vec::new(),
        &format!("scrub-plant-{tag}"),
        1,
        DurableCrypto::none(),
        #[cfg(feature = "security")]
        &mut crate::redb_store::AuditTailCache::new(),
    )
    .expect("an unreadable payload commits: nothing on the write path opens it");
    (shard, path)
}

fn expected_finding() -> NodeUnreadable {
    NodeUnreadable::new(GRAPH, CORRUPT, UnsealFailure::SealedWithoutKey)
}

#[cfg(feature = "security")]
#[test]
fn one_pass_finds_and_names_the_unreadable_row() {
    let (shard, path) = planted_shard("one-pass");
    let pass = scrub_pass(
        &shard,
        DurableCrypto::none(),
        &ScrubCursor::default(),
        ScrubBudget { rows: 100 },
    )
    .expect("scrub pass");
    assert_eq!(pass.scanned, NODE_IDS.len() as u64, "every row opened");
    assert_eq!(pass.findings, vec![expected_finding()]);
    assert_eq!(
        pass.next,
        ScrubCursor {
            graph: None,
            after: None,
            cycles: 1
        }
    );
    let read = read_one_node(&shard, GRAPH, CORRUPT, DurableCrypto::none())
        .expect_err("a point read of the unreadable row fails closed");
    assert!(read.starts_with("NODE_UNREADABLE:"), "{read}");
    assert!(read.contains(CORRUPT), "{read}");
    drop(shard);
    let _ = std::fs::remove_file(&path);
}

#[cfg(feature = "security")]
#[test]
fn bounded_passes_resume_from_the_cursor_and_skip_nothing() {
    let (shard, path) = planted_shard("bounded");
    let budget = ScrubBudget { rows: 2 };
    let mut cursor = ScrubCursor::default();
    let mut scanned = Vec::new();
    let mut findings = Vec::new();
    for _ in 0..3 {
        let pass = scrub_pass(&shard, DurableCrypto::none(), &cursor, budget).expect("pass");
        assert!(pass.scanned <= 2, "a pass stays inside its budget");
        scanned.push(pass.scanned);
        findings.extend(pass.findings);
        cursor = pass.next;
    }
    assert_eq!(scanned, vec![2, 2, 1]);
    assert_eq!(findings, vec![expected_finding()]);
    assert_eq!(cursor.cycles, 1, "three bounded passes complete one cycle");
    assert_eq!(cursor.graph, None);
    drop(shard);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn an_empty_store_completes_a_cycle_with_nothing_scanned() {
    let path = temp_path("eg-scrub", "empty");
    let shard = Shard::open(&path).expect("open scrub shard");
    let pass = scrub_pass(
        &shard,
        DurableCrypto::none(),
        &ScrubCursor::default(),
        ScrubBudget { rows: 8 },
    )
    .expect("scrub pass");
    assert_eq!((pass.scanned, pass.findings.len()), (0, 0));
    assert_eq!(pass.next.cycles, 1);
    assert_eq!(
        load_scrub_cursor(&shard).expect("cursor"),
        ScrubCursor::default()
    );
    drop(shard);
    let _ = std::fs::remove_file(&path);
}
