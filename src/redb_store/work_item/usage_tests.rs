//! Native fenced usage persistence and same-snapshot isolation (R801.7).
use super::test_shard::{with_nodes, with_write, GRAPH};
use super::*;
use eg_types::run_usage::RunUsageObservation;
use std::collections::BTreeMap;

fn seed(shard: &Shard, run: &str, actor: &str, status: &str) -> String {
    let id = run_owner::work_item_id(GRAPH, "tenant", run).unwrap();
    with_nodes(shard, &format!("seed-{run}"), |nodes| {
        let mut row = serde_json::json!({"node_type":"WorkItem","tenant":"tenant","kind":"agent.execute","status":status,
            "lease_owner":"worker","lease_epoch":1,"fencing_token":2,"lease_expires_at":10.0,
            "attempt":1,"max_attempts":3,"downstream_ids":[],"metadata":{"a2a_context_id":"context"},
            "run_owner_binding":{"graph":GRAPH,"tenant":"tenant","principal":actor,"run_id":run,"work_item_id":id}}).as_object().unwrap().clone();
        write_work_item_props(nodes, GRAPH, &id, &mut row, DurableCrypto::none())
    });
    id
}
fn usage(run: &str) -> RunUsageObservation {
    RunUsageObservation {
        run_id: run.into(),
        context_id: "context".into(),
        counts: BTreeMap::from([("input_tokens".into(), 17), ("output_tokens".into(), 5)]),
    }
}
fn commit(
    shard: &Shard,
    id: &str,
    tag: &str,
    observation: &RunUsageObservation,
    fence: u64,
    retry: bool,
) -> Result<(), String> {
    with_write(shard, tag, 2000, |admitted| {
        let member = admitted.graph(GRAPH)?;
        let mut nodes = member.open_scoped_table(NODES)?;
        let mut holds = member.open_scoped_table(development_lane::HOLDS)?;
        let index = member.open_scoped_table(development_lane::WORK_ITEM_INDEX)?;
        let mut counters = member.open_scoped_table(development_lane::COUNTERS)?;
        let mut pressure = member.open_scoped_table(development_lane::PRESSURE_INDEX)?;
        let policies = member.open_scoped_table(development_lane::POLICIES)?;
        apply_commit_work_item_result_row(CommitWorkItemResultInput {
            graph: GRAPH,
            tenant: "tenant",
            work_item_id: id,
            worker_id: "worker",
            lease_epoch: 1,
            fencing_token: fence,
            outcome: if retry { "failed" } else { "succeeded" },
            result_ref: &None,
            error_ref: &None,
            retryable: retry,
            now_ms: 2000,
            outcome_extension: None,
            usage_observation: Some(observation),
            batch_id: tag,
            nodes: &mut nodes,
            holds: &mut holds,
            work_item_index: &index,
            counters: &mut counters,
            pressure_index: &mut pressure,
            policies: &policies,
            crypto: DurableCrypto::none(),
        })?;
        Ok(())
    })
}
fn read(shard: &Shard, actor: &str, context: &str) -> eg_types::run_usage::SessionUsage {
    read_session_usage(
        shard,
        GRAPH,
        "tenant",
        context,
        actor,
        DurableCrypto::none(),
    )
    .unwrap()
}
// spec: EG-DECISION-ENGINE-R801.7
#[test]
fn committed_usage_survives_reopen_and_is_owner_context_scoped() {
    let path = crate::redb_store::temp_path("eg-usage", "reopen");
    let shard = Shard::open(&path).unwrap();
    let id = seed(&shard, "one", "owner", "running");
    seed(&shard, "legacy", "owner", "succeeded");
    assert!(read(&shard, "owner", "context").counts.is_empty());
    commit(&shard, &id, "commit", &usage("one"), 2, false).unwrap();
    // A fresh terminal retry cannot overwrite or duplicate the first observation.
    let mut changed = usage("one");
    changed.counts.insert("input_tokens".into(), 99);
    commit(&shard, &id, "duplicate", &changed, 2, false).unwrap();
    let result = read(&shard, "owner", "context");
    assert_eq!(result.counts["input_tokens"], 17);
    assert_eq!(result.recorded_runs, 1);
    assert_eq!(result.missing_runs, 1);
    assert!(read(&shard, "other", "context").counts.is_empty());
    assert!(read(&shard, "owner", "other").counts.is_empty());
    assert!(read_session_usage(
        &shard,
        GRAPH,
        "foreign",
        "context",
        "owner",
        DurableCrypto::none()
    )
    .unwrap()
    .counts
    .is_empty());
    drop(shard);
    let reopened = Shard::open(&path).unwrap();
    assert_eq!(read(&reopened, "owner", "context"), result);
    drop(reopened);
    std::fs::remove_file(path).unwrap();
}
// spec: EG-DECISION-ENGINE-R801.7
#[test]
fn fenced_foreign_and_retry_scheduled_writes_publish_no_usage() {
    let tmp = test_shard::open("usage-refusals");
    let shard = &tmp.shard;
    let id = seed(shard, "one", "owner", "running");
    commit(shard, &id, "stale", &usage("one"), 1, false).unwrap();
    assert!(read(shard, "owner", "context").counts.is_empty());
    let mut foreign = usage("one");
    foreign.context_id = "foreign".into();
    assert!(commit(shard, &id, "foreign", &foreign, 2, false).is_err());
    commit(shard, &id, "retry", &usage("one"), 2, true).unwrap();
    assert!(read(shard, "owner", "context").counts.is_empty());
    let row = super::read::snapshot_row(shard, GRAPH, &id, DurableCrypto::none())
        .unwrap()
        .1
        .unwrap();
    assert!(!row.contains_key(eg_types::work_item_read::WORK_ITEM_COMMITTED_USAGE));
}
