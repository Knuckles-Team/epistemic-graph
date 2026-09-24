use super::super::validate_current_lane_links_in_wtx;
use super::*;

/// EH-290: the per-commit lane-link check reads only the WorkItems that
/// linked holds name, by point lookup, and still refuses an orphaning write.
/// A graph with no linked hold validates without touching its nodes.
#[test]
fn current_lane_links_check_only_the_work_items_holds_name() {
    let fixture = NativeLaneFixture::new(policy());
    let current = |write: &ShardWrite<'_>| {
        validate_current_lane_links_in_wtx(write, TEST_GRAPH, DurableCrypto::none())
    };
    fixture
        .with_write("links-without-holds", current)
        .expect("a graph without a linked hold validates");

    let accepted: DevelopmentLaneResult =
        fixture.decode(&fixture.commit(fixture.reserve_method("reserve:links"), TEST_NOW));
    let hold = accepted.hold.expect("reserved hold");
    fixture
        .with_write("links-with-hold", current)
        .expect("the named WorkItem satisfies its hold");

    let orphaned = fixture
        .with_write("links-orphaned", |write| {
            write
                .graph(TEST_GRAPH)?
                .open_scoped_table(NODES)?
                .remove((TEST_GRAPH, hold.work_item_id.as_str()))?;
            current(write)
        })
        .expect_err("removing the named WorkItem must orphan the hold");
    assert!(orphaned.contains("orphan"), "{orphaned}");
}
