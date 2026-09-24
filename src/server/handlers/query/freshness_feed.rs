//! `FreshnessFeed` (CONCEPT:EG-KG.coordination.dependency-scoped-cache-invalidation, EH-400): one poll returns
//! the request graph's per-class invalidation events after the caller's cursor, the declared
//! `eg:volatilityClass` policy when it changed, and the watermark freshness of the foreign
//! sources the caller can see. An out-of-process cache (AU, EH-401) invalidates by class from the
//! events and derives its TTLs from the policy.

use eg_types::freshness::{FreshnessFeed, InvalidationEvent, InvalidationScope};

use super::*;

/// The caller's cursor and what it already holds.
#[derive(Clone, Copy)]
pub(crate) struct FeedRequest {
    pub(crate) after_version: u64,
    pub(crate) limit: u32,
    pub(crate) policy_after: Option<u64>,
}

pub(crate) async fn handle_freshness_feed(
    ctx: &QueryHandlerCtx<'_>,
    request: FeedRequest,
) -> Response {
    let core = Arc::clone(ctx.core);
    let visible = row_filter(ctx);
    let built = compute_off_lock(ctx.req_id, move || build_feed(&core, request, visible)).await;
    match built.map(ResultPayload::of::<eg_types::result_contract::messaging::FreshnessFeed>) {
        Ok(Ok(payload)) => Response::ok(ctx.req_id, payload),
        Ok(Err(error)) => Response::err(ctx.req_id, error),
        Err(response) => response,
    }
}

/// The caller's row-security check over a node blob: a foreign-source watermark node is ordinary
/// graph data, so the feed reports only the ones the caller's view would contain.
#[cfg(feature = "security")]
fn row_filter(ctx: &QueryHandlerCtx<'_>) -> impl Fn(&[u8]) -> bool + Send + 'static {
    let (rls, caller) = (Arc::clone(ctx.rls), ctx.caller.to_string());
    move |blob: &[u8]| rls.can_see_row(&caller, &crate::isolation::row_visibility(blob))
}

#[cfg(not(feature = "security"))]
fn row_filter(_ctx: &QueryHandlerCtx<'_>) -> impl Fn(&[u8]) -> bool + Send + 'static {
    |_blob: &[u8]| true
}

/// Assemble one feed page. The policy is re-derived only when its version moved past what the
/// caller already holds.
pub(crate) fn build_feed(
    core: &GraphCore,
    request: FeedRequest,
    visible: impl Fn(&[u8]) -> bool,
) -> FreshnessFeed {
    let page = core
        .dep_clock()
        .invalidation_log()
        .read_after(request.after_version, request.limit as usize);
    let policy_version = eg_core::freshness::policy_version(core);
    let (policy, policy_diagnostics) = match request.policy_after {
        Some(known) if known == policy_version => (None, Vec::new()),
        _ => {
            let resolved = eg_core::freshness::volatility_policy(core);
            (Some(resolved.classes), resolved.diagnostics)
        }
    };
    let now_ms = crate::server::dispatch::authoritative_now_ms();
    FreshnessFeed {
        events: page.records.into_iter().map(wire_event).collect(),
        gap: page.gap,
        head_version: page.head_version,
        epoch: page.epoch,
        policy_version,
        policy,
        policy_diagnostics,
        foreign: eg_core::freshness::foreign_freshness(core, now_ms, visible),
    }
}

fn wire_event(record: eg_core::dep_scope::InvalidationRecord) -> InvalidationEvent {
    InvalidationEvent {
        version: record.version,
        scope: match record.scope {
            eg_core::dep_scope::InvalidationScope::Classes => InvalidationScope::Classes,
            eg_core::dep_scope::InvalidationScope::All => InvalidationScope::All,
        },
        classes: record.classes,
        edge_types: record.edge_types,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_core::index::{ChangeSet, NodeChange};
    use eg_types::freshness::{VolatilityClass, VOLATILITY_CLASS_KEY};

    fn commit_node(core: &GraphCore, id: &str, props: serde_json::Value) {
        let blob = rmp_serde::to_vec_named(&props).unwrap();
        core.add_node(id.to_string(), blob.clone());
        let mut change = ChangeSet::new();
        change
            .added_nodes
            .push(NodeChange::with_properties(id.to_string(), blob));
        core.maintain_indexes(&change);
        core.mark_dirty();
    }

    fn request(after_version: u64, policy_after: Option<u64>) -> FeedRequest {
        FeedRequest {
            after_version,
            limit: 0,
            policy_after,
        }
    }

    #[test]
    fn the_feed_names_each_commits_classes_and_resends_the_policy_only_when_it_moves() {
        let core = GraphCore::new();
        commit_node(
            &core,
            "Doc",
            serde_json::json!({ VOLATILITY_CLASS_KEY: "slow" }),
        );
        commit_node(&core, "d1", serde_json::json!({"type": "Doc"}));
        let first = build_feed(&core, request(0, None), |_| true);
        assert_eq!(first.events.len(), 2);
        assert_eq!(first.events[1].classes, vec!["Doc".to_string()]);
        assert_eq!(first.events[1].scope, InvalidationScope::Classes);
        let doc = first.policy.as_ref().unwrap();
        assert_eq!(doc[0].volatility, VolatilityClass::Slow);

        let cursor = first.head_version;
        commit_node(&core, "d2", serde_json::json!({"type": "Doc"}));
        let next = build_feed(&core, request(cursor, Some(first.policy_version)), |_| true);
        assert_eq!(next.events.len(), 1, "only the commit after the cursor");
        assert!(next.policy.is_none(), "an unchanged policy is not resent");

        commit_node(
            &core,
            "Doc",
            serde_json::json!({ VOLATILITY_CLASS_KEY: "fast" }),
        );
        let moved = build_feed(&core, request(0, Some(first.policy_version)), |_| true);
        assert_eq!(
            moved.policy.unwrap()[0].volatility,
            VolatilityClass::Fast,
            "re-declaring a class resends the policy"
        );
    }
}
