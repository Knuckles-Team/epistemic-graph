//! `GapTransition`, `GapSettle` and `WorkOfferPut`: row-local writes of one
//! Gap. Each loads the tenant's Gap, applies the pure rule from
//! [`eg_types::work_market`], and writes the row only when the rule changed it
//! ([`rewrite_gap`] is the one load/apply/store shape they share).

use eg_types::result_contract::coordination::{GapSettle, GapTransition, WorkOfferPut};
use eg_types::work_item_read::{WorkItemView, WORK_ITEM_OUTCOME_REF};
use eg_types::work_market::lifecycle::SettledWorkItem;
use eg_types::work_market::{
    gap_row_key, GapSettleOutcome, GapSettled, GapTransitionOutcome, GapTransitioned, GapView,
    WorkOfferPutOutcome, WorkOfferRecorded,
};

use super::*;

/// One row-local Gap write.
pub(super) struct GapLifecycleWrite<'args, 'table, 'crypto> {
    pub(super) graph: &'args str,
    pub(super) method: &'args Method,
    pub(super) nodes: &'args mut NodeRows<'table>,
    pub(super) now_ms: u64,
    pub(super) crypto: DurableCrypto<'crypto>,
}

/// A rule's verdict on one loaded Gap.
struct Rewrite<R> {
    verdict: R,
    gap: GapView,
    changed_work_item_ids: Vec<String>,
}

/// Apply a Gap lifecycle write; `None` for any other method.
pub(super) fn apply_gap_lifecycle_rows(
    mut write: GapLifecycleWrite<'_, '_, '_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    let method = write.method;
    match method {
        Method::GapTransition { request } => {
            request.validate()?;
            let now_ms = write.now_ms;
            let answer = rewrite_gap(
                &mut write,
                (&request.tenant, &request.gap_id),
                |gap, _| Ok(gap.transition(request, now_ms)),
                |applied| *applied,
            )?;
            transitioned(answer).map(Some)
        }
        Method::GapSettle { request } => {
            request.validate()?;
            let answer = settle(&mut write, (&request.tenant, &request.gap_id))?;
            settled(answer).map(Some)
        }
        Method::WorkOfferPut { request } => {
            request.validate()?;
            let now_ms = write.now_ms;
            let answer = rewrite_gap(
                &mut write,
                (&request.tenant, &request.gap_id),
                |gap, _| gap.price(request, now_ms),
                |applied| *applied,
            )?;
            recorded(answer).map(Some)
        }
        _ => Ok(None),
    }
}

/// Load `(tenant, gap_id)`, apply `rule` (which may read the node table), and
/// store the Gap when `changes` says the verdict changed it. `None` when the
/// tenant has no such Gap.
fn rewrite_gap<R>(
    write: &mut GapLifecycleWrite<'_, '_, '_>,
    (tenant, gap_id): (&str, &str),
    rule: impl FnOnce(&mut GapView, &NodeRows<'_>) -> Result<R, String>,
    changes: impl Fn(&R) -> bool,
) -> Result<Option<Rewrite<R>>, String> {
    let Some(mut gap) = load_gap(&*write.nodes, write.graph, tenant, gap_id, write.crypto)? else {
        return Ok(None);
    };
    let verdict = rule(&mut gap, &*write.nodes)?;
    if !changes(&verdict) {
        return Ok(Some(Rewrite {
            verdict,
            gap,
            changed_work_item_ids: Vec::new(),
        }));
    }
    let gap = store_gap(write.nodes, write.graph, tenant, &gap, write.crypto)?;
    let changed_work_item_ids = vec![gap_row_key(tenant, &gap.gap_id)];
    Ok(Some(Rewrite {
        verdict,
        gap,
        changed_work_item_ids,
    }))
}

/// `GapSettle`: the engine reads the Gap's current WorkItem row in the same
/// transaction; the caller never states an outcome.
fn settle(
    write: &mut GapLifecycleWrite<'_, '_, '_>,
    scope: (&str, &str),
) -> Result<Option<Rewrite<GapSettleOutcome>>, String> {
    let (graph, crypto, now_ms) = (write.graph, write.crypto, write.now_ms);
    let tenant = scope.0;
    rewrite_gap(
        write,
        scope,
        |gap, nodes| {
            let item = current_work_item(nodes, (graph, tenant), gap, crypto)?;
            Ok(gap.settle(&item, now_ms))
        },
        |outcome| {
            !matches!(
                outcome,
                GapSettleOutcome::Pending | GapSettleOutcome::Unchanged
            )
        },
    )
}

/// The terminal facts of the Gap's current WorkItem, read from its row. A
/// WorkItem that is missing or not the tenant's cannot settle a Gap.
fn current_work_item(
    nodes: &NodeRows<'_>,
    (graph, tenant): (&str, &str),
    gap: &GapView,
    crypto: DurableCrypto<'_>,
) -> Result<SettledWorkItem, String> {
    let row = super::super::control_lease::load_row(nodes, graph, &gap.work_item_id, crypto)?
        .ok_or_else(|| format!("Gap '{}' WorkItem is missing", gap.gap_id))?;
    let view = WorkItemView::from_tenant_row(&gap.work_item_id, &row, tenant)?
        .ok_or_else(|| format!("Gap '{}' WorkItem is not the tenant's", gap.gap_id))?;
    let reference = [WORK_ITEM_OUTCOME_REF, "result_ref", "error_ref"]
        .iter()
        .find_map(|key| row.get(*key).and_then(serde_json::Value::as_str))
        .unwrap_or_default()
        .to_string();
    Ok(SettledWorkItem {
        work_item_id: view.work_item_id,
        status: view.status,
        reference,
    })
}

/// Split a rule's answer into the wire triple, naming the outcome from the
/// verdict (or `not_found` when the tenant has no such Gap).
fn split<R, O>(
    answer: Option<Rewrite<R>>,
    not_found: O,
    outcome: impl FnOnce(R) -> O,
) -> (O, Option<GapView>, Vec<String>) {
    match answer {
        None => (not_found, None, Vec::new()),
        Some(Rewrite {
            verdict,
            gap,
            changed_work_item_ids,
        }) => (outcome(verdict), Some(gap), changed_work_item_ids),
    }
}

fn transitioned(answer: Option<Rewrite<bool>>) -> Result<crate::protocol::ResultPayload, String> {
    let (outcome, gap, changed_work_item_ids) = split(
        answer,
        GapTransitionOutcome::NotFound,
        |applied| match applied {
            true => GapTransitionOutcome::Applied,
            false => GapTransitionOutcome::Conflict,
        },
    );
    crate::protocol::ResultPayload::of::<GapTransition>(GapTransitioned {
        outcome,
        gap,
        changed_work_item_ids,
    })
}

fn settled(
    answer: Option<Rewrite<GapSettleOutcome>>,
) -> Result<crate::protocol::ResultPayload, String> {
    let (outcome, gap, changed_work_item_ids) =
        split(answer, GapSettleOutcome::NotFound, |outcome| outcome);
    crate::protocol::ResultPayload::of::<GapSettle>(GapSettled {
        outcome,
        gap,
        changed_work_item_ids,
    })
}

fn recorded(answer: Option<Rewrite<bool>>) -> Result<crate::protocol::ResultPayload, String> {
    let (outcome, gap, changed_work_item_ids) = split(
        answer,
        WorkOfferPutOutcome::NotFound,
        |applied| match applied {
            true => WorkOfferPutOutcome::Applied,
            false => WorkOfferPutOutcome::Conflict,
        },
    );
    crate::protocol::ResultPayload::of::<WorkOfferPut>(WorkOfferRecorded {
        outcome,
        gap,
        changed_work_item_ids,
    })
}
