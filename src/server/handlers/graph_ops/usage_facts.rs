//! Bounded, verified-tenant usage fact reads over durable graph nodes.

use std::sync::Arc;

use serde_json::Value;

use super::{GraphCore, GraphReadAuthority, Method, Response, ResultPayload};
use eg_types::result_contract::graph::{UsageEventRow, UsageFactTotals, UsageFactsPage};

const LABEL: &str = "UsageEvent";
const MAX_PAGE: usize = 200;

fn opaque_ref(value: &str, kind: &str) -> bool {
    value
        .strip_prefix(&format!("pref_{kind}_"))
        .is_some_and(|digest| {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        })
}

struct Filter<'a> {
    from_ms: Option<i64>,
    to_ms: Option<i64>,
    origin: Option<&'a str>,
    model_ref: Option<&'a str>,
}

impl Filter<'_> {
    fn accepts(&self, row: &Value) -> bool {
        let Some(ms) = row.get("occurred_at_ms").and_then(Value::as_i64) else {
            return false;
        };
        if self.from_ms.is_some_and(|from| ms < from) || self.to_ms.is_some_and(|to| ms >= to) {
            return false;
        }
        if self.origin.is_some_and(|origin| row["origin"] != origin)
            || self
                .model_ref
                .is_some_and(|model| row["model_ref"] != model)
        {
            return false;
        }
        row["schema"] == "usage-event-fact-v1"
    }
}

fn event(row: &Value) -> Result<UsageEventRow, String> {
    let fields = [
        "event_ref",
        "run_ref",
        "origin",
        "occurred_at",
        "input_tokens",
        "output_tokens",
        "cache_creation_tokens",
        "cache_read_tokens",
        "reasoning_tokens",
        "cost_microusd",
        "model_ref",
    ];
    let mut selected = serde_json::Map::new();
    for field in fields {
        selected.insert(field.to_string(), row[field].clone());
    }
    serde_json::from_value(Value::Object(selected))
        .map_err(|_| "invalid usage event fact".to_string())
}

fn add_totals(totals: &mut UsageFactTotals, row: &UsageEventRow) -> Result<(), String> {
    macro_rules! checked_add {
        ($field:ident, $amount:expr) => {
            totals.$field = totals
                .$field
                .checked_add($amount)
                .ok_or("usage counter overflow")?;
        };
    }
    checked_add!(event_count, 1);
    checked_add!(input_tokens, row.input_tokens);
    checked_add!(output_tokens, row.output_tokens);
    checked_add!(cache_creation_tokens, row.cache_creation_tokens);
    checked_add!(cache_read_tokens, row.cache_read_tokens);
    checked_add!(reasoning_tokens, row.reasoning_tokens);
    checked_add!(cost_microusd, row.cost_microusd.unwrap_or(0));
    Ok(())
}

pub(super) fn handle(
    req_id: u64,
    core: &Arc<GraphCore>,
    authority: &GraphReadAuthority,
    method: Method,
) -> Response {
    let Method::UsageFacts {
        mode,
        after,
        limit,
        from_ms,
        to_ms,
        origin,
        model_ref,
    } = method
    else {
        return Response::err(req_id, "INVALID_ARGUMENT: expected UsageFacts");
    };
    if !matches!(mode.as_str(), "events" | "summary") || !(1..=MAX_PAGE).contains(&limit) {
        return Response::err(req_id, "INVALID_ARGUMENT: usage mode or limit");
    }
    if after
        .as_deref()
        .is_some_and(|value| !opaque_ref(value, "usage_dedup"))
        || model_ref
            .as_deref()
            .is_some_and(|value| !opaque_ref(value, "model"))
        || origin
            .as_deref()
            .is_some_and(|value| !matches!(value, "runtime" | "ingested"))
        || from_ms.zip(to_ms).is_some_and(|(from, to)| from >= to)
    {
        return Response::err(req_id, "INVALID_ARGUMENT: usage filter");
    }
    let Some(carrier) = authority.carrier() else {
        return Response::err(req_id, "ACCESS_DENIED: verified usage tenant required");
    };
    let prefix = format!("usage:event:{}:", carrier.tenant_scope());
    let cursor = after
        .as_ref()
        .map(|reference| format!("{prefix}{reference}"))
        .unwrap_or_else(|| prefix.clone());
    let rows = core.get_nodes_by_label_page(LABEL, Some(&cursor), limit + 1);
    let has_more = rows
        .get(limit)
        .is_some_and(|(id, _)| id.starts_with(&prefix));
    let filter = Filter {
        from_ms,
        to_ms,
        origin: origin.as_deref(),
        model_ref: model_ref.as_deref(),
    };
    let mut events = Vec::new();
    let mut totals = UsageFactTotals::default();
    let mut last = None;
    let mut scanned = 0;
    for (id, blob) in rows.into_iter().take(limit) {
        if !id.starts_with(&prefix) {
            break;
        }
        last = id.strip_prefix(&prefix).map(str::to_string);
        scanned += 1;
        // `usage:read` is a tenant-wide accounting permission. A runtime
        // emitter's private graph-row owner is often a different agent from
        // GraphOS's reader, so the generic node RLS predicate is deliberately
        // not the authority for this dedicated method. The verified carrier's
        // tenant prefix and the method's scope gate are both mandatory; generic
        // graph reads still apply the row's `_owner`/private visibility tags.
        let Ok(row) = eg_types::msgpack::decode_property_value(&blob) else {
            continue;
        };
        if row["tenant_ref"] != carrier.tenant_scope() || !filter.accepts(&row) {
            continue;
        }
        let row = match event(&row) {
            Ok(row) => row,
            Err(error) => return Response::err(req_id, format!("USAGE_FACT_INVALID: {error}")),
        };
        if let Err(error) = add_totals(&mut totals, &row) {
            return Response::err(req_id, format!("USAGE_FACT_INVALID: {error}"));
        }
        if mode == "events" {
            events.push(row);
        }
    }
    Response::ok(
        req_id,
        ResultPayload::of::<eg_types::result_contract::graph::UsageFacts>(UsageFactsPage {
            events,
            totals,
            next_after: last,
            has_more,
            scanned,
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usage_filter_and_totals_are_bounded_and_explicit() {
        let row = serde_json::json!({
            "schema": "usage-event-fact-v1",
            "event_ref": format!("pref_usage_dedup_{}", "a".repeat(64)),
            "run_ref": format!("pref_run_{}", "b".repeat(64)),
            "origin": "runtime",
            "occurred_at": "2026-09-25T00:00:00Z",
            "occurred_at_ms": 1_790_294_400_000i64,
            "input_tokens": 12,
            "output_tokens": 5,
            "cache_creation_tokens": 0,
            "cache_read_tokens": 2,
            "reasoning_tokens": 1,
            "cost_microusd": 12345,
            "model_ref": null,
        });
        let filter = Filter {
            from_ms: Some(1_790_294_400_000),
            to_ms: Some(1_790_294_400_001),
            origin: Some("runtime"),
            model_ref: None,
        };
        assert!(filter.accepts(&row));
        let event = event(&row).expect("typed fact");
        let mut totals = UsageFactTotals::default();
        add_totals(&mut totals, &event).expect("counters");
        assert_eq!(totals.event_count, 1);
        assert_eq!(totals.input_tokens, 12);
        assert_eq!(totals.cost_microusd, 12345);
        assert!(!opaque_ref("plain", "usage_dedup"));
        assert!(opaque_ref(&event.event_ref, "usage_dedup"));
    }
}
