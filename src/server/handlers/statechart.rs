//! The native statechart engine's protocol surface (CONCEPT:INT-P2-2, feature
//! `statechart`): `Method::Statechart { op }` — define/instantiate/send_event/
//! get_state/list over `eg-statechart`'s redb-backed `StatechartDef` + `MachineInstance`
//! store.
//!
//! NOT graph-scoped: definitions and instances are keyed by `def_id`/`instance_id` in
//! their own `statecharts.redb`, so this module self-routes `dispatch.rs`'s top-level
//! match, ahead of the per-graph `dispatch_graph_op` chain — exactly like
//! `handlers::jobs` (`Method::AnalyticsJob`). It therefore takes `state` directly (only
//! to read the configured `persist_dir`) rather than a resolved `GraphCore`.
//!
//! ## Ownership
//!
//! `Instantiate` stamps the instance with the caller's opaque `(tenant, actor)` scope.
//! `SendEvent`/`GetState`/`List` are scoped to instances the caller owns (exact
//! `(tenant, actor)` match) — a `def_id`/`instance_id` from another tenant is reported
//! as "not found", never served. Definitions themselves are content-addressed and
//! shared (a `def_id` reveals nothing but the chart's own bytes), so `Define`/`Define`
//! replay is not owner-scoped.
//!
//! ## The transition boundary
//!
//! This handler is the *interpreter* half of the engine: the pure, side-effect-free
//! `eg_statechart::transition` function DECIDES the next `(state, context)` + the
//! ordered actions inside the store's `send_event`, and this handler returns them to
//! the caller. Context-mutating actions are already reflected in the persisted context;
//! the non-context effects (`Emit`/`Log`/`Custom`) are surfaced in the response for a
//! caller/agent to act on (executing them engine-side is a deferred interpreter layer).

use std::path::Path;
use std::sync::{Arc, OnceLock};

use tokio::sync::RwLock;

use eg_statechart::{Context, EventInput, StatechartDef, StatechartError, StatechartStore};
use eg_types::statechart::StatechartOp;

use crate::protocol::{Response, ResultPayload};
use crate::server::access::CarrierAuthority;
use crate::server::state::ServerState;

/// Bounded decode limits for a submitted `StatechartDef` MessagePack blob — the same
/// belt-and-braces shape `jobs.rs` applies to `ProgramOptimize` request payloads.
const MAX_DEF_BYTES: usize = 4 * 1024 * 1024;
const MAX_DEF_ITEMS: usize = 500_000;

/// Lazily-opened local statechart store (`{persist_dir}/statecharts.redb`). Mirrors
/// `jobs.rs::job_store`: a served process must have a configured persistence root;
/// there is no process-temp store that can vanish across a restart.
fn statechart_store(persist_dir: Option<&str>) -> Result<Arc<StatechartStore>, String> {
    static STORE: OnceLock<Result<Arc<StatechartStore>, String>> = OnceLock::new();
    let persist_dir = persist_dir.filter(|value| !value.is_empty()).ok_or_else(|| {
        "statechart engine requires a configured durable persistence directory".to_string()
    })?;
    STORE
        .get_or_init(|| {
            StatechartStore::open_in_dir(Path::new(persist_dir))
                .map(Arc::new)
                .map_err(|_| "statechart instance store is unavailable".to_string())
        })
        .clone()
}

/// Handle `Method::Statechart { op }` (CONCEPT:INT-P2-2). Self-contained: resolves its
/// own `StatechartStore` off `state.persist_dir`, so the dispatch shell can call this
/// directly with no per-graph routing.
pub(crate) async fn handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    authority: &CarrierAuthority,
    op: StatechartOp,
) -> Response {
    let persist_dir = { state.read().await.persist_dir.clone() };
    let store = match statechart_store(persist_dir.as_deref()) {
        Ok(store) => store,
        Err(error) => return Response::err(req_id, error),
    };
    let tenant = authority.tenant_scope();
    let actor = authority.actor_scope();

    let result = match op {
        StatechartOp::Define { def_msgpack } => op_define(&store, &def_msgpack),
        StatechartOp::Instantiate { def_id, context } => {
            op_instantiate(&store, &def_id, context, tenant, actor)
        }
        StatechartOp::SendEvent {
            instance_id,
            event,
            payload,
            expected_version,
        } => op_send_event(
            &store,
            tenant,
            actor,
            &instance_id,
            event,
            payload,
            expected_version,
        ),
        StatechartOp::GetState { instance_id } => {
            op_get_state(&store, tenant, actor, &instance_id)
        }
        StatechartOp::List { def_id } => op_list(&store, tenant, actor, def_id.as_deref()),
    };

    match result {
        Ok(value) => Response::ok(req_id, ResultPayload::Json(value)),
        Err(error) => Response::err(req_id, error),
    }
}

// ── Per-op logic (pure of ServerState / async, so it is directly unit-testable) ──────

fn op_define(store: &StatechartStore, def_msgpack: &[u8]) -> Result<serde_json::Value, String> {
    let def: StatechartDef = eg_types::msgpack::decode_bounded(
        def_msgpack,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_DEF_BYTES,
            MAX_DEF_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "statechart definition failed bounded MessagePack decoding".to_string())?;
    let def_id = store.define(&def).map_err(|e| e.to_string())?;
    Ok(serde_json::json!({ "def_id": def_id }))
}

fn op_instantiate(
    store: &StatechartStore,
    def_id: &str,
    context: serde_json::Value,
    tenant: &str,
    actor: &str,
) -> Result<serde_json::Value, String> {
    let context = Context::from_value(context)?;
    let instance = store
        .instantiate(def_id, context, tenant, actor)
        .map_err(|e| e.to_string())?;
    serde_json::to_value(&instance).map_err(|e| e.to_string())
}

#[allow(clippy::too_many_arguments)]
fn op_send_event(
    store: &StatechartStore,
    tenant: &str,
    actor: &str,
    instance_id: &str,
    event: String,
    payload: serde_json::Value,
    expected_version: Option<u64>,
) -> Result<serde_json::Value, String> {
    // Ownership: refuse (as "not found") an instance the caller does not own, so an
    // instance id never becomes a cross-tenant read/write handle.
    owned_instance(store, tenant, actor, instance_id)?;
    let event_input = EventInput::with_payload(event, payload);
    let outcome = store
        .send_event(instance_id, &event_input, expected_version)
        .map_err(|e| e.to_string())?;
    let effects: Vec<&eg_statechart::Action> = outcome.outcome.effects().collect();
    Ok(serde_json::json!({
        "instance": serde_json::to_value(&outcome.instance).map_err(|e| e.to_string())?,
        "fired": outcome.outcome.fired,
        "no_op_reason": outcome.outcome.no_op_reason.map(|r| format!("{r:?}")),
        "fired_label": outcome.outcome.fired_label,
        "actions": serde_json::to_value(&outcome.outcome.actions).map_err(|e| e.to_string())?,
        "effects": serde_json::to_value(&effects).map_err(|e| e.to_string())?,
    }))
}

fn op_get_state(
    store: &StatechartStore,
    tenant: &str,
    actor: &str,
    instance_id: &str,
) -> Result<serde_json::Value, String> {
    let instance = owned_instance(store, tenant, actor, instance_id)?;
    serde_json::to_value(&instance).map_err(|e| e.to_string())
}

fn op_list(
    store: &StatechartStore,
    tenant: &str,
    actor: &str,
    def_id: Option<&str>,
) -> Result<serde_json::Value, String> {
    let instances = store
        .list_owned_instances(tenant, actor, def_id)
        .map_err(|e| e.to_string())?;
    let ids: Vec<&str> = instances.iter().map(|i| i.instance_id.as_str()).collect();
    Ok(serde_json::json!({
        "instance_ids": ids,
        "count": ids.len(),
    }))
}

/// Fetch an instance only if the caller owns it; otherwise report "not found" (never
/// leaking existence across tenants).
fn owned_instance(
    store: &StatechartStore,
    tenant: &str,
    actor: &str,
    instance_id: &str,
) -> Result<eg_statechart::MachineInstance, String> {
    let not_found = || "statechart instance not found or not owned by caller".to_string();
    let instance = store.get_instance(instance_id).map_err(|error| match error {
        StatechartError::NotFound(_) => not_found(),
        other => other.to_string(),
    })?;
    if instance.tenant == tenant && instance.actor == actor {
        Ok(instance)
    } else {
        crate::metrics::access_denied();
        Err(not_found())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_statechart::model::{State, Transition};

    fn tmp_store() -> (StatechartStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        (StatechartStore::open_in_dir(dir.path()).unwrap(), dir)
    }

    fn turnstile_msgpack() -> Vec<u8> {
        let def = StatechartDef {
            name: "turnstile".into(),
            schema_version: 1,
            states: vec![State::new("locked"), State::new("unlocked")],
            alphabet: vec!["coin".into(), "push".into()],
            transitions: vec![
                Transition::new("locked", "coin", "unlocked"),
                Transition::new("unlocked", "push", "locked"),
            ],
            initial: "locked".into(),
            finals: vec![],
            meta: Default::default(),
        };
        rmp_serde::to_vec_named(&def).unwrap()
    }

    #[test]
    fn define_decodes_bounded_and_returns_a_def_id() {
        let (store, _dir) = tmp_store();
        let value = op_define(&store, &turnstile_msgpack()).unwrap();
        let def_id = value.get("def_id").and_then(|v| v.as_str()).unwrap();
        assert!(def_id.starts_with("eg:statechart:"));
        // corrupt bytes are rejected, not panicked on
        assert!(op_define(&store, b"\xff\xff\xff not msgpack").is_err());
    }

    #[test]
    fn full_lifecycle_define_instantiate_send_get_list() {
        let (store, _dir) = tmp_store();
        let def_id = op_define(&store, &turnstile_msgpack()).unwrap()["def_id"]
            .as_str()
            .unwrap()
            .to_string();

        let inst = op_instantiate(&store, &def_id, serde_json::Value::Null, "t1", "a1").unwrap();
        let instance_id = inst["instance_id"].as_str().unwrap().to_string();
        assert_eq!(inst["state"], serde_json::json!("locked"));

        // fire coin -> unlocked
        let sent = op_send_event(
            &store,
            "t1",
            "a1",
            &instance_id,
            "coin".into(),
            serde_json::Value::Null,
            None,
        )
        .unwrap();
        assert_eq!(sent["fired"], serde_json::json!(true));
        assert_eq!(sent["instance"]["state"], serde_json::json!("unlocked"));
        assert_eq!(sent["instance"]["version"], serde_json::json!(1));

        // undefined edge (coin from unlocked) is a no-op, not an error
        let noop = op_send_event(
            &store,
            "t1",
            "a1",
            &instance_id,
            "coin".into(),
            serde_json::Value::Null,
            None,
        )
        .unwrap();
        assert_eq!(noop["fired"], serde_json::json!(false));

        // get + list are owner-scoped
        let got = op_get_state(&store, "t1", "a1", &instance_id).unwrap();
        assert_eq!(got["state"], serde_json::json!("unlocked"));
        let listed = op_list(&store, "t1", "a1", None).unwrap();
        assert_eq!(listed["count"], serde_json::json!(1));
    }

    #[test]
    fn cross_tenant_access_is_reported_as_not_found() {
        let (store, _dir) = tmp_store();
        let def_id = op_define(&store, &turnstile_msgpack()).unwrap()["def_id"]
            .as_str()
            .unwrap()
            .to_string();
        let inst = op_instantiate(&store, &def_id, serde_json::Value::Null, "owner", "a").unwrap();
        let instance_id = inst["instance_id"].as_str().unwrap().to_string();

        // A different tenant cannot read it...
        let err = op_get_state(&store, "intruder", "a", &instance_id).unwrap_err();
        assert!(err.contains("not found or not owned"));
        // ...cannot drive it...
        assert!(op_send_event(
            &store,
            "intruder",
            "a",
            &instance_id,
            "coin".into(),
            serde_json::Value::Null,
            None,
        )
        .is_err());
        // ...and does not see it in their listing.
        assert_eq!(op_list(&store, "intruder", "a", None).unwrap()["count"], serde_json::json!(0));
    }

    #[test]
    fn instantiate_rejects_a_non_object_context() {
        let (store, _dir) = tmp_store();
        let def_id = op_define(&store, &turnstile_msgpack()).unwrap()["def_id"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(op_instantiate(&store, &def_id, serde_json::json!(42), "t", "a").is_err());
    }

    #[test]
    fn send_event_honors_optimistic_concurrency() {
        let (store, _dir) = tmp_store();
        let def_id = op_define(&store, &turnstile_msgpack()).unwrap()["def_id"]
            .as_str()
            .unwrap()
            .to_string();
        let inst = op_instantiate(&store, &def_id, serde_json::Value::Null, "t", "a").unwrap();
        let instance_id = inst["instance_id"].as_str().unwrap().to_string();
        // stored version is 0; a stale expected_version is rejected
        let err = op_send_event(
            &store,
            "t",
            "a",
            &instance_id,
            "coin".into(),
            serde_json::Value::Null,
            Some(9),
        )
        .unwrap_err();
        assert!(err.contains("occ conflict"));
    }
}
