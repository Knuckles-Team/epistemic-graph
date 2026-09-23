//! Fleet catalog writes: exactly one engine compare-and-set per request.
//!
//! A record is created with `CreateNodeIfAbsent` and replaced with
//! `CompareAndSetNodeFields` conditioned on the revision the request read, both
//! dispatched through the ordinary `__commons__` graph gateway -- so the write
//! is durable, audited and CDC-emitted like `RegisterServer`'s, and a writer
//! that lost a race is refused by the engine rather than silently overwriting.
//! One graph operation per request is deliberate: the gateway's replay
//! protection is keyed by the request, so a second write inside the same
//! request would be answered as a replay of the first.

use eg_types::fleet_catalog::{
    FleetDiscoveryRecordRequest, FleetOverrideClearRequest, FleetOverrideSetRequest,
    FleetWriteDisposition, FleetWriteReceipt,
};
use eg_types::result_contract::cluster::{
    FleetCatalogClearOverride, FleetCatalogRecordDiscovery, FleetCatalogSetOverride,
};
use eg_types::result_contract::MethodResult;
use serde::Serialize;

use super::super::registry::REGISTRY_GRAPH;
use super::records::{
    content_digest, decode_record, discovery_node_id, discovery_record_id, lost_race,
    override_node_id, override_record_id, plan_write, record_properties, revision_condition,
    DiscoveryBody, OverrideBody, RecordMeta, RecordWriter, WritePlan, DISCOVERY_NODE_TYPE,
    OVERRIDE_NODE_TYPE,
};
use super::*;

/// One record write, decided except for the stored state it races.
struct RecordWrite {
    node_type: &'static str,
    node_id: String,
    record_id: String,
    body: serde_json::Value,
    content_digest: String,
    expected_revision: Option<u64>,
}

impl RecordWrite {
    fn new<B: Serialize>(
        node_type: &'static str,
        ids: (String, String),
        tenant_id: &str,
        body: &B,
        expected_revision: Option<u64>,
    ) -> Result<Self, String> {
        let (node_id, record_id) = ids;
        Ok(Self {
            node_type,
            node_id,
            record_id,
            body: serde_json::to_value(body)
                .map_err(|error| format!("fleet catalog record encoding failed: {error}"))?,
            content_digest: content_digest(tenant_id, body)?,
            expected_revision,
        })
    }

    fn meta(&self, tenant_id: &str, revision: u64) -> RecordMeta {
        RecordMeta {
            tenant_id: tenant_id.to_string(),
            revision,
            content_digest: self.content_digest.clone(),
            written_at_ms: 0,
        }
    }

    /// The single graph operation `plan` needs, and the revision it commits.
    fn method(
        &self,
        plan: WritePlan,
        tenant_id: &str,
        writer: &RecordWriter<'_>,
    ) -> Result<Option<(u64, Method)>, String> {
        let (revision, conditions) = match plan {
            WritePlan::Replay { .. } => return Ok(None),
            WritePlan::Create => (1, None),
            WritePlan::Update { from } => (from + 1, Some(revision_condition(tenant_id, from)?)),
        };
        let properties_msgpack = record_properties(
            self.node_type,
            &self.meta(tenant_id, revision),
            &self.body,
            writer,
        )?;
        let node_id = self.node_id.clone();
        let method = match conditions {
            None => Method::CreateNodeIfAbsent {
                node_id,
                properties_msgpack,
            },
            Some(conditions_msgpack) => Method::CompareAndSetNodeFields {
                node_id,
                conditions_msgpack,
                updates_msgpack: properties_msgpack,
            },
        };
        Ok(Some((revision, method)))
    }
}

/// The record's current bookkeeping, read from the committed `__commons__`
/// image. Racing writers are caught by the engine's compare-and-set, not here.
async fn current_meta(state: &Arc<RwLock<ServerState>>, write: &RecordWrite) -> Option<RecordMeta> {
    let current = timed_read(state).await;
    let properties = current
        .registry
        .get(REGISTRY_GRAPH)?
        .core
        .get_node_properties(&write.node_id)?;
    decode_record::<serde_json::Value>(write.node_type, &properties).map(|record| record.meta)
}

/// Whether the graph operation committed. `false` from either primitive means
/// another writer got there first.
fn applied(response: Response, record_id: &str) -> Result<(), String> {
    match response {
        Response {
            error: Some(error), ..
        } => Err(error),
        Response {
            result: Some(ResultPayload::Bool(true)),
            ..
        } => Ok(()),
        Response {
            result: Some(ResultPayload::Bool(false)),
            ..
        } => Err(lost_race(record_id)),
        Response { .. } => Err("fleet catalog record write answered no acknowledgement".into()),
    }
}

async fn commit(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    verified: &VerifiedRequestContext,
    write: RecordWrite,
) -> Result<FleetWriteReceipt, String> {
    let tenant_id = verified.tenant();
    let current = current_meta(state, &write).await;
    let plan = plan_write(
        current.as_ref(),
        write.expected_revision,
        &write.content_digest,
    )?;
    let observed_at_ms = authoritative_now_ms();
    let writer = RecordWriter {
        agent_id: verified.agent_id(),
        written_at_ms: observed_at_ms,
    };
    let (revision, disposition) = match write.method(plan, tenant_id, &writer)? {
        Some((revision, method)) => {
            let response =
                dispatch_graph_op(state, REGISTRY_GRAPH, req_id, caller, verified, method).await;
            applied(response, &write.record_id)?;
            (revision, FleetWriteDisposition::Written)
        }
        None => (
            current.map_or(0, |meta| meta.revision),
            FleetWriteDisposition::Replayed,
        ),
    };
    Ok(FleetWriteReceipt {
        record_id: write.record_id,
        revision,
        disposition,
        observed_at_ms,
    })
}

fn discovery_write(
    verified: &VerifiedRequestContext,
    request: FleetDiscoveryRecordRequest,
) -> Result<RecordWrite, String> {
    let tenant_id = verified.tenant();
    let ids = (
        discovery_node_id(tenant_id, &request.server_name, &request.scope),
        discovery_record_id(&request.server_name, &request.scope),
    );
    let body = DiscoveryBody {
        server_name: request.server_name,
        scope: request.scope,
        connector: request.connector,
        outcome: request.outcome,
        counts: request.counts,
        observer: verified.principal_persistence_id(),
    };
    RecordWrite::new(
        DISCOVERY_NODE_TYPE,
        ids,
        tenant_id,
        &body,
        request.expected_revision,
    )
}

fn override_write(
    verified: &VerifiedRequestContext,
    body: OverrideBody,
    expected_revision: Option<u64>,
) -> Result<RecordWrite, String> {
    let tenant_id = verified.tenant();
    let ids = (
        override_node_id(tenant_id, body.field, &body.component_id),
        override_record_id(body.field, &body.component_id),
    );
    RecordWrite::new(OVERRIDE_NODE_TYPE, ids, tenant_id, &body, expected_revision)
}

/// The written receipt, or the refusal, as `M`'s declared result.
async fn commit_and_respond<M: MethodResult<Body = FleetWriteReceipt>>(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    verified: &VerifiedRequestContext,
    write: Result<RecordWrite, String>,
) -> Response {
    let receipt = match write {
        Ok(write) => commit(state, req_id, caller, verified, write).await,
        Err(error) => Err(error),
    };
    respond::<M>(req_id, receipt)
}

pub(super) async fn record_discovery(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    verified: &VerifiedRequestContext,
    request: FleetDiscoveryRecordRequest,
) -> Response {
    let write = discovery_write(verified, request);
    commit_and_respond::<FleetCatalogRecordDiscovery>(state, req_id, caller, verified, write).await
}

pub(super) async fn set_override(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    verified: &VerifiedRequestContext,
    request: FleetOverrideSetRequest,
) -> Response {
    let body = OverrideBody {
        field: request.value.field(),
        component_id: request.component_id,
        value: Some(request.value),
        set_by: verified.principal_persistence_id(),
    };
    let write = override_write(verified, body, request.expected_revision);
    commit_and_respond::<FleetCatalogSetOverride>(state, req_id, caller, verified, write).await
}

pub(super) async fn clear_override(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    verified: &VerifiedRequestContext,
    request: FleetOverrideClearRequest,
) -> Response {
    let body = OverrideBody {
        field: request.field,
        component_id: request.component_id,
        value: None,
        set_by: verified.principal_persistence_id(),
    };
    let write = override_write(verified, body, request.expected_revision);
    commit_and_respond::<FleetCatalogClearOverride>(state, req_id, caller, verified, write).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_types::fleet_catalog::{DiscoveryCounts, DiscoveryOutcome, DiscoveryScope};

    fn write() -> RecordWrite {
        let body = DiscoveryBody {
            server_name: "github".to_string(),
            scope: DiscoveryScope::TenantLocal,
            connector: eg_types::contract::ResourceId::new("github").unwrap(),
            outcome: DiscoveryOutcome::Reachable,
            counts: DiscoveryCounts::default(),
            observer: "principal:sha256:ab".to_string(),
        };
        let ids = (
            discovery_node_id("tenant-a", "github", &body.scope),
            discovery_record_id("github", &body.scope),
        );
        RecordWrite::new(DISCOVERY_NODE_TYPE, ids, "tenant-a", &body, None).unwrap()
    }

    fn writer() -> RecordWriter<'static> {
        RecordWriter {
            agent_id: "agent-a",
            written_at_ms: 42,
        }
    }

    #[test]
    fn a_first_write_creates_revision_one_only_if_absent() {
        let (revision, method) = write()
            .method(WritePlan::Create, "tenant-a", &writer())
            .unwrap()
            .unwrap();
        assert_eq!(revision, 1);
        let Method::CreateNodeIfAbsent {
            node_id,
            properties_msgpack,
        } = method
        else {
            panic!("a first write is a create-if-absent");
        };
        assert!(node_id.starts_with("fleetobs:"));
        let stored = decode_record::<DiscoveryBody>(DISCOVERY_NODE_TYPE, &properties_msgpack)
            .expect("the created node decodes as the record it wrote");
        assert_eq!(stored.meta.revision, 1);
        assert_eq!(stored.meta.written_at_ms, 42);
    }

    #[test]
    fn a_later_write_is_conditioned_on_the_revision_it_read() {
        let (revision, method) = write()
            .method(WritePlan::Update { from: 4 }, "tenant-a", &writer())
            .unwrap()
            .unwrap();
        assert_eq!(revision, 5);
        let Method::CompareAndSetNodeFields {
            conditions_msgpack, ..
        } = method
        else {
            panic!("a later write is a compare-and-set");
        };
        let conditions = eg_types::msgpack::decode_property_value(&conditions_msgpack).unwrap();
        assert_eq!(
            conditions,
            serde_json::json!({"tenant_id": "tenant-a", "revision": 4})
        );
        assert!(write()
            .method(WritePlan::Replay { revision: 4 }, "tenant-a", &writer())
            .unwrap()
            .is_none());
    }

    #[test]
    fn a_false_acknowledgement_is_a_lost_race_not_a_success() {
        let ok = Response::ok(1, ResultPayload::Bool(true));
        assert!(applied(ok, "srvobs:x").is_ok());
        let lost = Response::ok(1, ResultPayload::Bool(false));
        assert!(applied(lost, "srvobs:x")
            .unwrap_err()
            .starts_with("FLEET_REVISION_CONFLICT"));
        assert_eq!(
            applied(Response::err(1, "denied"), "srvobs:x"),
            Err("denied".into())
        );
    }
}
