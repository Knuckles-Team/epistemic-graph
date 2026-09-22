use super::*;

fn source_ingestion_digest_vector() -> String {
    use eg_types::contract::{BoundedVec, Digest256, ResourceId};
    use eg_types::source_ingestion::{
        SourceCheckpoint, SourceIngestionBatch, SourceIngestionMode, SourceIngestionRequest,
        SourceJson, SourceRecord, SourceRecordProvenance,
    };

    let connector = ResourceId::new("demo-connector").expect("canonical connector id");
    let stream = ResourceId::new("items").expect("canonical stream id");
    let request = SourceIngestionRequest::new(SourceIngestionBatch {
        connector: connector.clone(),
        mode: SourceIngestionMode::Delta,
        strict_schema: true,
        records: BoundedVec::new(vec![SourceRecord {
            stream: stream.clone(),
            record_id: "item-1".into(),
            mapping_reference: "manifest:demo-connector#schema_mappings/item".into(),
            payload: SourceJson::new(serde_json::json!({"name": "one", "id": 1}))
                .expect("bounded source JSON"),
            updated_at: Some("2026-09-20T00:00:00Z".into()),
            provenance: SourceRecordProvenance {
                connector,
                adapter_kind: ResourceId::new("mcp").expect("canonical adapter id"),
                server: "demo-server".into(),
                tool: "list_items".into(),
                tool_schema_sha256: Digest256::from_bytes([7; 32]),
                source_uri: "demo://items/item-1".into(),
            },
        }])
        .expect("bounded source records"),
        relationships: BoundedVec::new(Vec::new()).expect("bounded source relationships"),
        provider_checkpoint: SourceCheckpoint {
            stream: stream.clone(),
            position: SourceJson::new(serde_json::json!({"page": 2})).expect("bounded cursor JSON"),
            content_hash: None,
            watermark: Some("2026-09-20T00:00:00Z".into()),
            pending_watermark: None,
        },
        expected_previous_checkpoint: Some(SourceCheckpoint {
            stream,
            position: SourceJson::new(serde_json::json!({"page": 1}))
                .expect("bounded previous cursor JSON"),
            content_hash: None,
            watermark: None,
            pending_watermark: None,
        }),
        authoritative_live_ids: None,
        empty_authoritative_approval: None,
        withdrawals: BoundedVec::new(Vec::new()).expect("bounded source withdrawals"),
    })
    .expect("valid source ingestion request");
    request
        .batch_digest()
        .expect("canonical source ingestion digest")
        .to_hex()
}

fn execute_source_ingestion_modules(generated: &BTreeMap<String, String>) {
    use std::fs;
    use std::process::Command;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!(
        "eg-generated-python-{}-{}",
        std::process::id(),
        NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
    ));
    let package = root.join("epistemic_graph");
    let generated_package = package.join("generated");
    fs::create_dir_all(&generated_package).expect("create generated Python test package");
    fs::write(package.join("__init__.py"), "").expect("write package init");
    fs::write(generated_package.join("__init__.py"), "").expect("write generated package init");
    for module in [
        "_ids",
        "_runtime",
        "digest",
        "index_repository",
        "source_ingestion",
        "ingestion",
    ] {
        let path = format!("epistemic_graph/generated/{module}.py");
        fs::write(
            generated_package.join(format!("{module}.py")),
            generated
                .get(&path)
                .expect("generated Python module")
                .as_bytes(),
        )
        .expect("write generated Python module");
    }

    let script = r#"
import asyncio
import sys

from epistemic_graph.generated import ingestion
from epistemic_graph.generated.ingestion import SourceIngestStatusRequest
from epistemic_graph.generated.source_ingestion import (
    SourceIngestStatus,
    SourceIngestionReceipt,
    SourceIngestionRequest,
)

request_data = {
    "connector": "demo-connector",
    "mode": "delta",
    "strict_schema": True,
    "records": [{
        "stream": "items",
        "record_id": "item-1",
        "mapping_reference": "manifest:demo-connector#schema_mappings/item",
        "payload": {"name": "one", "id": 1},
        "updated_at": "2026-09-20T00:00:00Z",
        "provenance": {
            "connector": "demo-connector",
            "adapter_kind": "mcp",
            "server": "demo-server",
            "tool": "list_items",
            "tool_schema_sha256": "07" * 32,
            "source_uri": "demo://items/item-1",
        },
    }],
    "relationships": [],
    "provider_checkpoint": {
        "stream": "items",
        "position": {"page": 2},
        "watermark": "2026-09-20T00:00:00Z",
    },
    "expected_previous_checkpoint": {
        "stream": "items",
        "position": {"page": 1},
    },
    "authoritative_live_ids": None,
    "empty_authoritative_approval": None,
    "withdrawals": [],
}
request = SourceIngestionRequest.model_validate(request_data)
assert request.canonical_digest() == sys.argv[1]
assert request.model_dump(mode="json")["records"][0]["payload"] == {
    "name": "one",
    "id": 1,
}

arbitrary_json = dict(request_data)
arbitrary_json["records"] = [dict(request_data["records"][0])]
arbitrary_json["records"][0]["payload"] = ["nested", {"value": True}]
arbitrary_json["provider_checkpoint"] = dict(request_data["provider_checkpoint"])
arbitrary_json["provider_checkpoint"]["position"] = "opaque-cursor"
arbitrary = SourceIngestionRequest.model_validate(arbitrary_json)
dumped = arbitrary.model_dump(mode="json")
assert dumped["records"][0]["payload"] == ["nested", {"value": True}]
assert dumped["provider_checkpoint"]["position"] == "opaque-cursor"
initial_page = dict(request_data)
initial_page["expected_previous_checkpoint"] = None
assert len(SourceIngestionRequest.model_validate(initial_page).canonical_digest()) == 64

digest = "00" * 32
receipt_payload = {
    "receipt_id": "source-ingest:" + sys.argv[1],
    "disposition": "committed",
    "mode": "delta",
    "batch_digest": sys.argv[1],
    "mappings": [{
        "kind": "entity",
        "mapping_reference": request_data["records"][0]["mapping_reference"],
        "mapping_digest": digest,
        "connector_pack_digest": digest,
        "catalog": {
            "configuration_revision": 7,
            "catalog_generation": 11,
            "snapshot_digest": digest,
            "child_connection_generation": 3,
            "authorization_scope_digest": digest,
        },
    }],
    "raw_admissions": [{
        "stream": "items",
        "record_id": "item-1",
        "raw_digest": digest,
        "deduplicated": False,
    }],
    "relationship_raw_admissions": [],
    "tombstones": [],
    "relationship_tombstones": [],
    "accepted_checkpoint": request_data["provider_checkpoint"],
    "accepted_checkpoint_digest": digest,
    "content_hash": None,
    "live_set_digest": None,
    "relationship_live_set_digest": None,
    "affected_count": 1,
    "relationship_count": 0,
    "tombstoned_count": 0,
    "relationship_tombstoned_count": 0,
    "committed_graph_version": 7,
    "receipt_digest": digest,
}

status_payload = {
    "connector": "demo-connector",
    "stream": "items",
    "accepted_checkpoint": request_data["provider_checkpoint"],
    "accepted_checkpoint_digest": digest,
    "content_hash": None,
    "live_set_digest": None,
    "relationship_live_set_digest": None,
    "last_batch_digest": sys.argv[1],
    "last_receipt_id": receipt_payload["receipt_id"],
    "committed_graph_version": 7,
}

class Client:
    async def _send(self, method, params, graph, *, idempotency_key):
        assert graph == "source"
        if method == "SourceIngest":
            assert params["request"]["connector"] == "demo-connector"
            assert idempotency_key == "ingest-1"
            return receipt_payload
        assert method == "SourceIngestStatus"
        assert params["connector"] == "demo-connector"
        assert params == {"connector": "demo-connector", "stream": "items"}
        assert idempotency_key is None
        return status_payload

receipt = asyncio.run(ingestion.send_source_ingest(
    Client(),
    {"request": request.model_dump(mode="json")},
    "source",
    idempotency_key="ingest-1",
))
assert isinstance(receipt, SourceIngestionReceipt)
assert receipt.committed_graph_version == 7
status = asyncio.run(ingestion.send_source_ingest_status(
    Client(),
    SourceIngestStatusRequest(connector="demo-connector", stream="items"),
    "source",
))
assert isinstance(status, SourceIngestStatus)
assert status.last_receipt_id == receipt.receipt_id
"#;
    let output = Command::new("python3")
        .arg("-c")
        .arg(script)
        .arg(source_ingestion_digest_vector())
        .env("PYTHONPATH", &root)
        .output()
        .expect("execute generated Python modules");
    let _ = fs::remove_dir_all(&root);
    assert!(
        output.status.success(),
        "generated Python execution failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn generated_python_respects_the_formatter_line_limit() {
    let catalog = Catalog::collect();
    let failures: Vec<String> = artifacts(&catalog)
        .into_iter()
        .flat_map(|artifact| {
            let path = artifact.path;
            String::from_utf8(artifact.bytes)
                .expect("generated Python is UTF-8")
                .lines()
                .enumerate()
                .filter(|(_, line)| line.len() > 88)
                .map(move |(index, line)| {
                    format!("{path}:{}: {} columns: {line}", index + 1, line.len())
                })
                .collect::<Vec<_>>()
        })
        .collect();

    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn generated_send_docs_list_errors_individually() {
    let catalog = Catalog::collect();
    let generated: BTreeMap<String, String> = artifacts(&catalog)
        .into_iter()
        .map(|artifact| {
            (
                artifact.path,
                String::from_utf8(artifact.bytes).expect("generated Python is UTF-8"),
            )
        })
        .collect();

    for descriptor in crate::method_descriptors() {
        if !descriptor.serves(ConsumerProfile::PythonClient) {
            continue;
        }
        let path = format!("epistemic_graph/generated/{}.py", descriptor.domain);
        let module = generated.get(&path).expect("generated domain module");
        let send_marker = format!("async def send_{}(", snake_case(descriptor.id.as_str()));
        let send = module
            .split_once(&send_marker)
            .expect("generated send function")
            .1
            .split("\n\nclass ")
            .next()
            .expect("generated send section");
        let bullets = descriptor
            .error_set
            .iter()
            .map(|error| format!("        - {error}"))
            .collect::<Vec<_>>()
            .join("\n");
        let expected = format!("    Errors:\n{bullets}\n");

        assert!(
            send.contains(&expected),
            "{} does not render one bullet per declared error",
            descriptor.id.as_str()
        );
        assert!(!send
            .lines()
            .any(|line| line.trim_start().starts_with("Errors: ")));
    }
}

#[test]
fn nested_ref_surface_emits_concrete_models_and_typed_request() {
    let catalog = Catalog::collect();
    let generated: BTreeMap<String, String> = artifacts(&catalog)
        .into_iter()
        .map(|artifact| {
            (
                artifact.path,
                String::from_utf8(artifact.bytes).expect("generated Python is UTF-8"),
            )
        })
        .collect();
    let module = generated
        .get("epistemic_graph/generated/write_back.py")
        .expect("write-back DTO module");
    for declaration in [
        "class SourceChangeSet(BaseModel):",
        "class WriteBackAuthorizationDecision(BaseModel):",
        "class WriteBackAuthorizationMode(str, Enum):",
        "class WriteBackAttempt(BaseModel):",
        "class ReconciliationObservation(BaseModel):",
        "class WriteBackReceipt(BaseModel):",
        "class ReconciliationReceipt(BaseModel):",
        "def canonical_digest(self) -> str:",
        "def patch_digest(self) -> str:",
    ] {
        assert!(module.contains(declaration), "missing {declaration}");
    }
    assert!(module.contains("PROPOSAL_APPROVAL = \"proposal_approval\""));
    assert!(module.contains("OUTCOME_UNCERTAIN = \"outcome_uncertain\""));
    let storage = generated
        .get("epistemic_graph/generated/storage.py")
        .expect("storage domain module");
    assert!(storage.contains("from .write_back import (\n    WriteBackOp,"));
    let request = storage
        .split_once("class WriteBackRequest(BaseModel):")
        .expect("WriteBack request model")
        .1
        .split_once("async def send_write_back(")
        .expect("WriteBack send function")
        .0;
    assert!(request.contains("    op: WriteBackOp"));
    assert!(!request.contains("    op: Any"));
}

#[test]
fn agent_component_search_emits_typed_kind_only_adapter() {
    let catalog = Catalog::collect();
    let generated: BTreeMap<String, String> = artifacts(&catalog)
        .into_iter()
        .map(|artifact| {
            (
                artifact.path,
                String::from_utf8(artifact.bytes).expect("generated Python is UTF-8"),
            )
        })
        .collect();
    let dto = generated
        .get("epistemic_graph/generated/agent_component.py")
        .expect("agent-component DTO module");
    assert!(dto.contains("class AgentComponentSearchRequest(BaseModel):"));
    assert!(dto.contains("class AgentComponentSearchPage(BaseModel):"));
    assert!(dto.contains("class AgentComponentKind(str, Enum):"));

    let storage = generated
        .get("epistemic_graph/generated/storage.py")
        .expect("storage domain module");
    assert!(storage.contains("    op: AgentComponentOp"));
    assert!(storage.contains(
            "async def send_agent_component_search(\n    client: Any,\n    request: AgentComponentSearchRequest,"
        ));
    assert!(storage.contains(") -> AgentComponentSearchPage:"));
    assert!(storage.contains("len(request.cursor.encode(\"utf-8\")) > 16384"));
    assert!(storage.contains("return AgentComponentSearchPage.model_validate(payload)"));
}

#[test]
fn connector_pack_status_emits_typed_catalog_reconciliation_adapter() {
    let catalog = Catalog::collect();
    let generated: BTreeMap<String, String> = artifacts(&catalog)
        .into_iter()
        .map(|artifact| {
            (
                artifact.path,
                String::from_utf8(artifact.bytes).expect("generated Python is UTF-8"),
            )
        })
        .collect();
    let dto = generated
        .get("epistemic_graph/generated/connector_pack.py")
        .expect("connector-pack DTO module");
    assert!(dto.contains("class McpCatalogSnapshotBinding(BaseModel):"));
    assert!(dto.contains("CONNECTOR_PACK_SCHEMA_VERSION = 2"));
    assert!(dto.contains("class ConnectorPackStatus(BaseModel):"));
    assert!(dto.contains("class PackImportResultImported(BaseModel):"));
    assert!(dto.contains("class PackImportResultUnchanged(BaseModel):"));
    assert!(dto.contains("class PackImportResultRejected(BaseModel):"));
    assert!(dto.contains("PackImportResult = Annotated["));
    assert!(dto.contains("class PackWriteErrorCode(str, Enum):"));
    assert!(dto.contains("    PACK_HEAD_CONFLICT = \"PACK_HEAD_CONFLICT\""));

    let storage = generated
        .get("epistemic_graph/generated/storage.py")
        .expect("storage domain module");
    assert!(storage.contains(
        "async def send_connector_pack_status(\n    client: Any,\n    request: ConnectorPackStatusRequest,"
    ));
    assert!(storage.contains(") -> ConnectorPackStatus:"));
    assert!(storage.contains("return ConnectorPackStatus.model_validate(payload)"));
    assert!(storage.contains(
        "async def send_connector_pack_import(\n    client: Any,\n    request: ConnectorPackImportRequest,"
    ));
    assert!(storage.contains(") -> PackImportResult:"));
    assert!(storage.contains("return TypeAdapter(PackImportResult).validate_python(payload)"));
}

#[test]
fn agent_component_content_emits_typed_body_adapter() {
    let catalog = Catalog::collect();
    let generated: BTreeMap<String, String> = artifacts(&catalog)
        .into_iter()
        .map(|artifact| {
            (
                artifact.path,
                String::from_utf8(artifact.bytes).expect("generated Python is UTF-8"),
            )
        })
        .collect();
    let dto = generated
        .get("epistemic_graph/generated/agent_component.py")
        .expect("agent-component DTO module");
    assert!(dto.contains("class AgentComponentContentRequest(BaseModel):"));
    assert!(dto.contains("class AgentComponentContentResult(BaseModel):"));
    assert!(dto.contains("    body: bytes"));

    let storage = generated
        .get("epistemic_graph/generated/storage.py")
        .expect("storage domain module");
    assert!(storage.contains(
        "async def send_agent_component_content(\n    client: Any,\n    request: AgentComponentContentRequest,"
    ));
    assert!(storage.contains(") -> AgentComponentContentResult:"));
    assert!(storage.contains("return AgentComponentContentResult.model_validate(payload)"));
}

#[test]
fn agent_component_current_uses_the_exact_flattened_operation_shape() {
    let catalog = Catalog::collect();
    let generated: BTreeMap<String, String> = artifacts(&catalog)
        .into_iter()
        .map(|artifact| {
            (
                artifact.path,
                String::from_utf8(artifact.bytes).expect("generated Python is UTF-8"),
            )
        })
        .collect();
    let storage = generated
        .get("epistemic_graph/generated/storage.py")
        .expect("storage domain module");
    assert!(storage.contains(
        "async def send_agent_component_current(\n    client: Any,\n    request: AgentComponentOpCurrent,"
    ));
    assert!(
        storage.contains("params = {\"op\": request.model_dump(mode=\"json\", exclude_none=True)}")
    );
    assert!(
        storage.contains("return TypeAdapter(AgentComponentEntry | None).validate_python(payload)")
    );
}

#[test]
fn registered_server_list_emits_typed_request_page_and_sender() {
    let catalog = Catalog::collect();
    let generated: BTreeMap<String, String> = artifacts(&catalog)
        .into_iter()
        .map(|artifact| {
            (
                artifact.path,
                String::from_utf8(artifact.bytes).expect("generated Python is UTF-8"),
            )
        })
        .collect();
    let dto = generated
        .get("epistemic_graph/generated/server_registry.py")
        .expect("server-registry DTO module");
    assert!(dto.contains("class RegisteredServerListRequest(BaseModel):"));
    assert!(dto.contains("class RegisteredServerCursor(BaseModel):"));
    assert!(dto.contains("class RegisteredServerView(BaseModel):"));
    assert!(dto.contains("class RegisteredServerListPage(BaseModel):"));

    let cluster = generated
        .get("epistemic_graph/generated/cluster.py")
        .expect("cluster domain module");
    assert!(cluster.contains("    request: RegisteredServerListRequest"));
    assert!(cluster.contains("async def send_list_registered_servers(\n    client: Any,"));
    assert!(cluster.contains(") -> RegisteredServerListPage:"));
    assert!(cluster.contains("return RegisteredServerListPage.model_validate(payload)"));
}

#[test]
fn generated_source_ingestion_imports_and_executes_with_rust_digest_parity() {
    let catalog = Catalog::collect();
    let generated: BTreeMap<String, String> = artifacts(&catalog)
        .into_iter()
        .map(|artifact| {
            (
                artifact.path,
                String::from_utf8(artifact.bytes).expect("generated Python is UTF-8"),
            )
        })
        .collect();

    let dto = generated
        .get("epistemic_graph/generated/source_ingestion.py")
        .expect("source ingestion DTO module");
    assert!(dto.contains("SourceJson = Any"));
    assert!(dto.contains("last_receipt_id: str | None = None"));
    assert!(dto.contains("watermark: str | None = None"));
    let source_record = dto
        .find("class SourceRecord(BaseModel):")
        .expect("SourceRecord class");
    let record_alias = dto
        .find("BoundedVec_SourceRecord_1024 =")
        .expect("bounded SourceRecord alias");
    assert!(source_record < record_alias);
    let domain = generated
        .get("epistemic_graph/generated/ingestion.py")
        .expect("ingestion domain module");
    assert!(domain.contains(") -> SourceIngestionReceipt:"));
    assert!(domain.contains("return SourceIngestionReceipt.model_validate(payload)"));
    assert!(domain.contains("request: SourceIngestStatusRequest,"));
    assert!(domain.contains(") -> SourceIngestStatus:"));
    assert!(domain.contains("return SourceIngestStatus.model_validate(payload)"));

    execute_source_ingestion_modules(&generated);
}

#[test]
fn index_repository_emits_typed_ordered_file_outcomes() {
    let catalog = Catalog::collect();
    let generated: BTreeMap<String, String> = artifacts(&catalog)
        .into_iter()
        .map(|artifact| {
            (
                artifact.path,
                String::from_utf8(artifact.bytes).expect("generated Python is UTF-8"),
            )
        })
        .collect();

    let dto = generated
        .get("epistemic_graph/generated/index_repository.py")
        .expect("IndexRepository DTO module");
    for declaration in [
        "class IndexDiagnostic(BaseModel):",
        "class IndexFileOutcome(BaseModel):",
        "class IndexFileStatus(str, Enum):",
        "class IndexResult(BaseModel):",
    ] {
        assert!(dto.contains(declaration), "missing {declaration}");
    }
    assert!(dto.contains("SUCCESS = \"success\""));
    assert!(dto.contains("UNSUPPORTED = \"unsupported\""));
    assert!(dto.contains("ERROR = \"error\""));
    assert!(dto.contains("file_outcomes: list[IndexFileOutcome]"));
    assert!(dto.contains("max_length=8"));

    let ingestion = generated
        .get("epistemic_graph/generated/ingestion.py")
        .expect("ingestion domain module");
    assert!(ingestion.contains("from .index_repository import (\n    IndexResult,"));
    assert!(ingestion.contains("    files_msgpack: bytes"));
    assert!(ingestion.contains(") -> IndexResult:"));
    assert!(ingestion.contains("return IndexResult.model_validate(payload)"));
}

#[test]
fn digest_projection_names_exact_source_change_set_fields() {
    let catalog = Catalog::collect();
    let document = schema::method_request_document();
    let definitions = merged_definitions(&document, &catalog, "storage");
    let source = definitions
        .get("SourceChangeSet")
        .and_then(|node| node.get("properties"))
        .and_then(|node| node.as_object())
        .expect("SourceChangeSet object schema");
    let spec = CANONICAL_DIGEST_SPECS
        .iter()
        .find(|spec| spec.model == "SourceChangeSet")
        .expect("SourceChangeSet digest projection");
    let projected: std::collections::BTreeSet<_> = spec.projection_fields.iter().copied().collect();
    let declared: std::collections::BTreeSet<_> = source.keys().map(String::as_str).collect();
    assert_eq!(projected, declared);
    assert!(spec
        .digest_field
        .is_none_or(|field| projected.contains(field)));
    assert!(spec
        .canonical_json_paths
        .iter()
        .map(|path| path.split(['.', '[']).next().expect("nonempty path"))
        .all(|field| projected.contains(field)));
    assert!(spec
        .omit_none_paths
        .iter()
        .map(|path| path.split(['.', '[']).next().expect("nonempty path"))
        .all(|field| projected.contains(field)));
}

#[test]
fn source_ingestion_digest_marker_matches_rust_contract_constants() {
    use eg_types::source_ingestion::{
        SOURCE_INGESTION_CANONICAL_JSON_PATHS, SOURCE_INGESTION_DIGEST_PROJECTION_FIELDS,
        SOURCE_INGESTION_OMIT_NONE_PATHS,
    };

    let spec = CANONICAL_DIGEST_SPECS
        .iter()
        .find(|spec| spec.model == "SourceIngestionRequest")
        .expect("SourceIngestionRequest digest projection");
    assert_eq!(
        spec.projection_fields,
        SOURCE_INGESTION_DIGEST_PROJECTION_FIELDS
    );
    assert_eq!(
        spec.canonical_json_paths,
        SOURCE_INGESTION_CANONICAL_JSON_PATHS
    );
    assert_eq!(spec.omit_none_paths, SOURCE_INGESTION_OMIT_NONE_PATHS);
}
