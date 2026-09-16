use super::*;
use crate::server::sql_catalog_acl::{create_owned_table, grant, SqlPrivilege};
use eg_query::{Column, ColumnType, TableSchema};
use eg_types::acl::RequestContextClaims;
use eg_types::contract::{BoundedVec, RecordBytes, ResourceId};
use eg_types::storage_wire::{
    SqlSourceBatch, SqlSourceCell, SqlSourceDescriptor, SqlSourceJson, SqlSourceMappingDescriptor,
    SqlSourceText,
};

pub(super) struct Fixture {
    pub(super) directory: PathBuf,
    pub(super) owner: CarrierAuthority,
    pub(super) schema: TableSchema,
}

impl Fixture {
    pub(super) fn new() -> Self {
        let directory = crate::server::sql_tables::test_persist_dir();
        let owner = authority("alice", "tenant-a", "owner", true);
        let schema = TableSchema::new(
            "issues",
            vec![
                Column::new("id", ColumnType::BigInt, false, true),
                Column::new("owner_tag", ColumnType::Text, false, false),
                Column::new("payload", ColumnType::Json, true, false),
            ],
        );
        create_owned_table(&owner, &directory, &schema, false).unwrap();
        Self {
            directory,
            owner,
            schema,
        }
    }

    pub(super) fn store(&self) -> TableStore {
        crate::server::sql_tables::tenant_table_store(self.owner.tenant_scope(), &self.directory)
            .unwrap()
    }

    pub(super) fn grant_insert(&self, agent: &str) {
        grant(
            &self.directory,
            &self.owner,
            "issues",
            agent,
            &[SqlPrivilege::Insert],
            uuid::Uuid::new_v4(),
        )
        .unwrap();
    }

    pub(super) fn evict_tenant(&self, tenant: &str) {
        use crate::server::sql_tables::{
            evict_for_test, tenant_acl_path_for_test, tenant_table_path_for_test,
        };
        evict_for_test(&tenant_table_path_for_test(tenant, &self.directory));
        evict_for_test(&tenant_acl_path_for_test(tenant, &self.directory));
    }

    pub(super) fn enable_rls(&self) {
        crate::server::sql_catalog_acl::set_row_level_column(
            &self.directory,
            &self.owner,
            "issues",
            Some("owner_tag"),
            uuid::Uuid::new_v4(),
        )
        .unwrap();
    }

    pub(super) fn request(&self, stamp: &str) -> SqlSourceBatchRequest {
        SqlSourceBatchRequest::new(SqlSourceBatch {
            source: id("jira"),
            partition: SqlSourceText::new("project-a".into()).unwrap(),
            position: eg_types::change_envelope::CursorPosition::Sequence(1),
            expected_previous: None,
            source_descriptor: SqlSourceDescriptor {
                provider: id("jira"),
                dataset: id("issues"),
                metadata: SqlSourceJson::new(serde_json::json!({"deployment":"internal"})).unwrap(),
            },
            mapping_descriptor: SqlSourceMappingDescriptor {
                format: id("json"),
                content: RecordBytes::new(b"issue-id maps to id".to_vec()).unwrap(),
            },
            table: id("issues"),
            columns: BoundedVec::new(vec![id("id"), id("owner_tag"), id("payload")]).unwrap(),
            rows: BoundedVec::new(vec![BoundedVec::new(vec![
                SqlSourceCell::Int(1),
                SqlSourceCell::Text(SqlSourceText::new(stamp.into()).unwrap()),
                SqlSourceCell::Json(SqlSourceJson::new(serde_json::Value::Null).unwrap()),
            ])
            .unwrap()])
            .unwrap(),
            expected_schema_version: self.store().schema_version("issues").unwrap(),
            expected_schema_digest: Digest256::parse(&self.schema.schema_digest().unwrap())
                .unwrap(),
        })
        .unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        use crate::server::sql_tables::{
            evict_for_test, tenant_acl_path_for_test, tenant_table_path_for_test,
        };
        evict_for_test(&tenant_table_path_for_test(
            self.owner.tenant_scope(),
            &self.directory,
        ));
        evict_for_test(&tenant_acl_path_for_test(
            self.owner.tenant_scope(),
            &self.directory,
        ));
        std::fs::remove_dir_all(&self.directory).unwrap();
    }
}

pub(super) fn verified(
    agent: &str,
    tenant: &str,
    key: &str,
    write: bool,
) -> VerifiedRequestContext {
    let template = VerifiedRequestContext::verified_for_test_with_scopes(
        agent,
        tenant,
        &["kg:read", "kg:write"],
    );
    let mut claims: RequestContextClaims = template.claims().clone();
    if !write {
        claims.scopes = vec!["kg:read".into()];
    }
    VerifiedRequestContext::from_verified_claims(claims, key.into())
}

pub(super) fn authority(agent: &str, tenant: &str, key: &str, write: bool) -> CarrierAuthority {
    CarrierAuthority::from_verified(&verified(agent, tenant, key, write)).unwrap()
}

pub(super) fn id(name: &str) -> ResourceId {
    ResourceId::new(name).unwrap()
}

pub(super) fn change(
    request: &SqlSourceBatchRequest,
    edit: impl FnOnce(&mut SqlSourceBatch),
) -> SqlSourceBatchRequest {
    let mut batch = request.as_batch().clone();
    edit(&mut batch);
    SqlSourceBatchRequest::new(batch).unwrap()
}

pub(super) fn submit(
    fixture: &Fixture,
    carrier: &CarrierAuthority,
    request: SqlSourceBatchRequest,
    nonce: u8,
) -> Result<SqlSourceBatchResult, String> {
    publish(
        10,
        carrier,
        Some(Nonce::from_bytes([nonce; 32])),
        request,
        &fixture.directory,
        100,
    )
}
