use super::*;
use crate::server::sql_catalog_acl::{create_owned_table, grant, SqlPrivilege};
use eg_query::{Column, ColumnType, TableSchema};
use eg_types::acl::RequestContextClaims;
use eg_types::storage_wire::{SqlSourceCell, SqlSourceJson, SqlSourceText};
pub(super) use eg_types::test_support::sql_source::{self, change, id, SqlSourceTarget};

pub(super) struct Fixture {
    pub(super) directory: PathBuf,
    pub(super) owner: CarrierAuthority,
    pub(super) schema: TableSchema,
}

impl Fixture {
    pub(super) fn new() -> Self {
        Self::for_tenant("tenant-a")
    }

    /// A fixture whose owner, ACL and table store belong to `tenant`. The served
    /// path derives its tenant scope from the verified envelope's own tenant
    /// claim, so an end-to-end test must build the catalog under the tenant the
    /// deployment's request-context policy expects, not the direct-call default.
    pub(super) fn for_tenant(tenant: &str) -> Self {
        let directory = crate::server::sql_tables::test_persist_dir();
        let owner = authority("alice", tenant, "owner", true);
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
        let target = SqlSourceTarget {
            table: "issues",
            columns: &["id", "owner_tag", "payload"],
            schema_version: self.store().schema_version("issues").unwrap(),
            schema_digest: Digest256::parse(&self.schema.schema_digest().unwrap()).unwrap(),
        };
        let row = vec![
            SqlSourceCell::Int(1),
            SqlSourceCell::Text(SqlSourceText::new(stamp.into()).unwrap()),
            SqlSourceCell::Json(SqlSourceJson::new(serde_json::Value::Null).unwrap()),
        ];
        SqlSourceBatchRequest::new(sql_source::batch(&target, vec![row])).unwrap()
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
