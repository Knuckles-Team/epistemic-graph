//! Maintained-ANN narrowing of the served SQL read projection (RF-019).
//!
//! The served read path copies every table the caller may select into an
//! ephemeral projection, row-level security applied, and runs the statement
//! there. For a nearest-neighbour read of a user table that copy would be a full
//! scan per query; instead the tenant catalog's maintained ANN authority returns
//! the table's nearest `LIMIT + OFFSET` rows with the caller's row-level-security
//! predicate — and the statement's admissible `WHERE` — applied INSIDE the probe,
//! and only those rows enter the projection. A row whose visibility identity is
//! unresolved (a NULL or absent discriminator) is admitted by nothing, exactly as
//! in [`AuthorizedTable::select`] (the CX-022 fail-closed invariant).
//!
//! The projection also carries the managed-index status rows the caller may see
//! (EH-352), so `information_schema.eg_index_status` is answered under the same
//! ACL as the tables: only indexes of tables the caller may `SELECT`.

use std::path::Path;
use std::sync::Arc;

use eg_query::{Cell, TableStore, UserAnnDecision, UserAnnPushdown};

use super::{
    open_authorized_table, project_read_store, selectable_tables, AuthorizedReadStore,
    AuthorizedTable, SqlPrivilege, ACCESS_DENIED,
};
use crate::graph::GraphCore;

/// The request graph of a served read: its edge indexes' statuses join the
/// status relation beside the tenant's table indexes (EH-352).
#[derive(Clone)]
pub(crate) struct RequestGraph {
    pub(crate) name: String,
    pub(crate) core: Arc<GraphCore>,
}
use crate::server::access::CarrierAuthority;
use crate::server::sql_tables;

/// The authorized read projection for `query`. When `query` is a maintained-ANN
/// read of a user table, that table holds only its nearest visible rows; every
/// other table is the full visible copy [`super::authorized_read_store`] makes. The
/// caller's read-only relations (EH-066) join the projection when `query` can see them.
pub(crate) fn authorized_read_store_for_query(
    authority: &CarrierAuthority,
    persist_dir: &Path,
    query: &str,
    read_only: Option<&dyn super::ReadOnlyRelations>,
    graph: &RequestGraph,
) -> Result<AuthorizedReadStore, String> {
    let tenant = sql_tables::tenant_table_store(authority.tenant_scope(), persist_dir)?;
    let narrowing = match eg_query::user_ann_decision(query, &tenant)? {
        UserAnnDecision::Pushdown(pushdown) => Some(pushdown),
        UserAnnDecision::Declined(reason) => {
            tracing::debug!(
                ?reason,
                "maintained ANN narrowing declined for a served read"
            );
            None
        }
        UserAnnDecision::NotApplicable => None,
    };
    let read_only = read_only.filter(|_| super::wants_read_only_relations(query));
    let projection = project_read_store(authority, persist_dir, narrowing.as_ref(), read_only)?;
    adopt_index_status(authority, persist_dir, &tenant, projection.store())?;
    adopt_edge_index_status(&tenant, graph, projection.store())?;
    Ok(projection)
}

/// The request graph's edge-index statuses, after installing every edge index
/// the tenant registered for it, so an index is listed even before its first
/// edge operation after a restart. The ONE builder every served read path
/// (Method::Sql, KnowledgeStream, pgwire) goes through.
fn adopt_edge_index_status(
    tenant: &TableStore,
    graph: &RequestGraph,
    projection: &TableStore,
) -> Result<(), String> {
    eg_query::edge_index::install_edge_indexes(tenant, &graph.name, &graph.core)?;
    projection
        .ann_authority()
        .adopt_statuses(graph.core.indexes().managed_statuses());
    Ok(())
}

/// The table of ANN index `name` when THIS caller may ALTER it. An index on a
/// table the caller may not alter is reported exactly like a missing one, so
/// `DROP INDEX [IF EXISTS]` never discloses an index across an authorization
/// boundary (EH-352).
pub(crate) fn alterable_ann_index_table(
    authority: &CarrierAuthority,
    persist_dir: &Path,
    name: &str,
) -> Result<Option<String>, String> {
    let tenant = sql_tables::tenant_table_store(authority.tenant_scope(), persist_dir)?;
    let Some(table) = tenant.ann_index_table(name)? else {
        return Ok(None);
    };
    match open_authorized_table(authority, persist_dir, &table, SqlPrivilege::Alter) {
        Ok(_) => Ok(Some(table)),
        Err(error) if error == ACCESS_DENIED => Ok(None),
        Err(error) => Err(error),
    }
}

/// Hand `projection` the tenant's managed-index status rows the caller may see:
/// those of tables it may `SELECT`, with no row count for a table under
/// row-level security (the count would include rows hidden from the caller).
fn adopt_index_status(
    authority: &CarrierAuthority,
    persist_dir: &Path,
    tenant: &TableStore,
    projection: &TableStore,
) -> Result<(), String> {
    let selectable = selectable_tables(authority, persist_dir)?;
    let mut visible = Vec::new();
    for mut status in tenant.managed_index_status()? {
        let relation = status.target.relation();
        let Some(table) = selectable
            .iter()
            .find(|name| name.eq_ignore_ascii_case(relation))
        else {
            continue;
        };
        let authorized =
            open_authorized_table(authority, persist_dir, table, SqlPrivilege::Select)?;
        if authorized.rls_column.is_some() {
            status.indexed = None;
        }
        visible.push(status);
    }
    projection.ann_authority().adopt_statuses(visible);
    Ok(())
}

/// The rows of `table` the projection holds: the maintained nearest rows when
/// `narrowing` targets it, else every visible row.
pub(super) fn visible_rows(
    authorized: &AuthorizedTable,
    table: &str,
    narrowing: Option<&UserAnnPushdown>,
) -> Result<Vec<Vec<Cell>>, String> {
    match narrowing.filter(|pushdown| pushdown.table().eq_ignore_ascii_case(table)) {
        Some(pushdown) => authorized.ann_top_k(pushdown),
        None => authorized.select(None),
    }
}

impl AuthorizedTable {
    /// The nearest rows of `pushdown` this actor may see: row-level security AND
    /// the statement's admissible `WHERE` run inside the maintained probe, under
    /// the same current-authority check as [`Self::select`].
    fn ann_top_k(&self, pushdown: &UserAnnPushdown) -> Result<Vec<Vec<Cell>>, String> {
        self.with_current_authority(|| {
            let prefilter = self.combined_predicate(pushdown.filter.as_ref());
            Ok(self
                .store
                .ann_top_k(&pushdown.request(Some(&prefilter)))?
                .rows)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        create_owned_table, grant, open_authorized_table, set_row_level_column, SqlPrivilege,
    };
    use super::*;
    use crate::server::auth::VerifiedRequestContext;
    use crate::server::sql_tables::test_persist_dir;
    use eg_query::{
        AnnIndexPlan, AnnMethod, AnnRefreshPolicy, AnnServingPath, Column, ColumnType, TableSchema,
        VectorMetric,
    };
    use serde_json::{json, Value};

    const TENANT: &str = "tenant-ann-projection";
    const QUERY: [f32; 4] = [1.0, 1.0, 1.0, 1.0];

    fn authority(agent_id: &str) -> CarrierAuthority {
        CarrierAuthority::from_verified(&VerifiedRequestContext::verified_for_test_in_tenant(
            agent_id, TENANT,
        ))
        .unwrap()
    }

    fn row(id: &str, scale: f32) -> Vec<Value> {
        vec![json!(id), json!(QUERY.map(|x| x * scale))]
    }

    /// `docs` with row-level security on `owner_tag`: bob's rows are the nearest,
    /// then an owner-less row (CX-022's unresolved identity), then alice's.
    fn tenant_docs(dir: &Path) -> AnnIndexPlan {
        let alice = authority("alice");
        let schema = TableSchema::new(
            "docs",
            vec![
                Column::new("id", ColumnType::Text, false, true),
                Column::new("emb", ColumnType::Vector(Some(4)), true, false),
                Column::new("owner_tag", ColumnType::Text, true, false),
            ],
        );
        create_owned_table(&alice, dir, &schema, false).unwrap();
        set_row_level_column(dir, &alice, "docs", Some("owner_tag"), uuid::Uuid::new_v4()).unwrap();
        let privileges = [SqlPrivilege::Select, SqlPrivilege::Insert];
        grant(
            dir,
            &alice,
            "docs",
            "bob",
            &privileges,
            uuid::Uuid::new_v4(),
        )
        .unwrap();
        let columns = ["id".to_string(), "emb".to_string()];
        let insert = |who: &CarrierAuthority, rows: &[Vec<Value>]| {
            open_authorized_table(who, dir, "docs", SqlPrivilege::Insert)
                .unwrap()
                .insert(&columns, rows)
                .unwrap();
        };
        insert(&alice, &[row("a1", 3.0), row("a2", 4.0), row("a3", 5.0)]);
        insert(&authority("bob"), &[row("b1", 1.0), row("b2", 1.1)]);
        let tenant = sql_tables::tenant_table_store(alice.tenant_scope(), dir).unwrap();
        // The unresolved-identity row, written beneath the ACL layer on purpose.
        tenant
            .insert_rows("docs", &columns, &[row("orphan", 1.0)])
            .unwrap();
        let index = AnnIndexPlan {
            name: Some("docs_emb".to_string()),
            table: "docs".to_string(),
            column: "emb".to_string(),
            method: AnnMethod::Hnsw,
            metric: VectorMetric::L2,
            if_not_exists: false,
        };
        tenant.put_ann_index(&index).unwrap();
        tenant
            .refresh_ann_generations(AnnRefreshPolicy::Immediate)
            .unwrap();
        index
    }

    fn no_graph() -> RequestGraph {
        RequestGraph {
            name: "ann-projection-graph".to_string(),
            core: Arc::new(GraphCore::new()),
        }
    }

    fn projected_ids(dir: &Path, who: &str, sql: &str) -> Vec<String> {
        let projection =
            authorized_read_store_for_query(&authority(who), dir, sql, None, &no_graph()).unwrap();
        let view = crate::graph::GraphView::default();
        eg_query::exec_sql_typed_with_tables(&view, projection.store(), sql)
            .unwrap()
            .rows
            .iter()
            .map(|cells| cells[0].as_str().unwrap().to_string())
            .collect()
    }

    #[test]
    fn a_served_ann_read_probes_with_row_level_security_inside_the_probe() {
        let dir = test_persist_dir();
        tenant_docs(&dir);
        let sql = "SELECT id FROM docs ORDER BY emb <-> '[1,1,1,1]' LIMIT 2";

        assert_eq!(projected_ids(&dir, "alice", sql), ["a1", "a2"]);
        assert_eq!(projected_ids(&dir, "bob", sql), ["b1", "b2"]);

        let tenant =
            sql_tables::tenant_table_store(authority("alice").tenant_scope(), &dir).unwrap();
        let receipts = tenant.ann_authority().recent_receipts();
        assert_eq!(
            receipts.len(),
            2,
            "both reads went through the maintained authority"
        );
        assert!(receipts.iter().all(|receipt| matches!(
            receipt.path,
            AnnServingPath::MaintainedIndex { generation: 1 }
        )));
        assert!(
            receipts.iter().all(|receipt| receipt.returned_rows == 2),
            "hidden rows never occupy the k slots"
        );
    }

    #[test]
    fn an_unresolved_visibility_identity_is_never_served() {
        let dir = test_persist_dir();
        tenant_docs(&dir);
        let sql = "SELECT id FROM docs ORDER BY emb <-> '[1,1,1,1]' LIMIT 10";

        for who in ["alice", "bob"] {
            let ids = projected_ids(&dir, who, sql);
            assert!(!ids.contains(&"orphan".to_string()), "{who} saw {ids:?}");
        }
        let with_filter =
            "SELECT id FROM docs WHERE id = 'orphan' ORDER BY emb <-> '[1,1,1,1]' LIMIT 1";
        assert!(projected_ids(&dir, "alice", with_filter).is_empty());
    }

    /// `secrets`: alice's own indexed table, no grant, no row-level security.
    fn tenant_secrets(dir: &Path) {
        let alice = authority("alice");
        let schema = TableSchema::new(
            "secrets",
            vec![
                Column::new("id", ColumnType::Text, false, true),
                Column::new("emb", ColumnType::Vector(Some(4)), true, false),
            ],
        );
        create_owned_table(&alice, dir, &schema, false).unwrap();
        open_authorized_table(&alice, dir, "secrets", SqlPrivilege::Insert)
            .unwrap()
            .insert(&["id".to_string(), "emb".to_string()], &[row("s1", 2.0)])
            .unwrap();
        let tenant = sql_tables::tenant_table_store(alice.tenant_scope(), dir).unwrap();
        tenant
            .put_ann_index(&AnnIndexPlan {
                name: Some("secrets_emb".to_string()),
                table: "secrets".to_string(),
                column: "emb".to_string(),
                method: AnnMethod::Hnsw,
                metric: VectorMetric::L2,
                if_not_exists: false,
            })
            .unwrap();
        tenant
            .refresh_ann_generations(AnnRefreshPolicy::Immediate)
            .unwrap();
    }

    fn status_rows(dir: &Path, who: &str) -> Vec<Vec<Value>> {
        let sql = "SELECT index_name, state, indexed FROM information_schema.eg_index_status \
                   ORDER BY index_name";
        let projection =
            authorized_read_store_for_query(&authority(who), dir, sql, None, &no_graph()).unwrap();
        let view = crate::graph::GraphView::default();
        eg_query::exec_sql_typed_with_tables(&view, projection.store(), sql)
            .unwrap()
            .rows
    }

    /// EH-352: the status relation is answered under the table ACL — a caller
    /// sees only the indexes of tables it may select, and no row count of a
    /// table under row-level security.
    #[test]
    fn the_index_status_relation_shows_only_what_the_caller_may_select() {
        let dir = test_persist_dir();
        tenant_docs(&dir);
        tenant_secrets(&dir);

        assert_eq!(
            status_rows(&dir, "bob"),
            vec![vec![json!("docs_emb"), json!("active"), Value::Null]],
            "bob may select docs only; its row count is RLS-withheld"
        );
        assert_eq!(
            status_rows(&dir, "alice"),
            vec![
                vec![json!("docs_emb"), json!("active"), Value::Null],
                vec![json!("secrets_emb"), json!("active"), json!(1)],
            ]
        );
        assert!(status_rows(&dir, "mallory").is_empty());
    }
}
