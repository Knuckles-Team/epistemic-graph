//! NE-045 acceptance test — EG cache/eviction, graph-delete mutation purge,
//! graph-lifecycle wire, durable owner/visibility.
//!
//! The "prior mutation-authority state is purged, not inherited" half of this
//! ledger row is already PROVEN by `src/redb_store.rs`'s own unit test
//! `delete_graph_purges_prior_incarnation_mutation_authority` (D-P0-U04) and is
//! not re-proven here. This file exercises this row's two remaining conditions,
//! neither of which has an existing test anywhere in the tree (grep-confirmed:
//! no test combines `evict_resident`/`is_resident`/`catalog_len` with a
//! delete-then-recreate cycle, and no test reopens a durable RBAC store across a
//! process restart at all):
//!
//! 1. **"cache and eviction registry agree after a whole-graph transition"** --
//!    `GraphRegistry` IS both the bounded resident-`GraphCore` cache (`self.
//!    graphs`) and the eviction/materialization bookkeeping (`self.catalog`,
//!    `self.materialization`) for the SAME graph name (`crates/eg-core/src/
//!    registry.rs`). `evict_resident` demotes a graph to catalog-only WITHOUT
//!    touching the catalog entry (only cancelling in-flight page tasks and
//!    resetting the materialization manifest to catalog-only) -- so a graph
//!    that was evicted, then deleted, then recreated under the SAME name must
//!    come back with `is_resident() == true`, exactly one entry in each of the
//!    resident cache and the catalog, and a genuinely NEW incarnation id, not a
//!    phantom entry inherited from the evicted-then-deleted incarnation.
//! 2. **"owner/visibility survives restart"** -- `IsolationLayer::
//!    with_persist_dir` durably persists RBAC roles/grants AND registered agent
//!    identities to `{dir}/rbac.redb`, reloading them at boot (module doc:
//!    "Any previously-persisted RBAC policy + registered agent identities are
//!    LOADED at boot"). `IsolationLayer::provision_tenant_graph_access` (the
//!    CreateGraph-time auto-provisioning that makes a tenant graph's owner
//!    role/grant durable, `crates/eg-core/src/isolation.rs`) is exercised here
//!    directly (the same call `Method::CreateGraph`'s dispatch handler makes),
//!    then the WHOLE `IsolationLayer` is dropped and reopened on the identical
//!    persist dir -- a genuine process-restart proof, not merely re-reading an
//!    in-memory value -- and the owner's visibility is proven to survive
//!    (positive) while a different tenant's principal, registered fresh AFTER
//!    the restart, remains denied (negative half: restart durability is not a
//!    reset to open-access).

#![cfg(feature = "security")]

use epistemic_graph::acl::{AgentIdentity, AgentRole};
use epistemic_graph::isolation::{AccessLevel, IsolationLayer};
use epistemic_graph::protocol::GraphType;
use epistemic_graph::registry::GraphRegistry;

/// NE-045, condition 1: cache (resident map) and eviction registry (catalog)
/// agree after evict -> delete -> recreate under the SAME graph name.
#[test]
fn cache_and_eviction_registry_agree_after_delete_recreate_of_an_evicted_graph() {
    let mut reg = GraphRegistry::new();
    const NAME: &str = "adopt:ne045-cache-evict";

    // `GraphRegistry::new()` pre-populates `__commons__` in BOTH the resident map
    // and the catalog, so neither starts empty. Measure the baseline instead of
    // hardcoding 0/1 -- the original absolute counts asserted an empty registry
    // and failed on the built-in entry, not on any real cache/catalog divergence.
    let base_catalog = reg.catalog_len();
    let base_resident = reg.resident_len();

    reg.create_graph_with_incarnation(
        NAME,
        GraphType::Agent,
        None,
        "incarnation:one".to_string(),
        0,
    )
    .expect("create first incarnation");
    assert!(reg.is_resident(NAME), "freshly created graph is resident");

    // Evict it to catalog-only (the LRU/memory-budget path, `cost.rs`'s own
    // `registry.evict_resident` call) -- resident cache loses it, catalog keeps
    // it.
    assert!(
        reg.evict_resident(NAME),
        "evict_resident must find the resident entry"
    );
    assert!(
        !reg.is_resident(NAME),
        "evicted graph is no longer resident"
    );
    assert_eq!(
        reg.catalog_len(),
        base_catalog + 1,
        "eviction does not remove the catalog entry"
    );

    // Delete the (now catalog-only) graph outright.
    reg.delete_graph(NAME).expect("delete the evicted graph");
    assert!(!reg.is_resident(NAME));
    assert_eq!(
        reg.catalog_len(),
        base_catalog,
        "delete must clear BOTH resident and catalog state"
    );
    assert_eq!(reg.resident_len(), base_resident);

    // Recreate under the identical name with a genuinely new incarnation id.
    reg.create_graph_with_incarnation(
        NAME,
        GraphType::Agent,
        None,
        "incarnation:two".to_string(),
        0,
    )
    .expect("create second incarnation");

    // The cache and the eviction/catalog registry must AGREE on the new
    // incarnation: exactly one resident entry, exactly one catalog entry, no
    // phantom leftover from the evicted-then-deleted first incarnation.
    assert!(
        reg.is_resident(NAME),
        "the new incarnation must be resident, not silently left catalog-only \
         by a stale eviction record"
    );
    assert_eq!(
        reg.resident_len(),
        base_resident + 1,
        "exactly the new incarnation, nothing phantom"
    );
    assert_eq!(
        reg.catalog_len(),
        base_catalog + 1,
        "cache and catalog counts must agree"
    );
    let handle = reg.handle(NAME).expect("handle for the new incarnation");
    assert_eq!(
        handle.incarnation_id, "incarnation:two",
        "the new incarnation id must win -- the evicted/deleted first incarnation \
         must not still be what the registry answers with"
    );
}

/// NE-045, condition 2: durable owner/visibility (RBAC role + grant + the
/// registered principal) survives a genuine restart -- drop the whole
/// `IsolationLayer` (releasing every `Arc<redb::Database>` handle) and reopen a
/// FRESH one on the identical persist dir.
#[tokio::test]
async fn tenant_graph_owner_visibility_survives_an_isolation_layer_restart() {
    let dir = std::env::temp_dir().join(format!(
        "adopt-ne045-rbac-restart-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let dir_s = dir.to_string_lossy().into_owned();

    const GRAPH: &str = "tenant__ne045acme____commons__";
    const OWNER_PRINCIPAL: &str = "ne045-webui-end-user";

    {
        let mut isolation = IsolationLayer::with_persist_dir(&dir_s).expect("open rbac store");
        isolation.register_agent(AgentIdentity {
            agent_id: OWNER_PRINCIPAL.to_string(),
            role: AgentRole::Agent,
            teams: Vec::new(),
            roles: vec!["tenant:ne045acme".to_string()],
        });
        // The exact call `Method::CreateGraph`'s dispatch handler makes on a
        // successful tenant-graph creation (`src/server/dispatch.rs`).
        isolation
            .provision_tenant_graph_access(GRAPH, None)
            .expect("auto-provision tenant RBAC access");

        assert!(
            isolation.check_access(
                OWNER_PRINCIPAL,
                GRAPH,
                GraphType::Agent,
                None,
                AccessLevel::Read
            ),
            "the tenant principal must see the graph BEFORE any restart"
        );
        assert!(isolation.check_access(
            OWNER_PRINCIPAL,
            GRAPH,
            GraphType::Agent,
            None,
            AccessLevel::Write
        ));

        // Drop every handle this scope owns (the sole `Arc<RbacStore>` clone),
        // releasing redb's advisory per-file lock, before reopening below.
        drop(isolation);
    }

    // Reopen a genuinely FRESH `IsolationLayer` on the identical dir -- the
    // restart proof (mirrors `graphql_crossmodal_durable.rs`'s
    // shutdown-drop-reopen pattern for the graph-row durable tier; RBAC's own
    // store has no separate `shutdown()`, so dropping every owning handle is
    // the release step here).
    let reopened = {
        let mut attempt = 0;
        loop {
            match IsolationLayer::with_persist_dir(&dir_s) {
                Ok(layer) => break layer,
                Err(error) if attempt < 100 => {
                    attempt += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    let _ = error;
                }
                Err(error) => panic!("reopen rbac store: {error:?}"),
            }
        }
    };

    // Positive half: the SAME owner principal, never re-registered, is still
    // visible post-restart -- both the identity and the RBAC role/grant were
    // durably reloaded, not reconstructed from nothing.
    assert!(
        reopened.check_access(
            OWNER_PRINCIPAL,
            GRAPH,
            GraphType::Agent,
            None,
            AccessLevel::Read
        ),
        "owner visibility must survive a restart on the same durable store"
    );
    assert!(reopened.check_access(
        OWNER_PRINCIPAL,
        GRAPH,
        GraphType::Agent,
        None,
        AccessLevel::Write
    ));

    // Negative half: restart durability is not a reset to open access. A
    // DIFFERENT tenant's principal, registered fresh only AFTER the restart
    // (so it could not possibly have ridden along on the reload), must still
    // be denied -- the isolation boundary itself, not just the data, survives.
    let mut reopened = reopened;
    reopened.register_agent(AgentIdentity {
        agent_id: "ne045-other-tenant-user".to_string(),
        role: AgentRole::Agent,
        teams: Vec::new(),
        roles: vec!["tenant:someone-else".to_string()],
    });
    assert!(
        !reopened.check_access(
            "ne045-other-tenant-user",
            GRAPH,
            GraphType::Agent,
            None,
            AccessLevel::Read
        ),
        "a different tenant's principal must remain denied after restart -- \
         durability must not degrade into open access"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
