//! Consistency test: cross-checks [`eg_capabilities::policy`] against the REAL existing
//! classifiers, as of this commit:
//!   - `src/server/access.rs::requires_write`   (mutates)
//!   - `src/wal.rs::is_durable_mutation`         (durability)
//!   - `src/audit.rs::audit_line`                (audited)
//!   - `src/server/cdc.rs::emit_for_method`      (emits_cdc)
//!
//! ## Known limitation: this is a snapshot mirror, not a live call
//!
//! `eg-capabilities` is a leaf crate (`eg-types` only, see `Cargo.toml`) so that it can be
//! built and tested in isolation. `access::requires_write` is `pub(crate)` inside a private
//! `server::access` submodule of the root `epistemic-graph` binary crate, and pulling that
//! whole crate in as a dependency here (even a dev-dependency) would defeat the entire point
//! of keeping this crate a fast, targeted, leaf build. So instead of a live function call,
//! the constants below are a literal, hand-transcribed snapshot of those four functions'
//! match arms, each cited by file + function name. If `access.rs`/`wal.rs`/`audit.rs`/
//! `cdc.rs` change without updating this file, this test will start passing against a STALE
//! mirror instead of the real thing -- it will not catch that drift by itself.
//!
//! Closing that gap for real (making the mirror unnecessary) is exactly what EG-P0-2/EG-P0-6
//! are for: once the handlers CONSUME `policy()` instead of their own separate classifiers,
//! there is only one implementation and nothing left to mirror. That refactor is explicitly
//! out of scope for EG-P0-1 (this workstream) per the task brief -- this test's job today is
//! only to prove the two descriptions agree RIGHT NOW, and to print every place they don't.

use eg_capabilities::DurabilityDomain;
use eg_types::protocol::Method;

// ── Mirrored classifier snapshots ──────────────────────────────────────────────────

/// Mirrors `src/server/access.rs::requires_write`'s UNCONDITIONAL-true set (both the
/// feature-gated `if matches!(...) { return true; }` blocks -- rdf/kv/broker -- and the
/// final unconditional `matches!(...)` tail). For every one of these, `requires_write`
/// returns `true` no matter what the request's fields are.
const ACCESS_RS_MUTATES_UNCONDITIONAL: &[&str] = &[
    "AddEdge", "AddEmbedding", "AddNode", "AddSceneObject", "AddTriples", "AppendStep",
    "ApplyLedger", "ApplyMultisigMutation", "ApplyMutation", "BatchUpdate", "BindQueue", "BrokerAck",
    "BrokerConsume", "BrokerReject", "ClaimNext", "ClearGraph", "ClearLedger", "CompactNodesByType",
    "CompareAndSetNodeFields", "Consolidate", "CreateSummaryNode", "DecayMemories", "DecayNode", "DecaySweep",
    "DeclareExchange", "DeclareQueue", "DeleteExchange", "DeleteGraph", "DropNamedGraph", "EvictBelow",
    "EvictLRU", "FromMsgpack", "IcvConfigure", "InvalidateEdge", "KvCas", "KvDelete",
    "KvPut", "Maintain", "ParseRepository", "PruneByLifecycle", "Publish", "PublishEx",
    "Reconcile", "Reinforce", "RemoveEdge", "RemoveNode", "RemoveTriples", "Reparent",
    "RunDatalogReasoning", "SetPose", "StartTrajectory", "SupersedeEdge", "SweepExpired", "TouchNodes",
    "UnbindQueue",
];

/// Mirrors `src/server/access.rs::requires_write`'s RUNTIME-CONDITIONAL set: the Mine*/
/// GraphLearn* families (`return *writeback;`) and the Sql/CypherQuery/GraphQl variants
/// (`sql_is_write`/`cypher_is_write`/`graphql_is_mutation`, which parse the query text).
/// `policy()`'s `mutates: true` for these is a conservative UPPER BOUND, not an equality --
/// the real answer depends on data the static table cannot see.
const ACCESS_RS_MUTATES_CONDITIONAL: &[&str] = &[
    "CypherQuery", "GraphLearnFit", "GraphLearnPredict", "GraphQl", "MineAnomaly", "MineAssociate",
    "MineCausalImpact", "MineClassifyPredict", "MineCluster", "MineCommunity", "MineEntityResolve", "MineForecast",
    "MineOntologyGap", "MineProcess", "MineReduce", "MineRetrievalQuality", "MineRiskPropagation", "MineRootCause",
    "MineSequence", "MineSubgraph", "MineText", "Sql",
];

/// Mirrors `access.rs`'s ONE explicit-false special case: `MineClassifyFit` always returns
/// `false` from `requires_write` regardless of any field (it never writes back).
const ACCESS_RS_MUTATES_EXPLICIT_FALSE: &[&str] = &[
    "MineClassifyFit",
];

/// Mirrors `src/wal.rs::is_durable_mutation`'s GraphRedb-domain true set (the plain
/// node/edge/memory/scene/trajectory primitives + RDF triples + the writeback-true Mine*/
/// GraphLearn* variants that DO make the durable list).
const WAL_RS_DURABLE_GRAPHREDB: &[&str] = &[
    "AddEdge", "AddNode", "AddSceneObject", "AddTriples", "AppendStep", "BatchUpdate",
    "ClaimNext", "ClearGraph", "CompareAndSetNodeFields", "Consolidate", "CreateSummaryNode", "DecayMemories",
    "DecayNode", "DropNamedGraph", "EvictBelow", "GraphLearnFit", "GraphLearnPredict", "InvalidateEdge",
    "Maintain", "MineAnomaly", "MineAssociate", "MineCausalImpact", "MineClassifyPredict", "MineCluster",
    "MineCommunity", "MineEntityResolve", "MineOntologyGap", "MineProcess", "MineReduce", "MineRetrievalQuality",
    "MineRiskPropagation", "MineRootCause", "Reinforce", "RemoveEdge", "RemoveNode", "RemoveTriples",
    "Reparent", "SetPose", "StartTrajectory", "SupersedeEdge",
];

/// Mirrors `src/wal.rs::is_durable_mutation`'s message-broker/stream true set.
const WAL_RS_DURABLE_OUTBOX: &[&str] = &[
    "BindQueue", "BrokerAck", "BrokerAckTag", "BrokerConsume", "BrokerNackTag", "BrokerReject",
    "DeclareExchange", "DeclareQueue", "DeleteExchange", "Publish", "PublishConfirmed", "PublishEx",
    "PublishIdempotent", "StreamCommitOffset", "StreamDeclare", "StreamPublish", "StreamTrim", "SweepExpired",
    "UnbindQueue",
];

/// Mirrors `src/audit.rs::audit_line`'s explicit match (everything else falls to its
/// `_ => return None` catch-all, i.e. NOT chained into the tamper-evident hash log).
const AUDIT_RS_AUDITED: &[&str] = &[
    "AddEdge", "AddNode", "BatchUpdate", "ClearGraph", "CompareAndSetNodeFields", "RemoveEdge",
    "RemoveNode",
];

/// Mirrors `src/server/cdc.rs::emit_for_method`'s explicit match (everything else falls to
/// its `_ => {}` catch-all, i.e. emits NO Change-Data-Capture event).
const CDC_RS_EMITS_CDC: &[&str] = &[
    "AddEdge", "AddNode", "ClearGraph", "CompareAndSetNodeFields", "RemoveEdge", "RemoveNode",
];

// ── KNOWN_DIVERGENCE ────────────────────────────────────────────────────────────────
//
// Where the policy table and the mirrored classifiers above disagree TODAY, on purpose:
// this is the audit value of the whole exercise -- surfacing the seams instead of
// papering over them. Each entry is `(variant, workstream, reason)`.

/// Category 1: `policy(m).mutates == true` is a conservative UPPER BOUND because the REAL
/// `access::requires_write(m)` answer depends on a runtime field (`writeback: bool`) or a
/// parsed query string, which a static per-variant table cannot model. Closing this for
/// real means making the handlers consult a PER-INVOCATION policy (EG-P0-2/EG-P0-6), not a
/// per-variant one.
const RUNTIME_CONDITIONAL: &[(&str, &str, &str)] = &[
    ("CypherQuery", "EG-P0-2", "mutates is conservative; real answer is the `writeback` field / parsed query at runtime"),
    ("GraphLearnFit", "EG-P0-2", "mutates is conservative; real answer is the `writeback` field / parsed query at runtime"),
    ("GraphLearnPredict", "EG-P0-2", "mutates is conservative; real answer is the `writeback` field / parsed query at runtime"),
    ("GraphQl", "EG-P0-2", "mutates is conservative; real answer is the `writeback` field / parsed query at runtime"),
    ("MineAnomaly", "EG-P0-2", "mutates is conservative; real answer is the `writeback` field / parsed query at runtime"),
    ("MineAssociate", "EG-P0-2", "mutates is conservative; real answer is the `writeback` field / parsed query at runtime"),
    ("MineCausalImpact", "EG-P0-2", "mutates is conservative; real answer is the `writeback` field / parsed query at runtime"),
    ("MineClassifyPredict", "EG-P0-2", "mutates is conservative; real answer is the `writeback` field / parsed query at runtime"),
    ("MineCluster", "EG-P0-2", "mutates is conservative; real answer is the `writeback` field / parsed query at runtime"),
    ("MineCommunity", "EG-P0-2", "mutates is conservative; real answer is the `writeback` field / parsed query at runtime"),
    ("MineEntityResolve", "EG-P0-2", "mutates is conservative; real answer is the `writeback` field / parsed query at runtime"),
    ("MineForecast", "EG-P0-2", "mutates is conservative; real answer is the `writeback` field / parsed query at runtime"),
    ("MineOntologyGap", "EG-P0-2", "mutates is conservative; real answer is the `writeback` field / parsed query at runtime"),
    ("MineProcess", "EG-P0-2", "mutates is conservative; real answer is the `writeback` field / parsed query at runtime"),
    ("MineReduce", "EG-P0-2", "mutates is conservative; real answer is the `writeback` field / parsed query at runtime"),
    ("MineRetrievalQuality", "EG-P0-2", "mutates is conservative; real answer is the `writeback` field / parsed query at runtime"),
    ("MineRiskPropagation", "EG-P0-2", "mutates is conservative; real answer is the `writeback` field / parsed query at runtime"),
    ("MineRootCause", "EG-P0-2", "mutates is conservative; real answer is the `writeback` field / parsed query at runtime"),
    ("MineSequence", "EG-P0-2", "mutates is conservative; real answer is the `writeback` field / parsed query at runtime"),
    ("MineSubgraph", "EG-P0-2", "mutates is conservative; real answer is the `writeback` field / parsed query at runtime"),
    ("MineText", "EG-P0-2", "mutates is conservative; real answer is the `writeback` field / parsed query at runtime"),
    ("Sql", "EG-P0-2", "mutates is conservative; real answer is the `writeback` field / parsed query at runtime"),
];

/// Category 2: `policy(m).mutates == true` per `access.rs` (unconditionally, or as the
/// upper bound of a runtime-conditional write), yet the method is ABSENT from
/// `wal.rs::is_durable_mutation`'s durable set -- a crash between checkpoints loses the
/// effect. Some of these are DELIBERATE per `wal.rs`'s own doc comment (derived/
/// recomputable maintenance ops); others (`ApplyMutation`, `ApplyMultisigMutation`,
/// `Reconcile`, the Ledger ops) look like real gaps. This is exactly the class of finding
/// the task brief names EG-P0-3 for.
const WAL_DURABILITY_GAP: &[(&str, &str, &str)] = &[
    ("AddEmbedding", "EG-P0-3", "write per access.rs but absent from wal.rs::is_durable_mutation -- a crash loses this mutation"),
    ("ApplyLedger", "EG-P0-3", "write per access.rs but absent from wal.rs::is_durable_mutation -- a crash loses this mutation"),
    ("ApplyMultisigMutation", "EG-P0-3", "write per access.rs but absent from wal.rs::is_durable_mutation -- a crash loses this mutation"),
    ("ApplyMutation", "EG-P0-3", "write per access.rs but absent from wal.rs::is_durable_mutation -- a crash loses this mutation"),
    ("ClearLedger", "EG-P0-3", "write per access.rs but absent from wal.rs::is_durable_mutation -- a crash loses this mutation"),
    ("CompactNodesByType", "EG-P0-3", "write per access.rs but absent from wal.rs::is_durable_mutation -- a crash loses this mutation"),
    ("DecaySweep", "EG-P0-3", "write per access.rs but absent from wal.rs::is_durable_mutation -- a crash loses this mutation"),
    ("DeleteGraph", "EG-P0-3", "write per access.rs but absent from wal.rs::is_durable_mutation -- a crash loses this mutation"),
    ("EvictLRU", "EG-P0-3", "write per access.rs but absent from wal.rs::is_durable_mutation -- a crash loses this mutation"),
    ("FromMsgpack", "EG-P0-3", "write per access.rs but absent from wal.rs::is_durable_mutation -- a crash loses this mutation"),
    ("IcvConfigure", "EG-P0-3", "write per access.rs but absent from wal.rs::is_durable_mutation -- a crash loses this mutation"),
    ("ParseRepository", "EG-P0-3", "write per access.rs but absent from wal.rs::is_durable_mutation -- a crash loses this mutation"),
    ("PruneByLifecycle", "EG-P0-3", "write per access.rs but absent from wal.rs::is_durable_mutation -- a crash loses this mutation"),
    ("Reconcile", "EG-P0-3", "write per access.rs but absent from wal.rs::is_durable_mutation -- a crash loses this mutation"),
    ("RunDatalogReasoning", "EG-P0-3", "write per access.rs but absent from wal.rs::is_durable_mutation -- a crash loses this mutation"),
    ("TouchNodes", "EG-P0-3", "write per access.rs but absent from wal.rs::is_durable_mutation -- a crash loses this mutation"),
    ("CypherQuery", "EG-P0-3", "write-conditional per access.rs (when the condition holds) but absent from wal.rs::is_durable_mutation"),
    ("GraphQl", "EG-P0-3", "write-conditional per access.rs (when the condition holds) but absent from wal.rs::is_durable_mutation"),
    ("MineForecast", "EG-P0-3", "write-conditional per access.rs (when the condition holds) but absent from wal.rs::is_durable_mutation"),
    ("MineSequence", "EG-P0-3", "write-conditional per access.rs (when the condition holds) but absent from wal.rs::is_durable_mutation"),
    ("MineSubgraph", "EG-P0-3", "write-conditional per access.rs (when the condition holds) but absent from wal.rs::is_durable_mutation"),
    ("MineText", "EG-P0-3", "write-conditional per access.rs (when the condition holds) but absent from wal.rs::is_durable_mutation"),
    ("Sql", "EG-P0-3", "write-conditional per access.rs (when the condition holds) but absent from wal.rs::is_durable_mutation"),
];

/// Category 3: `policy(m).mutates == true` on plain semantic/documentation grounds (the
/// method plainly changes server state), yet the variant is not mentioned ANYWHERE in
/// `access.rs::requires_write` at all -- not even in its unconditional-false fallthrough
/// path in a way that was deliberate. Some of these are legitimately governed by a
/// DIFFERENT mechanism (the OCC `Txn*` family self-routes before `dispatch_graph_op` and is
/// gated once at `BeginTxn`; KV/Blob/series ops are namespace-scoped and self-route too, and
/// in KV's case `access.rs` itself says as much in a comment) -- for those this is an
/// observation, not necessarily a bug. Others (channel/trigger/continuous-query/catalog/
/// identity/rbac/matview/foreign-source/udf-registration ops) have NO comment anywhere
/// explaining why they're absent from the classifier, which is a genuine open question this
/// workstream surfaces but does not resolve (no assigned workstream number exists yet for
/// this bucket -- recommend triaging it as a new EG-P0-x).
const ACCESS_RS_COVERAGE_GAP: &[(&str, &str, &str)] = &[
    ("BlobBegin", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("BlobChunkPut", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("BlobCommit", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("BlobGc", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("BlobRef", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("BlobUnref", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("BrokerAckTag", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("BrokerNackTag", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("CatalogAssign", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("CatalogReassign", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("CatalogRemove", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("CepSubscribe", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("CepUnsubscribe", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("Checkpoint", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("CloseChannel", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("Commit", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("CreateChannel", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("CreateGraph", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("CreateMatView", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("DistributedCompute", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("DropContinuousQuery", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("DropTrigger", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("ImportSqliteFile", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("JoinChannel", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("LeaveChannel", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("MultiGraphBatchUpdate", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("PlanMatViewDefine", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("PlanMatViewDrop", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("PlanMatViewRefresh", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("PublishConfirmed", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("PublishIdempotent", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("RbacAdmin", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("RebalanceExecute", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("RefreshMatView", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("RegisterContinuousQuery", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("RegisterForeignSource", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("RegisterIdentity", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("RegisterTrigger", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("RegisterUdf", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("Reshard", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("Restore", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("RunRules", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("SendMessage", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("Shutdown", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("StreamCommitOffset", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("StreamDeclare", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("StreamPublish", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("StreamTrim", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("TsAppend", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("TxnAddEdge", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("TxnAddEmbedding", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("TxnAddMeasurement", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("TxnAddNode", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("TxnAxiom", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("TxnBlobRef", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("TxnCas", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("TxnConstruct", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("TxnMaterializeBelief", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("TxnPlanWriteback", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("TxnRemoveEdge", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("TxnRemoveNode", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
];

fn all_known_divergence_names() -> std::collections::HashSet<&'static str> {
    RUNTIME_CONDITIONAL
        .iter()
        .chain(WAL_DURABILITY_GAP.iter())
        .chain(ACCESS_RS_COVERAGE_GAP.iter())
        .map(|(n, _, _)| *n)
        .collect()
}

// ── The actual cross-checks ─────────────────────────────────────────────────────────

#[test]
fn mutates_matches_access_rs_for_every_governed_variant() {
    let conditional: std::collections::HashSet<&str> = ACCESS_RS_MUTATES_CONDITIONAL.iter().copied().collect();
    let mut failures = Vec::new();
    for (name, p, _note) in eg_capabilities::ALL_METHODS {
        if conditional.contains(name) {
            // Upper-bound check only: policy must say true (never silently under-approximate
            // a method access.rs can classify as a write).
            if !p.mutates {
                failures.push(format!(
                    "{name}: access.rs classifies this as write-conditional, but policy().mutates is false (must be >= true as a conservative upper bound)"
                ));
            }
            continue;
        }
        if ACCESS_RS_MUTATES_EXPLICIT_FALSE.contains(name) {
            if p.mutates {
                failures.push(format!("{name}: access.rs::requires_write always returns false for this variant, but policy().mutates is true"));
            }
            continue;
        }
        let expected = ACCESS_RS_MUTATES_UNCONDITIONAL.contains(name);
        if p.mutates != expected {
            failures.push(format!(
                "{name}: policy().mutates = {}, access::requires_write(m) = {expected} -- {}",
                p.mutates,
                if all_known_divergence_names().contains(name) {
                    "documented in a KNOWN_DIVERGENCE table"
                } else {
                    "UNDOCUMENTED divergence -- add it to a KNOWN_DIVERGENCE table or fix policy()"
                },
            ));
        }
    }
    // Every failure that is NOT in a KNOWN_DIVERGENCE table is a real bug; every failure
    // that IS documented is expected and this assertion just double-checks the mirror
    // itself hasn't drifted from the ACCESS_RS_MUTATES_* constants above.
    let undocumented: Vec<_> = failures
        .iter()
        .filter(|f| f.contains("UNDOCUMENTED"))
        .collect();
    assert!(
        undocumented.is_empty(),
        "undocumented mutates divergences:\n{}",
        undocumented
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn durability_domain_matches_wal_rs_for_the_graph_wal() {
    // wal.rs::is_durable_mutation only knows about the per-graph WAL. `KvRedb`/`BlobRedb`/
    // `SeriesRedb` are real, but they are DIFFERENT durability domains living in their own
    // redb files (kv.redb / blob.redb / series.redb) that wal.rs never touches, so those
    // domains are excluded from this specific cross-check (they are not a wal.rs divergence
    // at all -- see the module doc on `DurabilityDomain`).
    let mut failures = Vec::new();
    for (name, p, _note) in eg_capabilities::ALL_METHODS {
        match p.durability_domain {
            DurabilityDomain::KvRedb | DurabilityDomain::BlobRedb | DurabilityDomain::SeriesRedb => continue,
            DurabilityDomain::GraphRedb => {
                if !WAL_RS_DURABLE_GRAPHREDB.contains(name) {
                    failures.push(format!("{name}: policy says GraphRedb-durable, wal.rs::is_durable_mutation disagrees"));
                }
            }
            DurabilityDomain::Outbox => {
                if !WAL_RS_DURABLE_OUTBOX.contains(name) {
                    failures.push(format!("{name}: policy says Outbox-durable, wal.rs::is_durable_mutation disagrees"));
                }
            }
            DurabilityDomain::None => {
                if WAL_RS_DURABLE_GRAPHREDB.contains(name) || WAL_RS_DURABLE_OUTBOX.contains(name) {
                    failures.push(format!("{name}: policy says not durable, but wal.rs::is_durable_mutation says it IS"));
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn audited_matches_audit_rs_exactly() {
    let mut failures = Vec::new();
    for (name, p, _note) in eg_capabilities::ALL_METHODS {
        let expected = AUDIT_RS_AUDITED.contains(name);
        if p.audited != expected {
            failures.push(format!("{name}: policy().audited = {}, audit::audit_line(m).is_some() = {expected}", p.audited));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn emits_cdc_matches_cdc_rs_exactly() {
    let mut failures = Vec::new();
    for (name, p, _note) in eg_capabilities::ALL_METHODS {
        let expected = CDC_RS_EMITS_CDC.contains(name);
        if p.emits_cdc != expected {
            failures.push(format!("{name}: policy().emits_cdc = {}, cdc::emit_for_method match = {expected}", p.emits_cdc));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Not a pass/fail gate -- prints the full audit findings so `cargo test -p eg-capabilities
/// -- --nocapture` surfaces them for a human. This is the "valuable audit output" the task
/// brief asks for.
#[test]
fn print_known_divergence_report() {
    eprintln!("\n=== EG-P0-1 capability ledger: KNOWN_DIVERGENCE report ===\n");
    eprintln!("-- Category 1: RUNTIME_CONDITIONAL ({} variants; workstream EG-P0-2) --", RUNTIME_CONDITIONAL.len());
    for (name, ws, reason) in RUNTIME_CONDITIONAL {
        eprintln!("  {name:<24} [{ws}] {reason}");
    }
    eprintln!("\n-- Category 2: WAL_DURABILITY_GAP ({} variants; workstream EG-P0-3) --", WAL_DURABILITY_GAP.len());
    for (name, ws, reason) in WAL_DURABILITY_GAP {
        eprintln!("  {name:<24} [{ws}] {reason}");
    }
    eprintln!("\n-- Category 3: ACCESS_RS_COVERAGE_GAP ({} variants; workstream UNASSIGNED) --", ACCESS_RS_COVERAGE_GAP.len());
    for (name, ws, reason) in ACCESS_RS_COVERAGE_GAP {
        eprintln!("  {name:<24} [{ws}] {reason}");
    }
    eprintln!(
        "\ntotals: {} runtime-conditional + {} wal-durability-gap + {} access.rs-coverage-gap = {} documented divergences\n",
        RUNTIME_CONDITIONAL.len(),
        WAL_DURABILITY_GAP.len(),
        ACCESS_RS_COVERAGE_GAP.len(),
        RUNTIME_CONDITIONAL.len() + WAL_DURABILITY_GAP.len() + ACCESS_RS_COVERAGE_GAP.len(),
    );
}

#[test]
fn generated_ledger_is_not_stale() {
    let repo_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("crates/eg-capabilities is two levels below the repo root")
        .to_path_buf();
    let checked_in_path = repo_root.join("docs").join("capabilities.generated.md");
    let checked_in = std::fs::read_to_string(&checked_in_path).unwrap_or_else(|e| {
        panic!(
            "failed to read {}: {e} -- run `cargo run -p eg-capabilities --bin gen_ledger` first",
            checked_in_path.display()
        )
    });
    let fresh = eg_capabilities::gen_ledger();
    assert_eq!(
        checked_in, fresh,
        "docs/capabilities.generated.md is STALE -- regenerate with `cargo run -p eg-capabilities --bin gen_ledger` and commit the result"
    );
}

/// Sanity check that `Method` itself really does have the variant count this whole
/// analysis was built against, so a future protocol.rs edit that adds/removes variants
/// is caught here too (in addition to the exhaustive-match compile error in `lib.rs`).
#[test]
fn all_methods_table_has_the_expected_variant_count() {
    assert_eq!(eg_capabilities::ALL_METHODS.len(), 337);
}

// Silence "unused" for the `Method` import: it documents which wire-protocol type this
// whole file is about even though every reference to it goes through `policy()`'s already-
// compiled exhaustive match rather than a fresh one here.
#[allow(dead_code)]
fn _type_anchor(_m: &Method) {}
