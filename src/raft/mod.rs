//! In-engine Raft replication (CONCEPT:AU-KG.ingest.source-sync-canonical) — the `cluster` tier.
//!
//! Runs the engine as a multi-node, highly-available cluster that replicates its
//! AUTHORITATIVE state through [`openraft`]. This whole module is behind the
//! `raft` cargo feature (cluster-tier only): a default / `pi` / `full` build links
//! NO openraft, so the Raspberry-Pi contract (no DataFusion AND no openraft) holds.
//!
//! ## What is replicated, and how it stays durable
//!
//! The replicated app data is a typed, AEAD-sealed durable command. Ordinary graph
//! mutations are deterministically staged from the authoritative pre-image and
//! committed through the universal state-backed `MutationBatch` kernel before RAM
//! publication. An `ApplyChangeEnvelope` remains one Raft entry and is committed
//! through its native redb transaction instead, preserving graph rows, content
//! version, typed cursor, governance, evidence, lineage, and outbox at one commit
//! point on every replica. Graph methods, ChangeEnvelopes, native methods, and
//! transaction participant plans enter the log only as authenticated ciphertext.
//! Served document/media mutations use the bounded, HMAC-authenticated
//! `SanitizedModalityRaftCommand`; source-bearing public methods never enter
//! consensus or durable receipts.
//! So a Raft node IS an M2 authoritative node — its graph data and auxiliary
//! authority live in the canonical authoritative shard, committed-before-applied.
//!
//! ## Durable redb Raft log (CONCEPT:EG-KG.storage.one-fsync-covers-raft)
//!
//! The Raft LOG, the vote, the applied state, and the graph data are ALL durable in
//! the SAME authoritative shard Database (the M2 store), keyed by `(group_id, index)` /
//! `(group_id, key)`. The log shares M2's off-reactor group-commit writer, so a log
//! append and its graph mutation ride ONE `WriteTransaction` / one fsync. A restarted
//! node recovers its log tail LOCALLY from redb — it no longer needs the leader to
//! refill an un-snapshotted tail. (The old separate `raft.redb` sidecar is gone.)
//!
//! ## Multi-Raft scaffold (CONCEPT:EG-KG.sharding.raft-resharding)
//!
//! [`multi::MultiRaft`] holds N openraft groups keyed by [`GroupId`], each its own
//! state machine + `GraphCore`, sharing ONE TCP listener per node (RPC frames are
//! tagged and demuxed by group id) and ONE shared authoritative shard (composite-key
//! log/meta — NOT a file per group). [`multi::GroupRouter`] maps `graph_name →
//! GroupId`.
//!
//! Multi-group transaction commits use a typed prepare/decision/commit/finalize
//! protocol. Participant commands are sent to the engine-owned placement group,
//! forwarded to that group's leader over the authenticated Raft peer channel, and
//! never issued recursively from state-machine apply.
//!
//! ## Write-routing barrier
//!
//! When Raft is active (built `--features raft` AND configured), a durable write is
//! routed through [`RaftHandle::client_write`] on the leader BEFORE it is
//! applied+acked — consensus is the replication barrier. Followers redirect the
//! client to the leader. When Raft is NOT active the dispatch path is byte-for-byte
//! unchanged (the `Option<RaftHandle>` is `None` and the normal apply path runs).

#![cfg(feature = "raft")]

use std::collections::BTreeMap;
use std::sync::Arc;

use openraft::BasicNode;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::server::ServerState;

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    <Option<T> as serde::Deserialize>::deserialize(deserializer)
}

pub mod config;
/// Pure, policy-only M3 cross-node elasticity proposals with resumable move identities.
pub mod cross_node_elasticity;
pub mod cross_shard_txn;
/// Bounded shard/Raft drain state machine (NE-167): admission is stopped and
/// observed before a voter is removed, every acknowledgement is revision/fence
/// bound, and restart recovery fails closed.
pub mod drain;
/// X4 — the distributed cross-shard DAG EXCHANGE operator (CONCEPT:EG-KG.query.dag-distributed-exchange),
/// the cluster-tier extension of E5's `eg_plan::dag::PlanDag`: ships a branch subtree to
/// its owning Raft group over a length-prefixed-MessagePack transport, runs it there via
/// the unmodified `eg_plan::execute_dag`, and merges the partials back through
/// `eg_plan::execute_dag_with`'s unmodified multi-branch join. Additionally gated on
/// `query` (needs `eg_plan`'s `PlanCtx`/`dag_exec`, which a plain `raft`-without-`query`
/// build does not link) — `cluster` implies both.
#[cfg(feature = "query")]
pub mod exchange;
/// Durable drain/safety contract for Raft membership shrink.
pub mod membership_shrink;
pub mod multi;
pub mod network;
pub mod node;
/// The placement catalog (CONCEPT:EG-KG.sharding.placement-catalog, DIST-P2-1) — the ONE durable,
/// Raft-replicated virtual-partition → group authority [`multi::MultiRaft::route_graph`]
/// consults before returning the engine's explicit unplaced policy.
pub mod placement;
/// Distributed graph compute — the Pregel/GAS cross-shard superstep engine
/// (CONCEPT:EG-KG.storage.feature). Behind `compute-dist` (which implies `raft`): runs PageRank /
/// connected-components / BFS across graphs spanning multiple Raft groups, plus the
/// incremental/streaming variant and materialized views.
#[cfg(feature = "compute-dist")]
pub mod pregel;
pub mod reshard;
pub mod store;
/// Cross-shard READ fan-out + merge (CONCEPT:EG-KG.sharding.placement-catalog, DIST-P2-2):
/// bounded, epoch-fenced pages routed to each group's leader over the authenticated
/// shared peer channel.  It reports independent per-group ReadIndex/version barriers
/// and never misrepresents them as a global snapshot.
pub mod xread;

/// Shared setup helpers for in-process Raft harnesses.
#[cfg(any(test, feature = "harness", feature = "compute-dist"))]
pub(crate) mod harness_support;

/// The in-process Raft cluster fixture. It is shared by the `harness` gauntlets
/// (through `harness::cluster::fixture`) and the `compute-dist` modality harness,
/// so it is declared once, here, under the union of their gates. Loading the file
/// from both places compiled it twice (`clippy::duplicate_mod`).
#[cfg(any(test, feature = "harness", feature = "compute-dist"))]
#[path = "harness/fixture.rs"]
pub(crate) mod fixture;

/// Correctness + load harness (CONCEPT:AU-KG.ontology.emits-database-ontology-entities) — the standing proof-engine that
/// gates every distributed/durability claim. Compiled under tests OR the explicit
/// `harness` feature; never in a production tier build.
#[cfg(any(test, feature = "harness"))]
pub mod harness;

#[cfg(test)]
mod tests;

// The cross-shard 2PC atomicity + recovery gauntlet (CONCEPT:EG-KG.storage.lane-n-increment) — the nemesis
// harness proving NO PARTIAL COMMIT under participant-kill + partition. Gated behind
// the `harness` feature so the default `raft` test set is unchanged; run it with
// `cargo test --features "raft harness"`.
#[cfg(all(test, feature = "harness"))]
mod xshard_harness;

// The online-resharding + tenant-hibernation gauntlet (CONCEPT:EG-KG.storage.100m-tenant) — proves a
// graph reshareded A→B keeps all data + serves correctly, and a hibernated graph
// rehydrates intact. Gated behind `harness` so a normal `raft` build links nothing.
#[cfg(all(test, feature = "harness"))]
mod reshard_harness;

// The placement-catalog gauntlet (CONCEPT:EG-KG.sharding.placement-catalog, DIST-P2-1) — proves
// assign→route, a stale-epoch redirect, persistence-across-restart, an online move
// (snapshot→catch-up→fenced cutover) preserving data, and a tenant split spanning two
// groups. Gated behind `harness` so a normal `raft` build links nothing.
#[cfg(all(test, feature = "harness"))]
mod placement_harness;

// The cross-shard READ gauntlet (CONCEPT:EG-KG.sharding.placement-catalog, DIST-P2-2) — proves a
// read spanning two groups gathers + union-merges both, each leg routes via the
// PlacementCatalog, a single-group read is not flagged cross-shard, and an unreachable
// leg errors loudly. Gated behind `harness` so a normal `raft` build links nothing.
#[cfg(all(test, feature = "harness"))]
mod xread_harness;

/// In-process cross-shard 2PC **modality-spanning** coordinator-kill harness
/// (CONCEPT:EG-KG.txn.crossshard-2pc-modality-harness) — the `--features cluster`
/// proof that closes EG-396: a cross-shard txn spanning the property-graph + RDF
/// modalities across two Raft groups stays all-or-nothing when the coordinator is
/// killed mid-2PC (recovery resolves to a SINGLE decision). Gated on `compute-dist`
/// (which `cluster` implies) so the pub proof entry is reachable from an external
/// `--features cluster` integration test, and its `#[cfg(test)]` scenarios ride the
/// `cargo test --features cluster` gate.
#[cfg(feature = "compute-dist")]
pub mod xshard_modality_harness;

/// Raft node id — a small integer assigned per cluster member.
pub type NodeId = u64;

/// A Raft GROUP id (CONCEPT:EG-KG.sharding.raft-resharding). One consensus group per keyspace; today one
/// graph maps to one group via the [`multi::GroupRouter`]. It is the composite-key
/// prefix the durable redb log + meta rows are keyed by, so ONE authoritative shard serves
/// every group's log (the spike's FD-ceiling fix — no file per group).
///
/// Defined by `eg_storage::direct_state`, not here: the direct-state whole-generation
/// module inside the storage kernel is the other consumer of both, and the kernel may
/// not depend on this binary. Re-exported so every `crate::raft::GroupId` /
/// `crate::raft::DEFAULT_GROUP` path in this tree keeps naming the SAME type and
/// constant rather than a second, silently-divergent pair.
pub use eg_storage::direct_state::{GroupId, DEFAULT_GROUP};
const RAFT_RESPONSE_SCHEMA_VERSION: u16 = 2;
#[cfg(feature = "modality-serving")]
mod modality;
#[cfg(feature = "modality-serving")]
pub use modality::SanitizedModalityRaftCommand;
#[cfg(feature = "modality-serving")]
pub(crate) use modality::{decode_sanitized_modality_result, SanitizedModalityMutation};

mod command;
pub use command::{
    NativeMutationCommand, ReplicatedMutation, SealedNativeMethod, TransactionParticipantPhase,
    NATIVE_CONSENSUS_METHODS,
};

/// The application request replicated through Raft: one typed command targeted at
/// a named graph and bound to verified mutation authority.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftRequest {
    /// The SANITIZED graph file-name (the same key the persistence tier uses).
    pub graph_fname: String,
    /// Human-readable graph name (used to create the graph in the registry if a
    /// follower has never seen it). For `__commons__` both are the same.
    pub graph_name: String,
    /// The graph's type, used only when the follower must create the graph.
    pub graph_type: crate::protocol::GraphType,
    /// The typed deterministic state-machine command.
    pub command: ReplicatedMutation,
    /// Leader-selected commit time for operations whose atomic durable record
    /// includes it; ordinary mutations carry their verified boundary timestamp.
    pub committed_at_ms: u64,
    /// Universal mutation authority supplied by the verified request boundary or
    /// an explicit engine-internal constructor. It is mandatory in every log entry.
    pub mutation: RaftMutationContext,
}

/// Privacy-safe request authority replicated with an ordinary Raft graph write.
/// The principal is already a one-way fingerprint; raw caller identity is never
/// copied into the consensus log or durable mutation ledger.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftMutationContext {
    pub batch_id: String,
    pub request_id: u64,
    /// Authenticated transport nonce carried into the mutation envelope on the
    /// state-machine apply path. Internal control-plane entries leave this
    /// absent; replicated caller mutations must preserve it end to end.
    pub attempt_nonce: Option<eg_types::contract::Nonce>,
    /// Opaque tenant scope derived from the verified carrier. Raw tenant names are
    /// never copied into consensus or MutationBatch authority.
    pub tenant_scope: String,
    pub principal_fingerprint: String,
    /// True only when the verified leader boundary admitted the exact one-time
    /// identity bootstrap. Followers use this mandatory bit to apply the same
    /// atomic bootstrap transition rather than re-authorizing from opaque claims.
    pub identity_bootstrap: bool,
    pub placement_epoch: u64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub fencing_token: Option<u64>,
    pub created_at_ms: u64,
}

/// The timing and placement fence attached to an already verified mutation.
/// Grouping these adjacent fields keeps the request-boundary constructor small
/// without changing the serialized authority shape.
pub(crate) struct RaftMutationTiming {
    pub(crate) placement_epoch: u64,
    pub(crate) fencing_token: Option<u64>,
    pub(crate) created_at_ms: u64,
}

impl RaftMutationContext {
    /// Construct caller authority only from the already-verified, privacy-safe
    /// carrier facts at the dispatch boundary.
    pub(crate) fn from_verified_request(
        batch_id: String,
        request_id: u64,
        attempt_nonce: Option<eg_types::contract::Nonce>,
        tenant_scope: &str,
        principal_fingerprint: String,
        identity_bootstrap: bool,
        timing: RaftMutationTiming,
    ) -> Result<Self, String> {
        let context = Self {
            batch_id,
            request_id,
            attempt_nonce,
            tenant_scope: tenant_scope.to_string(),
            principal_fingerprint,
            identity_bootstrap,
            placement_epoch: timing.placement_epoch,
            fencing_token: timing.fencing_token,
            created_at_ms: timing.created_at_ms,
        };
        context.validate()?;
        if context.tenant_scope == Self::internal_tenant_scope() {
            return Err(
                "verified Raft authority cannot claim the internal tenant scope".to_string(),
            );
        }
        Ok(context)
    }

    /// Construct engine-owned control-plane authority. The tenant and principal
    /// are fixed opaque digests, while the child batch id deterministically binds
    /// the operation to its graph and coordinator without persisting either raw id.
    pub(crate) fn internal(
        namespace: &str,
        graph: &str,
        coordinator_id: &str,
        request_id: u64,
        created_at_ms: u64,
    ) -> Self {
        Self {
            batch_id: crate::server::mutation_batch::opaque_coordinator_key(
                namespace,
                graph,
                coordinator_id,
            ),
            request_id,
            attempt_nonce: None,
            tenant_scope: Self::internal_tenant_scope(),
            principal_fingerprint: Self::internal_principal_fingerprint(),
            identity_bootstrap: false,
            placement_epoch: 0,
            fencing_token: None,
            created_at_ms,
        }
    }

    fn internal_tenant_scope() -> String {
        crate::server::mutation_batch::opaque_coordinator_key(
            "raft-internal-tenant",
            "control-plane",
            "authority",
        )
    }

    fn internal_principal_fingerprint() -> String {
        crate::server::mutation_batch::opaque_coordinator_key(
            "principal:sha256",
            "epistemic-graph-raft-control-plane",
            "authority",
        )
    }

    pub(in crate::raft) fn is_internal(&self) -> bool {
        self.tenant_scope == Self::internal_tenant_scope()
            && self.principal_fingerprint == Self::internal_principal_fingerprint()
            && self.attempt_nonce.is_none()
            && !self.identity_bootstrap
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.batch_id.trim().is_empty() {
            return Err("Raft mutation batch authority must not be empty".to_string());
        }
        if !opaque_scope_is_valid(&self.tenant_scope) {
            return Err("Raft mutation tenant authority must be an opaque scope".to_string());
        }
        let principal = self
            .principal_fingerprint
            .strip_prefix("principal:sha256:")
            .filter(|digest| lowercase_sha256_is_valid(digest));
        if principal.is_none() {
            return Err("Raft mutation principal authority must be an opaque digest".to_string());
        }
        if self.placement_epoch > 0 && self.fencing_token.is_none() {
            return Err("placed Raft mutation authority requires a fencing token".to_string());
        }
        Ok(())
    }
}

impl RaftRequest {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.graph_fname.trim().is_empty() || self.graph_name.trim().is_empty() {
            return Err("Raft request graph authority must not be empty".to_string());
        }
        if self.graph_fname != crate::persist::sanitize(&self.graph_name) {
            return Err(
                "Raft request graph file name does not match the sanitized graph name".to_string(),
            );
        }
        self.mutation.validate()?;
        self.validate_identity_bootstrap()?;
        self.validate_command_shape()
    }

    fn validate_identity_bootstrap(&self) -> Result<(), String> {
        if self.mutation.identity_bootstrap
            && (self.graph_name != "__commons__"
                || self.graph_fname != crate::persist::sanitize("__commons__")
                || self.mutation.tenant_scope == RaftMutationContext::internal_tenant_scope()
                || !matches!(
                    &self.command,
                    ReplicatedMutation::Native {
                        command: NativeMutationCommand::Identity { .. }
                    }
                ))
        {
            return Err(
                "Raft identity bootstrap authority is bound to a verified __commons__ identity command"
                    .to_string(),
            );
        }
        Ok(())
    }

    fn validate_command_shape(&self) -> Result<(), String> {
        match &self.command {
            ReplicatedMutation::Graph { sealed_method } => sealed_method.validate_shape()?,
            ReplicatedMutation::Native {
                command: NativeMutationCommand::ChangeEnvelope { sealed_envelope },
            } => sealed_envelope.validate_shape()?,
            #[cfg(feature = "modality-serving")]
            ReplicatedMutation::Native {
                command: NativeMutationCommand::ServedModality { .. },
            } => {}
            ReplicatedMutation::Native { command } => command.validate_shape()?,
        }
        Ok(())
    }

    pub(crate) fn bind_graph_command(
        &mut self,
        server_secret: &str,
        group_id: GroupId,
    ) -> Result<(), String> {
        if self
            .mutation
            .fencing_token
            .is_some_and(|token| token != group_id)
        {
            return Err("Raft mutation fencing token does not match selected group".to_string());
        }
        let mut command = self.command.clone();
        command.bind_graph_command(server_secret, self, group_id)?;
        self.command = command;
        Ok(())
    }

    pub(crate) fn validate_graph_command(
        &self,
        server_secret: &str,
        group_id: GroupId,
    ) -> Result<(), String> {
        if self
            .mutation
            .fencing_token
            .is_some_and(|token| token != group_id)
        {
            return Err("Raft mutation fencing token does not match applying group".to_string());
        }
        self.command
            .validate_graph_command(server_secret, self, group_id)
    }
}

fn opaque_scope_is_valid(value: &str) -> bool {
    value.rsplit_once(':').is_some_and(|(namespace, digest)| {
        !namespace.is_empty() && lowercase_sha256_is_valid(digest)
    })
}

fn lowercase_sha256_is_valid(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

#[cfg(test)]
mod raft_authority_contract_tests {
    use super::*;
    use crate::protocol::Method;

    #[derive(Serialize)]
    struct RequestWithoutAuthority {
        graph_fname: String,
        graph_name: String,
        graph_type: crate::protocol::GraphType,
        command: ReplicatedMutation,
        committed_at_ms: u64,
    }

    #[derive(Serialize)]
    struct RequestWithoutCommitTime {
        graph_fname: String,
        graph_name: String,
        graph_type: crate::protocol::GraphType,
        command: ReplicatedMutation,
        mutation: RaftMutationContext,
    }

    #[derive(Serialize)]
    struct RequestWithPublicMethodField {
        graph_fname: String,
        graph_name: String,
        graph_type: crate::protocol::GraphType,
        method: Method,
        committed_at_ms: u64,
        mutation: RaftMutationContext,
    }

    #[derive(Serialize)]
    struct MutationWithoutIdentityBootstrap {
        batch_id: String,
        request_id: u64,
        tenant_scope: String,
        principal_fingerprint: String,
        placement_epoch: u64,
        fencing_token: Option<u64>,
        created_at_ms: u64,
    }

    fn method() -> Method {
        Method::RemoveNode {
            node_id: "node".to_string(),
        }
    }

    #[test]
    fn current_request_requires_explicit_commit_and_mutation_authority_fields() {
        let mutation =
            RaftMutationContext::internal("raft-contract-test", "graph", "operation", 1, 1);
        let current = RaftRequest {
            graph_fname: "graph".to_string(),
            graph_name: "graph".to_string(),
            graph_type: crate::protocol::GraphType::Global,
            command: ReplicatedMutation::graph(method(), "cluster-test-key").unwrap(),
            committed_at_ms: 1,
            mutation: mutation.clone(),
        };
        let encoded = rmp_serde::to_vec_named(&current).unwrap();
        assert!(rmp_serde::from_slice::<RaftRequest>(&encoded).is_ok());

        let missing_bootstrap_authority = MutationWithoutIdentityBootstrap {
            batch_id: mutation.batch_id.clone(),
            request_id: mutation.request_id,
            tenant_scope: mutation.tenant_scope.clone(),
            principal_fingerprint: mutation.principal_fingerprint.clone(),
            placement_epoch: mutation.placement_epoch,
            fencing_token: mutation.fencing_token,
            created_at_ms: mutation.created_at_ms,
        };
        let encoded = rmp_serde::to_vec_named(&missing_bootstrap_authority).unwrap();
        assert!(rmp_serde::from_slice::<RaftMutationContext>(&encoded).is_err());

        let missing_authority = RequestWithoutAuthority {
            graph_fname: "graph".to_string(),
            graph_name: "graph".to_string(),
            graph_type: crate::protocol::GraphType::Global,
            command: ReplicatedMutation::graph(method(), "cluster-test-key").unwrap(),
            committed_at_ms: 1,
        };
        let encoded = rmp_serde::to_vec_named(&missing_authority).unwrap();
        assert!(rmp_serde::from_slice::<RaftRequest>(&encoded).is_err());

        let missing_commit_time = RequestWithoutCommitTime {
            graph_fname: "graph".to_string(),
            graph_name: "graph".to_string(),
            graph_type: crate::protocol::GraphType::Global,
            command: ReplicatedMutation::graph(method(), "cluster-test-key").unwrap(),
            mutation: mutation.clone(),
        };
        let encoded = rmp_serde::to_vec_named(&missing_commit_time).unwrap();
        assert!(rmp_serde::from_slice::<RaftRequest>(&encoded).is_err());

        let obsolete_shape = RequestWithPublicMethodField {
            graph_fname: "graph".to_string(),
            graph_name: "graph".to_string(),
            graph_type: crate::protocol::GraphType::Global,
            method: method(),
            committed_at_ms: 1,
            mutation,
        };
        let encoded = rmp_serde::to_vec_named(&obsolete_shape).unwrap();
        assert!(rmp_serde::from_slice::<RaftRequest>(&encoded).is_err());
    }

    fn request_for_graph(graph_name: &str, graph_fname: String) -> RaftRequest {
        RaftRequest {
            graph_fname,
            graph_name: graph_name.to_string(),
            graph_type: crate::protocol::GraphType::Global,
            command: ReplicatedMutation::graph(method(), "cluster-test-key").unwrap(),
            committed_at_ms: 1,
            mutation: RaftMutationContext::internal(
                "raft-contract-test",
                graph_name,
                "graph-name-binding",
                1,
                1,
            ),
        }
    }

    #[test]
    fn request_rejects_logical_graph_names_in_the_physical_file_name_slot() {
        let hashed_graph = "x".repeat(2_048);
        for graph_name in ["a:b".to_string(), hashed_graph] {
            let graph_fname = crate::persist::sanitize(&graph_name);
            assert_ne!(graph_name, graph_fname);
            assert!(request_for_graph(&graph_name, graph_fname.clone())
                .validate()
                .is_ok());

            let logical_name = request_for_graph(&graph_name, graph_name.clone())
                .validate()
                .expect_err("logical graph name must not be accepted as a file name");
            assert!(
                logical_name.contains("sanitized graph name"),
                "{logical_name}"
            );

            let mismatched_name =
                request_for_graph(&graph_name, crate::persist::sanitize("different-graph"))
                    .validate()
                    .expect_err("a different sanitized graph must not be accepted");
            assert!(
                mismatched_name.contains("sanitized graph name"),
                "{mismatched_name}"
            );
        }
    }
}

/// openraft 0.10's `AppData` bound now requires `Display` (the log entry is
/// `Display`). A terse graph-only form is sufficient for the trace/log lines
/// openraft emits and avoids rendering command payloads.
impl std::fmt::Display for RaftRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RaftRequest(graph={})", self.graph_name)
    }
}

/// The application response from applying a [`RaftRequest`]. The dispatch path only
/// needs success/failure (the in-memory apply already produced the client-facing
/// Response), so this is a thin ack.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftResponse {
    pub schema_version: u16,
    /// `true` when the committed typed command applied cleanly on this node.
    pub applied: bool,
    /// Present only for an engine-native ChangeEnvelope entry.
    #[serde(deserialize_with = "deserialize_required_option")]
    pub change_envelope_commit: Option<crate::change_envelope::ChangeEnvelopeCommit>,
    /// Exact result produced by a bounded engine-native state-machine command or
    /// an ordinary atomic graph method whose apply result is part of its
    /// consensus contract (for example, a CAS or create-if-absent).
    #[serde(deserialize_with = "deserialize_required_option")]
    pub native_result: Option<crate::protocol::ResultPayload>,
    /// Exact durable MutationBatch receipt for a command whose caller needs
    /// the committed identity and replay marker in addition to its terminal
    /// result.  The field is optional so older Raft responses remain readable;
    /// a receipt is never reconstructed from `applied` or `native_result`.
    #[serde(
        default,
        deserialize_with = "deserialize_required_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub native_commit: Option<crate::mutation_batch::MutationBatchCommit>,
    /// Deterministic domain rejection produced while applying a committed command.
    /// Transport/internal errors still fail the state machine rather than entering
    /// this field.
    #[serde(deserialize_with = "deserialize_required_option")]
    pub native_error: Option<String>,
    /// The durable commit succeeded but the leader's in-memory projection needs
    /// asynchronous repair from the transactional outbox.
    pub projection_pending: bool,
}

impl Default for RaftResponse {
    fn default() -> Self {
        Self {
            schema_version: RAFT_RESPONSE_SCHEMA_VERSION,
            applied: false,
            change_envelope_commit: None,
            native_result: None,
            native_commit: None,
            native_error: None,
            projection_pending: false,
        }
    }
}

impl RaftResponse {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.schema_version != RAFT_RESPONSE_SCHEMA_VERSION {
            return Err(format!(
                "unsupported Raft response schema {} (expected {})",
                self.schema_version, RAFT_RESPONSE_SCHEMA_VERSION
            ));
        }
        if self.native_result.is_some() && self.native_error.is_some() {
            return Err("Raft native response cannot contain both result and error".to_string());
        }
        if self.native_commit.is_some() && self.native_error.is_some() {
            return Err("Raft native response cannot contain both commit and error".to_string());
        }
        if self.change_envelope_commit.is_some()
            && (self.native_result.is_some() || self.native_commit.is_some())
        {
            return Err(
                "Raft response cannot contain both change-envelope and native receipts".to_string(),
            );
        }
        if let Some(commit) = &self.native_commit {
            self.validate_native_commit(commit)?;
        }
        Ok(())
    }

    fn validate_native_commit(
        &self,
        commit: &crate::mutation_batch::MutationBatchCommit,
    ) -> Result<(), String> {
        if !self.applied {
            return Err("Raft native commit receipt requires applied=true".to_string());
        }
        commit
            .validate()
            .map_err(|error| format!("Raft native commit receipt is invalid: {error}"))?;
        let durable_result = commit
            .record
            .result_msgpack
            .as_deref()
            .filter(|bytes| !bytes.is_empty())
            .ok_or_else(|| {
                "Raft native commit receipt is missing its terminal result".to_string()
            })?;
        rmp_serde::from_slice::<crate::protocol::ResultPayload>(durable_result)
            .map_err(|_| "Raft native commit receipt has an invalid terminal result".to_string())?;
        if let Some(result) = &self.native_result {
            let encoded = rmp_serde::to_vec_named(result)
                .map_err(|error| format!("Raft native result cannot be encoded: {error}"))?;
            if durable_result != encoded.as_slice() {
                return Err(
                    "Raft native result does not match its durable commit receipt".to_string(),
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod raft_response_receipt_tests {
    use super::*;

    fn cas_receipt(replayed: bool) -> crate::mutation_batch::MutationBatchCommit {
        let conditions = rmp_serde::to_vec_named(&serde_json::json!({"epoch": 0})).unwrap();
        let updates = rmp_serde::to_vec_named(&serde_json::json!({"epoch": 1})).unwrap();
        let batch = crate::server::mutation_batch::compile_methods(
            crate::server::mutation_batch::CompileBatch {
                batch_id: "raft-receipt-cas",
                request_id: 7,
                attempt_nonce: None,
                principal: Some("receipt-test-principal"),
                tenant: "receipt-tenant",
                graph: "receipt-graph",
                placement_epoch: 0,
                idempotency_key: "raft-receipt-cas",
                expected_graph_version: Some(0),
                fencing_token: None,
                created_at_ms: 11,
                default_surface: crate::mutation_batch::MutationSurface::Graph,
                authoritative_state: None,
            },
            vec![crate::protocol::Method::CompareAndSetNodeFields {
                node_id: "reservation".to_string(),
                conditions_msgpack: conditions,
                updates_msgpack: updates,
            }],
        )
        .unwrap();
        let identity = batch.identity.clone();
        let record = crate::mutation_batch::MutationBatchRecord {
            batch,
            identity: identity.clone(),
            status: crate::mutation_batch::MutationBatchStatus::Committed,
            committed_version: crate::mutation_batch::CommittedVersion::Graph {
                source: 0,
                target: 1,
            },
            result_msgpack: Some(
                rmp_serde::to_vec_named(&crate::protocol::ResultPayload::Bool(false)).unwrap(),
            ),
            committed_at_ms: 11,
        };
        let commit = crate::mutation_batch::MutationBatchCommit {
            record,
            identity,
            replayed,
        };
        commit.validate().unwrap();
        commit
    }

    #[test]
    fn native_commit_round_trips_fresh_replay_and_false_result() {
        for replayed in [false, true] {
            let response = RaftResponse {
                applied: true,
                native_result: Some(crate::protocol::ResultPayload::Bool(false)),
                native_commit: Some(cas_receipt(replayed)),
                ..Default::default()
            };
            response.validate().unwrap();
            let encoded = rmp_serde::to_vec_named(&response).unwrap();
            let decoded: RaftResponse = rmp_serde::from_slice(&encoded).unwrap();
            assert!(matches!(
                decoded.native_result,
                Some(crate::protocol::ResultPayload::Bool(false))
            ));
            assert_eq!(
                decoded.native_commit.as_ref().map(|commit| commit.replayed),
                Some(replayed)
            );
            assert!(decoded.native_commit.is_some());
        }
    }

    #[test]
    fn native_commit_validation_rejects_inconsistent_envelopes() {
        let commit = cas_receipt(false);
        let mut response = RaftResponse {
            applied: false,
            native_commit: Some(commit.clone()),
            ..Default::default()
        };
        let error = response
            .validate()
            .expect_err("a durable receipt cannot claim an unapplied response");
        assert!(error.contains("applied=true"), "{error}");

        response.applied = true;
        response.native_result = Some(crate::protocol::ResultPayload::Bool(true));
        let error = response
            .validate()
            .expect_err("the wire result cannot disagree with the durable false receipt");
        assert!(error.contains("does not match"), "{error}");

        response.native_result = Some(crate::protocol::ResultPayload::Bool(false));
        response.change_envelope_commit = Some(crate::change_envelope::ChangeEnvelopeCommit {
            envelope_id: "envelope".to_string(),
            batch_id: "batch".to_string(),
            content_version: crate::change_envelope::ContentVersion {
                object_id: "object".to_string(),
                digest_algorithm: "sha256".to_string(),
                digest: "a".repeat(64),
                previous_digest: None,
                source_version: crate::change_envelope::ContentVersionPosition::Sequence(1),
            },
            cursor: None,
            outbox_count: 0,
            replayed: false,
        });
        let error = response
            .validate()
            .expect_err("native and change-envelope receipts are separate response families");
        assert!(error.contains("both change-envelope and native"), "{error}");
    }

    #[test]
    fn native_commit_validation_rejects_missing_or_corrupt_terminal_result() {
        for result_msgpack in [None, Some(Vec::new()), Some(vec![0xc1])] {
            let mut commit = cas_receipt(false);
            commit.record.result_msgpack = result_msgpack;
            let response = RaftResponse {
                applied: true,
                native_commit: Some(commit),
                ..Default::default()
            };
            let error = response
                .validate()
                .expect_err("a native receipt must carry a decodable terminal result");
            assert!(error.contains("terminal result"), "{error}");
        }
    }

    #[test]
    fn legacy_response_defaults_native_commit_to_none() {
        #[derive(Serialize)]
        struct LegacyRaftResponse {
            schema_version: u16,
            applied: bool,
            change_envelope_commit: Option<crate::change_envelope::ChangeEnvelopeCommit>,
            native_result: Option<crate::protocol::ResultPayload>,
            native_error: Option<String>,
            projection_pending: bool,
        }

        let encoded = rmp_serde::to_vec_named(&LegacyRaftResponse {
            schema_version: RAFT_RESPONSE_SCHEMA_VERSION,
            applied: true,
            change_envelope_commit: None,
            native_result: Some(crate::protocol::ResultPayload::Bool(true)),
            native_error: None,
            projection_pending: false,
        })
        .unwrap();
        let decoded: RaftResponse = rmp_serde::from_slice(&encoded).unwrap();
        assert!(decoded.native_commit.is_none());
        assert!(matches!(
            decoded.native_result,
            Some(crate::protocol::ResultPayload::Bool(true))
        ));
    }
}

openraft::declare_raft_types!(
    /// The single Raft type configuration for the engine cluster.
    ///
    /// openraft 0.10 (CONCEPT:AU-KG.backend.authority-has-already-acked): the macro fills the absent associated types
    /// with their defaults — `NodeId = u64` (= our [`NodeId`] alias), `Node =
    /// BasicNode`, `Entry = openraft::Entry<…>`, `SnapshotData = Cursor<Vec<u8>>`,
    /// `AsyncRuntime = TokioRuntime` — so only `D`/`R` need to be named here.
    pub TypeConfig:
        D = RaftRequest,
        R = RaftResponse,
);

/// A running Raft instance (`openraft::Raft`) for our [`TypeConfig`].
///
/// openraft 0.10's `Raft<C, SM = ()>` carries the state-machine type as a second
/// generic; `Raft::new` returns it carrying the concrete SM. Our state machine is
/// `Arc<EgStore>`, so the alias names it (CONCEPT:AU-KG.backend.authority-has-already-acked).
pub type EgRaft = openraft::Raft<TypeConfig, Arc<store::EgStore>>;

/// Cloneable handle the dispatch path uses to route writes through consensus.
///
/// Held in `ServerState` as `Option<RaftHandle>`: `None` ⇒ single-node (the normal
/// path, unchanged); `Some` ⇒ the cluster path routes writes through Raft.
#[derive(Clone)]
pub struct RaftHandle {
    pub raft: EgRaft,
    pub node_id: NodeId,
}

impl RaftHandle {
    /// Route a durable mutation through Raft consensus. On the LEADER this awaits
    /// a quorum-committed + locally-applied write (the replication barrier). On a
    /// FOLLOWER, openraft returns a `ForwardToLeader` error carrying the current
    /// leader id, which the caller surfaces so the client retries against the
    /// leader. Returns `Ok` only after the entry is committed AND applied here.
    pub async fn client_write(&self, req: RaftRequest) -> Result<RaftResponse, String> {
        req.validate()?;
        match self.raft.client_write(req).await {
            Ok(resp) => {
                resp.data.validate()?;
                Ok(resp.data)
            }
            Err(e) => Err(format!("raft client_write: {e}")),
        }
    }

    /// The current cluster leader as this node sees it (for redirect hints).
    pub async fn current_leader(&self) -> Option<NodeId> {
        self.raft.current_leader().await
    }
}

/// Parsed peer set: node id → MessagePack-RPC address (`host:port`).
pub type PeerMap = BTreeMap<NodeId, BasicNode>;

/// Shared application context the state machine needs to APPLY a committed entry:
/// the live `ServerState` (registry + persistence). Cloned into the store.
#[derive(Clone)]
pub struct AppCtx {
    pub state: Arc<RwLock<ServerState>>,
    /// The group router (CONCEPT:AU-KG.ingest.mirror-inbound), present when the store runs under a
    /// [`multi::MultiRaft`]. A group's snapshot dump uses it to SCOPE the dump to the
    /// graphs in THIS group's tenant range (CONCEPT:AU-KG.ingest.staged). `None` ⇒ a direct /
    /// single-store open dumps the whole registry (the unscoped scaffold behavior).
    pub router: Option<Arc<multi::GroupRouter>>,
}
