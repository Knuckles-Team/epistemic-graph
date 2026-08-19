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

use crate::protocol::{CypherMode, Method};
use crate::server::ServerState;

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    <Option<T> as serde::Deserialize>::deserialize(deserializer)
}

pub mod config;
pub mod cross_shard_txn;
/// X4 — the distributed cross-shard DAG EXCHANGE operator (CONCEPT:EG-KG.query.dag-distributed-exchange),
/// the cluster-tier extension of E5's `eg_plan::dag::PlanDag`: ships a branch subtree to
/// its owning Raft group over a length-prefixed-MessagePack transport, runs it there via
/// the unmodified `eg_plan::execute_dag`, and merges the partials back through
/// `eg_plan::execute_dag_with`'s unmodified multi-branch join. Additionally gated on
/// `query` (needs `eg_plan`'s `PlanCtx`/`dag_exec`, which a plain `raft`-without-`query`
/// build does not link) — `cluster` implies both.
#[cfg(feature = "query")]
pub mod exchange;
pub mod multi;
/// Durable drain/safety contract for Raft membership shrink.
pub mod membership_shrink;
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
pub type GroupId = u64;

/// The single default group every graph routes to in this increment (one group =
/// today's single-group behavior, now with a durable redb log). Multi-group routing
/// machinery exists ([`multi`]) and is proven by tests, but the default router maps
/// all graphs here so behavior is unchanged from the single-group path.
pub const DEFAULT_GROUP: GroupId = 0;
const RAFT_RESPONSE_SCHEMA_VERSION: u16 = 2;
const MAX_REPLICATED_COMMAND_PAYLOAD_BYTES: usize =
    crate::server::mutation::MAX_NATIVE_COORDINATOR_PAYLOAD_BYTES;

#[cfg(feature = "modality-serving")]
const MAX_REPLICATED_MODALITY_STATE_BYTES: usize = 128 * 1024 * 1024;

#[cfg(feature = "modality-serving")]
const MAX_REPLICATED_MODALITY_RESULT_BYTES: usize = 4 * 1024;
#[cfg(feature = "modality-serving")]
const MAX_REPLICATED_MODALITY_RESULT_ITEMS: usize = 64;
#[cfg(feature = "modality-serving")]
const SANITIZED_MODALITY_CODEC_VERSION: u16 = 1;

#[cfg(feature = "modality-serving")]
fn deserialize_bounded_modality_outcomes<'de, D>(
    deserializer: D,
) -> Result<Vec<eg_modality::ApplyOutcome>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct BoundedOutcomes;

    impl<'de> serde::de::Visitor<'de> for BoundedOutcomes {
        type Value = Vec<eg_modality::ApplyOutcome>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a bounded sequence of modality outcomes")
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut outcomes = Vec::new();
            while let Some(outcome) = sequence.next_element()? {
                if outcomes.len() >= MAX_REPLICATED_MODALITY_RESULT_ITEMS {
                    return Err(serde::de::Error::custom(
                        "sanitized modality result cardinality is outside bounds",
                    ));
                }
                outcomes.push(outcome);
            }
            Ok(outcomes)
        }
    }

    deserializer.deserialize_seq(BoundedOutcomes)
}

/// Mutation category retained by the sanitized command. It is sufficient for CDC
/// classification but contains no occurrence, source, tenant, user, or endpoint.
#[cfg(feature = "modality-serving")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SanitizedModalityMutation {
    Ingest,
    IngestStream,
    Delete,
    MoveToCold,
    Restore,
    CollectTombstones,
}

#[cfg(feature = "modality-serving")]
impl SanitizedModalityMutation {
    pub(crate) fn from_served(
        op: &eg_types::ServedModalityOp,
    ) -> Option<(Self, eg_types::ServedModalityKind)> {
        use eg_types::ServedModalityOp;
        match op {
            ServedModalityOp::Ingest { modality, .. } => Some((Self::Ingest, *modality)),
            ServedModalityOp::IngestStream { modality, .. } => {
                Some((Self::IngestStream, *modality))
            }
            ServedModalityOp::Delete { modality, .. } => Some((Self::Delete, *modality)),
            ServedModalityOp::MoveToCold { modality, .. } => Some((Self::MoveToCold, *modality)),
            ServedModalityOp::Restore { modality, .. } => Some((Self::Restore, *modality)),
            ServedModalityOp::CollectTombstones { modality, .. } => {
                Some((Self::CollectTombstones, *modality))
            }
            _ => None,
        }
    }

    fn as_bytes(self) -> &'static [u8] {
        match self {
            Self::Ingest => b"ingest",
            Self::IngestStream => b"ingest_stream",
            Self::Delete => b"delete",
            Self::MoveToCold => b"move_to_cold",
            Self::Restore => b"restore",
            Self::CollectTombstones => b"collect_tombstones",
        }
    }
}

/// The only result schema allowed in the sanitized modality Raft command.
///
/// The public response remains the compact `ResultPayload::Raw` envelope for
/// client compatibility, but the replicated command carries this typed,
/// versioned interpretation alongside that safe response. It contains only
/// bounded outcome metadata — never source bytes, bundles, paths, or encrypted
/// runtime material. Keeping the schema here gives every replica one canonical
/// decode/validation path instead of accepting an arbitrary nested MessagePack
/// value from a leader.
#[cfg(feature = "modality-serving")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum SanitizedModalityResultKind {
    Single,
    Stream,
}

#[cfg(feature = "modality-serving")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) struct SanitizedModalityResult {
    pub(crate) schema_version: u16,
    pub(crate) modality: eg_types::ServedModalityKind,
    pub(crate) operation: SanitizedModalityMutation,
    pub(crate) kind: SanitizedModalityResultKind,
    #[serde(deserialize_with = "deserialize_bounded_modality_outcomes")]
    pub(crate) outcomes: Vec<eg_modality::ApplyOutcome>,
}

#[cfg(feature = "modality-serving")]
impl SanitizedModalityResult {
    fn validate(&self) -> Result<(), String> {
        if self.schema_version != SANITIZED_MODALITY_CODEC_VERSION {
            return Err("sanitized modality result schema version is unsupported".to_string());
        }
        if self.outcomes.is_empty()
            || self.outcomes.len() > MAX_REPLICATED_MODALITY_RESULT_ITEMS
        {
            return Err("sanitized modality result cardinality is outside bounds".to_string());
        }
        match (&self.kind, self.operation) {
            (SanitizedModalityResultKind::Single, operation)
                if !matches!(operation, SanitizedModalityMutation::IngestStream)
                    && self.outcomes.len() == 1 =>
            Ok(()),
            (SanitizedModalityResultKind::Stream, SanitizedModalityMutation::IngestStream)
                if self.outcomes.len() >= 2 => Ok(()),
            _ => Err("sanitized modality result type does not match operation".to_string()),
        }
    }

    fn from_wire(
        modality: eg_types::ServedModalityKind,
        operation: SanitizedModalityMutation,
        result_msgpack: &[u8],
    ) -> Result<Self, String> {
        if result_msgpack.is_empty() || result_msgpack.len() > MAX_REPLICATED_MODALITY_RESULT_BYTES
        {
            return Err("sanitized modality Raft result is invalid".to_string());
        }
        let payload: crate::protocol::ResultPayload = eg_types::msgpack::decode_bounded(
            result_msgpack,
            eg_types::msgpack::MsgpackLimits::new(
                MAX_REPLICATED_MODALITY_RESULT_BYTES,
                MAX_REPLICATED_MODALITY_RESULT_ITEMS,
                64,
            ),
        )
        .map_err(|_| "sanitized modality Raft result is malformed".to_string())?;
        // `ResultPayload` is intentionally untagged and its two byte variants
        // (`Raw` and `PropertiesMsgpack`) are wire-identical. Serde therefore
        // may select either name when decoding a committed bin. The canonical
        // modality contract is the bounded inner outcome schema below, not the
        // non-existent enum discriminant; all non-byte payloads remain invalid.
        let outcome_bytes = match payload {
            crate::protocol::ResultPayload::Raw(bytes)
            | crate::protocol::ResultPayload::PropertiesMsgpack(bytes) => bytes,
            _ => {
                return Err("sanitized modality Raft result has the wrong payload type".to_string())
            }
        };
        let (kind, outcomes) = if matches!(operation, SanitizedModalityMutation::IngestStream) {
            let outcomes: Vec<eg_modality::ApplyOutcome> = eg_types::msgpack::decode_bounded(
                &outcome_bytes,
                eg_types::msgpack::MsgpackLimits::new(
                    MAX_REPLICATED_MODALITY_RESULT_BYTES,
                    MAX_REPLICATED_MODALITY_RESULT_ITEMS,
                    64,
                ),
            )
            .map_err(|_| "sanitized modality Raft stream result is malformed".to_string())?;
            (SanitizedModalityResultKind::Stream, outcomes)
        } else {
            let outcome: eg_modality::ApplyOutcome = eg_types::msgpack::decode_bounded(
                &outcome_bytes,
                eg_types::msgpack::MsgpackLimits::new(
                    MAX_REPLICATED_MODALITY_RESULT_BYTES,
                    MAX_REPLICATED_MODALITY_RESULT_ITEMS,
                    64,
                ),
            )
            .map_err(|_| "sanitized modality Raft result is malformed".to_string())?;
            (SanitizedModalityResultKind::Single, vec![outcome])
        };
        let result = Self {
            schema_version: SANITIZED_MODALITY_CODEC_VERSION,
            modality,
            operation,
            kind,
            outcomes,
        };
        result.validate()?;
        if result.to_wire()?.as_slice() != result_msgpack {
            return Err("sanitized modality Raft result is not canonical".to_string());
        }
        Ok(result)
    }

    fn to_wire(&self) -> Result<Vec<u8>, String> {
        self.validate()?;
        let payload = self.response_payload();
        rmp_serde::to_vec_named(&payload).map_err(|_| {
            "sanitized modality Raft result could not be canonically encoded".to_string()
        })
    }

    fn response_payload(&self) -> crate::protocol::ResultPayload {
        match self.kind {
            SanitizedModalityResultKind::Single => {
                crate::protocol::ResultPayload::raw(&self.outcomes[0])
            }
            SanitizedModalityResultKind::Stream => {
                crate::protocol::ResultPayload::raw(&self.outcomes)
            }
        }
    }

    fn canonical_bytes(&self) -> Result<Vec<u8>, String> {
        self.validate()?;
        let bytes = rmp_serde::to_vec_named(self).map_err(|_| {
            "sanitized modality Raft result could not be canonically encoded".to_string()
        })?;
        if bytes.len() > MAX_REPLICATED_MODALITY_RESULT_BYTES {
            return Err("sanitized modality Raft result exceeds resource limits".to_string());
        }
        Ok(bytes)
    }
}

/// Decode the only result representation accepted for a sanitized modality
/// receipt and return the safe client payload. This is the shared authority for
/// both Raft state-machine apply and leader retry/replay; callers must not
/// reimplement the untagged `ResultPayload` or stream-cardinality checks.
#[cfg(feature = "modality-serving")]
pub(crate) fn decode_sanitized_modality_result(
    modality: eg_types::ServedModalityKind,
    operation: SanitizedModalityMutation,
    result_msgpack: &[u8],
) -> Result<crate::protocol::ResultPayload, String> {
    let result = SanitizedModalityResult::from_wire(modality, operation, result_msgpack)?;
    Ok(result.response_payload())
}

/// A Raft-log-safe modality command. Native decoding and policy checks happen on
/// the verified leader request; consensus receives only an AEAD-sealed runtime
/// value, its opaque partition node, a small non-identifying result, and integrity
/// metadata. The raw document/media body is not a field and cannot be serialized
/// into the consensus log by this type.
#[cfg(feature = "modality-serving")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SanitizedModalityRaftCommand {
    pub(crate) schema_version: u16,
    pub(crate) modality: eg_types::ServedModalityKind,
    pub(crate) operation: SanitizedModalityMutation,
    pub(crate) node_id: String,
    #[serde(with = "serde_bytes")]
    pub(crate) sealed_runtime_state: Vec<u8>,
    pub(crate) state_sha256: String,
    pub(crate) receipt_query: String,
    #[serde(with = "serde_bytes")]
    pub(crate) result_msgpack: Vec<u8>,
    pub(crate) result: SanitizedModalityResult,
    pub(crate) result_sha256: String,
    authentication_tag: String,
}

#[cfg(feature = "modality-serving")]
impl SanitizedModalityRaftCommand {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        server_secret: &str,
        modality: eg_types::ServedModalityKind,
        operation: SanitizedModalityMutation,
        node_id: String,
        sealed_runtime_state: Vec<u8>,
        receipt_query: String,
        result_msgpack: Vec<u8>,
    ) -> Result<Self, String> {
        use sha2::{Digest, Sha256};
        let state_sha256 = hex::encode(Sha256::digest(&sealed_runtime_state));
        let result = SanitizedModalityResult::from_wire(modality, operation, &result_msgpack)?;
        let result_sha256 = hex::encode(Sha256::digest(result.canonical_bytes()?));
        let authentication_tag = sanitized_modality_tag(
            server_secret,
            SANITIZED_MODALITY_CODEC_VERSION,
            modality,
            operation,
            &node_id,
            &state_sha256,
            &receipt_query,
            &result_sha256,
            &result_msgpack,
        )?;
        let command = Self {
            schema_version: SANITIZED_MODALITY_CODEC_VERSION,
            modality,
            operation,
            node_id,
            sealed_runtime_state,
            state_sha256,
            receipt_query,
            result_msgpack,
            result,
            result_sha256,
            authentication_tag,
        };
        command.validate(server_secret)?;
        Ok(command)
    }

    pub(crate) fn receipt_method(&self) -> Method {
        Method::ApplyMutation {
            event_type: "served_modality_v1".to_string(),
            query: self.receipt_query.clone(),
        }
    }

    fn validate(&self, server_secret: &str) -> Result<(), String> {
        use sha2::{Digest, Sha256};
        if server_secret.is_empty() || self.schema_version != SANITIZED_MODALITY_CODEC_VERSION
        {
            return Err("sanitized modality Raft command schema version is unsupported".to_string());
        }
        if self.result.schema_version != self.schema_version
            || self.result.modality != self.modality
            || self.result.operation != self.operation
        {
            return Err("sanitized modality Raft result type does not match command".to_string());
        }
        self.result.validate()?;
        let canonical_result = self.result.canonical_bytes()?;
        let observed_result = hex::encode(Sha256::digest(&canonical_result));
        if observed_result != self.result_sha256
            || self.result_sha256.len() != 64
            || !self
                .result_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return Err("sanitized modality Raft result digest does not match".to_string());
        }
        if self.result.to_wire()? != self.result_msgpack {
            return Err("sanitized modality Raft result is not canonical".to_string());
        }
        if server_secret.is_empty()
            || self.sealed_runtime_state.is_empty()
            || self.sealed_runtime_state.len() > MAX_REPLICATED_MODALITY_STATE_BYTES
            || !crate::crypto::is_sealed(&self.sealed_runtime_state)
        {
            return Err("sanitized modality Raft state is invalid".to_string());
        }
        let expected_prefix = format!(
            "__eg_internal_served_{}_",
            sanitized_modality_name(self.modality)
        );
        let Some(partition) = self.node_id.strip_prefix(&expected_prefix) else {
            return Err("sanitized modality Raft partition is invalid".to_string());
        };
        if partition.len() != 64
            || !partition
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return Err("sanitized modality Raft partition is invalid".to_string());
        }
        if self.receipt_query.len() != 71
            || !self.receipt_query.starts_with("sha256:")
            || !self.receipt_query[7..]
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return Err("sanitized modality Raft receipt is invalid".to_string());
        }
        let observed_state = hex::encode(Sha256::digest(&self.sealed_runtime_state));
        if observed_state != self.state_sha256 {
            return Err("sanitized modality Raft state digest does not match".to_string());
        }
        if self.result_msgpack.is_empty()
            || self.result_msgpack.len() > MAX_REPLICATED_MODALITY_RESULT_BYTES
        {
            return Err("sanitized modality Raft result is invalid".to_string());
        }
        let expected_tag = sanitized_modality_tag(
            server_secret,
            self.schema_version,
            self.modality,
            self.operation,
            &self.node_id,
            &self.state_sha256,
            &self.receipt_query,
            &self.result_sha256,
            &self.result_msgpack,
        )?;
        if !constant_time_eq(expected_tag.as_bytes(), self.authentication_tag.as_bytes()) {
            return Err("sanitized modality Raft authentication failed".to_string());
        }
        Ok(())
    }
}

#[cfg(feature = "modality-serving")]
fn sanitized_modality_name(modality: eg_types::ServedModalityKind) -> &'static str {
    match modality {
        eg_types::ServedModalityKind::Document => "document",
        eg_types::ServedModalityKind::Image => "image",
        eg_types::ServedModalityKind::Audio => "audio",
        eg_types::ServedModalityKind::Video => "video",
    }
}

#[cfg(feature = "modality-serving")]
#[allow(clippy::too_many_arguments)]
fn sanitized_modality_tag(
    server_secret: &str,
    schema_version: u16,
    modality: eg_types::ServedModalityKind,
    operation: SanitizedModalityMutation,
    node_id: &str,
    state_sha256: &str,
    receipt_query: &str,
    result_sha256: &str,
    result_msgpack: &[u8],
) -> Result<String, String> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut mac = Hmac::<Sha256>::new_from_slice(server_secret.as_bytes())
        .map_err(|_| "sanitized modality Raft authentication failed".to_string())?;
    for value in [
        b"sanitized-modality-raft-v1".as_slice(),
        sanitized_modality_name(modality).as_bytes(),
        operation.as_bytes(),
        node_id.as_bytes(),
        state_sha256.as_bytes(),
        receipt_query.as_bytes(),
        result_sha256.as_bytes(),
        result_msgpack,
    ] {
        mac.update(&(value.len() as u64).to_be_bytes());
        mac.update(value);
    }
    mac.update(&schema_version.to_be_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

#[cfg(feature = "modality-serving")]
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0u8, |difference, (left, right)| {
            difference | (*left ^ *right)
        })
        == 0
}

#[cfg(all(test, feature = "modality-serving"))]
mod sanitized_modality_command_tests {
    use super::*;

    fn result() -> Vec<u8> {
        let outcome = eg_modality::ApplyOutcome {
            disposition: eg_modality::ApplyDisposition::Applied,
            observation_version: 1,
            event_sequence: 1,
        };
        rmp_serde::to_vec_named(&crate::protocol::ResultPayload::raw(&outcome)).unwrap()
    }

    fn stream_result() -> Vec<u8> {
        let outcomes = vec![
            eg_modality::ApplyOutcome {
                disposition: eg_modality::ApplyDisposition::Applied,
                observation_version: 1,
                event_sequence: 1,
            },
            eg_modality::ApplyOutcome {
                disposition: eg_modality::ApplyDisposition::Applied,
                observation_version: 2,
                event_sequence: 2,
            },
        ];
        rmp_serde::to_vec_named(&crate::protocol::ResultPayload::raw(&outcomes)).unwrap()
    }

    #[test]
    fn encrypted_command_round_trips_without_raw_source() {
        let source = b"ephemeral non-identifying source fixture";
        let cipher = crate::crypto::ValueCipher::from_key_material(b"replica-state-key");
        let sealed = cipher.seal(source);
        let command = SanitizedModalityRaftCommand::new(
            "cluster-auth-secret",
            eg_types::ServedModalityKind::Document,
            SanitizedModalityMutation::Ingest,
            format!("__eg_internal_served_document_{}", "a".repeat(64)),
            sealed,
            format!("sha256:{}", "b".repeat(64)),
            result(),
        )
        .unwrap();
        let replicated = ReplicatedMutation::served_modality(command.clone());
        let encoded = rmp_serde::to_vec_named(&replicated).unwrap();
        assert!(!encoded.windows(source.len()).any(|window| window == source));
        let decoded: ReplicatedMutation = rmp_serde::from_slice(&encoded).unwrap();
        let ReplicatedMutation::Native {
            command: NativeMutationCommand::ServedModality { command: decoded },
        } = decoded
        else {
            panic!("sanitized modality command did not round-trip as its typed variant");
        };
        decoded.validate("cluster-auth-secret").unwrap();
        assert_eq!(decoded.schema_version, SANITIZED_MODALITY_CODEC_VERSION);
        assert_eq!(decoded.result.schema_version, SANITIZED_MODALITY_CODEC_VERSION);
        assert_eq!(decoded.result.kind, SanitizedModalityResultKind::Single);
    }

    #[test]
    fn stream_result_uses_the_typed_bounded_result_schema() {
        let cipher = crate::crypto::ValueCipher::from_key_material(b"replica-state-key");
        let command = SanitizedModalityRaftCommand::new(
            "cluster-auth-secret",
            eg_types::ServedModalityKind::Document,
            SanitizedModalityMutation::IngestStream,
            format!("__eg_internal_served_document_{}", "a".repeat(64)),
            cipher.seal(b"opaque runtime state"),
            format!("sha256:{}", "b".repeat(64)),
            stream_result(),
        )
        .unwrap();
        assert_eq!(command.result.kind, SanitizedModalityResultKind::Stream);
        assert_eq!(command.result.outcomes.len(), 2);
        command.validate("cluster-auth-secret").unwrap();
    }

    #[test]
    fn malformed_result_type_length_version_and_digest_fail_closed() {
        let cipher = crate::crypto::ValueCipher::from_key_material(b"replica-state-key");
        let node_id = format!("__eg_internal_served_audio_{}", "a".repeat(64));
        let receipt = format!("sha256:{}", "b".repeat(64));
        let wrong_type = rmp_serde::to_vec_named(&crate::protocol::ResultPayload::Bool(true))
            .unwrap();
        assert!(SanitizedModalityRaftCommand::new(
            "cluster-auth-secret",
            eg_types::ServedModalityKind::Audio,
            SanitizedModalityMutation::Delete,
            node_id.clone(),
            cipher.seal(b"opaque runtime state"),
            receipt.clone(),
            wrong_type,
        )
        .is_err());

        assert!(SanitizedModalityRaftCommand::new(
            "cluster-auth-secret",
            eg_types::ServedModalityKind::Audio,
            SanitizedModalityMutation::Delete,
            node_id.clone(),
            cipher.seal(b"opaque runtime state"),
            receipt.clone(),
            vec![0u8; MAX_REPLICATED_MODALITY_RESULT_BYTES + 1],
        )
        .is_err());

        let mut command = SanitizedModalityRaftCommand::new(
            "cluster-auth-secret",
            eg_types::ServedModalityKind::Audio,
            SanitizedModalityMutation::Delete,
            node_id,
            cipher.seal(b"opaque runtime state"),
            receipt,
            result(),
        )
        .unwrap();
        command.schema_version = SANITIZED_MODALITY_CODEC_VERSION + 1;
        assert!(command.validate("cluster-auth-secret").is_err());

        let mut command = SanitizedModalityRaftCommand::new(
            "cluster-auth-secret",
            eg_types::ServedModalityKind::Audio,
            SanitizedModalityMutation::Delete,
            format!("__eg_internal_served_audio_{}", "a".repeat(64)),
            cipher.seal(b"opaque runtime state"),
            format!("sha256:{}", "b".repeat(64)),
            result(),
        )
        .unwrap();
        command.result.operation = SanitizedModalityMutation::IngestStream;
        assert!(command.validate("cluster-auth-secret").is_err());

        let mut command = SanitizedModalityRaftCommand::new(
            "cluster-auth-secret",
            eg_types::ServedModalityKind::Audio,
            SanitizedModalityMutation::Delete,
            format!("__eg_internal_served_audio_{}", "a".repeat(64)),
            cipher.seal(b"opaque runtime state"),
            format!("sha256:{}", "b".repeat(64)),
            result(),
        )
        .unwrap();
        command.result_sha256 = "0".repeat(64);
        assert!(command.validate("cluster-auth-secret").is_err());

        let mut command = SanitizedModalityRaftCommand::new(
            "cluster-auth-secret",
            eg_types::ServedModalityKind::Audio,
            SanitizedModalityMutation::Delete,
            format!("__eg_internal_served_audio_{}", "a".repeat(64)),
            cipher.seal(b"opaque runtime state"),
            format!("sha256:{}", "b".repeat(64)),
            result(),
        )
        .unwrap();
        command.result.outcomes[0].event_sequence = 99;
        assert!(command.validate("cluster-auth-secret").is_err());
    }

    #[test]
    fn unsealed_or_forged_replica_state_fails_closed() {
        assert!(SanitizedModalityRaftCommand::new(
            "cluster-auth-secret",
            eg_types::ServedModalityKind::Audio,
            SanitizedModalityMutation::Delete,
            format!("__eg_internal_served_audio_{}", "a".repeat(64)),
            b"plaintext".to_vec(),
            format!("sha256:{}", "b".repeat(64)),
            result(),
        )
        .is_err());

        let cipher = crate::crypto::ValueCipher::from_key_material(b"replica-state-key");
        let command = SanitizedModalityRaftCommand::new(
            "cluster-auth-secret",
            eg_types::ServedModalityKind::Video,
            SanitizedModalityMutation::Restore,
            format!("__eg_internal_served_video_{}", "a".repeat(64)),
            cipher.seal(b"opaque state"),
            format!("sha256:{}", "b".repeat(64)),
            result(),
        )
        .unwrap();
        assert!(command.validate("wrong-secret").is_err());
    }

    #[test]
    fn mutation_batch_audit_and_outbox_retain_only_the_safe_receipt() {
        let source = b"ephemeral source excluded from durable coordination";
        let cipher = crate::crypto::ValueCipher::from_key_material(b"replica-state-key");
        let sealed = cipher.seal(source);
        let command = SanitizedModalityRaftCommand::new(
            "cluster-auth-secret",
            eg_types::ServedModalityKind::Image,
            SanitizedModalityMutation::MoveToCold,
            format!("__eg_internal_served_image_{}", "a".repeat(64)),
            sealed.clone(),
            format!("sha256:{}", "b".repeat(64)),
            result(),
        )
        .unwrap();
        let safe_receipt = command.receipt_method();
        let batch = crate::server::mutation_batch::compile_methods(
            crate::server::mutation_batch::CompileBatch {
                batch_id: "opaque-batch",
                request_id: 1,
                principal: Some("principal:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                tenant: "opaque-tenant",
                graph: "opaque-graph",
                placement_epoch: 1,
                idempotency_key: "opaque-idempotency",
                expected_graph_version: Some(0),
                fencing_token: Some(1),
                created_at_ms: 1,
                default_surface: crate::mutation_batch::MutationSurface::Graph,
                authoritative_state: Some(crate::mutation_batch::MutationStateDescriptor {
                    algorithm: "sha256".to_string(),
                    digest: "c".repeat(64),
                    source_graph_version: 0,
                    target_graph_version: 1,
                }),
            },
            vec![safe_receipt],
        )
        .unwrap();
        let encoded = rmp_serde::to_vec_named(&batch).unwrap();
        assert!(!encoded.windows(source.len()).any(|window| window == source));
        assert!(!encoded.windows(sealed.len()).any(|window| window == sealed));
        assert_eq!(batch.outbox.len(), 1);
        assert!(crate::audit::audit_line(&batch.operations[0].method)
            .is_some_and(|line| line.starts_with("AUTHORITATIVE_STATE_MUTATION|sha256:")));
    }
}

/// A deterministic command accepted by the replicated state machine.
///
/// Public RPC methods are deliberately not the Raft wire schema. Every
/// caller-controlled payload is AEAD-sealed before it reaches the log, while the
/// outer variants retain the bounded state-machine domain needed for static
/// inventory and fail-closed dispatch.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ReplicatedMutation {
    Graph { sealed_method: SealedNativeMethod },
    Native { command: NativeMutationCommand },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionParticipantPhase {
    Prepare,
    Commit,
    Abort,
}

/// Engine-native commands whose durable effect is not reducible to a GraphCore
/// mutation. New native stores must add a typed variant and deterministic replica
/// apply/snapshot support before clustered dispatch may acknowledge them.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum NativeMutationCommand {
    ChangeEnvelope {
        sealed_envelope: SealedNativeMethod,
    },
    #[cfg(feature = "modality-serving")]
    ServedModality {
        command: SanitizedModalityRaftCommand,
    },
    /// One graph participant in the engine-owned prepare/decision/commit protocol.
    /// Prepare/commit carry the same sealed canonical plan; abort carries no plan.
    TransactionParticipant {
        phase: TransactionParticipantPhase,
        coordinator_id: String,
        participant_id: u64,
        #[serde(deserialize_with = "deserialize_required_option")]
        sealed_plan: Option<SealedNativeMethod>,
    },
    /// Atomic transaction outcome recorded in the control group's consensus log.
    TransactionDecision {
        coordinator_id: String,
        commit: bool,
    },
    /// Terminalize the prepared parent only after all decided participants finish.
    TransactionFinalize {
        coordinator_id: String,
        commit: bool,
    },
    /// Commit a scheduler-prepared analytics result in its target graph group.
    #[cfg(feature = "jobs")]
    JobPublicationCommit {
        coordinator_id: String,
        sealed_plan: SealedNativeMethod,
    },
    /// Terminalize the scheduler row only after target-group commit succeeds.
    #[cfg(feature = "jobs")]
    JobPublicationFinalize {
        coordinator_id: String,
        sealed_receipt: SealedNativeMethod,
    },
    /// Graph-adjacent state whose deterministic kernel is not `GraphCore` alone
    /// (query catalogs, ICV policy, or an explicitly materialized state image).
    GraphState {
        sealed_method: SealedNativeMethod,
    },
    /// Named OCC staging and commit coordination.
    Transaction {
        sealed_method: SealedNativeMethod,
    },
    /// Durable work-item lease/result transitions.
    /// Resource reservations and host-capacity updates use this same sealed
    /// command domain so their result-producing native apply path is ordered
    /// with the WorkItem lifecycle without introducing a second authority.
    WorkItem {
        sealed_method: SealedNativeMethod,
    },
    /// Content-addressed blob cursor/chunk/refcount transitions.
    #[cfg(feature = "blob")]
    Blob {
        sealed_method: SealedNativeMethod,
    },
    /// Namespaced key/value transitions.
    #[cfg(feature = "kv")]
    KeyValue {
        sealed_method: SealedNativeMethod,
    },
    /// Time-series append transitions.
    #[cfg(feature = "tsdb")]
    TimeSeries {
        sealed_method: SealedNativeMethod,
    },
    /// Durable analytics-job state-machine transitions.
    #[cfg(feature = "jobs")]
    AnalyticsJob {
        sealed_method: SealedNativeMethod,
    },
    /// Durable native statechart definition/instance transitions (CONCEPT:INT-P2-2),
    /// structurally identical to `AnalyticsJob` above -- own `statecharts.redb`,
    /// not graph-scoped.
    #[cfg(feature = "statechart")]
    Statechart {
        sealed_method: SealedNativeMethod,
    },
    /// SQLite catalog import transitions.
    #[cfg(feature = "sqlite-file")]
    SqliteCatalog {
        sealed_method: SealedNativeMethod,
    },
    /// Channel, federation, UDF, streaming, trigger, and CEP control state.
    SessionControl {
        sealed_method: SealedNativeMethod,
    },
    /// Identity and RBAC policy state.
    Identity {
        sealed_method: SealedNativeMethod,
    },
    /// Cluster-wide catalog, reshard, restore, and materialized-view state.
    ClusterAdmin {
        sealed_method: SealedNativeMethod,
    },
    /// Graph registry lifecycle and multi-graph parent coordination.
    GraphLifecycle {
        sealed_method: SealedNativeMethod,
    },
    /// Threshold-authorized mutation translation.
    Multisig {
        sealed_method: SealedNativeMethod,
    },
}

/// AEAD-protected payload carried by a bounded typed command. Consensus
/// persists only ciphertext and a digest; identifiers, endpoints, paths, query
/// text, and user-controlled payloads cannot appear in a Raft log or snapshot as
/// plaintext.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealedNativeMethod {
    #[serde(with = "serde_bytes")]
    ciphertext: Vec<u8>,
    plaintext_sha256: String,
}

impl SealedNativeMethod {
    fn seal_value<T: Serialize>(server_secret: &str, value: &T) -> Result<Self, String> {
        use sha2::{Digest, Sha256};
        if server_secret.is_empty() {
            return Err("native Raft command requires cluster key material".to_string());
        }
        let plaintext = rmp_serde::to_vec_named(value).map_err(|error| error.to_string())?;
        if plaintext.is_empty() || plaintext.len() > MAX_REPLICATED_COMMAND_PAYLOAD_BYTES {
            return Err("native Raft command exceeds resource limits".to_string());
        }
        let key = native_method_key(server_secret);
        let ciphertext = crate::crypto::ValueCipher::from_key_material(&key).seal(&plaintext);
        let command = Self {
            ciphertext,
            plaintext_sha256: hex::encode(Sha256::digest(&plaintext)),
        };
        command.validate_shape()?;
        Ok(command)
    }

    fn open_value<T: serde::de::DeserializeOwned>(&self, server_secret: &str) -> Result<T, String> {
        use sha2::{Digest, Sha256};
        self.validate_shape()?;
        if server_secret.is_empty() {
            return Err("native Raft command requires cluster key material".to_string());
        }
        let key = native_method_key(server_secret);
        let plaintext = crate::crypto::ValueCipher::from_key_material(&key)
            .unseal(&self.ciphertext)
            .map_err(|_| "native Raft command authentication failed".to_string())?;
        if plaintext.is_empty() || plaintext.len() > MAX_REPLICATED_COMMAND_PAYLOAD_BYTES {
            return Err("native Raft command exceeds resource limits".to_string());
        }
        let observed = hex::encode(Sha256::digest(&plaintext));
        if observed != self.plaintext_sha256 {
            return Err("native Raft command digest mismatch".to_string());
        }
        eg_types::msgpack::decode_bounded(
            &plaintext,
            eg_types::msgpack::MsgpackLimits::new(
                MAX_REPLICATED_COMMAND_PAYLOAD_BYTES,
                4_000_000,
                eg_types::msgpack::DEFAULT_MAX_DEPTH,
            ),
        )
        .map_err(|_| "native Raft command payload is invalid".to_string())
    }

    fn new(server_secret: &str, method: &Method) -> Result<Self, String> {
        Self::seal_value(server_secret, method)
    }

    fn open(&self, server_secret: &str) -> Result<Method, String> {
        self.open_value(server_secret)
    }

    fn validate_shape(&self) -> Result<(), String> {
        if self.ciphertext.is_empty()
            || self.ciphertext.len() > MAX_REPLICATED_COMMAND_PAYLOAD_BYTES + 64
            || !crate::crypto::is_sealed(&self.ciphertext)
            || self.plaintext_sha256.len() != 64
            || !self
                .plaintext_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return Err("native Raft command envelope is invalid".to_string());
        }
        Ok(())
    }
}

fn native_method_key(server_secret: &str) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"epistemic-graph-native-raft-v1\0");
    digest.update(server_secret.as_bytes());
    digest.finalize().into()
}

/// Public mutation tags with an explicit engine-native consensus command.
///
/// This is deliberately data, not prose: the clustered-admission test joins it
/// with the graph gateway and the two specialized commands, then compares that
/// union with `eg_capabilities::ALL_METHODS`. Adding a mutating protocol variant
/// without a replicated command therefore fails the source-level inventory gate.
pub const NATIVE_CONSENSUS_METHODS: &[&str] = &[
    "EvictLRU",
    "DecaySweep",
    "TouchNodes",
    "FromMsgpack",
    "Reconcile",
    "ApplyMutation",
    "PruneByLifecycle",
    "ClearLedger",
    "ApplyLedger",
    "CompactNodesByType",
    "RecomputeMaterialization",
    "Sql",
    "CypherQuery",
    "GraphQl",
    "RunDatalogReasoning",
    "IcvConfigure",
    "BeginTxn",
    "TxnAddNode",
    "TxnRemoveNode",
    "TxnAddEdge",
    "TxnRemoveEdge",
    "TxnCas",
    "TxnAddEmbedding",
    "TxnBlobRef",
    "TxnAddMeasurement",
    "TxnAxiom",
    "TxnConstruct",
    "TxnPlanWriteback",
    "TxnMaterializeBelief",
    "Commit",
    "Rollback",
    "ClaimWorkItem",
    "MintWorkItemClaimCapability",
    "RenewWorkItemLease",
    "CommitWorkItemResult",
    "CancelWorkItem",
    "DeferWorkItem",
    "CasWorkItemMetadata",
    "ReserveWorkItemResources",
    "ReleaseWorkItemResources",
    "ReclaimWorkItemResources",
    "UpdateResourceHost",
    "BlobBegin",
    "BlobChunkPut",
    "BlobCommit",
    "BlobGc",
    "BlobRef",
    "BlobUnref",
    "KvPut",
    "KvDelete",
    "KvCas",
    "TsAppend",
    "AnalyticsJob",
    "ImportSqliteFile",
    "CreateChannel",
    "JoinChannel",
    "LeaveChannel",
    "CloseChannel",
    "SendMessage",
    "RegisterForeignSource",
    "RegisterUdf",
    "RegisterContinuousQuery",
    "DropContinuousQuery",
    "RegisterTrigger",
    "DropTrigger",
    "CepSubscribe",
    "CepUnsubscribe",
    "RegisterIdentity",
    "RbacAdmin",
    "Reshard",
    "CatalogAssign",
    "CatalogReassign",
    "CatalogRemove",
    "RebalanceExecute",
    "Restore",
    "NodeInfoUpsert",
    "CreateMatView",
    "RefreshMatView",
    "PlanMatViewDefine",
    "PlanMatViewRefresh",
    "PlanMatViewDrop",
    "CreateGraph",
    "DeleteGraph",
    "ApplyMultisigMutation",
    "Statechart",
    "ReserveDevelopmentLane",
    "RenewDevelopmentLane",
    "ObserveDevelopmentLane",
    "FinishDevelopmentLane",
    "CleanupDevelopmentLane",
    "UpdateDevelopmentLaneQuota",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NativeMutationDomain {
    GraphState,
    Transaction,
    WorkItem,
    #[cfg(feature = "blob")]
    Blob,
    #[cfg(feature = "kv")]
    KeyValue,
    #[cfg(feature = "tsdb")]
    TimeSeries,
    #[cfg(feature = "jobs")]
    AnalyticsJob,
    #[cfg(feature = "statechart")]
    Statechart,
    #[cfg(feature = "sqlite-file")]
    SqliteCatalog,
    SessionControl,
    Identity,
    ClusterAdmin,
    GraphLifecycle,
    Multisig,
}

fn native_domain(method: &Method) -> Option<NativeMutationDomain> {
    match method {
        Method::EvictLRU { .. }
        | Method::DecaySweep { .. }
        | Method::TouchNodes { .. }
        | Method::FromMsgpack { .. }
        | Method::Reconcile { .. }
        | Method::ApplyMutation { .. }
        | Method::PruneByLifecycle { .. }
        | Method::ClearLedger
        | Method::ApplyLedger { .. }
        | Method::CompactNodesByType { .. }
        | Method::Sql { .. } => Some(NativeMutationDomain::GraphState),
        #[cfg(feature = "epistemic")]
        Method::RecomputeMaterialization { .. } => Some(NativeMutationDomain::GraphState),
        Method::CypherQuery {
            mode: CypherMode::Write,
            ..
        } => Some(NativeMutationDomain::GraphState),
        #[cfg(feature = "graphql")]
        Method::GraphQl { .. } => Some(NativeMutationDomain::GraphState),
        #[cfg(feature = "reasoning")]
        Method::RunDatalogReasoning { .. } => Some(NativeMutationDomain::GraphState),
        Method::IcvConfigure { .. } => Some(NativeMutationDomain::GraphState),
        // The 6 DevelopmentLane write methods (RMDD-28) are graph-scoped (keyed by
        // `req.graph`, sanitized the same way `dispatch_graph_op_inner`'s
        // `is_development_lane_method` block does) and their commit
        // (`redb_store::development_lane::commit_development_lane`) already reads
        // ONLY the authority-normalized `authoritative_now_ms()` clock the caller
        // binds before dispatch (never a local wall-clock read) -- so, exactly
        // like `EvictLRU`/`Sql` above, replaying the SAME committed Method on every
        // replica is deterministic. They share this domain rather than a dedicated
        // one because they are, structurally, one more heterogeneous graph-scoped
        // native operation -- `dispatch_graph_op_inner`'s own `is_development_lane_
        // method` block (not this domain tag) decides they skip the generic
        // MutationBatch/audit/CDC gateway, exactly as it already does today.
        Method::ReserveDevelopmentLane { .. }
        | Method::RenewDevelopmentLane { .. }
        | Method::ObserveDevelopmentLane { .. }
        | Method::FinishDevelopmentLane { .. }
        | Method::CleanupDevelopmentLane { .. }
        | Method::UpdateDevelopmentLaneQuota { .. } => Some(NativeMutationDomain::GraphState),

        Method::BeginTxn { .. }
        | Method::TxnAddNode { .. }
        | Method::TxnRemoveNode { .. }
        | Method::TxnAddEdge { .. }
        | Method::TxnRemoveEdge { .. }
        | Method::TxnCas { .. }
        | Method::TxnAddEmbedding { .. }
        | Method::TxnBlobRef { .. }
        | Method::Commit { .. }
        | Method::Rollback { .. } => Some(NativeMutationDomain::Transaction),
        #[cfg(feature = "tsdb")]
        Method::TxnAddMeasurement { .. } => Some(NativeMutationDomain::Transaction),
        #[cfg(feature = "owl")]
        Method::TxnAxiom { .. } => Some(NativeMutationDomain::Transaction),
        #[cfg(feature = "sparql")]
        Method::TxnConstruct { .. } => Some(NativeMutationDomain::Transaction),
        #[cfg(feature = "query")]
        Method::TxnPlanWriteback { .. } => Some(NativeMutationDomain::Transaction),
        #[cfg(feature = "epistemic")]
        Method::TxnMaterializeBelief { .. } => Some(NativeMutationDomain::Transaction),

        Method::ClaimWorkItem { .. } | Method::MintWorkItemClaimCapability { .. }
        | Method::RenewWorkItemLease { .. }
        | Method::CommitWorkItemResult { .. }
        | Method::CancelWorkItem { .. }
        | Method::DeferWorkItem { .. }
        | Method::CasWorkItemMetadata { .. }
        | Method::ReserveWorkItemResources { .. }
        | Method::ReleaseWorkItemResources { .. }
        | Method::ReclaimWorkItemResources { .. }
        | Method::UpdateResourceHost { .. } => Some(NativeMutationDomain::WorkItem),

        #[cfg(feature = "blob")]
        Method::BlobBegin { .. }
        | Method::BlobChunkPut { .. }
        | Method::BlobCommit { .. }
        | Method::BlobGc
        | Method::BlobRef { .. }
        | Method::BlobUnref { .. } => Some(NativeMutationDomain::Blob),

        #[cfg(feature = "kv")]
        Method::KvPut { .. } | Method::KvDelete { .. } | Method::KvCas { .. } => {
            Some(NativeMutationDomain::KeyValue)
        }

        #[cfg(feature = "tsdb")]
        Method::TsAppend { .. } => Some(NativeMutationDomain::TimeSeries),
        #[cfg(feature = "jobs")]
        Method::AnalyticsJob { .. } => Some(NativeMutationDomain::AnalyticsJob),
        // Not graph-scoped (own `statecharts.redb`, keyed by def_id/instance_id) --
        // structurally identical to `AnalyticsJob` above, just gated `statechart`
        // instead of `jobs` (see `handlers::statechart` module docs). Deterministic
        // replay across replicas: `StatechartStore::instantiate_batch`/`send_event`
        // take a caller-supplied request-derived id / explicit `now_ms` rather than
        // reading a local counter/clock (CONCEPT:INT-P2-2, D-DE7-2 closed).
        #[cfg(feature = "statechart")]
        Method::Statechart { .. } => Some(NativeMutationDomain::Statechart),
        #[cfg(feature = "sqlite-file")]
        Method::ImportSqliteFile { .. } => Some(NativeMutationDomain::SqliteCatalog),

        Method::CreateChannel { .. }
        | Method::JoinChannel { .. }
        | Method::LeaveChannel { .. }
        | Method::CloseChannel { .. }
        | Method::SendMessage { .. } => Some(NativeMutationDomain::SessionControl),
        #[cfg(feature = "federation")]
        Method::RegisterForeignSource { .. } => Some(NativeMutationDomain::SessionControl),
        #[cfg(feature = "wasm-udf")]
        Method::RegisterUdf { .. } => Some(NativeMutationDomain::SessionControl),
        #[cfg(feature = "streaming")]
        Method::RegisterContinuousQuery { .. }
        | Method::DropContinuousQuery { .. }
        | Method::RegisterTrigger { .. }
        | Method::DropTrigger { .. } => Some(NativeMutationDomain::SessionControl),
        #[cfg(all(feature = "streaming", feature = "stream"))]
        Method::CepSubscribe { .. } | Method::CepUnsubscribe { .. } => {
            Some(NativeMutationDomain::SessionControl)
        }

        Method::RegisterIdentity { .. } | Method::RbacAdmin { .. } => {
            Some(NativeMutationDomain::Identity)
        }

        Method::Reshard { .. }
        | Method::CatalogAssign { .. }
        | Method::CatalogReassign { .. }
        | Method::CatalogRemove { .. }
        | Method::RebalanceExecute { .. }
        | Method::Restore { .. }
        // ADR-1 / W1.1: each node's self-report into the durable cluster-topology
        // store (`server::persistence::node_info_store`) -- same ClusterAdmin
        // bucket as its M3 catalog siblings above, so the SAME committed log entry
        // applies deterministically on every replica.
        | Method::NodeInfoUpsert { .. } => Some(NativeMutationDomain::ClusterAdmin),
        #[cfg(feature = "compute-dist")]
        Method::CreateMatView { .. } | Method::RefreshMatView { .. } => {
            Some(NativeMutationDomain::ClusterAdmin)
        }
        #[cfg(feature = "matview")]
        Method::PlanMatViewDefine { .. }
        | Method::PlanMatViewRefresh { .. }
        | Method::PlanMatViewDrop { .. } => Some(NativeMutationDomain::ClusterAdmin),

        Method::CreateGraph { .. } | Method::DeleteGraph { .. } => {
            Some(NativeMutationDomain::GraphLifecycle)
        }
        Method::ApplyMultisigMutation { .. } => Some(NativeMutationDomain::Multisig),
        _ => None,
    }
}

impl NativeMutationCommand {
    /// Convert only an explicitly inventoried public mutation into its bounded,
    /// encrypted native consensus domain. There is no raw public-method variant.
    pub(crate) fn from_public_method(
        method: Method,
        server_secret: &str,
    ) -> Result<Self, Box<Method>> {
        let Some(domain) = native_domain(&method) else {
            return Err(Box::new(method));
        };
        let sealed_method = match SealedNativeMethod::new(server_secret, &method) {
            Ok(value) => value,
            Err(_) => return Err(Box::new(method)),
        };
        Ok(match domain {
            NativeMutationDomain::GraphState => Self::GraphState { sealed_method },
            NativeMutationDomain::Transaction => Self::Transaction { sealed_method },
            NativeMutationDomain::WorkItem => Self::WorkItem { sealed_method },
            #[cfg(feature = "blob")]
            NativeMutationDomain::Blob => Self::Blob { sealed_method },
            #[cfg(feature = "kv")]
            NativeMutationDomain::KeyValue => Self::KeyValue { sealed_method },
            #[cfg(feature = "tsdb")]
            NativeMutationDomain::TimeSeries => Self::TimeSeries { sealed_method },
            #[cfg(feature = "jobs")]
            NativeMutationDomain::AnalyticsJob => Self::AnalyticsJob { sealed_method },
            #[cfg(feature = "statechart")]
            NativeMutationDomain::Statechart => Self::Statechart { sealed_method },
            #[cfg(feature = "sqlite-file")]
            NativeMutationDomain::SqliteCatalog => Self::SqliteCatalog { sealed_method },
            NativeMutationDomain::SessionControl => Self::SessionControl { sealed_method },
            NativeMutationDomain::Identity => Self::Identity { sealed_method },
            NativeMutationDomain::ClusterAdmin => Self::ClusterAdmin { sealed_method },
            NativeMutationDomain::GraphLifecycle => Self::GraphLifecycle { sealed_method },
            NativeMutationDomain::Multisig => Self::Multisig { sealed_method },
        })
    }

    pub(crate) fn domain(&self) -> Option<NativeMutationDomain> {
        match self {
            Self::ChangeEnvelope { .. } => None,
            #[cfg(feature = "modality-serving")]
            Self::ServedModality { .. } => None,
            Self::TransactionParticipant { .. }
            | Self::TransactionDecision { .. }
            | Self::TransactionFinalize { .. } => Some(NativeMutationDomain::Transaction),
            #[cfg(feature = "jobs")]
            Self::JobPublicationCommit { .. } | Self::JobPublicationFinalize { .. } => {
                Some(NativeMutationDomain::AnalyticsJob)
            }
            Self::GraphState { .. } => Some(NativeMutationDomain::GraphState),
            Self::Transaction { .. } => Some(NativeMutationDomain::Transaction),
            Self::WorkItem { .. } => Some(NativeMutationDomain::WorkItem),
            #[cfg(feature = "blob")]
            Self::Blob { .. } => Some(NativeMutationDomain::Blob),
            #[cfg(feature = "kv")]
            Self::KeyValue { .. } => Some(NativeMutationDomain::KeyValue),
            #[cfg(feature = "tsdb")]
            Self::TimeSeries { .. } => Some(NativeMutationDomain::TimeSeries),
            #[cfg(feature = "jobs")]
            Self::AnalyticsJob { .. } => Some(NativeMutationDomain::AnalyticsJob),
            #[cfg(feature = "statechart")]
            Self::Statechart { .. } => Some(NativeMutationDomain::Statechart),
            #[cfg(feature = "sqlite-file")]
            Self::SqliteCatalog { .. } => Some(NativeMutationDomain::SqliteCatalog),
            Self::SessionControl { .. } => Some(NativeMutationDomain::SessionControl),
            Self::Identity { .. } => Some(NativeMutationDomain::Identity),
            Self::ClusterAdmin { .. } => Some(NativeMutationDomain::ClusterAdmin),
            Self::GraphLifecycle { .. } => Some(NativeMutationDomain::GraphLifecycle),
            Self::Multisig { .. } => Some(NativeMutationDomain::Multisig),
        }
    }

    fn sealed_method(&self) -> Option<&SealedNativeMethod> {
        match self {
            Self::ChangeEnvelope { .. } => None,
            #[cfg(feature = "modality-serving")]
            Self::ServedModality { .. } => None,
            Self::TransactionParticipant { .. }
            | Self::TransactionDecision { .. }
            | Self::TransactionFinalize { .. } => None,
            #[cfg(feature = "jobs")]
            Self::JobPublicationCommit { .. } | Self::JobPublicationFinalize { .. } => None,
            Self::GraphState { sealed_method }
            | Self::Transaction { sealed_method }
            | Self::WorkItem { sealed_method }
            | Self::SessionControl { sealed_method }
            | Self::Identity { sealed_method }
            | Self::ClusterAdmin { sealed_method }
            | Self::GraphLifecycle { sealed_method }
            | Self::Multisig { sealed_method } => Some(sealed_method),
            #[cfg(feature = "blob")]
            Self::Blob { sealed_method } => Some(sealed_method),
            #[cfg(feature = "kv")]
            Self::KeyValue { sealed_method } => Some(sealed_method),
            #[cfg(feature = "tsdb")]
            Self::TimeSeries { sealed_method } => Some(sealed_method),
            #[cfg(feature = "jobs")]
            Self::AnalyticsJob { sealed_method } => Some(sealed_method),
            #[cfg(feature = "statechart")]
            Self::Statechart { sealed_method } => Some(sealed_method),
            #[cfg(feature = "sqlite-file")]
            Self::SqliteCatalog { sealed_method } => Some(sealed_method),
        }
    }

    pub(crate) fn open_public_method(&self, server_secret: &str) -> Result<Option<Method>, String> {
        let Some(sealed) = self.sealed_method() else {
            return Ok(None);
        };
        let method = sealed.open(server_secret)?;
        if native_domain(&method) != self.domain() {
            return Err("native Raft command method is outside its declared domain".to_string());
        }
        Ok(Some(method))
    }

    fn validate_shape(&self) -> Result<(), String> {
        match self {
            Self::TransactionParticipant {
                phase,
                coordinator_id,
                sealed_plan,
                ..
            } => {
                if !opaque_scope_is_valid(coordinator_id) {
                    return Err("transaction participant coordinator is invalid".to_string());
                }
                match (phase, sealed_plan.as_ref()) {
                    (TransactionParticipantPhase::Prepare, Some(plan))
                    | (TransactionParticipantPhase::Commit, Some(plan)) => plan.validate_shape()?,
                    (TransactionParticipantPhase::Abort, None) => {}
                    _ => {
                        return Err(
                            "transaction participant command has an invalid plan shape".to_string()
                        )
                    }
                }
                return Ok(());
            }
            Self::TransactionDecision { coordinator_id, .. }
            | Self::TransactionFinalize { coordinator_id, .. } => {
                if !opaque_scope_is_valid(coordinator_id) {
                    return Err("transaction coordinator is invalid".to_string());
                }
                return Ok(());
            }
            #[cfg(feature = "jobs")]
            Self::JobPublicationCommit {
                coordinator_id,
                sealed_plan,
            }
            | Self::JobPublicationFinalize {
                coordinator_id,
                sealed_receipt: sealed_plan,
            } => {
                if !opaque_scope_is_valid(coordinator_id) {
                    return Err("job publication coordinator is invalid".to_string());
                }
                sealed_plan.validate_shape()?;
                return Ok(());
            }
            _ => {}
        }
        if let Some(sealed) = self.sealed_method() {
            sealed.validate_shape()?;
        }
        Ok(())
    }

    /// Validate and authenticate a persisted native-history command without
    /// applying it. Snapshot install runs this over the complete history before
    /// replaying the first entry, so an invalid encrypted tail cannot leave a
    /// valid prefix applied.
    pub(crate) fn validate_replay_authentication(&self, server_secret: &str) -> Result<(), String> {
        self.validate_shape()?;
        match self {
            Self::TransactionParticipant { .. } => {
                self.open_transaction_plan(server_secret)?;
            }
            Self::TransactionDecision { .. } | Self::TransactionFinalize { .. } => {}
            #[cfg(feature = "jobs")]
            Self::JobPublicationCommit { .. } | Self::JobPublicationFinalize { .. } => {
                self.open_job_publication_payload(server_secret)?;
            }
            _ => {
                self.open_public_method(server_secret)?
                    .ok_or_else(|| "native history command is not replayable".to_string())?;
            }
        }
        Ok(())
    }

    pub(crate) fn transaction_participant(
        phase: TransactionParticipantPhase,
        coordinator_id: String,
        participant_id: u64,
        plan: Option<&[u8]>,
        server_secret: &str,
    ) -> Result<Self, String> {
        let sealed_plan = match plan {
            Some(bytes) => Some(SealedNativeMethod::seal_value(
                server_secret,
                &bytes.to_vec(),
            )?),
            None => None,
        };
        let command = Self::TransactionParticipant {
            phase,
            coordinator_id,
            participant_id,
            sealed_plan,
        };
        command.validate_shape()?;
        Ok(command)
    }

    pub(crate) fn open_transaction_plan(
        &self,
        server_secret: &str,
    ) -> Result<Option<Vec<u8>>, String> {
        match self {
            Self::TransactionParticipant {
                sealed_plan: Some(plan),
                ..
            } => plan.open_value(server_secret).map(Some),
            Self::TransactionParticipant {
                sealed_plan: None, ..
            } => Ok(None),
            _ => Err("command is not a transaction participant".to_string()),
        }
    }

    #[cfg(feature = "jobs")]
    pub(crate) fn job_publication_commit(
        coordinator_id: String,
        plan: &[u8],
        server_secret: &str,
    ) -> Result<Self, String> {
        let command = Self::JobPublicationCommit {
            coordinator_id,
            sealed_plan: SealedNativeMethod::seal_value(server_secret, &plan.to_vec())?,
        };
        command.validate_shape()?;
        Ok(command)
    }

    #[cfg(feature = "jobs")]
    pub(crate) fn job_publication_finalize(
        coordinator_id: String,
        receipt: &[u8],
        server_secret: &str,
    ) -> Result<Self, String> {
        let command = Self::JobPublicationFinalize {
            coordinator_id,
            sealed_receipt: SealedNativeMethod::seal_value(server_secret, &receipt.to_vec())?,
        };
        command.validate_shape()?;
        Ok(command)
    }

    #[cfg(feature = "jobs")]
    pub(crate) fn open_job_publication_payload(
        &self,
        server_secret: &str,
    ) -> Result<Vec<u8>, String> {
        match self {
            Self::JobPublicationCommit { sealed_plan, .. } => sealed_plan.open_value(server_secret),
            Self::JobPublicationFinalize { sealed_receipt, .. } => {
                sealed_receipt.open_value(server_secret)
            }
            _ => Err("command is not a job publication command".to_string()),
        }
    }
}

impl ReplicatedMutation {
    pub(crate) fn graph(method: Method, server_secret: &str) -> Result<Self, String> {
        Ok(Self::Graph {
            sealed_method: SealedNativeMethod::new(server_secret, &method)?,
        })
    }

    pub(crate) fn open_graph(&self, server_secret: &str) -> Result<Option<Method>, String> {
        match self {
            Self::Graph { sealed_method } => sealed_method.open(server_secret).map(Some),
            Self::Native { .. } => Ok(None),
        }
    }

    pub(crate) fn change_envelope(
        envelope: &crate::change_envelope::ChangeEnvelope,
        server_secret: &str,
    ) -> Result<Self, String> {
        Ok(Self::Native {
            command: NativeMutationCommand::ChangeEnvelope {
                sealed_envelope: SealedNativeMethod::seal_value(server_secret, envelope)?,
            },
        })
    }

    pub(crate) fn open_change_envelope(
        &self,
        server_secret: &str,
    ) -> Result<Option<crate::change_envelope::ChangeEnvelope>, String> {
        match self {
            Self::Native {
                command: NativeMutationCommand::ChangeEnvelope { sealed_envelope },
            } => sealed_envelope.open_value(server_secret).map(Some),
            _ => Ok(None),
        }
    }

    #[cfg(feature = "modality-serving")]
    pub(crate) fn served_modality(command: SanitizedModalityRaftCommand) -> Self {
        Self::Native {
            command: NativeMutationCommand::ServedModality { command },
        }
    }

    pub(crate) fn native_method(method: Method, server_secret: &str) -> Result<Self, Box<Method>> {
        NativeMutationCommand::from_public_method(method, server_secret)
            .map(|command| Self::Native { command })
    }
}

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

impl RaftMutationContext {
    /// Construct caller authority only from the already-verified, privacy-safe
    /// carrier facts at the dispatch boundary.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_verified_request(
        batch_id: String,
        request_id: u64,
        tenant_scope: &str,
        principal_fingerprint: String,
        identity_bootstrap: bool,
        placement_epoch: u64,
        fencing_token: Option<u64>,
        created_at_ms: u64,
    ) -> Result<Self, String> {
        let context = Self {
            batch_id,
            request_id,
            tenant_scope: tenant_scope.to_string(),
            principal_fingerprint,
            identity_bootstrap,
            placement_epoch,
            fencing_token,
            created_at_ms,
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
            tenant_scope: Self::internal_tenant_scope(),
            principal_fingerprint: crate::server::mutation_batch::opaque_coordinator_key(
                "principal:sha256",
                "epistemic-graph-raft-control-plane",
                "authority",
            ),
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
        self.mutation.validate()?;
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
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

#[cfg(test)]
mod raft_authority_contract_tests {
    use super::*;

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

    fn work_item_mutations() -> Vec<Method> {
        use crate::epistemic_operations::{
            ClaimWorkItemRequest, ClaimWorkItemRequestSchemaVersion, ResourceCapacity,
            ResourceHostUpdateRequest, ResourceHostUpdateRequestSchemaVersion,
            ResourceHostUpdateRequestTargetKind, ResourceRequirement, ResourceReservationRequest,
            ResourceReservationRequestSchemaVersion, ResourceReservationRequestTargetKind,
        };
        use crate::epistemic_operations_ext::{
            CasWorkItemMetadataRequest, CasWorkItemMetadataRequestSchemaVersion,
            WorkItemClaimCapabilityMintRequest, WorkItemClaimCapabilityRequestSchemaVersion,
        };

        let requirement = ResourceRequirement {
            cpu_weight: 2,
            memory_mib: 128,
            disk_mib: 256,
            process_slots: 1,
        };
        let reservation = ResourceReservationRequest {
            schema_version: ResourceReservationRequestSchemaVersion::V1,
            tenant_ref: "tenant-ref".to_string(),
            work_item_id: "work-item".to_string(),
            owner_id: "worker".to_string(),
            fence: "fence".to_string(),
            lease_epoch: 3,
            fencing_token: 7,
            attempt: 1,
            reservation_id: "reservation".to_string(),
            input_fingerprint: "input-fingerprint".to_string(),
            profile_name: "cpu-small".to_string(),
            profile_version: "1".to_string(),
            host_ref: "host".to_string(),
            requirement,
            target_kind: ResourceReservationRequestTargetKind::Local,
            target_alias: None,
            repository_id: "repository".to_string(),
            branch: "main".to_string(),
            concurrency_key: "tenant:repository".to_string(),
            concurrency_limit: Some(2),
            repository_exclusive: false,
            branch_exclusive: true,
            required_labels: vec!["linux".to_string()],
            anti_affinity: vec!["gpu".to_string()],
            fairness_group: "default".to_string(),
            fairness_cost: 4,
            disk_low_watermark_mib: Some(512),
            disk_high_watermark_mib: Some(1024),
            disk_policy_key: "default".to_string(),
            reserved_at_ms: 10,
            expires_at_ms: 20,
            idempotency_key: "reservation-idempotency".to_string(),
            now_ms: 10,
            expected_host_revision: Some(4),
            expected_lifecycle_revision: Some(8),
        };
        let host = ResourceHostUpdateRequest {
            schema_version: ResourceHostUpdateRequestSchemaVersion::V1,
            tenant_ref: "tenant-ref".to_string(),
            host_ref: "host".to_string(),
            revision: 5,
            capacity: ResourceCapacity {
                cpu_weight: 16,
                memory_mib: 4096,
                disk_mib: 8192,
                process_slots: 8,
            },
            observed: ResourceCapacity {
                cpu_weight: 12,
                memory_mib: 3072,
                disk_mib: 6144,
                process_slots: 6,
            },
            heartbeat_at_ms: 10,
            heartbeat_ttl_ms: 100,
            now_ms: 10,
            draining: false,
            quarantined: false,
            labels: vec!["linux".to_string()],
            target_kind: ResourceHostUpdateRequestTargetKind::Local,
            target_alias: None,
            disk_used_mib: 2048,
            disk_capacity_mib: 8192,
        };

        vec![
            Method::ClaimWorkItem {
                request: ClaimWorkItemRequest {
                    schema_version: ClaimWorkItemRequestSchemaVersion::V1,
                    tenant_ref: "tenant-ref".to_string(),
                    work_item_id: Some("work-item".to_string()),
                    queue_ref: Some("queue".to_string()),
                    resource_class: Some("cpu-small".to_string()),
                    fairness_group: Some("default".to_string()),
                    worker_ref: "worker".to_string(),
                    now_ms: 10,
                    lease_ms: 100,
                    max_tenant_in_flight: 2,
                },
            },
            Method::MintWorkItemClaimCapability {
                request: WorkItemClaimCapabilityMintRequest {
                    schema_version: WorkItemClaimCapabilityRequestSchemaVersion::V1,
                    work_item_id: "work-item".to_string(),
                },
            },
            Method::RenewWorkItemLease {
                tenant: "tenant-ref".to_string(),
                work_item_id: "work-item".to_string(),
                worker_id: "worker".to_string(),
                lease_epoch: 3,
                fencing_token: 7,
                now_ms: 20,
                lease_ms: 100,
            },
            Method::CommitWorkItemResult {
                tenant: "tenant-ref".to_string(),
                work_item_id: "work-item".to_string(),
                worker_id: "worker".to_string(),
                lease_epoch: 3,
                fencing_token: 7,
                idempotency_key: "result-idempotency".to_string(),
                outcome: "succeeded".to_string(),
                result_ref: Some("result-ref".to_string()),
                error_ref: None,
                retryable: false,
                now_ms: 30,
            },
            Method::CancelWorkItem {
                tenant: "tenant-ref".to_string(),
                work_item_id: "work-item".to_string(),
                idempotency_key: "cancel-idempotency".to_string(),
                reason_ref: Some("reason-ref".to_string()),
                now_ms: 40,
            },
            Method::DeferWorkItem {
                tenant: "tenant-ref".to_string(),
                work_item_id: "work-item".to_string(),
                worker_id: "worker".to_string(),
                lease_epoch: 3,
                fencing_token: 7,
                idempotency_key: "defer-idempotency".to_string(),
                next_retry_at_ms: 80,
                reason_ref: Some("barrier".to_string()),
                now_ms: 50,
            },
            Method::CasWorkItemMetadata {
                request: CasWorkItemMetadataRequest {
                    schema_version: CasWorkItemMetadataRequestSchemaVersion::V1,
                    tenant_ref: "tenant-ref".to_string(),
                    work_item_id: "work-item".to_string(),
                    expected_lease: None,
                    expected_status: vec!["leased".to_string(), "running".to_string()],
                    expected_checkpoint_id: None,
                    set_checkpoint_id: Some("checkpoint:1".to_string()),
                    expected_metadata_msgpack: None,
                    set_metadata_msgpack: None,
                    expected_prio_bucket: None,
                    set_prio_bucket: None,
                    now_ms: 60,
                },
            },
            Method::ReserveWorkItemResources {
                request: reservation.clone(),
            },
            Method::ReleaseWorkItemResources {
                request: reservation.clone(),
            },
            Method::ReclaimWorkItemResources {
                request: reservation,
            },
            Method::UpdateResourceHost { request: host },
        ]
    }

    #[test]
    fn work_item_mutations_round_trip_through_sealed_native_command() {
        for method in work_item_mutations() {
            let expected = rmp_serde::to_vec_named(&method).unwrap();
            let command =
                NativeMutationCommand::from_public_method(method.clone(), "cluster-work-item-key")
                    .expect("work-item mutation has a native consensus command");
            assert!(matches!(&command, NativeMutationCommand::WorkItem { .. }));
            assert_eq!(command.domain(), Some(NativeMutationDomain::WorkItem));

            let opened = command
                .open_public_method("cluster-work-item-key")
                .unwrap()
                .expect("sealed work-item method opens");
            assert_eq!(rmp_serde::to_vec_named(&opened).unwrap(), expected);

            let replicated = ReplicatedMutation::native_method(method, "cluster-work-item-key")
                .expect("work-item mutation enters ReplicatedMutation::Native");
            assert!(replicated
                .open_graph("cluster-work-item-key")
                .unwrap()
                .is_none());
        }
    }

    #[test]
    fn work_item_native_inventory_excludes_read_only_reservation_queries() {
        for method in [
            "ClaimWorkItem",
            "MintWorkItemClaimCapability",
            "RenewWorkItemLease",
            "CommitWorkItemResult",
            "CancelWorkItem",
            "DeferWorkItem",
            "CasWorkItemMetadata",
            "ReserveWorkItemResources",
            "ReleaseWorkItemResources",
            "ReclaimWorkItemResources",
            "UpdateResourceHost",
        ] {
            assert!(
                NATIVE_CONSENSUS_METHODS.contains(&method),
                "missing WorkItem native inventory entry: {method}"
            );
        }
        for query in ["QueryWorkItemReservation", "ResourceReservationStatus"] {
            assert!(
                !NATIVE_CONSENSUS_METHODS.contains(&query),
                "read-only reservation query must not enter consensus: {query}"
            );
        }
    }

    #[test]
    fn native_command_excludes_plaintext_environment_values() {
        let marker = "private-environment-marker";
        let command = NativeMutationCommand::from_public_method(
            Method::CreateGraph {
                graph_name: marker.to_string(),
                graph_type: crate::protocol::GraphType::Global,
            },
            "cluster-test-key",
        )
        .unwrap();
        let encoded = rmp_serde::to_vec_named(&command).unwrap();
        assert!(!encoded
            .windows(marker.len())
            .any(|window| window == marker.as_bytes()));
        let opened = command
            .open_public_method("cluster-test-key")
            .unwrap()
            .unwrap();
        assert!(matches!(
            opened,
            Method::CreateGraph { graph_name, .. } if graph_name == marker
        ));
        assert!(command.open_public_method("different-key").is_err());

        let graph = ReplicatedMutation::graph(
            Method::RemoveNode {
                node_id: marker.to_string(),
            },
            "cluster-test-key",
        )
        .unwrap();
        let encoded = rmp_serde::to_vec_named(&graph).unwrap();
        assert!(!encoded
            .windows(marker.len())
            .any(|window| window == marker.as_bytes()));
        assert!(matches!(
            graph.open_graph("cluster-test-key").unwrap(),
            Some(Method::RemoveNode { node_id }) if node_id == marker
        ));
        assert!(graph.open_graph("different-key").is_err());
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
    /// Exact result produced by a bounded engine-native state-machine command.
    #[serde(deserialize_with = "deserialize_required_option")]
    pub native_result: Option<crate::protocol::ResultPayload>,
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
        Ok(())
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
