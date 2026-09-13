//! Declared results of the `security` contract domain.

use serde::{Deserialize, Serialize};

use crate::acl::{AgentIdentity, Grant, Role};

/// Outcome of walking a graph's tamper-evident hash-chained audit log
/// (CONCEPT:EG-KG.sharding.row-level-security, `Method::AuditVerify`). `ok` is true when every entry's stored
/// hash matches the recomputed chain hash AND the sequence is contiguous from 0;
/// `first_broken_seq` carries the position of the first detected break.
#[cfg(feature = "security")]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AuditReport {
    pub graph: String,
    pub ok: bool,
    pub entries: u64,
    pub first_broken_seq: Option<u64>,
    pub detail: String,
}

/// Which side of its parent a Merkle audit-path sibling sits on (provenance
/// anchoring, CONCEPT:EG-KG.sharding.row-level-security). The verifier folds the running hash with each
/// step's sibling on the side named here — RFC 6962 §2.1.1 Merkle audit path
/// semantics.
#[cfg(feature = "security")]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum MerkleSide {
    Left,
    Right,
}

/// One Merkle audit-path step, wire-encoded: a hex-encoded sibling subtree hash
/// plus which side it sits on. See [`MerkleInclusionReport`].
#[cfg(feature = "security")]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct MerkleProofStep {
    pub sibling_sha256: String,
    pub side: MerkleSide,
}

/// Result of `Method::AuditProveInclusion` (provenance anchoring, CONCEPT:EG-KG.sharding.row-level-security): a
/// Merkle inclusion proof for one node against a prior provenance anchor,
/// ALREADY VERIFIED server-side. `verified` is the tamper signal: `false`
/// whenever the node's CURRENT durable content does not re-hash to the leaf
/// folded into `anchored_root_sha256` at anchor time — including when the node
/// was altered by an otherwise-ordinary later write, not just raw byte-level
/// tampering. `included == false` only means `node_id` was never part of that
/// anchor's window (a different anchor, a non-provenance node, or a node created
/// after this anchor ran) — not itself evidence of tampering.
#[cfg(feature = "security")]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct MerkleInclusionReport {
    pub graph: String,
    pub node_id: String,
    pub anchor_seq: u64,
    pub window_size: usize,
    pub included: bool,
    pub verified: bool,
    pub anchored_root_sha256: String,
    pub computed_root_sha256: String,
    pub proof: Vec<MerkleProofStep>,
    pub detail: String,
}

/// Materialized result of a `Method::GetLedger` call (BUG A1, 2026-08-12: `GetLedger`
/// returned an empty list even after real mutations had committed). The prior wire
/// shape was a bare `Vec<String>`, which made "the ledger is genuinely empty" and
/// "the ledger could not be read for this request's scope" indistinguishable — both
/// serialized to `[]`, so a caller (e.g. `agent_utilities.workflows.epistemic_sync`'s
/// `flush_ledger_to_backend`, a real production sync path) could not tell "nothing to
/// sync" from "I silently read nothing". Root cause: the terminal handler
/// unconditionally reads through the row-level-security PROJECTED core (built by
/// `GraphReadAuthority::project_core` / `build_projection`, which constructs its
/// detached copy via `add_node_no_ledger`/`add_edge_no_ledger` and therefore never
/// carries a ledger — see that function's own doc), even though `GetLedger` is
/// authorized by its own dedicated `ledger:read` RBAC action (`eg_capabilities`)
/// enforced upstream in `dispatch.rs` before any handler runs at all — the row
/// projection was a redundant SECOND gate that, instead of narrowing visibility,
/// destroyed the data outright. Because `security` (and therefore
/// `GraphReadAuthority::is_active()`) is compiled into the default `full` build,
/// this fired on every request in production, unconditionally.
///
/// `populated: true` means `entries` reflects the graph's REAL, authoritative
/// ledger (however many entries — legitimately empty when nothing has mutated
/// since the last flush/clear: that is a genuine `[]`, not a lie). `populated:
/// false` means the ledger could not be read for this request's scope; `entries`
/// is always empty in that case and callers MUST NOT treat it as "nothing to
/// sync". Any future call site that cannot certify it is reading the
/// authoritative (non-projected) core must answer `not_populated_for_scope()`
/// explicitly rather than silently returning a `populated: true` empty list —
/// that explicit branch is what stops this exact regression from shipping silent
/// a second time.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct LedgerReadResult {
    pub populated: bool,
    pub entries: Vec<String>,
    /// BUG A1 follow-up (2026-08-12): the mutation ledger is a purely
    /// IN-MEMORY, capped ring (100k entries, drop-oldest-half — see
    /// `GraphTxn::push_ledger` in `eg_core::graph`) — NOT a durable change
    /// log. A cold-tenant idle
    /// offload/hibernate cycle, `MAX_RESIDENT_GRAPHS` eviction + lazy
    /// rehydrate, a process restart, or simply exceeding the cap can all
    /// empty or truncate `entries` while the underlying mutations remain
    /// fully durable in redb — a typed `populated: true` alone does not
    /// prove `entries` is COMPLETE. `watermark`
    /// (`GraphCore::ledger_watermark`) is the 0-based sequence of the
    /// OLDEST entry `entries` can vouch for: `0` on a graph that has never
    /// dropped anything (from THIS in-memory instance's point of view); a
    /// caller that tracks watermark across reads can detect it advancing
    /// (or a previously-nonzero watermark resetting to a lower value after
    /// an eviction/restart) as proof that history was silently dropped,
    /// rather than inferring completeness from `entries` merely being
    /// nonzero.
    pub watermark: u64,
}

impl LedgerReadResult {
    /// The ledger was read from the authoritative core; `entries` is its real
    /// content (possibly, legitimately, empty), and `watermark` is the
    /// sequence of the oldest entry it can vouch for (see the field's doc).
    pub fn populated(entries: Vec<String>, watermark: u64) -> Self {
        Self {
            populated: true,
            entries,
            watermark,
        }
    }

    /// The ledger could not be read for this request's scope. `entries` is
    /// always empty here and must never be read as "nothing to sync";
    /// `watermark` is meaningless (`0`) in this case.
    pub fn not_populated_for_scope() -> Self {
        Self {
            populated: false,
            entries: Vec::new(),
            watermark: 0,
        }
    }
}

/// `RbacAdmin` op `RemoveGrant`: whether the grant was present.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RbacGrantRemoval {
    pub removed: bool,
}

/// `RbacAdmin` op `List`: the whole current RBAC policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RbacPolicyListing {
    pub roles: Vec<Role>,
    pub grants: Vec<Grant>,
}

method_results! {
    visit_security;
    GetLedger(GetLedger) => Json<LedgerReadResult>;
    #[cfg(feature = "security")]
    AuditVerify(AuditVerify) => Raw<AuditReport>;
    #[cfg(feature = "security")]
    AuditProveInclusion(AuditProveInclusion) => Raw<MerkleInclusionReport>;
    RegisterIdentity(RegisterIdentity) => Text<String>;
    RbacAddRole(RbacAdmin / "AddRole") => Text<String>;
    RbacRemoveRole(RbacAdmin / "RemoveRole") => Text<String>;
    RbacAddGrant(RbacAdmin / "AddGrant") => Text<String>;
    RbacRemoveGrant(RbacAdmin / "RemoveGrant") => Json<RbacGrantRemoval>;
    RbacList(RbacAdmin / "List") => Json<RbacPolicyListing>;
    // `null` when no identity is registered for the agent; an identity holding no
    // roles is a present identity with an empty `roles` list.
    GetIdentity(GetIdentity) => Json<Option<AgentIdentity>>;
}
