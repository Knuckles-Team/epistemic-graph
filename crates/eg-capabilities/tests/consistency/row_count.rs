//! The expected size of the method-policy registry, shared by the consistency
//! and invariant test binaries.

/// 420 unconditional rows plus one row for each optional feature surface.
///
/// 403 -> 406: the three RF-ADR-008 agent-hierarchy rows beyond `AgentLibrary`
/// -- `AgentGraph`, `AgentComponent` and `AgentTemplate`.
/// 406 -> 407: RF-019's `SemanticIndex`, the S1-S6 tiered semantic ingestion
/// queue. Unconditional like the four agent layers: the wire contract is one
/// contract in every build, and the `ann-redb`/`query` gate lives on the
/// dispatch arm that serves it, not on whether the method exists.
/// 407 -> 408: `SqlSourceBatch`, typed SQL source rows admitted through the
/// native SQL-catalog owner.
/// 408 -> 418: the 2.27.x contract wave's ten declared surfaces --
/// `AgentAssemble`, `DecisionCommit` and `ConnectorPack` (storage), `Decide`
/// (query), `DecisionFit` and `DecisionEval` (coordination), `Solve`
/// (compute), `GraphSchema` and `GraphSchemaList` (reasoning) and
/// `MutationOutbox` (transactions). Unconditional for the same reason
/// `SemanticIndex` is: the wire contract is one contract in every build, and
/// the feature gate lives on the handler rather than on whether the method
/// exists.
/// 418 -> 419: RF-ADR-009's served `SourceIngest` raw-record authority.
/// 419 -> 420: governed source-system `WriteBack` authority.
/// 420 -> 421: typed, bounded live fleet `ListRegisteredServers` authority.
///
/// This is a tripwire against an unnoticed protocol edit, not a ratchet;
/// `scripts/method_policy_inventory.py`'s `EXPECTED_METHOD_POLICY_ROWS` is the
/// same count seen from the other side and the two must agree
/// (421 + 7 feature rows = 428). Keep the formula aligned with the cfg rows in
/// the domain row inventory so every supported feature combination checks the
/// same coverage invariant.
pub fn expected_method_policy_rows() -> usize {
    421 + usize::from(cfg!(feature = "jobs"))
        + usize::from(cfg!(feature = "statechart"))
        + usize::from(cfg!(feature = "modality-serving"))
        + usize::from(cfg!(feature = "knowledge-batch"))
        + usize::from(cfg!(feature = "quantum"))
        + usize::from(cfg!(feature = "viz"))
        + usize::from(cfg!(feature = "asr-native"))
}
