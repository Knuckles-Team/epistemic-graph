//! Public smoke invariants for the capability ledger.

use eg_capabilities::{
    gen_ledger, method_policy_entries, policy, protocol_policy_inventory, DurabilityDomain,
    MethodPolicy, PolicyAccess, TxnParticipation,
};
use eg_types::protocol::{CypherMode, Method};

#[test]
fn method_policy_registry_has_no_duplicates() {
    use std::collections::HashSet;
    let mut seen = HashSet::new();
    for (name, table_policy, _note) in method_policy_entries() {
        assert!(
            seen.insert(name),
            "duplicate method-policy declaration: {name}"
        );
        // We cannot construct a real `Method` value for every variant generically
        // (many carry required, non-Default fields), so this smoke test only checks
        // internal self-consistency of the domain-owned registry. The policy
        // lookup and its return-path invariants guard the served policy; the mirrored
        // classifier comparisons live in `tests/consistency.rs`.
        let _ = table_policy;
    }
    // The ledger currently contains 414 rows: 407 unconditional rows plus one
    // row for each of the seven feature-gated surfaces below. Keep this formula
    // aligned with the cfg rows in the domain row inventory so every supported
    // feature combination checks the same coverage invariant.
    //
    // 403 -> 406: the three RF-ADR-008 agent-hierarchy rows beyond
    // `AgentLibrary` -- `AgentGraph` and `AgentComponent` (landed 2026-09-10
    // without this formula being raised) and `AgentTemplate` (item C).
    // 406 -> 407: RF-019's `SemanticIndex`, the S1-S6 tiered semantic ingestion
    // queue. Unconditional, like the four agent layers: the `ann-redb`/`query`
    // gate lives on the dispatch arm that serves the method, not on whether the
    // method is in the contract.
    let expected = 407
        + usize::from(cfg!(feature = "jobs"))
        + usize::from(cfg!(feature = "statechart"))
        + usize::from(cfg!(feature = "modality-serving"))
        + usize::from(cfg!(feature = "knowledge-batch"))
        + usize::from(cfg!(feature = "quantum"))
        + usize::from(cfg!(feature = "viz"))
        + usize::from(cfg!(feature = "asr-native"));
    assert_eq!(
        seen.len(),
        expected,
        "expected exactly {expected} Method variants"
    );
}

#[test]
fn gen_ledger_renders_every_variant() {
    let md = gen_ledger();
    for (name, _, _) in method_policy_entries() {
        assert!(
            md.contains(&format!("`{name}`")),
            "gen_ledger() output is missing a row for {name}"
        );
    }
}

#[test]
fn is_durable_implies_a_non_none_domain() {
    for (name, p, _) in method_policy_entries() {
        if p.is_durable() {
            assert_ne!(
                p.durability_domain,
                DurabilityDomain::None,
                "{name}: is_durable() true but durability_domain is None"
            );
        }
    }
}

#[test]
fn every_mutation_has_an_explicit_state_domain() {
    for (name, p, _) in method_policy_entries() {
        assert!(
            !p.mutates || !matches!(p.durability_domain, DurabilityDomain::None),
            "{name}: a mutating Method must name its durable or volatile state domain"
        );
    }
}

#[test]
fn volatile_control_is_narrow_and_never_claims_durability() {
    const VOLATILE_METHODS: &[&str] = &["Shutdown"];
    for (name, p, _) in method_policy_entries() {
        let volatile = matches!(p.durability_domain, DurabilityDomain::VolatileControl);
        assert_eq!(
            volatile,
            VOLATILE_METHODS.contains(&name),
            "{name}: VolatileControl may only describe explicit process/session state"
        );
        if volatile {
            assert!(
                p.mutates,
                "{name}: volatile control must change session state"
            );
            assert!(!p.is_durable(), "{name}: volatile control is not durable");
            assert!(
                !p.audited,
                "{name}: volatile control must not claim a durable audit"
            );
            assert!(
                !p.emits_cdc,
                "{name}: volatile control must not emit data CDC"
            );
        }
    }
}

#[test]
fn generated_protocol_policy_inventory_covers_every_primitive() {
    let inventory = protocol_policy_inventory();
    assert_eq!(inventory.len(), method_policy_entries().count());
    for row in &inventory {
        assert!(
            !row.primitive.is_empty(),
            "{} has empty primitive",
            row.method
        );
        assert!(!row.verb.is_empty(), "{} has empty policy verb", row.method);
        if !row.policy.mutates {
            assert_eq!(
                row.access,
                PolicyAccess::Read,
                "{} is a read primitive without a read policy",
                row.method
            );
        }
    }

    let ts: Vec<_> = inventory
        .iter()
        .filter(|row| row.primitive == "timeseries")
        .collect();
    // TsAppend/TsRange/TsAsofJoin/TsWindow/TsGapFill (5) plus the retention-
    // reachability wiring's TsEvict/TsDeleteSeries/TsListSeries (3) = 8.
    assert_eq!(ts.len(), 8);
    assert_eq!(
        ts.iter()
            .filter(|row| row.access == PolicyAccess::Write)
            .count(),
        // TsAppend, plus TsEvict/TsDeleteSeries (content-idempotent unlike
        // TsAppend, but still series.redb WRITES -- see their MethodPolicy).
        3
    );
    assert_eq!(
        ts.iter()
            .filter(|row| row.access == PolicyAccess::Read)
            .count(),
        // TsRange/TsAsofJoin/TsWindow/TsGapFill, plus TsListSeries.
        5
    );
}

#[test]
fn runtime_conditional_policy_uses_query_mode_and_modality_operation() {
    let read = Method::CypherQuery {
        query: String::new(),
        mode: CypherMode::Read,
    };
    let write = Method::CypherQuery {
        query: String::new(),
        mode: CypherMode::Write,
    };
    assert_eq!(
        policy(&read),
        MethodPolicy {
            mutates: false,
            durability_domain: DurabilityDomain::None,
            authz_action: "query:cypher",
            idempotent: true,
            audited: false,
            emits_cdc: false,
            txn_participation: TxnParticipation::Snapshot,
        }
    );
    assert_eq!(
        policy(&write),
        MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::GraphRedb,
            authz_action: "query:cypher",
            idempotent: false,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        }
    );

    #[cfg(feature = "modality-serving")]
    {
        use eg_types::modality::{ServedModalityKind, ServedModalityOp};

        let query = Method::ServedModality {
            op: ServedModalityOp::Query {
                modality: ServedModalityKind::Document,
                segment_kind: None,
                after_occurrence_id: None,
                limit: 1,
                include_cold: false,
            },
        };
        let cold = Method::ServedModality {
            op: ServedModalityOp::MoveToCold {
                modality: ServedModalityKind::Document,
                occurrence_id: String::new(),
            },
        };
        assert_eq!(
            policy(&query),
            MethodPolicy {
                mutates: false,
                durability_domain: DurabilityDomain::None,
                authz_action: "modality:read",
                idempotent: true,
                audited: false,
                emits_cdc: false,
                txn_participation: TxnParticipation::Snapshot,
            }
        );
        assert_eq!(
            policy(&cold),
            MethodPolicy {
                mutates: true,
                durability_domain: DurabilityDomain::GraphRedb,
                authz_action: "modality:write",
                idempotent: false,
                audited: true,
                emits_cdc: true,
                txn_participation: TxnParticipation::Atomic,
            }
        );
    }
}
