//! Public smoke invariants for the capability ledger.

use eg_capabilities::{
    gen_ledger, method_policy_entries, policy, protocol_policy_inventory, DurabilityDomain,
    MethodPolicy, PolicyAccess, TxnParticipation,
};
use eg_types::protocol::{CypherMode, Method};

#[path = "consistency/row_count.rs"]
mod row_count;

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
    let expected = row_count::expected_method_policy_rows();
    assert_eq!(
        seen.len(),
        expected,
        "expected exactly {expected} Method variants"
    );
}

#[test]
fn sql_source_batch_policy_is_native_and_audited() {
    let (_, policy, note) = method_policy_entries()
        .find(|(name, _, _)| *name == "SqlSourceBatch")
        .expect("missing SQL source batch policy");
    assert_eq!(
        policy,
        MethodPolicy {
            mutates: true,
            durability_domain: DurabilityDomain::ControlRedb,
            authz_action: "query:sql",
            idempotent: true,
            audited: true,
            emits_cdc: false,
            txn_participation: TxnParticipation::Atomic,
        }
    );
    assert!(note.contains("native SQL-catalog MutationBatch"));
    assert!(note.contains("refused in clustered mode"));
    assert!(!note.contains("runtime-conditional"));
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

// ── 2.27.x contract wave ───────────────────────────────────────────────────

/// The static registry row every contract-wave method declares.
///
/// Table-driven rather than ten near-identical tests: ten copies of the same
/// three assertions are ten structural clones, and a table makes the ROW the
/// reviewable thing, which is what actually differs between them.
#[test]
fn contract_wave_rows_declare_their_static_policy() {
    let flags = |mutates, domain, action, txn| MethodPolicy {
        mutates,
        durability_domain: domain,
        authz_action: action,
        idempotent: true,
        audited: false,
        emits_cdc: false,
        txn_participation: txn,
    };
    let expected: [(&str, MethodPolicy, &str); 10] = [
        (
            "AgentAssemble",
            flags(
                false,
                DurabilityDomain::None,
                "agent:assemble-read",
                TxnParticipation::Snapshot,
            ),
            "commits nothing",
        ),
        (
            "DecisionCommit",
            flags(
                true,
                DurabilityDomain::ControlRedb,
                "agent:decision-write",
                TxnParticipation::Atomic,
            ),
            "local-only authority",
        ),
        (
            "Decide",
            flags(
                false,
                DurabilityDomain::None,
                "query:decide",
                TxnParticipation::Snapshot,
            ),
            "Evaluate-only",
        ),
        (
            "DecisionFit",
            flags(
                true,
                DurabilityDomain::JobsRedb,
                "admin:decision-fit",
                TxnParticipation::Atomic,
            ),
            "runtime-conditional",
        ),
        (
            "DecisionEval",
            flags(
                true,
                DurabilityDomain::JobsRedb,
                "admin:decision-eval",
                TxnParticipation::Atomic,
            ),
            "runtime-conditional",
        ),
        (
            "Solve",
            flags(
                false,
                DurabilityDomain::None,
                "compute:solve",
                TxnParticipation::None,
            ),
            "pure compute",
        ),
        (
            "ConnectorPack",
            flags(
                true,
                DurabilityDomain::ControlRedb,
                "agent:pack-control",
                TxnParticipation::Atomic,
            ),
            "runtime-conditional",
        ),
        (
            "GraphSchema",
            MethodPolicy {
                audited: true,
                emits_cdc: true,
                ..flags(
                    true,
                    DurabilityDomain::GraphRedb,
                    "security:admin",
                    TxnParticipation::Atomic,
                )
            },
            "Gateway-routed",
        ),
        (
            "GraphSchemaList",
            flags(
                false,
                DurabilityDomain::None,
                "security:admin",
                TxnParticipation::Snapshot,
            ),
            "separate method",
        ),
        (
            "MutationOutbox",
            flags(
                true,
                DurabilityDomain::ControlRedb,
                "admin:outbox",
                TxnParticipation::Saga,
            ),
            "runtime-conditional",
        ),
    ];
    for (name, policy, note_fragment) in expected {
        let (_, declared, note) = method_policy_entries()
            .find(|(candidate, _, _)| *candidate == name)
            .unwrap_or_else(|| panic!("the contract wave declares no {name} row"));
        assert_eq!(declared, policy, "{name} declares a different policy");
        assert!(
            note.contains(note_fragment),
            "{name}'s note must say '{note_fragment}', got {note}"
        );
    }
}

/// Every contract-wave method and op is `Internal` with no consumer until its
/// handler lands, which is the ONLY shape the reachability gate accepts for a
/// refusal-only dispatch arm.
#[test]
fn contract_wave_rows_are_internal_with_no_consumer() {
    let wave = [
        "AgentAssemble",
        "DecisionCommit",
        "Decide",
        "DecisionFit",
        "DecisionEval",
        "Solve",
        "ConnectorPack",
        "GraphSchema",
        "GraphSchemaList",
        "MutationOutbox",
    ];
    let mut seen = 0;
    for descriptor in eg_capabilities::method_descriptors() {
        let id = descriptor.id.as_str();
        if !wave.contains(&id) {
            continue;
        }
        seen += 1;
        assert_eq!(
            descriptor.stability,
            eg_capabilities::Stability::Internal,
            "{id} must stay Internal until its handler lands"
        );
        assert!(
            descriptor.consumer_profiles.is_empty(),
            "{id} must declare no consumer until its handler lands"
        );
    }
    assert_eq!(seen, wave.len(), "a contract-wave method has no descriptor");
}

/// The runtime policy of every contract-wave OP follows its own
/// `is_mutation()` and `authz_action()`, so the ledger and
/// `server::access::requires_write` cannot disagree about an operation.
#[test]
fn contract_wave_op_policies_follow_their_ops() {
    use eg_types::test_support::contract_wave::contract_wave_samples;

    let reads = [
        "ConnectorPack.status",
        "DecisionFit.status",
        "DecisionEval.status",
        "MutationOutbox.status",
        "MutationOutbox.dead_letters",
        "AgentComponent.content",
        "AgentAssemble",
        "Decide",
        "Solve",
        "GraphSchemaList",
    ];
    for (label, method) in contract_wave_samples() {
        let resolved = policy(&method);
        assert_eq!(
            resolved.mutates,
            !reads.contains(&label),
            "{label} is classified on the wrong side of the read/write split"
        );
        assert_eq!(
            resolved.mutates,
            !matches!(resolved.durability_domain, DurabilityDomain::None),
            "{label} must name its durable domain exactly when it mutates"
        );
    }
}

/// A mass-withdrawal import needs the administrative grant, not the
/// connector's ordinary pack-control one.
#[test]
fn a_mass_withdrawal_import_needs_the_admin_action() {
    use eg_types::test_support::contract_wave::pack;

    let ordinary = pack::ops()
        .into_iter()
        .find(|(label, _)| *label == "ConnectorPack.import")
        .map(|(_, op)| op)
        .expect("the samples carry an ordinary import");
    assert_eq!(ordinary.authz_action(), "agent:pack-control");
    assert_eq!(
        pack::mass_withdrawal_import().authz_action(),
        "admin:connector-pack"
    );
}

/// Publishing one of the four statistical catalog kinds is administrative;
/// publishing an ordinary component is not.
#[test]
fn decide_governance_kinds_publish_under_admin_actions() {
    use eg_types::agent_component::AgentComponentKind;

    let expected = [
        (AgentComponentKind::DecisionPolicy, "admin:decision-policy"),
        (AgentComponentKind::DecisionHead, "admin:decision-head"),
        (AgentComponentKind::FeatureSchema, "admin:decision-catalog"),
        (AgentComponentKind::Rubric, "admin:decision-catalog"),
        (AgentComponentKind::NlTemplate, "admin:decision-catalog"),
        (AgentComponentKind::Tool, "agent:component-write"),
        (AgentComponentKind::ModelProfile, "agent:component-write"),
    ];
    for (kind, action) in expected {
        assert_eq!(
            kind.publish_authz_action(),
            action,
            "{} publishes under the wrong action",
            kind.as_str()
        );
    }
}
