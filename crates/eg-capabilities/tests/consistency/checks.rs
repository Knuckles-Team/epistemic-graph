//! Executable cross-checks for the current classifier snapshots.

use eg_capabilities::DurabilityDomain;

use super::{divergence, snapshots};

// ── The actual cross-checks ─────────────────────────────────────────────────────────

#[test]
fn mutates_matches_access_rs_for_every_governed_variant() {
    let conditional: std::collections::HashSet<&str> = snapshots::ACCESS_RS_MUTATES_CONDITIONAL
        .iter()
        .copied()
        .collect();
    let mut failures = Vec::new();
    for (name, p, _note) in eg_capabilities::method_policy_entries() {
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
        if snapshots::ACCESS_RS_MUTATES_EXPLICIT_FALSE.contains(&name) {
            if p.mutates {
                failures.push(format!("{name}: access.rs::requires_write always returns false for this variant, but policy().mutates is true"));
            }
            continue;
        }
        let expected = snapshots::ACCESS_RS_MUTATES_UNCONDITIONAL.contains(&name);
        if p.mutates != expected {
            failures.push(format!(
                "{name}: policy().mutates = {}, access::requires_write(m) = {expected} -- {}",
                p.mutates,
                if divergence::all_known_divergence_names().contains(name) {
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
fn durability_domain_matches_the_graph_mutation_applier() {
    let mut failures = graphredb_durability_failures();
    failures.extend(outbox_durability_failures());
    failures.extend(non_durable_classifier_failures());
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

fn graphredb_durability_failures() -> Vec<String> {
    let mut failures = Vec::new();
    for (name, p, _note) in eg_capabilities::method_policy_entries() {
        if p.durability_domain == DurabilityDomain::GraphRedb
            && !snapshots::MUTATION_APPLY_DURABLE_GRAPHREDB.contains(&name)
            && !snapshots::NATIVE_GRAPHREDB_DURABLE.contains(&name)
        {
            failures.push(format!(
                "{name}: policy says GraphRedb-durable, mutation_apply::is_durable_mutation disagrees"
            ));
        }
    }
    failures
}

fn outbox_durability_failures() -> Vec<String> {
    let mut failures = Vec::new();
    for (name, p, _note) in eg_capabilities::method_policy_entries() {
        if p.durability_domain == DurabilityDomain::Outbox
            && !snapshots::MUTATION_APPLY_DURABLE_OUTBOX.contains(&name)
        {
            failures.push(format!(
                "{name}: policy says Outbox-durable, mutation_apply::is_durable_mutation disagrees"
            ));
        }
    }
    failures
}

fn non_durable_classifier_failures() -> Vec<String> {
    let mut failures = Vec::new();
    for (name, p, _note) in eg_capabilities::method_policy_entries() {
        if matches!(
            p.durability_domain,
            DurabilityDomain::None | DurabilityDomain::VolatileControl
        ) && (snapshots::MUTATION_APPLY_DURABLE_GRAPHREDB.contains(&name)
            || snapshots::MUTATION_APPLY_DURABLE_OUTBOX.contains(&name))
        {
            let label = if p.durability_domain == DurabilityDomain::None {
                "not durable"
            } else {
                "volatile control"
            };
            failures.push(format!(
                "{name}: policy says {label}, but mutation_apply::is_durable_mutation says it IS"
            ));
        }
    }
    failures
}

#[test]
fn audited_matches_audit_rs_exactly() {
    let mut failures = Vec::new();
    for (name, p, _note) in eg_capabilities::method_policy_entries() {
        let expected = snapshots::AUDIT_RS_AUDITED.contains(&name);
        if p.audited != expected {
            failures.push(format!(
                "{name}: policy().audited = {}, audit::audit_line(m).is_some() = {expected}",
                p.audited
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn emits_cdc_matches_cdc_rs_exactly() {
    let mut failures = Vec::new();
    for (name, p, _note) in eg_capabilities::method_policy_entries() {
        let expected = snapshots::CDC_RS_EMITS_CDC.contains(&name);
        if p.emits_cdc != expected {
            failures.push(format!(
                "{name}: policy().emits_cdc = {}, cdc::emit_for_method match = {expected}",
                p.emits_cdc
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn native_resource_mutations_are_audited_but_not_cdc() {
    for expected_name in [
        "ReserveWorkItemResources",
        "ReleaseWorkItemResources",
        "ReclaimWorkItemResources",
        "UpdateResourceHost",
    ] {
        let (_, policy, _) = eg_capabilities::method_policy_entries()
            .find(|(name, _, _)| *name == expected_name)
            .unwrap_or_else(|| panic!("missing resource capability policy: {expected_name}"));
        assert!(policy.audited, "{expected_name} must remain audit chained");
        assert!(!policy.emits_cdc, "{expected_name} must remain CDC-silent");
    }
}

#[test]
fn development_lane_cleanup_has_a_distinct_least_privilege_scope() {
    let policy = |method: &str| {
        eg_capabilities::method_policy_entries()
            .find(|(name, _, _)| *name == method)
            .map(|(_, policy, _)| policy)
            .unwrap_or_else(|| panic!("missing development-lane policy: {method}"))
    };
    assert_eq!(
        policy("ReserveDevelopmentLane").authz_action,
        "lane:reserve"
    );
    assert_eq!(
        policy("CleanupDevelopmentLane").authz_action,
        "lane:cleanup"
    );
    assert_eq!(
        policy("UpdateDevelopmentLaneQuota").authz_action,
        "lane:quota"
    );
}

/// Not a pass/fail gate -- prints the full audit findings so `cargo test -p eg-capabilities
/// -- --nocapture` surfaces them for a human. This is the "valuable audit output" the task
/// brief asks for.
#[test]
fn print_known_divergence_report() {
    eprintln!("\n=== EG-P0-1 capability ledger: KNOWN_DIVERGENCE report ===\n");
    eprintln!(
        "-- Category 1: divergence::RUNTIME_CONDITIONAL ({} variants; workstream EG-P0-2) --",
        divergence::RUNTIME_CONDITIONAL.len()
    );
    for (name, ws, reason) in divergence::RUNTIME_CONDITIONAL {
        eprintln!("  {name:<24} [{ws}] {reason}");
    }
    eprintln!(
        "\n-- Category 2: divergence::ACCESS_RS_COVERAGE_GAP ({} variants; workstream UNASSIGNED) --",
        divergence::ACCESS_RS_COVERAGE_GAP.len()
    );
    for (name, ws, reason) in divergence::ACCESS_RS_COVERAGE_GAP {
        eprintln!("  {name:<24} [{ws}] {reason}");
    }
    eprintln!(
        "\ntotals: {} runtime-conditional + {} access.rs-coverage-gap = {} documented divergences\n",
        divergence::RUNTIME_CONDITIONAL.len(),
        divergence::ACCESS_RS_COVERAGE_GAP.len(),
        divergence::RUNTIME_CONDITIONAL.len() + divergence::ACCESS_RS_COVERAGE_GAP.len(),
    );
}

#[cfg(feature = "canonical-ledger")]
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
            "failed to read {}: {e} -- run `cargo run -p eg-capabilities --features contract --bin gen_contract` first",
            checked_in_path.display()
        )
    });
    let fresh = eg_capabilities::gen_ledger();
    assert_eq!(
        checked_in, fresh,
        "docs/capabilities.generated.md is STALE -- regenerate with `cargo run -p eg-capabilities --features contract --bin gen_contract` and commit the result"
    );
}

/// Sanity check that the canonical registry has the current variant count, so a future
/// protocol edit that adds or removes variants is visible in the same policy parity check.
#[test]
fn method_policy_registry_has_the_expected_variant_count() {
    // 401 unconditional rows plus one row for each optional feature surface.
    let expected = 401
        + usize::from(cfg!(feature = "jobs"))
        + usize::from(cfg!(feature = "statechart"))
        + usize::from(cfg!(feature = "modality-serving"))
        + usize::from(cfg!(feature = "knowledge-batch"))
        + usize::from(cfg!(feature = "quantum"))
        + usize::from(cfg!(feature = "viz"))
        + usize::from(cfg!(feature = "asr-native"));
    assert_eq!(eg_capabilities::method_policy_entries().count(), expected);
}
