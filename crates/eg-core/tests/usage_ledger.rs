//! Integration tests for the per-tenant usage ledger (EG-DECISION-ENGINE-R801
//! family). The ledger's reads are scoped to the verified tenant: a reader
//! sees only its own tenant's records, and a caller outside the tenant sees
//! an empty result (EG-DECISION-ENGINE-R801.3).

use eg_core::usage_ledger::UsageLedger;
use eg_types::usage::{UsageOutcome, UsageRecord};

fn record(tenant: &str, run: &str) -> UsageRecord {
    UsageRecord {
        tenant: tenant.into(),
        principal: "svc-usage".into(),
        run_id: run.into(),
        model_key: "gpt-x".into(),
        tokens_in: 10,
        tokens_out: 20,
        tokens_cached: 5,
        latency_ms: 100,
        outcome: UsageOutcome::Ok,
        content_digest: "sha256:0000".into(),
    }
}

// spec: EG-DECISION-ENGINE-R801.3
#[test]
fn reader_sees_only_own_tenant() {
    let mut ledger = UsageLedger::new();
    ledger.append(record("tenant-a", "run-1"));
    ledger.append(record("tenant-a", "run-2"));
    ledger.append(record("tenant-b", "run-3"));

    // The reader sees only its own tenant, in append order.
    let own = ledger.read_for_tenant("tenant-a");
    assert_eq!(own.len(), 2);
    assert!(own.iter().all(|r| r.tenant == "tenant-a"));
    assert_eq!(
        own.iter().map(|r| r.run_id.as_str()).collect::<Vec<_>>(),
        vec!["run-1", "run-2"]
    );

    // A caller outside the tenant sees an empty result.
    assert!(ledger.read_for_tenant("tenant-c").is_empty());

    // The other tenant's read never surfaces tenant-a's rows.
    let other = ledger.read_for_tenant("tenant-b");
    assert_eq!(other.len(), 1);
    assert_eq!(other[0].tenant, "tenant-b");
}
