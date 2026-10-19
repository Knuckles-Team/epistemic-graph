//! Per-tenant usage ledger (EG-DECISION-ENGINE-R801 family).
//!
//! This module owns the tenant-scoped READ half of the ledger: a reader sees
//! only the records of its own verified tenant, and a caller outside that
//! tenant sees an empty result, never another tenant's rows
//! (EG-DECISION-ENGINE-R801.3). `append` is the service-only write half
//! (`usage:write`); durable MutationBatch commit, cost derivation from the
//! price catalog, alias normalization and rollups are the remaining
//! R801.2/R801.4/R801.5 behaviors that land beside this one.

use std::collections::BTreeMap;

use eg_types::usage::UsageRecord;

/// The per-tenant usage ledger: model-call records keyed by owning tenant.
#[derive(Debug, Default)]
pub struct UsageLedger {
    by_tenant: BTreeMap<String, Vec<UsageRecord>>,
}

impl UsageLedger {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append one record under its owning tenant (the service-only write,
    /// `usage:write`). The record's tenant is its scope; a caller's verified
    /// tenant is what the served surface passes here, never a free parameter.
    pub fn append(&mut self, record: UsageRecord) {
        self.by_tenant
            .entry(record.tenant.clone())
            .or_default()
            .push(record);
    }

    /// Tenant-scoped read for the verified tenant: only that tenant's records,
    /// in append order. A caller outside the tenant sees an empty result.
    pub fn read_for_tenant(&self, tenant: &str) -> Vec<UsageRecord> {
        self.by_tenant
            .get(tenant)
            .cloned()
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use eg_types::usage::{UsageOutcome, UsageRecord};

    use super::UsageLedger;

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
    fn read_is_scoped_to_the_verified_tenant() {
        let mut ledger = UsageLedger::new();
        ledger.append(record("tenant-a", "run-1"));
        ledger.append(record("tenant-a", "run-2"));
        ledger.append(record("tenant-b", "run-3"));

        let own = ledger.read_for_tenant("tenant-a");
        assert_eq!(own.len(), 2);
        assert!(own.iter().all(|r| r.tenant == "tenant-a"));

        // A caller outside the tenant sees an empty result.
        assert!(ledger.read_for_tenant("tenant-c").is_empty());
        // The other tenant's read never surfaces tenant-a's rows.
        let b = ledger.read_for_tenant("tenant-b");
        assert_eq!(b.len(), 1);
        assert_eq!(b[0].tenant, "tenant-b");
    }
}
