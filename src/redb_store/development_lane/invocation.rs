use super::links::text;
use super::validation::decision_name;
use super::{MAX_INVOCATIONS_PER_TENANT, MAX_INVOCATION_REPAIR_SCAN};
use eg_storage::ScopedOwnerTableMut;

/// Invocation replay is a bounded acknowledgement-loss cache, not an event
/// log.  Lexical retention is deliberately deterministic because the native
/// redb transaction has no server-side wall clock in its replay key.  The
/// current key is always retained for the duration of the commit so an
/// uncertain acknowledgement can replay the exact result bytes.
///
/// Every replay key stored for exactly this graph/tenant prefix, bound-checked
/// and capped by `MAX_INVOCATION_REPAIR_SCAN`.
///
/// The walk is the scope's own rows, so it starts at the graph rather than at
/// the tenant: a scope-bounded table has no cursor-positioned range for a key
/// whose non-leading components are `&str`.  One tenant's rows are still
/// contiguous, so it skips the tenants below and stops at the first above.
pub(super) fn collect_invocation_keys(
    invocations: &ScopedOwnerTableMut<(&str, &str, &str), &[u8]>,
    tenant: &str,
) -> Result<Vec<String>, String> {
    let mut keys = Vec::new();
    for row in invocations.scope_rows()? {
        let (key, _) = row?;
        let (_, row_tenant, invocation_key) = key.value();
        if row_tenant < tenant {
            continue;
        }
        if row_tenant > tenant {
            break;
        }
        text(invocation_key, "stored lane invocation key")
            .map_err(|decision| format!("stored lane invocation: {}", decision_name(decision)))?;
        keys.push(invocation_key.to_string());
        if keys.len() > MAX_INVOCATION_REPAIR_SCAN {
            return Err("lane invocation table exceeds bounded repair capacity".to_string());
        }
    }
    Ok(keys)
}

/// Redb orders this key range lexically.  Retain the newest lexical keys as
/// the deterministic bounded history, then force the just-written key into the
/// retained set even when it sorts below that window.
pub(super) fn retained_invocation_keys(
    keys: &[String],
    current_key: &str,
) -> Result<std::collections::BTreeSet<String>, String> {
    let mut retained: std::collections::BTreeSet<String> = keys
        .iter()
        .rev()
        .take(MAX_INVOCATIONS_PER_TENANT)
        .cloned()
        .collect();
    retained.insert(current_key.to_string());
    while retained.len() > MAX_INVOCATIONS_PER_TENANT {
        let victim = retained
            .iter()
            .find(|key| key.as_str() != current_key)
            .cloned()
            .ok_or_else(|| "lane invocation retention cannot evict current key".to_string())?;
        retained.remove(&victim);
    }
    Ok(retained)
}

pub(super) fn prune_invocations(
    invocations: &mut ScopedOwnerTableMut<(&str, &str, &str), &[u8]>,
    graph: &str,
    tenant: &str,
    current_key: &str,
) -> Result<(), String> {
    let keys = collect_invocation_keys(invocations, tenant)?;
    if keys.len() <= MAX_INVOCATIONS_PER_TENANT {
        return Ok(());
    }
    if !keys.iter().any(|key| key == current_key) {
        return Err("lane invocation repair could not find current key".to_string());
    }
    // Remove every key outside the retained set from this exact graph/tenant
    // prefix, in the same transaction.
    let retained = retained_invocation_keys(&keys, current_key)?;
    for key in keys {
        if !retained.contains(&key) {
            invocations.remove((graph, tenant, key.as_str()))?;
        }
    }
    Ok(())
}
