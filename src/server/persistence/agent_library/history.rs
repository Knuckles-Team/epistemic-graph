//! Bounded Agent Library history reads and chain validation.

use super::*;

pub(super) fn read_history(
    read: &ScopedRead<'_, eg_storage::AgentLibraryOwner>,
    tenant_id: &str,
    agent_id: &str,
) -> Result<(Option<u64>, Vec<AgentLibraryEntry>), String> {
    let head = read_history_head(read, tenant_id, agent_id)?;
    let entries = read_history_entries(read, tenant_id, agent_id)?;
    validate_history_chain(head, &entries)?;
    Ok((head, entries))
}

fn read_history_head(
    read: &ScopedRead<'_, eg_storage::AgentLibraryOwner>,
    tenant_id: &str,
    agent_id: &str,
) -> Result<Option<u64>, String> {
    read.open_owner_table(eg_storage::AGENT_LIBRARY_HEADS)?
        .get((tenant_id, agent_id))
        .map_err(|error| error.to_string())
        .map(|value| value.map(|value| value.value()))
}

fn read_history_entries(
    read: &ScopedRead<'_, eg_storage::AgentLibraryOwner>,
    tenant_id: &str,
    agent_id: &str,
) -> Result<Vec<AgentLibraryEntry>, String> {
    let table = read.open_owner_table(eg_storage::AGENT_LIBRARY_REVISIONS)?;
    let mut entries = Vec::new();
    let mut bytes = 0usize;
    for row in table
        .range((tenant_id, agent_id, 0)..=(tenant_id, agent_id, u64::MAX))
        .map_err(|error| error.to_string())?
    {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (row_tenant, row_agent, revision) = key.value();
        if row_tenant != tenant_id || row_agent != agent_id {
            return Err("agent library revision range escaped its key prefix".to_string());
        }
        bytes = bytes
            .checked_add(value.value().len())
            .filter(|total| *total <= MAX_AGENT_LIBRARY_HISTORY_BYTES)
            .ok_or_else(|| "agent library revision history exceeds resource limits".to_string())?;
        if entries.len() >= MAX_AGENT_LIBRARY_REVISIONS {
            return Err("agent library revision history exceeds resource limits".to_string());
        }
        let entry = decode_entry(value.value())?;
        validate_entry_key(&entry, tenant_id, agent_id, revision)?;
        entries.push(entry);
    }
    Ok(entries)
}

fn validate_history_chain(head: Option<u64>, entries: &[AgentLibraryEntry]) -> Result<(), String> {
    validate_history_head(head, entries)?;
    for (index, entry) in entries.iter().enumerate() {
        validate_history_revision(index, entry, entries)?;
    }
    Ok(())
}

fn validate_history_head(head: Option<u64>, entries: &[AgentLibraryEntry]) -> Result<(), String> {
    match (head, entries.last()) {
        (None, None) => Ok(()),
        (None, Some(_)) => Err("agent library revisions exist without a head".to_string()),
        (Some(_), None) => Err("agent library head points to an empty revision chain".to_string()),
        (Some(head), Some(last)) if head != last.entry_revision => {
            Err("agent library head does not match the final revision".to_string())
        }
        (Some(_), Some(_)) => Ok(()),
    }
}

fn validate_history_revision(
    index: usize,
    entry: &AgentLibraryEntry,
    entries: &[AgentLibraryEntry],
) -> Result<(), String> {
    let expected = (index as u64).saturating_add(1);
    if entry.entry_revision != expected {
        return Err("agent library revision chain has a gap".to_string());
    }
    if !entry.is_retired() {
        return Ok(());
    }
    let Some(previous) = index.checked_sub(1).and_then(|i| entries.get(i)) else {
        return Err("agent library revision one cannot be a tombstone".to_string());
    };
    if previous.is_retired()
        || !entries[index + 1..].is_empty()
        || entry.as_draft() != previous.as_draft()
        || entry.created_at_ms != previous.created_at_ms
        || entry.updated_at_ms < previous.updated_at_ms
    {
        return Err("agent library tombstone does not preserve its prior definition".to_string());
    }
    Ok(())
}
