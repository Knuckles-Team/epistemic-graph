//! Kind-aware resolution of component pins inside an admitted write.

use std::collections::BTreeSet;

use super::super::agent_pin_resolution::{HeadTable, RevisionTable};
use super::{
    decode_component, AgentLibraryLifecycle, MAX_AGENT_COMPONENT_REVISIONS,
    MAX_COMPONENT_PIN_RESOLUTION_ROWS, MAX_RESOLVED_COMPONENT_PINS,
};

pub(super) fn resolve_component_pins_in_write(
    write: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    tenant_id: &str,
    subject: &str,
    pins: &[&eg_types::agent_component::ComponentDependency],
) -> Result<(), String> {
    let distinct = distinct_component_pins(subject, pins)?;
    let heads = write.open_read_table(eg_storage::AGENT_COMPONENT_HEADS)?;
    let revisions = write.open_read_table(eg_storage::AGENT_COMPONENT_REVISIONS)?;
    let mut rows = 0usize;
    for pin in distinct {
        resolve_component_pin(&heads, &revisions, tenant_id, subject, pin, &mut rows)?;
    }
    Ok(())
}

fn distinct_component_pins<'a>(
    subject: &str,
    pins: &[&'a eg_types::agent_component::ComponentDependency],
) -> Result<BTreeSet<(&'a str, &'a str, &'a str)>, String> {
    // Deduplicated: an agent that pins the same prompt component from two
    // slots should cost one lookup, not two, and the fan-out bound should
    // count what is actually resolved.
    let distinct = pins
        .iter()
        .map(|pin| {
            (
                pin.component_id.as_str(),
                pin.kind.as_str(),
                pin.definition_digest.as_str(),
            )
        })
        .collect::<BTreeSet<_>>();
    if distinct.len() > MAX_RESOLVED_COMPONENT_PINS {
        return Err(format!(
            "{subject} pins more than {MAX_RESOLVED_COMPONENT_PINS} distinct components"
        ));
    }
    Ok(distinct)
}

fn resolve_component_pin(
    heads: &HeadTable<'_>,
    revisions: &RevisionTable<'_>,
    tenant_id: &str,
    subject: &str,
    pin: (&str, &str, &str),
    rows: &mut usize,
) -> Result<(), String> {
    let (component_id, kind, definition_digest) = pin;
    let Some(head_revision) = heads
        .get((tenant_id, component_id))?
        .map(|value| value.value())
    else {
        return Err(format!(
            "{subject} pins component '{component_id}', which does not exist in this tenant"
        ));
    };
    *rows += 1;
    let head = revisions
        .get((tenant_id, component_id, head_revision))?
        .ok_or_else(|| "agent component head points to a missing revision".to_string())?;
    let head = decode_component(head.value())?;
    if head.lifecycle == AgentLibraryLifecycle::Retired {
        return Err(format!(
            "{subject} pins component '{component_id}', which is retired"
        ));
    }
    // The HEAD first: the overwhelmingly common pin is the current revision,
    // and hitting it turns the whole resolution into one read. The fallback
    // scan reuses the table handle opened above because redb refuses a second
    // open of the same table while the first handle is alive.
    let resolved = if head.definition_digest == definition_digest {
        Some(head)
    } else {
        find_component_revision(
            revisions,
            tenant_id,
            component_id,
            definition_digest,
            subject,
            rows,
        )?
    };
    let Some(resolved) = resolved else {
        return Err(format!(
            "{subject} pins a revision of component '{component_id}' that was never \
             published: no retained revision matches the pinned definition digest"
        ));
    };
    validate_component_pin(subject, tenant_id, component_id, kind, &resolved, *rows)
}

fn find_component_revision(
    revisions: &RevisionTable<'_>,
    tenant_id: &str,
    component_id: &str,
    definition_digest: &str,
    subject: &str,
    rows: &mut usize,
) -> Result<Option<eg_types::agent_component::AgentComponentEntry>, String> {
    let mut scanned = 0usize;
    for row in revisions.range_from((tenant_id, component_id, 0))? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (row_tenant, row_component, _) = key.value();
        // `range_from` is open-ended, so the prefix has to be re-checked per
        // row or the scan would walk into the next component's revisions.
        if row_tenant != tenant_id || row_component != component_id {
            break;
        }
        scanned += 1;
        *rows += 1;
        if scanned > MAX_AGENT_COMPONENT_REVISIONS {
            return Err("agent component history exceeds its retained revision bound".to_string());
        }
        if *rows > MAX_COMPONENT_PIN_RESOLUTION_ROWS {
            return Err(format!(
                "{subject} reference resolution exceeds its \
                 {MAX_COMPONENT_PIN_RESOLUTION_ROWS}-row bound"
            ));
        }
        let entry = decode_component(value.value())?;
        if entry.definition_digest == definition_digest {
            return Ok(Some(entry));
        }
    }
    Ok(None)
}

fn validate_component_pin(
    subject: &str,
    tenant_id: &str,
    component_id: &str,
    kind: &str,
    resolved: &eg_types::agent_component::AgentComponentEntry,
    rows: usize,
) -> Result<(), String> {
    // The scan is tenant-prefixed, but the row's own identity remains an
    // independent corruption check before the pin is admitted.
    if resolved.tenant_id != tenant_id {
        return Err(format!(
            "{subject} pins component '{component_id}', which belongs to another tenant"
        ));
    }
    if resolved.kind.as_str() != kind {
        return Err(format!(
            "{subject} pins component '{component_id}' as a {kind}, but it is a {}",
            resolved.kind.as_str()
        ));
    }
    if rows > MAX_COMPONENT_PIN_RESOLUTION_ROWS {
        return Err(format!(
            "{subject} reference resolution exceeds its {MAX_COMPONENT_PIN_RESOLUTION_ROWS}\
             -row bound"
        ));
    }
    Ok(())
}
