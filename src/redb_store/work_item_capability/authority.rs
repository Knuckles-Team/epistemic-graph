use super::*;

pub(super) fn bounded_text(value: &str, max: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max
}

pub(super) fn validate_authority(
    graph: &str,
    authority: &AuthenticatedAuthority,
) -> Result<(), String> {
    if !bounded_text(graph, MAX_AUTHORITY_TEXT_BYTES)
        || !bounded_text(&authority.tenant, MAX_AUTHORITY_TEXT_BYTES)
        || !bounded_text(&authority.audience, MAX_AUTHORITY_TEXT_BYTES)
        || !bounded_text(&authority.principal, MAX_AUTHORITY_TEXT_BYTES)
        || !bounded_text(&authority.agent_id, MAX_AUTHORITY_TEXT_BYTES)
        || !bounded_text(&authority.session, MAX_AUTHORITY_TEXT_BYTES)
        || !bounded_text(&authority.incarnation_id, MAX_AUTHORITY_TEXT_BYTES)
    {
        return Err("capability authority is invalid".to_string());
    }
    if authority.authority_epoch == 0 {
        return Err("capability authority is invalid".to_string());
    }
    Ok(())
}
