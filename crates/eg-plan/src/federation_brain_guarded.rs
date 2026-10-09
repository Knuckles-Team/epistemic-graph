//! EG-DURABLE-KERNEL-R024.6: the brain-guarded backend's `ForeignSourceSpec`
//! source.
//!
//! A brain-guarded spec names a `guard_policy` that MUST evaluate against the
//! live semantic/policy engine (the "brain") before any row from `query`
//! crosses the federation boundary. No guard-evaluation driver exists in this
//! workspace yet, so this slice is the typed validation half only (same
//! CONTRACT [`crate::federation_cypher_unbound`] follows: ship the typed
//! model plus refusal test when the bound driver is a later child). A
//! well-formed spec is validated and then explicitly REFUSED with a named,
//! typed reason -- never silently run unguarded, and never a generic/opaque
//! error.

use eg_types::wire::ForeignSourceSpec;

use crate::federation::ForeignSourceRegistry;

const MAX_FIELD_BYTES: usize = 1024;
const MAX_QUERY_BYTES: usize = 1024 * 1024;

/// Validate a `BrainGuarded` spec's shape, then refuse: no bound guard
/// driver exists yet. Never registers anything, and never runs `query`
/// unguarded.
pub fn register_brain_guarded(
    _registry: &mut ForeignSourceRegistry,
    _name: impl Into<String>,
    spec: &ForeignSourceSpec,
) -> Result<(), String> {
    validate_brain_guarded_shape(spec)?;
    Err(
        "federation: brain-guarded source has no bound guard driver yet \
         (EG-DURABLE-KERNEL-R024.6); a spec naming a guard policy is never run unguarded"
            .into(),
    )
}

/// Shared shape validation for the not-yet-bound brain-guarded source.
/// Mirrors `federation_cypher_unbound::validate_cypher_shape`'s field checks.
fn validate_brain_guarded_shape(spec: &ForeignSourceSpec) -> Result<(), String> {
    let ForeignSourceSpec::BrainGuarded {
        endpoint,
        guard_policy,
        query,
        id_field,
        score_field,
    } = spec
    else {
        return Err("federation: expected a brain-guarded source".into());
    };
    if endpoint.is_empty() || endpoint.len() > MAX_FIELD_BYTES {
        return Err("federation: invalid brain-guarded endpoint".into());
    }
    if guard_policy.is_empty() || guard_policy.len() > MAX_FIELD_BYTES {
        return Err("federation: invalid brain-guarded guard policy".into());
    }
    identifier(id_field, "id field")?;
    if let Some(score) = score_field {
        identifier(score, "score field")?;
        if score == id_field {
            return Err("federation: brain-guarded score and id fields must differ".into());
        }
    }
    if query.trim().is_empty() || query.len() > MAX_QUERY_BYTES || query.contains('\0') {
        return Err("federation: invalid brain-guarded query".into());
    }
    Ok(())
}

fn identifier(value: &str, field: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > MAX_FIELD_BYTES
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
    {
        return Err(format!("federation: invalid brain-guarded {field}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_spec() -> ForeignSourceSpec {
        ForeignSourceSpec::BrainGuarded {
            endpoint: "https://brain.example.invalid".into(),
            guard_policy: "pii-redaction-v1".into(),
            query: "SELECT id FROM objects".into(),
            id_field: "id".into(),
            score_field: None,
        }
    }

    // spec: EG-DURABLE-KERNEL-R014.1, EG-DURABLE-KERNEL-R024.5, EG-DURABLE-KERNEL-R024.6, EG-DURABLE-KERNEL-R024.8
    #[test]
    fn well_formed_spec_is_refused_not_silently_run_unguarded() {
        let mut registry = ForeignSourceRegistry::default();
        let err = register_brain_guarded(&mut registry, "brain", &valid_spec()).unwrap_err();
        assert!(err.contains("no bound guard driver"), "{err}");
    }

    // spec: EG-DURABLE-KERNEL-R014.1, EG-DURABLE-KERNEL-R024.5, EG-DURABLE-KERNEL-R024.6, EG-DURABLE-KERNEL-R024.8
    #[test]
    fn empty_guard_policy_is_rejected_before_reaching_the_refusal() {
        let mut registry = ForeignSourceRegistry::default();
        let mut spec = valid_spec();
        if let ForeignSourceSpec::BrainGuarded { guard_policy, .. } = &mut spec {
            guard_policy.clear();
        }
        let err = register_brain_guarded(&mut registry, "brain", &spec).unwrap_err();
        assert!(err.contains("invalid brain-guarded guard policy"), "{err}");
    }

    // spec: EG-DURABLE-KERNEL-R014.1, EG-DURABLE-KERNEL-R024.5, EG-DURABLE-KERNEL-R024.6, EG-DURABLE-KERNEL-R024.8
    #[test]
    fn score_field_matching_id_field_is_rejected() {
        let mut registry = ForeignSourceRegistry::default();
        let mut spec = valid_spec();
        if let ForeignSourceSpec::BrainGuarded {
            id_field,
            score_field,
            ..
        } = &mut spec
        {
            *score_field = Some(id_field.clone());
        }
        let err = register_brain_guarded(&mut registry, "brain", &spec).unwrap_err();
        assert!(err.contains("must differ"), "{err}");
    }

    #[test]
    fn wrong_spec_variant_is_rejected() {
        let mut registry = ForeignSourceRegistry::default();
        let spec = ForeignSourceSpec::Named {
            name: "other".into(),
        };
        let err = register_brain_guarded(&mut registry, "brain", &spec).unwrap_err();
        assert!(err.contains("expected a brain-guarded source"), "{err}");
    }
}
