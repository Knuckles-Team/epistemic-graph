//! EG-DURABLE-KERNEL-R024.8: the Spark job backend's `ForeignSourceSpec`
//! source.
//!
//! [`eg_types::wire::ForeignSourceSpec::SparkBatch`] already names a sealed,
//! completed batch artifact (interactive Spark SQL pushdown is deliberately
//! absent; a query may read only a registered artifact, never launch a job
//! or trust a caller-provided path). `federation::source_for`'s generic
//! dispatch already refuses it through `Oq2Unbound`; this module is the
//! EXPLICIT per-registration half -- the same shape `federation_age` and
//! `federation_trino` give their own backend -- validating the artifact
//! reference's shape before a registration is attempted, then refusing with
//! a named, typed reason because no bound Spark artifact-store driver exists
//! yet in this workspace. Never silently accepted as if a driver existed.

use eg_types::wire::ForeignSourceSpec;

use crate::federation::ForeignSourceRegistry;

const MAX_FIELD_BYTES: usize = 1024;
/// `SparkBatch::artifact_ref` must reference a REGISTERED artifact, never a
/// caller-provided path -- enforced here by requiring the `artifact://`
/// scheme the registration surface issues, matching the wire doc's "a
/// completed, registered artifact, never ... a caller-provided path".
const ARTIFACT_REF_SCHEME: &str = "artifact://";

/// Validate a `SparkBatch` spec's shape, then refuse: no bound Spark
/// artifact-store driver exists yet. Never registers anything.
pub fn register_spark(
    _registry: &mut ForeignSourceRegistry,
    _name: impl Into<String>,
    spec: &ForeignSourceSpec,
) -> Result<(), String> {
    validate_spark_batch_shape(spec)?;
    Err(
        "federation: Spark job backend has no bound artifact-store driver yet \
         (EG-DURABLE-KERNEL-R024.8); SparkBatch reads only a registered artifact"
            .into(),
    )
}

fn validate_spark_batch_shape(spec: &ForeignSourceSpec) -> Result<(), String> {
    let ForeignSourceSpec::SparkBatch {
        artifact_ref,
        id_field,
        score_field,
    } = spec
    else {
        return Err("federation: expected a Spark batch source".into());
    };
    if artifact_ref.len() > MAX_FIELD_BYTES || !artifact_ref.starts_with(ARTIFACT_REF_SCHEME) {
        return Err(format!(
            "federation: invalid Spark artifact reference (must start with {ARTIFACT_REF_SCHEME:?})"
        ));
    }
    if artifact_ref.len() == ARTIFACT_REF_SCHEME.len() {
        return Err("federation: invalid Spark artifact reference (empty artifact id)".into());
    }
    identifier(id_field, "id field")?;
    if let Some(score) = score_field {
        identifier(score, "score field")?;
        if score == id_field {
            return Err("federation: Spark score and id fields must differ".into());
        }
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
        return Err(format!("federation: invalid Spark {field}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_spec() -> ForeignSourceSpec {
        ForeignSourceSpec::SparkBatch {
            artifact_ref: "artifact://batches/2026-10-09/ingest-run".into(),
            id_field: "id".into(),
            score_field: None,
        }
    }

    #[test]
    fn well_formed_spec_is_refused_not_silently_accepted() {
        let mut registry = ForeignSourceRegistry::default();
        let err = register_spark(&mut registry, "spark", &valid_spec()).unwrap_err();
        assert!(err.contains("no bound artifact-store driver"), "{err}");
    }

    #[test]
    fn caller_provided_path_without_the_artifact_scheme_is_rejected() {
        let mut registry = ForeignSourceRegistry::default();
        let mut spec = valid_spec();
        if let ForeignSourceSpec::SparkBatch { artifact_ref, .. } = &mut spec {
            *artifact_ref = "/var/spark/output/part-00000".into();
        }
        let err = register_spark(&mut registry, "spark", &spec).unwrap_err();
        assert!(err.contains("must start with"), "{err}");
    }

    #[test]
    fn empty_artifact_id_after_the_scheme_is_rejected() {
        let mut registry = ForeignSourceRegistry::default();
        let mut spec = valid_spec();
        if let ForeignSourceSpec::SparkBatch { artifact_ref, .. } = &mut spec {
            *artifact_ref = "artifact://".into();
        }
        let err = register_spark(&mut registry, "spark", &spec).unwrap_err();
        assert!(err.contains("empty artifact id"), "{err}");
    }

    #[test]
    fn score_field_matching_id_field_is_rejected() {
        let mut registry = ForeignSourceRegistry::default();
        let mut spec = valid_spec();
        if let ForeignSourceSpec::SparkBatch {
            id_field,
            score_field,
            ..
        } = &mut spec
        {
            *score_field = Some(id_field.clone());
        }
        let err = register_spark(&mut registry, "spark", &spec).unwrap_err();
        assert!(err.contains("must differ"), "{err}");
    }

    #[test]
    fn wrong_spec_variant_is_rejected() {
        let mut registry = ForeignSourceRegistry::default();
        let spec = ForeignSourceSpec::Named {
            name: "other".into(),
        };
        let err = register_spark(&mut registry, "spark", &spec).unwrap_err();
        assert!(err.contains("expected a Spark batch source"), "{err}");
    }
}
