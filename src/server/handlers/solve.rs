//! `Method::Solve`: the general bounded 0-1 integer programme.
//!
//! Validates the model, runs the deterministic node-budgeted search, and then
//! re-verifies the certificate it is about to return with the independent
//! verifier: a reply either carries a proof that checks or is refused with
//! `SOLVE_CERTIFICATE_REJECTED`. Pure compute: it reads no store, no clock and
//! no float, so it takes neither the server state nor the verified context.

use crate::protocol::Response;
use eg_types::solve::SolveRequest;

#[cfg(feature = "solve")]
fn refuse(
    req_id: u64,
    code: eg_types::solve::SolveErrorCode,
    detail: impl std::fmt::Display,
) -> Response {
    Response::err(req_id, format!("{}: {detail}", code.as_str()))
}

/// Solve one model and answer its verified certificate.
#[cfg(feature = "solve")]
pub(crate) async fn handle_solve(req_id: u64, request: SolveRequest) -> Response {
    use crate::protocol::ResultPayload;
    use eg_compute::solve::{solve, verify, Model, SolverConfig};
    use eg_types::solve::{SolveErrorCode, SolveResult, SOLVE_RESULT_SCHEMA_VERSION};

    let model = match Model::try_from(request.model) {
        Ok(model) => model,
        Err(error) => return refuse(req_id, SolveErrorCode::ModelInvalid, error),
    };
    let config = match request.config.map(SolverConfig::try_from).transpose() {
        Ok(config) => config.unwrap_or_default(),
        Err(error) => return refuse(req_id, SolveErrorCode::ConfigInvalid, error),
    };
    // A search of up to the configured node budget is CPU work, not I/O.
    let solved = tokio::task::spawn_blocking(move || {
        let certificate = solve(&model, &config);
        verify(&model, &certificate).map(|_| certificate)
    })
    .await;
    match solved {
        Ok(Ok(certificate)) => {
            let result = SolveResult {
                schema_version: SOLVE_RESULT_SCHEMA_VERSION,
                certificate_digest: certificate.digest(),
                certificate,
            };
            Response::ok(
                req_id,
                ResultPayload::of_ref::<eg_types::result_contract::compute::Solve>(&result),
            )
        }
        Ok(Err(error)) => refuse(req_id, SolveErrorCode::CertificateRejected, error),
        Err(error) => Response::err(req_id, format!("solve task failed: {error}")),
    }
}

/// A build without the solver still serves the method: it refuses by name.
#[cfg(not(feature = "solve"))]
pub(crate) async fn handle_solve(req_id: u64, _request: SolveRequest) -> Response {
    Response::err(req_id, "Solve requires the `solve` feature")
}

#[cfg(all(test, feature = "solve"))]
mod tests {
    use super::*;
    use eg_types::solve::{
        Coefficient, ConstraintBody, ConstraintSpec, ModelSpec, ObjectiveLevelSpec, ObjectiveTerm,
        SolveResult, VarId,
    };

    fn cover_model() -> ModelSpec {
        ModelSpec {
            variables: vec!["a".into(), "b".into(), "c".into()],
            constraints: vec![ConstraintSpec {
                label: "cover".into(),
                body: ConstraintBody::AtLeast {
                    vars: vec![VarId(0), VarId(1), VarId(2)],
                    k: 2,
                },
            }],
            objective: vec![ObjectiveLevelSpec {
                label: "cost".into(),
                terms: (0..3)
                    .map(|index| ObjectiveTerm {
                        var: VarId(index),
                        coefficient: Coefficient::Known(i64::from(index) + 1),
                    })
                    .collect(),
            }],
        }
    }

    #[tokio::test]
    async fn a_model_is_solved_with_a_certificate_that_verifies() {
        let response = handle_solve(
            3,
            SolveRequest {
                model: cover_model(),
                config: None,
            },
        )
        .await;
        assert!(response.error.is_none(), "{:?}", response.error);
        let Some(crate::protocol::ResultPayload::Raw(bytes)) = response.result else {
            panic!("Solve answers a raw typed result");
        };
        let result: SolveResult = rmp_serde::from_slice(&bytes).expect("a verified certificate");
        assert_eq!(result.certificate_digest, result.certificate.digest());
        let incumbent = result.certificate.incumbent.expect("feasible");
        assert_eq!(incumbent.selected, vec![true, true, false]);
    }

    #[tokio::test]
    async fn an_invalid_model_is_refused_by_name() {
        let mut model = cover_model();
        model.variables.clear();
        let response = handle_solve(
            4,
            SolveRequest {
                model,
                config: None,
            },
        )
        .await;
        let error = response.error.expect("refused");
        assert!(error.starts_with("SOLVE_MODEL_INVALID: "), "{error}");
    }
}
