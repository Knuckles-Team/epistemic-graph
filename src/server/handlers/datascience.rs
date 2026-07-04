//! Pure-compute data-science ops (CONCEPT:EG-KG.compute.rust-native-training-loss): sklearn-parity estimators,
//! primitives, and training kernels. Stateless — no graph core, runs inline.

// See finance.rs: the Result router moves the large `Method` enum by value on the
// fall-through path; boxing the Err would allocate per non-DS request.
#![allow(clippy::result_large_err)]

use crate::protocol::{Method, Response, ResultPayload};

/// Handle a `Ds*` method. `Err(method)` hands a non-datascience method back to the
/// dispatcher (routing fall-through). (CONCEPT:EG-KG.query.dispatch-convention — server dispatch convention)
pub(crate) fn try_handle(req_id: u64, method: Method) -> Result<Response, Method> {
    let resp = match method {
        // ── Data Science Primitives (CONCEPT:EG-KG.compute.rust-native-training-loss) ─────────────────
        Method::DsLinearRegression { x, y } => {
            let result = crate::datascience::primitives::linear_regression(&x, &y);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(result)))
        }
        Method::DsKMeans { data, k, max_iter } => {
            let result = crate::datascience::primitives::kmeans(&data, k, max_iter);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(result)))
        }
        Method::DsPca { data, n_components } => {
            let result = crate::datascience::primitives::pca(&data, n_components);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(result)))
        }
        Method::DsComputeStats { data } => {
            let result = crate::datascience::primitives::compute_stats(&data);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(result)))
        }
        Method::DsTrainTestSplit {
            data,
            labels,
            test_ratio,
            shuffle,
            seed,
        } => {
            let (x_train, x_test, y_train, y_test) =
                crate::datascience::primitives::train_test_split(
                    &data, &labels, test_ratio, shuffle, seed,
                );
            Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!({
                    "x_train": x_train,
                    "x_test": x_test,
                    "y_train": y_train,
                    "y_test": y_test,
                })),
            )
        }
        Method::DsFitEstimator {
            estimator,
            x,
            y,
            params,
        } => match crate::datascience::estimators::fit_estimator(&estimator, &x, &y, &params) {
            Ok(model) => Response::ok(req_id, ResultPayload::Json(serde_json::json!(model))),
            Err(e) => Response::err(req_id, e),
        },
        Method::DsPredictEstimator { model, x } => {
            let preds = crate::datascience::estimators::predict(&model, &x);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(preds)))
        }

        // ── Training loss / optimizer kernels (CONCEPT:EG-KG.compute.rust-native-training-loss) ────────
        Method::DsSoftmax {
            logits,
            temperature,
        } => {
            let r = crate::datascience::training::softmax(&logits, temperature);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(r)))
        }
        Method::DsLogSoftmax { logits } => {
            let r = crate::datascience::training::log_softmax(&logits);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(r)))
        }
        Method::DsCrossEntropy { logits, labels } => {
            let r = crate::datascience::training::cross_entropy(&logits, &labels);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(r)))
        }
        Method::DsDpoLoss {
            policy_chosen,
            policy_rejected,
            ref_chosen,
            ref_rejected,
            beta,
        } => {
            let r = crate::datascience::training::dpo_loss(
                &policy_chosen,
                &policy_rejected,
                &ref_chosen,
                &ref_rejected,
                beta,
            );
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(r)))
        }
        Method::DsGrpoSurrogate {
            logprob,
            old_logprob,
            advantage,
            clip_eps,
        } => {
            let r = crate::datascience::training::grpo_surrogate(
                &logprob,
                &old_logprob,
                &advantage,
                clip_eps,
            );
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(r)))
        }
        Method::DsKlDivergence {
            logprob,
            ref_logprob,
        } => {
            let r = crate::datascience::training::kl_divergence(&logprob, &ref_logprob);
            Response::ok(req_id, ResultPayload::Float(r))
        }
        Method::DsAdamStep {
            params,
            grads,
            m,
            v,
            lr,
            beta1,
            beta2,
            eps,
            t,
        } => {
            let r = crate::datascience::training::adam_step(
                &params, &grads, &m, &v, lr, beta1, beta2, eps, t,
            );
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(r)))
        }
        Method::DsSgdStep { params, grads, lr } => {
            let r = crate::datascience::training::sgd_step(&params, &grads, lr);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(r)))
        }
        other => return Err(other),
    };
    Ok(resp)
}
