use eg_compute::datascience::primitives::train_test_split;
use eg_compute::datascience::{estimators, metrics};
use eg_compute::mining::classify;
use eg_types::wire::{EstimatorParams, FittedClassifier, FittedModel, ModelSpec, SplitSpec};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};

pub(super) struct TrainArtifact {
    pub(super) blob: Value,
    pub(super) classes: Value,
    pub(super) metrics: Value,
    pub(super) counts: (usize, usize, usize),
}
enum Fitted {
    Classifier(FittedClassifier),
    Estimator(FittedModel),
}
pub(super) fn fit_tabular(
    rows: &[Vec<f64>],
    labels: &[f64],
    split: &SplitSpec,
    spec: &ModelSpec,
    family: &str,
) -> Result<TrainArtifact, String> {
    let (x_train, x_test, y_train, y_test) =
        train_test_split(rows, labels, split.test_ratio, split.shuffle, split.seed);
    if x_train.is_empty() {
        return Err(
            "pipeline: empty training split (raise sample count or lower test_ratio)".into(),
        );
    }
    let fitted = match family {
        "classify" => {
            let y_train = to_i64_round(&y_train);
            classify::fit(&x_train, &y_train, classify_algo_from_spec(spec)?)
                .map(Fitted::Classifier)
                .map_err(|e| format!("pipeline: classify fit: {e}"))?
        }
        _ => {
            let name = if spec.algorithm.is_empty() {
                "ridge".to_string()
            } else {
                spec.algorithm.clone()
            };
            let params: EstimatorParams =
                serde_json::from_value(spec.params.clone()).unwrap_or_default();
            estimators::fit_estimator(&name, &x_train, &y_train, &params)
                .map(Fitted::Estimator)
                .map_err(|e| format!("pipeline: estimator fit: {e}"))?
        }
    };
    let metrics = json!({
        "train": metrics_for(&fitted, &x_train, &y_train),
        "test": metrics_for(&fitted, &x_test, &y_test),
    });
    let (blob, classes) = match &fitted {
        Fitted::Classifier(model) => (
            serde_json::to_value(model).unwrap_or(Value::Null),
            Value::from(classify_classes(model)),
        ),
        Fitted::Estimator(model) => (
            serde_json::to_value(model).unwrap_or(Value::Null),
            Value::Null,
        ),
    };
    Ok(TrainArtifact {
        blob,
        classes,
        metrics,
        counts: (x_train[0].len(), x_train.len(), x_test.len()),
    })
}
pub(super) fn evaluate_tabular(
    blob: &Value,
    family: &str,
    rows: &[Vec<f64>],
    labels: &[f64],
) -> Result<Value, String> {
    let fitted = match family {
        "classify" => decode_blob(blob, None).map(Fitted::Classifier)?,
        _ => decode_blob(blob, None).map(Fitted::Estimator)?,
    };
    Ok(metrics_for(&fitted, rows, labels))
}

pub(super) fn decode_blob<T: DeserializeOwned>(
    blob: &Value,
    family: Option<&str>,
) -> Result<T, String> {
    let prefix = family
        .map(|family| format!("pipeline: invalid {family} blob"))
        .unwrap_or_else(|| "pipeline: invalid blob".to_string());
    serde_json::from_value(blob.clone()).map_err(|e| format!("{prefix}: {e}"))
}

fn metrics_for(model: &Fitted, rows: &[Vec<f64>], labels: &[f64]) -> Value {
    match model {
        Fitted::Classifier(model) => {
            let predicted = classify::predict(model, rows).labels;
            let expected = to_i64_round(labels);
            json!({
                "accuracy": metrics::accuracy(&expected, &predicted),
                "macro_f1": metrics::macro_f1(&expected, &predicted),
            })
        }
        Fitted::Estimator(model) => {
            let predicted = estimators::predict(model, rows);
            json!({
                "r2": metrics::r2(labels, &predicted),
                "rmse": metrics::rmse(labels, &predicted),
            })
        }
    }
}

fn classify_algo_from_spec(m: &ModelSpec) -> Result<classify::Algorithm, String> {
    let p = &m.params;
    let f = |k: &str, d: f64| p.get(k).and_then(|v| v.as_f64()).unwrap_or(d);
    let u = |k: &str, d: usize| {
        p.get(k)
            .and_then(|v| v.as_u64())
            .map(|x| x as usize)
            .unwrap_or(d)
    };
    match m.algorithm.to_ascii_lowercase().as_str() {
        "" | "gaussiannb" => Ok(classify::Algorithm::GaussianNb),
        "multinomialnb" => Ok(classify::Algorithm::MultinomialNb {
            alpha: f("alpha", 1.0),
        }),
        "knn" => Ok(classify::Algorithm::Knn { k: u("k", 5) }),
        "logistic" => Ok(classify::Algorithm::Logistic {
            lr: f("lr", 0.1),
            epochs: u("epochs", 300),
            l2: f("l2", 0.0),
        }),
        "svc" => Ok(classify::Algorithm::LinearSvc {
            c: f("C", 1.0),
            epochs: u("epochs", 300),
            lr: f("lr", 0.1),
        }),
        other => Err(format!(
            "pipeline: unknown classify algorithm {other:?} \
             (gaussiannb | multinomialnb | knn | logistic | svc)"
        )),
    }
}

fn to_i64_round(values: &[f64]) -> Vec<i64> {
    values.iter().map(|&value| value.round() as i64).collect()
}
fn classify_classes(model: &FittedClassifier) -> Vec<i64> {
    match model {
        FittedClassifier::GaussianNb { classes, .. }
        | FittedClassifier::MultinomialNb { classes, .. }
        | FittedClassifier::Knn { classes, .. }
        | FittedClassifier::LinearOvr { classes, .. } => classes.clone(),
    }
}
