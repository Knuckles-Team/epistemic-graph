use eg_query::QueryResult;

pub fn rows(result: &QueryResult) -> Vec<Vec<serde_json::Value>> {
    result
        .rows
        .iter()
        .map(|batch| rmp_serde::from_slice::<Vec<serde_json::Value>>(batch).unwrap())
        .collect()
}
