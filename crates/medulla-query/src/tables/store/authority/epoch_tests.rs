use super::*;
use crate::tables::store::TableStore;

fn snapshot(authority: &SqlAuthority) -> SqlSourceSnapshot {
    let read = authority.read().unwrap();
    authority.source_snapshot(&read).unwrap()
}

#[test]
fn terminal_result_uses_the_epoch_in_the_same_owner_write() {
    let (store, path) = TableStore::open_temp().unwrap();
    let mutation = store
        .authority
        .begin_maintenance("epoch-result", "fixture")
        .unwrap();
    let batch = mutation.batch.clone();
    let result = mutation
        .owner_rows_with_epoch(
            |_| Ok(()),
            |write, (), epoch| {
                let table = write
                    .open_table(SQL_SOURCE_AUTHORITY)
                    .map_err(|error| error.to_string())?;
                let value = table
                    .get(SQL_SOURCE_AUTHORITY_KEY)
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| "missing staged source epoch".to_string())?;
                assert_eq!(&value.value()[32..], epoch.to_be_bytes().as_slice());
                rmp_serde::to_vec_named(&epoch).map_err(|error| error.to_string())
            },
        )
        .unwrap();
    mutation.finish(Some(result.clone()), 101).unwrap();
    mutation.commit_finished().unwrap();
    let record = store
        .mutation_batch(&batch.identity, &batch.batch_id)
        .unwrap()
        .unwrap();
    assert_eq!(record.result_msgpack, Some(result.clone()));
    let recorded_epoch: u64 = rmp_serde::from_slice(&result).unwrap();
    assert_eq!(recorded_epoch, 1);
    assert_eq!(snapshot(&store.authority).epoch, recorded_epoch);
    drop(store);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn finalization_failure_aborts_the_staged_epoch_and_receipt() {
    let (store, path) = TableStore::open_temp().unwrap();
    let before = snapshot(&store.authority);
    let mutation = store
        .authority
        .begin_maintenance("epoch-failure", "fixture")
        .unwrap();
    let batch = mutation.batch.clone();
    let result: Result<(), String> = mutation.owner_rows_with_epoch(
        |_| Ok(()),
        |_, (), epoch| {
            assert_eq!(epoch, before.epoch + 1);
            Err("fixture terminal encoding failure".to_string())
        },
    );
    assert_eq!(result.unwrap_err(), "fixture terminal encoding failure");
    mutation.abort().unwrap();
    assert_eq!(snapshot(&store.authority), before);
    assert!(store
        .mutation_batch(&batch.identity, &batch.batch_id)
        .unwrap()
        .is_none());
    drop(store);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn row_application_failure_does_not_run_the_finalizer() {
    let (store, path) = TableStore::open_temp().unwrap();
    let before = snapshot(&store.authority);
    let mutation = store
        .authority
        .begin_maintenance("epoch-apply-failure", "fixture")
        .unwrap();
    let result: Result<(), String> = mutation.owner_rows_with_epoch(
        |_| Err::<(), _>("fixture row failure".to_string()),
        |_, (), _| panic!("finalizer must not run after failed rows"),
    );
    assert_eq!(result.unwrap_err(), "fixture row failure");
    mutation.abort().unwrap();
    assert_eq!(snapshot(&store.authority), before);
    drop(store);
    std::fs::remove_file(path).unwrap();
}
