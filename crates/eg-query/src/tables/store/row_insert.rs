//! The shared native row-insertion pipeline for SQL literal inserts and typed
//! source cells.

use super::*;

/// The one native row-insertion pipeline. Inputs are already bounded and their
/// builder returns schema-aligned cells; allocation, encoding, indexes and all
/// post-write constraints are shared by SQL literals and typed source cells.
pub(super) fn insert_rows_in<T, F, E>(
    wtx: &SqlWrite<'_>,
    tenant_scope: &str,
    table: &str,
    schema: TableSchema,
    rows: &[T],
    mut build: F,
    mut encode: E,
) -> Result<Vec<Vec<Cell>>, String>
where
    F: FnMut(&TableSchema, &T, u64) -> Result<Vec<Cell>, String>,
    E: FnMut(&Vec<Cell>) -> Result<Vec<u8>, String>,
{
    // Allocate the rowid block up front so SERIAL columns get stable, contiguous ids.
    let first_rowid = alloc_rowids(wtx, table, rows.len() as u64)?;

    // Build each row's typed cells BEFORE writing so a coercion/constraint error never
    // leaves a half-applied INSERT (the txn is dropped on Err).
    let mut inserted: Vec<Vec<Cell>> = Vec::with_capacity(rows.len());
    let mut encoded: Vec<(u64, Vec<u8>)> = Vec::with_capacity(rows.len());
    for (ri, row) in rows.iter().enumerate() {
        let rowid = first_rowid + ri as u64;
        let cells = build(&schema, row, rowid)?;
        let blob = encode(&cells)?;
        if blob.len() > MAX_SQL_STORED_VALUE_BYTES {
            return Err("encoded SQL row exceeds storage value limit".to_string());
        }
        encoded.push((rowid, blob));
        inserted.push(cells);
    }

    {
        let mut rows_t = wtx.open_table(ROWS)?;
        for (rowid, blob) in &encoded {
            rows_t
                .insert((table, *rowid), blob.as_slice())
                .map_err(map_err)?;
        }
    }
    for (offset, cells) in inserted.iter().enumerate() {
        maintain_secondary_row_in(
            wtx,
            tenant_scope,
            table,
            &schema,
            first_rowid + offset as u64,
            None,
            Some(cells),
        )?;
    }
    // Uniqueness over the post-insert state (reads staged writes through `wtx`).
    validate_uniqueness_in(wtx, table, &schema)?;
    // CONCEPT:EG-KG.query.table-schema-constraints/NE-001 — table-level CHECK + outgoing FOREIGN KEY per inserted row.
    for cells in &inserted {
        validate_row_constraints_in(wtx, &schema, cells)?;
    }
    Ok(inserted)
}
