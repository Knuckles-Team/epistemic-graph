//! Row access for the maintained user-table ANN authority (RF-019).
//!
//! Every read here is ONE kernel-issued scoped read of the authoritative row
//! store, so a probe's visibility checks, tombstone checks and exact rescoring
//! all observe the same snapshot. Nothing here writes: building and serving an
//! ANN generation are reads of the source of record, never a second copy of it.

use eg_types::RowPredicate;

use super::{decode_stored, get_schema_read, map_err, row_map, RowsReadTable, TableStore, ROWS};
use crate::tables::ann_authority::UserAnnAuthority;
use crate::tables::schema::{Cell, ColumnType, TableSchema};

/// Every indexable vector of one column, read in one snapshot.
pub(crate) struct AnnSourceRows {
    /// Tenant SQL source epoch the snapshot observed.
    pub(crate) epoch: u64,
    /// The generation's vector width: the declared width, else the first
    /// non-empty vector's.
    pub(crate) dim: Option<usize>,
    /// Highest physical row id present in the snapshot. Row ids are never
    /// reused, so a row inserted after the snapshot always lands strictly above.
    pub(crate) max_rowid: Option<u64>,
    /// `(row id, vector)` for every row whose vector has `dim` elements.
    pub(crate) rows: Vec<(u64, Vec<f32>)>,
}

impl AnnSourceRows {
    /// Admit one stored row's vector, refusing to grow past `limit` rows.
    fn admit(
        &mut self,
        rowid: u64,
        cells: &[Cell],
        vector_index: usize,
        limit: usize,
    ) -> Result<(), String> {
        let Some(vector) = vector_cell(cells, vector_index) else {
            return Ok(());
        };
        if vector.is_empty() || *self.dim.get_or_insert(vector.len()) != vector.len() {
            return Ok(());
        }
        if self.rows.len() >= limit {
            return Err(format!(
                "ANN generation build exceeds its bound of {limit} indexed rows"
            ));
        }
        self.rows.push((rowid, vector.to_vec()));
        Ok(())
    }
}

/// How far a bounded range read got.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScanExtent {
    /// Every row in the range was examined.
    Complete,
    /// The row budget ran out before the range ended.
    Truncated,
}

/// One snapshot of one table, opened for one probe.
pub(crate) struct AnnRowReader<'a> {
    rows: RowsReadTable,
    table: &'a str,
    schema: TableSchema,
    vector_index: usize,
    epoch: u64,
}

impl AnnRowReader<'_> {
    /// Tenant SQL source epoch of this snapshot.
    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }

    /// The indexed vector of `cells`, when it holds one.
    pub(crate) fn vector<'c>(&self, cells: &'c [Cell]) -> Option<&'c [f32]> {
        vector_cell(cells, self.vector_index)
    }

    /// The current cells of `rowid` when the row exists and `visibility` admits
    /// it. A deleted row and a hidden row are both `None`: a caller cannot tell a
    /// tombstone from a row it may not see, which is the point.
    pub(crate) fn visible_row(
        &self,
        rowid: u64,
        visibility: Option<&RowPredicate>,
    ) -> Result<Option<Vec<Cell>>, String> {
        let Some(value) = self.rows.get((self.table, rowid)).map_err(map_err)? else {
            return Ok(None);
        };
        let cells: Vec<Cell> = decode_stored(value.value(), "row")?;
        Ok(self.admit(cells, visibility))
    }

    /// Visit every row with id `>= first` that `visibility` admits, examining at
    /// most `budget` rows (hidden rows count against the budget too). Returns how
    /// far the read got and how many rows it examined.
    pub(crate) fn scan_from(
        &self,
        first: u64,
        budget: usize,
        visibility: Option<&RowPredicate>,
        visit: &mut dyn FnMut(u64, Vec<Cell>),
    ) -> Result<(ScanExtent, usize), String> {
        let range = self
            .rows
            .range((self.table, first)..=(self.table, u64::MAX))
            .map_err(map_err)?;
        let mut examined = 0usize;
        for entry in range {
            if examined == budget {
                return Ok((ScanExtent::Truncated, examined));
            }
            examined += 1;
            let (key, value) = entry.map_err(map_err)?;
            let cells: Vec<Cell> = decode_stored(value.value(), "row")?;
            if let Some(cells) = self.admit(cells, visibility) {
                visit(key.value().1, cells);
            }
        }
        Ok((ScanExtent::Complete, examined))
    }

    /// Pad a stored row to the schema width, then apply `visibility`.
    fn admit(&self, mut cells: Vec<Cell>, visibility: Option<&RowPredicate>) -> Option<Vec<Cell>> {
        let width = self.schema.columns().len();
        if cells.len() < width {
            cells.resize(width, Cell::Null);
        }
        visibility
            .is_none_or(|predicate| predicate.eval(&row_map(&self.schema, &cells)))
            .then_some(cells)
    }
}

impl TableStore {
    /// This store's maintained ANN authority. Every clone of one opened store
    /// shares it.
    pub fn ann_authority(&self) -> &UserAnnAuthority {
        &self.ann
    }

    /// The tenant SQL source epoch right now.
    pub(crate) fn ann_source_epoch(&self) -> Result<u64, String> {
        let rtx = self.authority.read()?;
        Ok(self.authority.source_snapshot(&rtx)?.epoch)
    }

    /// Every indexable vector of `table.column`, read in ONE snapshot and
    /// bounded by `limit` indexed rows. The maintenance worker's only source.
    pub(crate) fn ann_source_rows(
        &self,
        table: &str,
        column: &str,
        limit: usize,
    ) -> Result<AnnSourceRows, String> {
        let rtx = self.authority.read()?;
        let schema = get_schema_read(&rtx, table)?
            .ok_or_else(|| format!("table `{table}` does not exist"))?;
        let (vector_index, declared) = vector_column(&schema, column)?;
        let mut source = AnnSourceRows {
            epoch: self.authority.source_snapshot(&rtx)?.epoch,
            dim: declared,
            max_rowid: None,
            rows: Vec::new(),
        };
        let rows = rtx.open_owner_table(ROWS)?;
        for entry in rows
            .range((table, 0u64)..=(table, u64::MAX))
            .map_err(map_err)?
        {
            let (key, value) = entry.map_err(map_err)?;
            let rowid = key.value().1;
            source.max_rowid = Some(rowid);
            let cells: Vec<Cell> = decode_stored(value.value(), "row")?;
            source.admit(rowid, &cells, vector_index, limit)?;
        }
        Ok(source)
    }

    /// Run `read` over one snapshot of `table`, whose `column` must be a vector
    /// column.
    pub(crate) fn with_ann_reader<T>(
        &self,
        table: &str,
        column: &str,
        read: impl FnOnce(&AnnRowReader<'_>) -> Result<T, String>,
    ) -> Result<T, String> {
        let rtx = self.authority.read()?;
        let schema = get_schema_read(&rtx, table)?
            .ok_or_else(|| format!("table `{table}` does not exist"))?;
        let (vector_index, _) = vector_column(&schema, column)?;
        let epoch = self.authority.source_snapshot(&rtx)?.epoch;
        let rows = rtx.open_owner_table(ROWS)?;
        read(&AnnRowReader {
            rows,
            table,
            schema,
            vector_index,
            epoch,
        })
    }
}

/// The position and declared width of `column`, which must be a vector column.
fn vector_column(schema: &TableSchema, column: &str) -> Result<(usize, Option<usize>), String> {
    schema
        .columns()
        .iter()
        .enumerate()
        .find(|(_, candidate)| candidate.name.eq_ignore_ascii_case(column))
        .and_then(|(index, candidate)| {
            if let ColumnType::Vector(dim) = candidate.ty {
                Some((index, dim))
            } else {
                None
            }
        })
        .ok_or_else(|| {
            format!(
                "ANN index column `{column}` is not a vector column of `{}`",
                schema.name
            )
        })
}

/// The vector held at `index` of a stored row.
fn vector_cell(cells: &[Cell], index: usize) -> Option<&[f32]> {
    if let Some(Cell::Vector(values)) = cells.get(index) {
        Some(values)
    } else {
        None
    }
}
