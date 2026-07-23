//! One-shot bulk-load SQLite `.db` writer. Rows are known in full, in advance, and
//! assigned sequential rowids (1..=N), so the b-tree is built bottom-up (pack leaves,
//! then interior levels from leaf boundaries) — a classic B+tree bulk loader, NOT an
//! online mutable-b-tree balancer. `sqlite_schema` is written into page 1 last; the
//! header is finalized in `finish()`. Rowid tables only.
//!
//! Interior-page rule (the easy-to-invert trap): a table-interior page with N children
//! has N-1 cells; each cell's key is the LARGEST rowid in that child's subtree, and the
//! Nth (last) child is the `rightmost_ptr`, referenced by no cell. Verified against a
//! real `sqlite3 PRAGMA integrity_check` in the facade's differential test.

use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::header::{DatabaseHeader, HEADER_SIZE};
use crate::overflow::table_leaf_split;
use crate::page::{write_interior_header, write_leaf_header, PageType};
use crate::record::encode_record;
use crate::value::{ColumnDef, Row};
use crate::varint::write_varint;

const DEFAULT_PAGE_SIZE: u32 = 4096;
const SCHEMA_ROOT_PAGE: u32 = 1;
const PAGE1_HEADER_OFFSET: usize = HEADER_SIZE; // page 1's b-tree header follows the file header

struct PendingTable {
    name: String,
    columns: Vec<ColumnDef>,
    rows: Vec<Row>,
}

/// A pager that materializes every page in memory; page numbers are 1-based.
struct Pager {
    page_size: usize,
    pages: Vec<Vec<u8>>,
}

impl Pager {
    fn new(page_size: usize) -> Self {
        Pager {
            page_size,
            pages: Vec::new(),
        }
    }

    /// Allocate a fresh zeroed page, returning its 1-based number.
    fn alloc(&mut self) -> u32 {
        self.pages.push(vec![0u8; self.page_size]);
        self.pages.len() as u32
    }

    fn page_mut(&mut self, page_no: u32) -> &mut [u8] {
        &mut self.pages[page_no as usize - 1]
    }

    fn count(&self) -> u32 {
        self.pages.len() as u32
    }
}

/// One packed leaf's cell bytes plus the largest rowid it covers.
struct LeafGroup {
    cells: Vec<Vec<u8>>,
    max_rowid: i64,
}

/// A child reference used when building interior levels.
#[derive(Clone, Copy)]
struct Child {
    page_no: u32,
    max_rowid: i64,
}

/// Where a b-tree's root page should land.
enum RootTarget {
    /// Allocate a fresh page for the root (ordinary user table).
    Alloc,
    /// Emit the root into this pre-reserved page at this header offset (sqlite_schema→page 1).
    Fixed { page_no: u32, header_offset: usize },
}

pub struct Writer {
    path: PathBuf,
    page_size: u32,
    tables: Vec<PendingTable>,
}

impl Writer {
    /// Create a writer targeting `path`. `page_size` must be a power of two in [512,65536].
    pub fn create(path: &Path, page_size: u32) -> Result<Self> {
        let page_size = if page_size == 0 {
            DEFAULT_PAGE_SIZE
        } else {
            page_size
        };
        if !(512..=65_536).contains(&page_size) || !page_size.is_power_of_two() {
            return Err(Error::unsupported(
                "page size must be a power of two in [512,65536]",
            ));
        }
        Ok(Writer {
            path: path.to_path_buf(),
            page_size,
            tables: Vec::new(),
        })
    }

    /// Declare a table (in the order it will appear in `sqlite_schema`).
    pub fn add_table(&mut self, name: &str, columns: &[ColumnDef]) -> Result<()> {
        if self.tables.iter().any(|t| t.name == name) {
            return Err(Error::unsupported(format!("duplicate table `{name}`")));
        }
        self.tables.push(PendingTable {
            name: name.to_string(),
            columns: columns.to_vec(),
            rows: Vec::new(),
        });
        Ok(())
    }

    /// Append `rows` to a previously-added table. Returns the number appended.
    pub fn insert_rows(&mut self, table: &str, rows: &[Row]) -> Result<usize> {
        let t = self
            .tables
            .iter_mut()
            .find(|t| t.name == table)
            .ok_or_else(|| Error::unsupported(format!("insert into unknown table `{table}`")))?;
        t.rows.extend_from_slice(rows);
        Ok(rows.len())
    }

    /// Serialize every table to a valid `.db` file and fsync it.
    pub fn finish(self) -> Result<()> {
        let usable = self.page_size as usize; // reserved space is always 0 for the writer
        let mut pager = Pager::new(self.page_size as usize);
        // Reserve page 1 for sqlite_schema's root.
        let _page1 = pager.alloc();
        debug_assert_eq!(_page1, SCHEMA_ROOT_PAGE);

        // Build each user table's b-tree; record (name, sql, root_page).
        let mut schema_entries: Vec<(String, String, u32)> = Vec::with_capacity(self.tables.len());
        for t in &self.tables {
            let rows: Vec<(i64, Vec<u8>)> = t
                .rows
                .iter()
                .enumerate()
                .map(|(i, row)| ((i as i64) + 1, encode_record(row)))
                .collect();
            let root = build_btree(
                &mut pager,
                self.page_size as usize,
                usable,
                0,
                rows,
                RootTarget::Alloc,
            )?;
            schema_entries.push((t.name.clone(), create_table_ddl(&t.name, &t.columns), root));
        }

        // Build sqlite_schema rows (one per table), rooted at page 1.
        let schema_rows: Vec<(i64, Vec<u8>)> = schema_entries
            .iter()
            .enumerate()
            .map(|(i, (name, sql, root))| {
                let record = encode_record(&[
                    crate::value::Value::Text("table".to_string()),
                    crate::value::Value::Text(name.clone()),
                    crate::value::Value::Text(name.clone()),
                    crate::value::Value::Integer(*root as i64),
                    crate::value::Value::Text(sql.clone()),
                ]);
                ((i as i64) + 1, record)
            })
            .collect();
        // Cap-reduce 100 so any schema page also fits page 1's 100-byte-offset root slot.
        build_btree(
            &mut pager,
            self.page_size as usize,
            usable,
            PAGE1_HEADER_OFFSET,
            schema_rows,
            RootTarget::Fixed {
                page_no: SCHEMA_ROOT_PAGE,
                header_offset: PAGE1_HEADER_OFFSET,
            },
        )?;

        // Finalize the database header into page 1's first 100 bytes.
        let header = DatabaseHeader {
            page_size: self.page_size,
            write_version: 1, // legacy/rollback-journal — the writer never produces a WAL
            read_version: 1,
            reserved_space: 0,
            change_counter: 1,
            database_size: pager.count(),
            freelist_trunk_page: 0,
            freelist_pages: 0,
            schema_cookie: 1,
            schema_format: 4,
            text_encoding: 1, // UTF-8
        };
        header.encode(&mut pager.page_mut(SCHEMA_ROOT_PAGE)[..HEADER_SIZE]);

        // Write all pages sequentially, then fsync.
        use std::io::Write as _;
        let file = std::fs::File::create(&self.path)?;
        let mut w = std::io::BufWriter::new(file);
        for page in &pager.pages {
            w.write_all(page)?;
        }
        w.flush()?;
        let file = w.into_inner().map_err(|e| Error::Io(e.into_error()))?;
        file.sync_all()?;
        Ok(())
    }
}

/// Build a table b-tree bottom-up. `cap_reduce` shrinks every page's usable region (100
/// for the schema tree so any of its pages fits page 1's header-offset slot). Returns the
/// root page number.
fn build_btree(
    pager: &mut Pager,
    page_size: usize,
    usable: usize,
    cap_reduce: usize,
    rows: Vec<(i64, Vec<u8>)>,
    root_target: RootTarget,
) -> Result<u32> {
    // Phase 1: pack rows into leaf groups (building cells + overflow chains eagerly).
    let leaves = pack_leaves(pager, page_size, usable, cap_reduce, rows)?;

    // Single leaf → it is the root.
    if leaves.len() == 1 {
        let (page_no, header_offset) = resolve_target(pager, &root_target);
        emit_leaf(pager, page_size, page_no, header_offset, &leaves[0].cells)?;
        return Ok(page_no);
    }

    // Multi-leaf: emit each leaf to a fresh page, collect child boundaries.
    let mut children: Vec<Child> = Vec::with_capacity(leaves.len());
    for leaf in &leaves {
        let page_no = pager.alloc();
        emit_leaf(pager, page_size, page_no, 0, &leaf.cells)?;
        children.push(Child {
            page_no,
            max_rowid: leaf.max_rowid,
        });
    }

    // Phase 2: build interior levels until a single root remains.
    loop {
        let interiors = build_interior_level(
            pager,
            page_size,
            usable,
            cap_reduce,
            &children,
            &root_target,
        )?;
        if interiors.len() == 1 {
            return Ok(interiors[0].page_no);
        }
        children = interiors;
    }
}

/// Resolve where the root page goes and reserve/allocate it.
fn resolve_target(pager: &mut Pager, target: &RootTarget) -> (u32, usize) {
    match target {
        RootTarget::Alloc => (pager.alloc(), 0),
        RootTarget::Fixed {
            page_no,
            header_offset,
        } => (*page_no, *header_offset),
    }
}

/// Pack rows into leaf groups. Each cell is built with overflow handled; a group is closed
/// when the next cell would not fit the (cap-reduced) page.
fn pack_leaves(
    pager: &mut Pager,
    page_size: usize,
    usable: usize,
    cap_reduce: usize,
    rows: Vec<(i64, Vec<u8>)>,
) -> Result<Vec<LeafGroup>> {
    // Space available for cells + their pointers on a leaf page.
    let leaf_space = usable
        .checked_sub(cap_reduce + 8)
        .ok_or_else(|| Error::corrupt("page too small"))?;

    let mut groups: Vec<LeafGroup> = Vec::new();
    let mut cur: Vec<Vec<u8>> = Vec::new();
    let mut cur_used = 0usize;
    let mut cur_max = 0i64;

    for (rowid, payload) in rows {
        let cell = build_table_leaf_cell(pager, page_size, usable, rowid, &payload)?;
        let need = cell.len() + 2;
        if need > leaf_space {
            return Err(Error::corrupt("leaf cell larger than a page"));
        }
        if !cur.is_empty() && cur_used + need > leaf_space {
            groups.push(LeafGroup {
                cells: std::mem::take(&mut cur),
                max_rowid: cur_max,
            });
            cur_used = 0;
        }
        cur_used += need;
        cur_max = rowid;
        cur.push(cell);
    }
    // Always emit at least one (possibly empty) leaf — an empty table still has a root page.
    groups.push(LeafGroup {
        cells: cur,
        max_rowid: cur_max,
    });
    Ok(groups)
}

/// Build one interior level from `children`; emits the single top page to `root_target`.
fn build_interior_level(
    pager: &mut Pager,
    page_size: usize,
    usable: usize,
    cap_reduce: usize,
    children: &[Child],
    root_target: &RootTarget,
) -> Result<Vec<Child>> {
    let interior_space = usable
        .checked_sub(cap_reduce + 12)
        .ok_or_else(|| Error::corrupt("page too small"))?;

    // Partition children into groups where (group.len()-1) cells fit.
    let mut groups: Vec<(usize, usize)> = Vec::new(); // [start, end)
    let mut start = 0usize;
    let mut used = 0usize;
    let mut j = start + 1;
    while j <= children.len() {
        if j == children.len() {
            groups.push((start, j));
            break;
        }
        // Extending to include children[j] turns children[j-1] into a cell.
        let cell_cost = 4 + crate::varint::varint_len(children[j - 1].max_rowid as u64) + 2;
        if used + cell_cost > interior_space {
            groups.push((start, j));
            start = j;
            used = 0;
        } else {
            used += cell_cost;
        }
        j += 1;
    }

    // Avoid a trailing single-child interior page (0 cells): steal one from the prior group.
    if groups.len() >= 2 {
        let last = groups.len() - 1;
        if groups[last].1 - groups[last].0 == 1 {
            groups[last - 1].1 -= 1;
            groups[last].0 -= 1;
        }
    }

    let is_root_level = groups.len() == 1;
    let mut out = Vec::with_capacity(groups.len());
    for (gi, (gstart, gend)) in groups.iter().copied().enumerate() {
        let group = &children[gstart..gend];
        let (page_no, header_offset) = if is_root_level && gi == 0 {
            resolve_target(pager, root_target)
        } else {
            (pager.alloc(), 0)
        };
        emit_interior(pager, page_size, page_no, header_offset, group)?;
        out.push(Child {
            page_no,
            max_rowid: group.last().unwrap().max_rowid,
        });
    }
    Ok(out)
}

/// Assemble a table-leaf cell: `[payload_size varint][rowid varint][local payload][overflow ptr?]`.
fn build_table_leaf_cell(
    pager: &mut Pager,
    page_size: usize,
    usable: usize,
    rowid: i64,
    payload: &[u8],
) -> Result<Vec<u8>> {
    let mut cell = Vec::new();
    write_varint(&mut cell, payload.len() as u64);
    write_varint(&mut cell, rowid as u64);

    let (overflows, local) = table_leaf_split(payload.len(), usable);
    if !overflows {
        cell.extend_from_slice(payload);
    } else {
        cell.extend_from_slice(&payload[..local]);
        let first = build_overflow_chain(pager, page_size, usable, &payload[local..])?;
        cell.extend_from_slice(&first.to_be_bytes());
    }
    Ok(cell)
}

/// Write an overflow chain for `data`, returning the first overflow page number.
/// Each page is `[next_page u32 BE][chunk]`; the last page's `next_page` is 0.
fn build_overflow_chain(
    pager: &mut Pager,
    _page_size: usize,
    usable: usize,
    data: &[u8],
) -> Result<u32> {
    let chunk = usable - 4;
    let n_pages = data.len().div_ceil(chunk);
    // Pre-allocate the pages so we know each page's successor.
    let page_nos: Vec<u32> = (0..n_pages).map(|_| pager.alloc()).collect();
    for (i, &page_no) in page_nos.iter().enumerate() {
        let next = if i + 1 < n_pages { page_nos[i + 1] } else { 0 };
        let start = i * chunk;
        let end = (start + chunk).min(data.len());
        let buf = pager.page_mut(page_no);
        buf[0..4].copy_from_slice(&next.to_be_bytes());
        buf[4..4 + (end - start)].copy_from_slice(&data[start..end]);
    }
    Ok(page_nos[0])
}

/// Emit a table-leaf page: cells packed from the page end, pointer array after the header.
fn emit_leaf(
    pager: &mut Pager,
    page_size: usize,
    page_no: u32,
    header_offset: usize,
    cells: &[Vec<u8>],
) -> Result<()> {
    let buf = pager.page_mut(page_no);
    let mut content = page_size;
    let ptr_base = header_offset + 8;
    for (i, cell) in cells.iter().enumerate() {
        content -= cell.len();
        buf[content..content + cell.len()].copy_from_slice(cell);
        let ptr = content as u16;
        buf[ptr_base + i * 2..ptr_base + i * 2 + 2].copy_from_slice(&ptr.to_be_bytes());
    }
    write_leaf_header(
        buf,
        header_offset,
        PageType::TableLeaf,
        cells.len() as u16,
        content,
    );
    Ok(())
}

/// Emit a table-interior page: N children → N-1 cells (`[child u32][max_rowid varint]`)
/// plus the last child as `rightmost_ptr`.
fn emit_interior(
    pager: &mut Pager,
    page_size: usize,
    page_no: u32,
    header_offset: usize,
    children: &[Child],
) -> Result<()> {
    debug_assert!(!children.is_empty());
    let rightmost = children.last().unwrap().page_no;
    let mut cells: Vec<Vec<u8>> = Vec::with_capacity(children.len() - 1);
    for child in &children[..children.len() - 1] {
        let mut cell = Vec::with_capacity(13);
        cell.extend_from_slice(&child.page_no.to_be_bytes());
        write_varint(&mut cell, child.max_rowid as u64);
        cells.push(cell);
    }

    let buf = pager.page_mut(page_no);
    let mut content = page_size;
    let ptr_base = header_offset + 12;
    for (i, cell) in cells.iter().enumerate() {
        content -= cell.len();
        buf[content..content + cell.len()].copy_from_slice(cell);
        let ptr = content as u16;
        buf[ptr_base + i * 2..ptr_base + i * 2 + 2].copy_from_slice(&ptr.to_be_bytes());
    }
    write_interior_header(
        buf,
        header_offset,
        PageType::TableInterior,
        cells.len() as u16,
        content,
        rightmost,
    );
    Ok(())
}

/// `CREATE TABLE "name" ("col" TYPE, …)` for the stored `sqlite_schema.sql`.
fn create_table_ddl(name: &str, columns: &[ColumnDef]) -> String {
    let cols: Vec<String> = columns
        .iter()
        .map(|c| {
            if c.decl_type.is_empty() {
                quote_ident(&c.name)
            } else {
                format!("{} {}", quote_ident(&c.name), c.decl_type)
            }
        })
        .collect();
    format!("CREATE TABLE {} ({})", quote_ident(name), cols.join(", "))
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
