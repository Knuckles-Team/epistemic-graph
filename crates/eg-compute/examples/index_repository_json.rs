//! Offline driver harness for the `IndexRepository` engine method.
//!
//! `Method::IndexRepository` is dispatched over the wire in `src/server/dispatch.rs`,
//! but the work it performs is `eg_compute::parser::resolve::index_repository` — a
//! pure, non-mutating, idempotent function over `(path, bytes)`. This example calls
//! that same function directly so an inventory pass can run with NO engine process,
//! no transport, and no wheel in the loop (the eg wheel is resolved by FILENAME
//! version elsewhere and can silently serve a stale engine).
//!
//! Usage:
//!   cargo run --release -p eg-compute --features ast,ast-extended \
//!       --example index_repository_json -- <request.json>
//!
//! Request (JSON):
//!   {"repository": "epistemic-graph",
//!    "root": "/abs/checkout",
//!    "files": ["rel/a.rs", "rel/b.py"],      // tracked, repo-relative, POSIX
//!    "out": "/abs/out/native-result.json"}
//!
//! Response (JSON, written to `out`): the verbatim `IndexResult` under `index`,
//! plus a per-file receipt (`sha256`, `bytes`, `parse_status`) so the caller can
//! prove the bytes it hashed are the bytes that were parsed.

use eg_compute::parser::resolve::index_repository;
use eg_compute::parser::tree_sitter::parse_file;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::Path;
use std::time::Instant;

#[derive(Deserialize)]
struct Request {
    repository: String,
    root: String,
    files: Vec<String>,
    out: String,
}

#[derive(Serialize)]
struct FileReceipt {
    path: String,
    sha256: String,
    bytes: usize,
    /// `parsed` | `unsupported` | `failed:<reason>` | `unreadable:<reason>`
    parse_status: String,
    symbols: usize,
}

#[derive(Serialize)]
struct Response {
    schema: &'static str,
    repository: String,
    root: String,
    files_requested: usize,
    files_read: usize,
    elapsed_index_ms: u128,
    elapsed_total_ms: u128,
    receipts: Vec<FileReceipt>,
    index: eg_compute::parser::resolve::IndexResult,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let t_all = Instant::now();
    let arg = std::env::args().nth(1).ok_or("usage: <request.json>")?;
    let req: Request = serde_json::from_slice(&std::fs::read(&arg)?)?;
    let out_path = req.out.clone();
    let root = Path::new(&req.root);

    // Read every requested file once; the SAME byte vectors are hashed, parsed for
    // the receipt, and shipped to `index_repository` — no re-read, no TOCTOU gap.
    let mut batch: Vec<(String, Vec<u8>)> = Vec::with_capacity(req.files.len());
    let mut receipts: Vec<FileReceipt> = Vec::with_capacity(req.files.len());
    for rel in &req.files {
        match std::fs::read(root.join(rel)) {
            Ok(bytes) => {
                let mut h = Sha256::new();
                h.update(&bytes);
                let digest = format!("{:x}", h.finalize());
                let (status, symbols) = match parse_file(rel, &bytes) {
                    Ok(r) => ("parsed".to_string(), r.symbols_extracted),
                    Err(e) if e == "Unsupported file extension" => ("unsupported".to_string(), 0),
                    Err(e) => (format!("failed:{e}"), 0),
                };
                receipts.push(FileReceipt {
                    path: rel.clone(),
                    sha256: digest,
                    bytes: bytes.len(),
                    parse_status: status,
                    symbols,
                });
                batch.push((rel.clone(), bytes));
            }
            Err(e) => receipts.push(FileReceipt {
                path: rel.clone(),
                sha256: String::new(),
                bytes: 0,
                parse_status: format!("unreadable:{e}"),
                symbols: 0,
            }),
        }
    }

    // The batch IS the resolution scope: ship the whole repository in one call so
    // intra-repo calls and imports bind.
    let t_index = Instant::now();
    let index = index_repository(&batch);
    let elapsed_index_ms = t_index.elapsed().as_millis();

    let resp = Response {
        schema: "eg-native-index/v1",
        repository: req.repository,
        root: req.root,
        files_requested: req.files.len(),
        files_read: batch.len(),
        elapsed_index_ms,
        elapsed_total_ms: t_all.elapsed().as_millis(),
        receipts,
        index,
    };
    let mut out = serde_json::to_vec(&resp)?;
    out.push(b'\n');
    std::fs::write(&out_path, &out)?;
    eprintln!(
        "index_repository: {} files, {} symbols, {} edges, {} ms",
        resp.files_read,
        resp.index.symbols_extracted,
        resp.index.edges.len(),
        elapsed_index_ms
    );
    Ok(())
}
