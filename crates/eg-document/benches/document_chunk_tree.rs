//! EG-REPO-INGEST-R652.6: chunk a 1 MiB Markdown document; JSON on stdout.
//! Run with `cargo bench -p eg-document --bench document_chunk_tree`.
//! Budget: p50 3 ms and p99 15 ms per document in the optimized profile.

use eg_document::processing::{chunk_text, ChunkingConfig};
use std::hint::black_box;
use std::time::Instant;

const SAMPLES: usize = 200;
const WARMUP: usize = 20;
const TARGET_BYTES: usize = 1024 * 1024;

fn markdown() -> String {
    let paragraph = "Retrieval addresses a passage by its offsets. Each window ends at a \
                     paragraph or sentence break, and windows overlap so a fact split across \
                     a cut is still whole in one chunk. Ünïcode text keeps exact offsets.\n\n";
    let mut text = String::with_capacity(TARGET_BYTES + 4096);
    let mut section = 0;
    while text.len() < TARGET_BYTES {
        section += 1;
        text.push_str(&format!("## Section {section}\n\n"));
        for _ in 0..6 {
            text.push_str(paragraph);
        }
    }
    text
}

fn percentile(sorted: &[u128], p: f64) -> u128 {
    sorted[((sorted.len() - 1) as f64 * p).round() as usize]
}

fn main() {
    let text = markdown();
    let config = ChunkingConfig::default();
    for _ in 0..WARMUP {
        black_box(chunk_text(black_box(&text), &config).unwrap());
    }
    let mut micros = Vec::with_capacity(SAMPLES);
    let mut chunks = 0;
    for _ in 0..SAMPLES {
        let start = Instant::now();
        chunks = black_box(chunk_text(black_box(&text), &config).unwrap()).len();
        micros.push(start.elapsed().as_micros());
    }
    micros.sort_unstable();
    let (p50, p99) = (percentile(&micros, 0.50), percentile(&micros, 0.99));
    println!(
        "{{\"bench\":\"document_chunk_tree\",\"bytes\":{},\"chunks\":{chunks},\"samples\":{SAMPLES},\
         \"p50_us\":{p50},\"p99_us\":{p99},\"budget_p50_us\":3000,\"budget_p99_us\":15000,\
         \"optimized\":{}}}",
        text.len(),
        !cfg!(debug_assertions)
    );
    if !cfg!(debug_assertions) {
        assert!(
            p50 <= 3_000 && p99 <= 15_000,
            "chunking exceeded its budget"
        );
    }
}
