//! Private IVF-PQ probing, ADC scoring, and SQ8 refinement phases.

use crate::ivfpq::{AdcCandidate, IvfPq, SearchParams, SearchResult, PQ_KSUB};

use super::{cell_distance_cmp, distance_cmp, retain_best_sorted_by, retain_best_unordered_by};

pub(super) fn search(
    index: &IvfPq,
    query: &[f32],
    k: usize,
    params: SearchParams,
) -> Vec<SearchResult> {
    search_filtered(index, query, k, params, None)
}

pub(super) fn search_filtered(
    index: &IvfPq,
    query: &[f32],
    k: usize,
    params: SearchParams,
    allow: Option<&dyn Fn(u64) -> bool>,
) -> Vec<SearchResult> {
    if k == 0 {
        return Vec::new();
    }
    let rq = super::rotate(&index.rotation, query, index.dim);
    let cells = probe_cells(index, &rq, params.nprobe);
    let mut candidates = Vec::new();
    let mut qresid = vec![0.0f32; index.dim];
    let mut table = vec![0.0f32; index.m * PQ_KSUB];
    for &(cell, _) in &cells {
        fill_query_residual(index, &rq, cell, &mut qresid);
        fill_adc_table(index, &qresid, &mut table);
        scan_postings(index, cell, &table, allow, &mut candidates);
    }
    finish_search(index, &rq, k, params, candidates)
}

fn probe_cells(index: &IvfPq, query: &[f32], nprobe: usize) -> Vec<(usize, f32)> {
    let mut cells: Vec<(usize, f32)> = (0..index.nlist)
        .map(|cell| {
            (
                cell,
                super::sq_dist(
                    query,
                    &index.coarse_centroids[cell * index.dim..(cell + 1) * index.dim],
                ),
            )
        })
        .collect();
    let nprobe = nprobe.max(1).min(cells.len());
    retain_best_sorted_by(&mut cells, nprobe, cell_distance_cmp);
    cells
}

fn fill_query_residual(index: &IvfPq, query: &[f32], cell: usize, residual: &mut [f32]) {
    let base = cell * index.dim;
    for dimension in 0..index.dim {
        residual[dimension] = query[dimension] - index.coarse_centroids[base + dimension];
    }
}

fn fill_adc_table(index: &IvfPq, residual: &[f32], table: &mut [f32]) {
    for subquantizer in 0..index.m {
        let subquery = &residual[subquantizer * index.dsub..(subquantizer + 1) * index.dsub];
        let book_base = subquantizer * PQ_KSUB * index.dsub;
        for codebook_entry in 0..PQ_KSUB {
            let centroid = book_base + codebook_entry * index.dsub;
            let mut distance = 0.0f32;
            for (query_value, centroid_value) in subquery
                .iter()
                .zip(&index.pq_centroids[centroid..centroid + index.dsub])
            {
                let difference = query_value - centroid_value;
                distance += difference * difference;
            }
            table[subquantizer * PQ_KSUB + codebook_entry] = distance;
        }
    }
}

fn scan_postings(
    index: &IvfPq,
    cell: usize,
    table: &[f32],
    allow: Option<&dyn Fn(u64) -> bool>,
    candidates: &mut Vec<AdcCandidate>,
) {
    for &row_number in &index.postings[cell] {
        let row = row_number as usize;
        if index.deleted[row] == 1 || !is_allowed(index, row, allow) {
            continue;
        }
        candidates.push(AdcCandidate {
            row,
            distance: adc_distance(index, row, table),
        });
    }
}

#[inline]
fn is_allowed(index: &IvfPq, row: usize, allow: Option<&dyn Fn(u64) -> bool>) -> bool {
    allow.is_none_or(|predicate| predicate(index.ids[row]))
}

fn adc_distance(index: &IvfPq, row: usize, table: &[f32]) -> f32 {
    let base = row * index.m;
    let mut distance = 0.0f32;
    for subquantizer in 0..index.m {
        let code = index.codes[base + subquantizer] as usize;
        distance += table[subquantizer * PQ_KSUB + code];
    }
    distance
}

fn finish_search(
    index: &IvfPq,
    query: &[f32],
    k: usize,
    params: SearchParams,
    mut candidates: Vec<AdcCandidate>,
) -> Vec<SearchResult> {
    let adc_cmp = |left: &AdcCandidate, right: &AdcCandidate| {
        distance_cmp(left.distance, right.distance)
            .then_with(|| index.ids[left.row].cmp(&index.ids[right.row]))
            .then_with(|| left.row.cmp(&right.row))
    };
    if !params.refine || index.sq_codes.is_empty() {
        retain_best_sorted_by(&mut candidates, k, adc_cmp);
        return candidates
            .into_iter()
            .map(|candidate| SearchResult {
                id: index.ids[candidate.row],
                distance: candidate.distance,
            })
            .collect();
    }

    let keep = params
        .refine_factor
        .max(1)
        .saturating_mul(k)
        .min(candidates.len());
    retain_best_unordered_by(&mut candidates, keep, adc_cmp);
    let mut refined: Vec<SearchResult> = candidates
        .into_iter()
        .map(|candidate| SearchResult {
            id: index.ids[candidate.row],
            distance: sq8_distance(index, query, candidate.row),
        })
        .collect();
    retain_best_sorted_by(&mut refined, k, super::search_result_cmp);
    refined
}

#[inline]
fn sq8_distance(index: &IvfPq, query: &[f32], row: usize) -> f32 {
    let base = row * index.dim;
    let minimum = index.sq_min[row];
    let scale = index.sq_scale[row];
    let mut distance = 0.0f32;
    for (query_value, code) in query.iter().zip(&index.sq_codes[base..base + index.dim]) {
        let value = minimum + (*code as f32) * scale;
        let difference = query_value - value;
        distance += difference * difference;
    }
    distance
}
