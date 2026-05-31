use ndarray::{Array1, Array2};
use rand::{SeedableRng};
use rand_chacha::ChaCha8Rng;
use rand_distr::{Distribution, StandardNormal};
use std::collections::{HashMap, HashSet};

pub fn batch_cosine_similarity(query: Vec<f32>, targets: Vec<Vec<f32>>) -> Vec<f32> {
    let query_vec = Array1::from_iter(query.into_iter());
    let query_norm = query_vec.dot(&query_vec).sqrt();

    if query_norm == 0.0 {
        return vec![0.0; targets.len()];
    }

    targets.into_iter().map(|t| {
        let t_vec = Array1::from_iter(t.into_iter());
        let t_norm = t_vec.dot(&t_vec).sqrt();
        if t_norm == 0.0 {
            0.0
        } else {
            query_vec.dot(&t_vec) / (query_norm * t_norm)
        }
    }).collect()
}

pub fn find_similar_pairs_dense(
    embeddings: Vec<Vec<f32>>,
    ids: Vec<String>,
    threshold: f32,
) -> Vec<(String, String, f32)> {
    let n = embeddings.len();
    if n == 0 {
        return Vec::new();
    }
    let dim = embeddings[0].len();

    let mut matrix = Array2::<f32>::zeros((n, dim));
    for (i, emb) in embeddings.iter().enumerate() {
        for (j, &val) in emb.iter().enumerate() {
            matrix[(i, j)] = val;
        }
    }

    let mut norms = Vec::with_capacity(n);
    for i in 0..n {
        let row = matrix.row(i);
        let norm = row.dot(&row).sqrt();
        norms.push(if norm == 0.0 { 1.0 } else { norm });
    }

    for i in 0..n {
        let norm = norms[i];
        for j in 0..dim {
            matrix[(i, j)] /= norm;
        }
    }

    let sim_matrix = matrix.dot(&matrix.t());

    let mut pairs = Vec::new();
    for i in 0..n {
        for j in (i + 1)..n {
            let sim = sim_matrix[(i, j)];
            if sim >= threshold {
                pairs.push((ids[i].clone(), ids[j].clone(), sim));
            }
        }
    }

    pairs
}

pub fn find_similar_pairs_lsh(
    embeddings: Vec<Vec<f32>>,
    ids: Vec<String>,
    threshold: f32,
    num_tables: usize,
    hash_size: usize,
    seed: u64,
) -> Vec<(String, String, f32)> {
    let n = embeddings.len();
    if n == 0 {
        return Vec::new();
    }
    let input_dim = embeddings[0].len();

    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut hyperplanes = Vec::with_capacity(num_tables);

    for _ in 0..num_tables {
        let mut table_planes = Array2::<f32>::zeros((hash_size, input_dim));
        for i in 0..hash_size {
            for j in 0..input_dim {
                table_planes[(i, j)] = StandardNormal.sample(&mut rng);
            }
        }
        hyperplanes.push(table_planes);
    }

    let mut tables: Vec<HashMap<String, HashSet<usize>>> = vec![HashMap::new(); num_tables];

    let hash_vector = |vec: &Array1<f32>, table_idx: usize| -> String {
        let planes = &hyperplanes[table_idx];
        let projections = planes.dot(vec);
        let mut bits = String::with_capacity(hash_size);
        for i in 0..hash_size {
            if projections[i] > 0.0 {
                bits.push('1');
            } else {
                bits.push('0');
            }
        }
        bits
    };

    let mut arr_embeddings = Vec::with_capacity(n);
    for emb in embeddings.iter() {
        arr_embeddings.push(Array1::from_iter(emb.iter().cloned()));
    }

    for i in 0..n {
        let vec = &arr_embeddings[i];
        for table_idx in 0..num_tables {
            let key = hash_vector(vec, table_idx);
            tables[table_idx].entry(key).or_default().insert(i);
        }
    }

    let mut pairs = Vec::new();
    let mut seen_pairs = HashSet::new();

    for i in 0..n {
        let vec = &arr_embeddings[i];
        let vec_norm = vec.dot(vec).sqrt();
        if vec_norm == 0.0 {
            continue;
        }

        let mut candidates = HashSet::new();
        for table_idx in 0..num_tables {
            let key = hash_vector(vec, table_idx);
            if let Some(bucket) = tables[table_idx].get(&key) {
                for &c in bucket {
                    if c != i {
                        candidates.insert(c);
                    }
                }
            }
        }

        for &c in candidates.iter() {
            let cand_vec = &arr_embeddings[c];
            let cand_norm = cand_vec.dot(cand_vec).sqrt();
            if cand_norm == 0.0 {
                continue;
            }

            let sim = vec.dot(cand_vec) / (vec_norm * cand_norm);
            if sim >= threshold {
                let mut min_idx = i;
                let mut max_idx = c;
                if min_idx > max_idx {
                    std::mem::swap(&mut min_idx, &mut max_idx);
                }

                let pair_key = (min_idx, max_idx);
                if !seen_pairs.contains(&pair_key) {
                    seen_pairs.insert(pair_key);
                    pairs.push((ids[min_idx].clone(), ids[max_idx].clone(), sim));
                }
            }
        }
    }

    pairs
}
