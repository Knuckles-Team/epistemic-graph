//! Code-prefix buckets, pause-step refinement and cosine ranking.

use std::collections::HashMap;

use super::{cosine, normalized, RecommendError, ResidualQuantizer};

/// Bucket neighbours each pause step moves toward.
const NEIGHBOURS: usize = 4;

/// One ranked catalog item: its catalog position, codes and cosine.
#[derive(Debug, Clone, PartialEq)]
pub struct Ranked {
    pub item: usize,
    pub codes: [u8; 4],
    pub score: f32,
}

/// A catalog encoded with frozen codebooks, bucketed by every code prefix.
#[derive(Debug, Clone)]
pub struct RecommendIndex {
    quantizer: ResidualQuantizer,
    vectors: Vec<Vec<f32>>,
    codes: Vec<[u8; 4]>,
    /// `(prefix length, prefix codes)` → catalog positions, ascending.
    buckets: HashMap<(usize, [u8; 4]), Vec<usize>>,
}

fn prefix(codes: [u8; 4], len: usize) -> [u8; 4] {
    let mut out = [0u8; 4];
    out[..len].copy_from_slice(&codes[..len]);
    out
}

impl RecommendIndex {
    /// Encode every item with `quantizer` and bucket it.
    pub fn build(
        quantizer: ResidualQuantizer,
        items: Vec<Vec<f32>>,
    ) -> Result<Self, RecommendError> {
        if items.is_empty() {
            return Err(RecommendError::EmptyCatalog);
        }
        let codes = items
            .iter()
            .map(|item| quantizer.encode(item))
            .collect::<Result<Vec<_>, _>>()?;
        let mut buckets: HashMap<(usize, [u8; 4]), Vec<usize>> = HashMap::new();
        for (position, code) in codes.iter().enumerate() {
            for len in 0..=quantizer.levels() {
                buckets
                    .entry((len, prefix(*code, len)))
                    .or_default()
                    .push(position);
            }
        }
        Ok(Self {
            quantizer,
            vectors: items,
            codes,
            buckets,
        })
    }

    pub fn codes(&self, item: usize) -> [u8; 4] {
        self.codes[item]
    }

    /// The longest-prefix bucket of `v` holding at least `want` items.
    fn bucket(&self, v: &[f32], want: usize) -> Result<&[usize], RecommendError> {
        let code = self.quantizer.encode(v)?;
        for len in (0..=self.quantizer.levels()).rev() {
            if let Some(items) = self.buckets.get(&(len, prefix(code, len))) {
                if items.len() >= want || len == 0 {
                    return Ok(items);
                }
            }
        }
        Ok(&[])
    }

    /// `pause_steps` refinements of `query`: each step moves the working
    /// vector halfway toward the mean of the history and of its nearest
    /// bucket items.
    pub fn refine(
        &self,
        query: &[f32],
        history: &[usize],
        pause_steps: u8,
    ) -> Result<Vec<f32>, RecommendError> {
        let dim = self.quantizer.dim();
        if query.len() != dim {
            return Err(RecommendError::DimensionMismatch {
                expected: dim,
                found: query.len(),
            });
        }
        let mut working = normalized(query.to_vec());
        for _ in 0..pause_steps {
            let mut pull = vec![0.0f32; dim];
            let mut count = 0usize;
            let mut nearest: Vec<(usize, f32)> = self
                .bucket(&working, NEIGHBOURS)?
                .iter()
                .map(|&i| (i, cosine(&working, &self.vectors[i])))
                .collect();
            nearest.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
            let anchors = history
                .iter()
                .copied()
                .filter(|&i| i < self.vectors.len())
                .chain(nearest.iter().take(NEIGHBOURS).map(|(i, _)| *i));
            for i in anchors {
                pull.iter_mut()
                    .zip(normalized(self.vectors[i].clone()))
                    .for_each(|(p, x)| *p += x);
                count += 1;
            }
            if count == 0 {
                break;
            }
            working = normalized(
                working
                    .iter()
                    .zip(pull)
                    .map(|(w, p)| 0.5 * w + 0.5 * p / count as f32)
                    .collect(),
            );
        }
        Ok(working)
    }

    /// The top `top_k` items by cosine within the candidate bucket of the
    /// refined query, history items excluded; ties break by catalog position.
    pub fn rank(
        &self,
        query: &[f32],
        history: &[usize],
        pause_steps: u8,
        top_k: usize,
    ) -> Result<Vec<Ranked>, RecommendError> {
        let working = self.refine(query, history, pause_steps)?;
        let mut scored: Vec<Ranked> = self
            .bucket(&working, top_k + history.len())?
            .iter()
            .copied()
            .filter(|i| !history.contains(i))
            .map(|item| Ranked {
                item,
                codes: self.codes[item],
                score: cosine(&working, &self.vectors[item]),
            })
            .collect();
        scored.sort_by(|a, b| b.score.total_cmp(&a.score).then(a.item.cmp(&b.item)));
        scored.truncate(top_k);
        Ok(scored)
    }
}
