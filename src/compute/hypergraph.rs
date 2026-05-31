use nalgebra::DMatrix;
use rand::{SeedableRng};
use rand_chacha::ChaCha8Rng;
use rand_distr::{Distribution, StandardNormal};
use std::collections::HashMap;
use std::sync::Mutex;

lazy_static::lazy_static! {
    static ref ENCODER_CACHE: Mutex<HashMap<String, PositionalInteractionEncoder>> = Mutex::new(HashMap::new());
}

pub struct PositionalInteractionEncoder {
    pos_dim: usize,
    hidden_dim: usize,
    out_dim: usize,
    seed: u64,
    w1: DMatrix<f64>,
    b1: DMatrix<f64>,
    w2: DMatrix<f64>,
    b2: DMatrix<f64>,
}

impl PositionalInteractionEncoder {
    pub fn new(pos_dim: usize, hidden_dim: usize, out_dim: usize, seed: u64) -> Self {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let in_dim = pos_dim * 2;

        let w1_scale = (2.0 / in_dim as f64).sqrt();
        let mut w1 = DMatrix::zeros(in_dim, hidden_dim);
        for i in 0..in_dim {
            for j in 0..hidden_dim {
                let val: f64 = StandardNormal.sample(&mut rng);
                w1[(i, j)] = val * w1_scale;
            }
        }
        let b1 = DMatrix::zeros(1, hidden_dim);

        let w2_scale = (2.0 / hidden_dim as f64).sqrt();
        let mut w2 = DMatrix::zeros(hidden_dim, out_dim);
        for i in 0..hidden_dim {
            for j in 0..out_dim {
                let val: f64 = StandardNormal.sample(&mut rng);
                w2[(i, j)] = val * w2_scale;
            }
        }
        let b2 = DMatrix::zeros(1, out_dim);

        Self {
            pos_dim,
            hidden_dim,
            out_dim,
            seed,
            w1,
            b1,
            w2,
            b2,
        }
    }

    fn sinusoidal_encoding(&self, pos: usize) -> DMatrix<f64> {
        let mut encoding = DMatrix::zeros(1, self.pos_dim);
        let mut i = 0;
        while i < self.pos_dim {
            let denominator = 10000.0_f64.powf(i as f64 / self.pos_dim as f64);
            encoding[(0, i)] = (pos as f64 / denominator).sin();
            if i + 1 < self.pos_dim {
                encoding[(0, i + 1)] = (pos as f64 / denominator).cos();
            }
            i += 2;
        }
        encoding
    }

    fn relu(&self, mut x: DMatrix<f64>) -> DMatrix<f64> {
        for v in x.iter_mut() {
            *v = v.max(0.0);
        }
        x
    }

    pub fn encode_interaction(&self, pos_a: usize, pos_b: usize) -> Vec<f64> {
        let pa = self.sinusoidal_encoding(pos_a);
        let pb = self.sinusoidal_encoding(pos_b);

        let mut x = DMatrix::zeros(1, self.pos_dim * 2);
        for i in 0..self.pos_dim {
            x[(0, i)] = pa[(0, i)];
            x[(0, self.pos_dim + i)] = pb[(0, i)];
        }

        let z1 = &x * &self.w1 + &self.b1;
        let a1 = self.relu(z1);

        let z2 = &a1 * &self.w2 + &self.b2;

        let mut result = Vec::with_capacity(self.out_dim);
        for i in 0..self.out_dim {
            result.push(z2[(0, i)]);
        }
        result
    }
}

pub fn get_or_create_encoder(pos_dim: usize, hidden_dim: usize, out_dim: usize, seed: u64) -> Vec<f64> {
    let key = format!("{}-{}-{}-{}", pos_dim, hidden_dim, out_dim, seed);
    let mut cache = ENCODER_CACHE.lock().unwrap();
    if !cache.contains_key(&key) {
        let encoder = PositionalInteractionEncoder::new(pos_dim, hidden_dim, out_dim, seed);
        cache.insert(key.clone(), encoder);
    }
    let encoder = cache.get(&key).unwrap();
    encoder.encode_interaction(1, 2)
}
