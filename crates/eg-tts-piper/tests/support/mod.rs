//! Test-only fixture builders: a hand-rolled minimal ONNX `ModelProto` protobuf
//! encoder and a matching Piper JSON config writer.
//!
//! No `onnx`/tooling dependency and no committed binary fixture — mirrors
//! `eg-audio`'s own `wav_fixture()` precedent (`crates/eg-audio/src/runtime.rs`) of
//! constructing valid file bytes programmatically in Rust at test time.
//!
//! The fixture graph is intentionally NOT a trained voice: it is a `Tile` of a
//! caller-supplied float32 template by `input_lengths[0]`, so output length scales
//! deterministically with phoneme count (proving "plausible duration for input text")
//! and output VALUES are exactly the template repeated (letting tests engineer clean,
//! clipping, and non-finite audio on demand). This deliberately contains no trained
//! weights and makes no claim to be a usable voice — it exists only to drive a REAL
//! `ort`/`piper-rs` inference call through this crate's real code paths.

use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

// ---- minimal protobuf wire-format writer -----------------------------------------

fn varint(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            buf.push(byte);
            break;
        }
        buf.push(byte | 0x80);
    }
}

fn tag(buf: &mut Vec<u8>, field: u32, wire_type: u8) {
    varint(buf, (u64::from(field) << 3) | u64::from(wire_type));
}

fn field_varint(buf: &mut Vec<u8>, field: u32, v: i64) {
    tag(buf, field, 0);
    varint(buf, v as u64);
}

fn field_bytes(buf: &mut Vec<u8>, field: u32, data: &[u8]) {
    tag(buf, field, 2);
    varint(buf, data.len() as u64);
    buf.extend_from_slice(data);
}

fn field_string(buf: &mut Vec<u8>, field: u32, s: &str) {
    field_bytes(buf, field, s.as_bytes());
}

fn field_message(buf: &mut Vec<u8>, field: u32, msg: &[u8]) {
    field_bytes(buf, field, msg);
}

fn field_packed_i64(buf: &mut Vec<u8>, field: u32, values: &[i64]) {
    let mut inner = Vec::new();
    for &v in values {
        varint(&mut inner, v as u64);
    }
    field_bytes(buf, field, &inner);
}

// ---- ONNX message builders (onnx.proto3, field numbers per the public spec) ------

const ELEM_TYPE_FLOAT: i64 = 1;
const ELEM_TYPE_INT64: i64 = 7;

/// `ValueInfoProto` with a dtype-only `TypeProto.Tensor` (no shape — fully dynamic).
fn value_info(name: &str, elem_type: i64) -> Vec<u8> {
    let mut tensor_type = Vec::new();
    field_varint(&mut tensor_type, 1, elem_type); // Tensor.elem_type = 1
    let mut type_proto = Vec::new();
    field_message(&mut type_proto, 1, &tensor_type); // TypeProto.tensor_type = 1
    let mut vi = Vec::new();
    field_string(&mut vi, 1, name); // ValueInfoProto.name = 1
    field_message(&mut vi, 2, &type_proto); // ValueInfoProto.type = 2
    vi
}

/// `TensorProto` initializer carrying `template`'s raw float32 LE bytes.
fn tensor_initializer(name: &str, template: &[f32]) -> Vec<u8> {
    let mut t = Vec::new();
    field_packed_i64(&mut t, 1, &[template.len() as i64]); // dims = 1 (repeated int64, packed)
    field_varint(&mut t, 2, ELEM_TYPE_FLOAT); // data_type = 2
    field_string(&mut t, 8, name); // name = 8
    let mut raw = Vec::with_capacity(template.len() * 4);
    for &v in template {
        raw.extend_from_slice(&v.to_le_bytes());
    }
    field_bytes(&mut t, 9, &raw); // raw_data = 9
    t
}

/// `NodeProto` for `Tile(template, input_lengths) -> audio`.
fn tile_node() -> Vec<u8> {
    let mut n = Vec::new();
    field_string(&mut n, 1, "template"); // input (repeated string) = 1
    field_string(&mut n, 1, "input_lengths");
    field_string(&mut n, 2, "audio"); // output (repeated string) = 2
    field_string(&mut n, 3, "tile_audio"); // name = 3
    field_string(&mut n, 4, "Tile"); // op_type = 4
    n
}

fn graph_proto(template: &[f32]) -> Vec<u8> {
    let mut g = Vec::new();
    field_message(&mut g, 1, &tile_node()); // node (repeated) = 1
    field_string(&mut g, 2, "eg-tts-piper-fixture-graph"); // name = 2
    field_message(&mut g, 5, &tensor_initializer("template", template)); // initializer (repeated) = 5
    field_message(&mut g, 11, &value_info("input", ELEM_TYPE_INT64)); // input (repeated) = 11
    field_message(&mut g, 11, &value_info("input_lengths", ELEM_TYPE_INT64));
    field_message(&mut g, 11, &value_info("scales", ELEM_TYPE_FLOAT));
    field_message(&mut g, 12, &value_info("audio", ELEM_TYPE_FLOAT)); // output (repeated) = 12
    g
}

fn opset_import(version: i64) -> Vec<u8> {
    let mut o = Vec::new();
    field_string(&mut o, 1, ""); // domain = 1 (default/ai.onnx)
    field_varint(&mut o, 2, version); // version = 2
    o
}

/// Build a complete, minimal, valid ONNX `ModelProto` byte stream implementing
/// `audio = Tile(template, input_lengths)`. `input` and `scales` are declared graph
/// inputs (matching piper-rs's `ort::inputs![input_t, lengths_t, scales_t]` positional
/// binding) but are never referenced by the single node — ONNX permits an unused
/// declared input.
pub fn build_fixture_onnx(template: &[f32]) -> Vec<u8> {
    let mut m = Vec::new();
    field_varint(&mut m, 1, 8); // ir_version = 1 (IR version 8 == ONNX 1.13-era)
    field_message(&mut m, 8, &opset_import(13)); // opset_import (repeated) = 8
    field_string(&mut m, 2, "eg-tts-piper-fixture"); // producer_name = 2
    field_message(&mut m, 7, &graph_proto(template)); // graph = 7
    m
}

// ---- Piper JSON config -------------------------------------------------------------

pub struct FixtureConfig {
    pub sample_rate: u32,
    pub num_speakers: u32,
    pub speaker_id_map: HashMap<String, i64>,
    /// Every non-BOS/EOS/PAD phoneme character this fixture's config recognizes.
    pub phoneme_chars: Vec<char>,
}

impl FixtureConfig {
    pub fn single_speaker(phoneme_chars: &[char]) -> Self {
        Self {
            sample_rate: 22_050,
            num_speakers: 1,
            speaker_id_map: HashMap::new(),
            phoneme_chars: phoneme_chars.to_vec(),
        }
    }

    pub fn to_json(&self) -> String {
        let mut phoneme_id_map = serde_json::Map::new();
        phoneme_id_map.insert("^".to_string(), serde_json::json!([1]));
        phoneme_id_map.insert("$".to_string(), serde_json::json!([2]));
        phoneme_id_map.insert("_".to_string(), serde_json::json!([0]));
        for (i, ch) in self.phoneme_chars.iter().enumerate() {
            phoneme_id_map.insert(ch.to_string(), serde_json::json!([10 + i as i64]));
        }
        let speaker_id_map: serde_json::Map<String, serde_json::Value> = self
            .speaker_id_map
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::json!(v)))
            .collect();
        serde_json::json!({
            "audio": {"sample_rate": self.sample_rate},
            "espeak": {"voice": "en-us"},
            "inference": {"noise_scale": 0.667, "length_scale": 1.0, "noise_w": 0.8},
            "num_speakers": self.num_speakers,
            "speaker_id_map": speaker_id_map,
            "phoneme_id_map": phoneme_id_map,
        })
        .to_string()
    }
}

pub struct Fixture {
    pub dir: PathBuf,
    pub model_id: String,
    pub config_id: String,
}

impl Fixture {
    pub fn model_path(&self) -> PathBuf {
        self.dir.join(format!("{}.onnx", self.model_id))
    }

    pub fn config_path(&self) -> PathBuf {
        self.dir.join(format!("{}.onnx.json", self.config_id))
    }

    pub fn model_digest(&self) -> String {
        sha256_hex_file(&self.model_path())
    }

    pub fn config_digest(&self) -> String {
        sha256_hex_file(&self.config_path())
    }
}

/// Write a complete, valid fixture voice (`.onnx` + `.onnx.json`) into a fresh
/// unique temp directory and return its paths/ids. `unique_name` becomes part of
/// the artifact id so concurrent test-thread fixtures never collide.
pub fn write_fixture(unique_name: &str, template: &[f32], config: &FixtureConfig) -> Fixture {
    let dir = std::env::temp_dir().join(format!(
        "eg-tts-piper-fixture-{}-{}",
        unique_name,
        std::process::id()
    ));
    fs::create_dir_all(&dir).expect("create fixture dir");
    let model_id = format!("model-{unique_name}");
    let config_id = format!("config-{unique_name}");
    fs::write(
        dir.join(format!("{model_id}.onnx")),
        build_fixture_onnx(template),
    )
    .expect("write fixture onnx");
    fs::write(dir.join(format!("{config_id}.onnx.json")), config.to_json())
        .expect("write fixture config");
    Fixture {
        dir,
        model_id,
        config_id,
    }
}

pub fn sha256_hex_file(path: &Path) -> String {
    let mut file = fs::File::open(path).expect("open file for digest");
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).expect("read file for digest");
    sha256_hex_bytes(&buf)
}

pub fn sha256_hex_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for b in digest {
        out.push_str(&format!("{b:02x}"));
    }
    out
}
