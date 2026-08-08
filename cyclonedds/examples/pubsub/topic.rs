use std::time::Duration;

use cdds_derive::Topic;
use cyclonedds_rs::*;

use crate::util::monotonic_ts;

pub trait SampleImpl {
    fn sample(seq: u32) -> Self;
}

/// Heap領域を使うトピック
#[derive(Debug, Clone, PartialEq, Topic, Serialize, Deserialize)]
pub struct ExTopic1 {
    id: u32,
    value: String,
}

impl ExTopic1 {
    /// 新しいインスタンスを生成する
    pub fn new(id: u32, value: &str) -> Self {
        Self {
            id,
            value: value.to_string(),
        }
    }
}

impl SampleImpl for ExTopic1 {
    fn sample(seq: u32) -> Self {
        let len = (seq % 128) + 42;
        Self {
            id: seq,
            value: format!("Hello DDS! id={} {}", seq, "x".repeat(len as usize)),
        }
    }
}

/// 固定長のトピック -> CDRエンコードされていないのでuntypedでは保存されない点に注意
#[derive(Debug, Clone, PartialEq, Topic, Serialize, Deserialize)]
#[cdds(fixed_size)]
pub struct ExTopic2 {
    a: i32,
    b: [u8; 32],
}

impl ExTopic2 {
    /// 新しいインスタンスを生成する
    pub fn new(a: i32, b: &[u8]) -> Self {
        let mut arr = [0; 32];
        let len = b.len().min(32);
        arr[..len].copy_from_slice(&b[..len]);
        Self { a, b: arr }
    }
}

impl SampleImpl for ExTopic2 {
    fn sample(seq: u32) -> Self {
        let buf = std::array::from_fn(|i| (i as u32 + seq) as u8);
        Self {
            a: seq as i32,
            b: buf,
        }
    }
}

/// 可変長の大きなデータを入れるのと処理時間を計測する
#[derive(Debug, Clone, PartialEq, Topic, Serialize, Deserialize)]
pub struct LargeChunk {
    pub ts_monotonic: Duration,
    pub seq: usize,
    pub hash: u32,
    pub bytes: Vec<u8>,
}

impl LargeChunk {
    /// 指定のサイズで初期化する
    pub fn new(seq: usize, initial: u8, size: usize) -> Self {
        let b = vec![initial; size];
        Self {
            ts_monotonic: monotonic_ts(),
            seq,
            hash: Self::calculate_hash(&b),
            bytes: b,
        }
    }

    /// 経過時間を取得する
    pub fn elapsed(&self) -> Duration {
        monotonic_ts() - self.ts_monotonic
    }

    pub fn calculate_hash(data: &[u8]) -> u32 {
        murmur3::murmur3_32(&mut &data[..], 0).unwrap_or_default()
    }

    pub fn verify(&self) -> bool {
        self.hash == Self::calculate_hash(&self.bytes)
    }
}
