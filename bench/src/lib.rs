use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cdr::Infinite;
use cdds_derive::Topic;
use cyclonedds_rs::{
    DDSError, DdsListener, DdsListenerBuilder, DdsParticipant, DdsQos, DdsReader, DdsSubscriber,
    DdsTopic, DdsWriter, ReaderBuilder, SampleBuffer, TopicType, WriterBuilder,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub const CASE_SCHEMA_VERSION: u32 = 1;
pub const CASES_JSON: &str = include_str!("../cases.json");
pub const DOMAIN_ID: u32 = 1;
pub const BOUNDARY_WIRE_BYTES: usize = 65_535;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Topic)]
pub struct BenchmarkMessage {
    pub sequence: u64,
    pub sent_monotonic_ns: u64,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CaseFile {
    pub schema_version: u32,
    pub domain_id: u32,
    pub interface: String,
    pub qos: QosDefinition,
    pub cases: Vec<BenchmarkCase>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct QosDefinition {
    pub reliability: String,
    pub durability: String,
    pub history: String,
    pub history_depth: i32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BenchmarkCase {
    pub id: String,
    pub kind: CaseKind,
    pub transport: String,
    pub shm: bool,
    pub payload: PayloadDefinition,
    pub rate_hz: Option<u64>,
    pub burst: u64,
    pub warmup_messages: u64,
    pub measured_messages: u64,
    pub timeout_seconds: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum CaseKind {
    Latency,
    Throughput,
    Smoke,
    Cdr,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PayloadDefinition {
    Fixed {
        bytes: usize,
    },
    WireBoundary {
        relation: BoundaryRelation,
        wire_bytes: usize,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum BoundaryRelation {
    Below,
    Above,
}

#[derive(Clone, Debug)]
pub struct ResolvedCase {
    pub definition: BenchmarkCase,
    pub payload_bytes: usize,
    pub serialized_bytes: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LatencySummary {
    pub count: usize,
    pub min_ns: u64,
    pub mean_ns: u64,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub max_ns: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ResultRecord {
    pub schema_version: u32,
    pub run_id: String,
    pub case_id: String,
    pub status: String,
    pub kind: CaseKind,
    pub transport: String,
    pub shm: bool,
    pub domain_id: u32,
    pub interface: String,
    pub payload_bytes: usize,
    pub serialized_bytes: usize,
    pub rate_hz: Option<u64>,
    pub burst: u64,
    pub warmup_messages: u64,
    pub measured_messages: u64,
    pub elapsed_ns: u128,
    pub messages_sent: u64,
    pub messages_received: u64,
    pub bytes_sent: u128,
    pub bytes_received: u128,
    pub missing: u64,
    pub duplicate: u64,
    pub out_of_order: u64,
    pub throughput_bytes_per_sec: Option<f64>,
    pub latency: Option<LatencySummary>,
    pub serialize_elapsed_ns: Option<u128>,
    pub deserialize_elapsed_ns: Option<u128>,
    pub roundtrip_ok: Option<bool>,
    pub unsupported: Vec<UnsupportedMetric>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct UnsupportedMetric {
    pub name: String,
    pub reason: String,
}

impl CaseFile {
    pub fn load() -> Result<Self, Box<dyn std::error::Error>> {
        let file: Self = serde_json::from_str(CASES_JSON)?;
        if file.schema_version != CASE_SCHEMA_VERSION {
            return Err(format!("unsupported case schema {}", file.schema_version).into());
        }
        Ok(file)
    }

    pub fn find(&self, case_id: &str) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
        self.cases
            .iter()
            .find(|case_definition| case_definition.id == case_id)
            .cloned()
            .ok_or_else(|| format!("unknown case: {case_id}").into())
    }
}

impl BenchmarkCase {
    pub fn resolve(&self) -> Result<ResolvedCase, Box<dyn std::error::Error>> {
        let payload_bytes = match self.payload {
            PayloadDefinition::Fixed { bytes } => bytes,
            PayloadDefinition::WireBoundary {
                ref relation,
                wire_bytes,
            } => find_boundary_payload(*relation, wire_bytes)?,
        };
        let serialized_bytes = serialize_message(&make_message(0, payload_bytes, 0))?.len();
        Ok(ResolvedCase {
            definition: self.clone(),
            payload_bytes,
            serialized_bytes,
        })
    }
}

pub fn make_message(
    sequence: u64,
    payload_bytes: usize,
    sent_monotonic_ns: u64,
) -> BenchmarkMessage {
    let payload = (0..payload_bytes)
        .map(|index| ((index as u64 + sequence) % 251) as u8)
        .collect();
    BenchmarkMessage {
        sequence,
        sent_monotonic_ns,
        payload,
    }
}

pub fn serialize_message(
    message: &BenchmarkMessage,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    Ok(cdr::serialize::<_, _, cdr::CdrBe>(message, Infinite)?)
}

pub fn find_boundary_payload(
    relation: BoundaryRelation,
    wire_bytes: usize,
) -> Result<usize, Box<dyn std::error::Error>> {
    let size_at = |payload_bytes| {
        serialize_message(&make_message(0, payload_bytes, 0)).map(|bytes| bytes.len())
    };
    let mut high = wire_bytes.max(1);
    while size_at(high)? <= wire_bytes {
        high = high.checked_mul(2).ok_or("wire boundary overflow")?;
    }
    let mut low = 0usize;
    while low < high {
        let middle = low + (high - low) / 2;
        if size_at(middle)? <= wire_bytes {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    let first_above = low;
    match relation {
        BoundaryRelation::Above => aligned_boundary_payload(first_above, wire_bytes, true),
        BoundaryRelation::Below => aligned_boundary_payload(
            first_above
                .checked_sub(1)
                .ok_or("wire boundary has no below value")?,
            wire_bytes,
            false,
        ),
    }
}

fn aligned_boundary_payload(
    mut payload_bytes: usize,
    wire_bytes: usize,
    above: bool,
) -> Result<usize, Box<dyn std::error::Error>> {
    loop {
        let serialized_bytes = serialize_message(&make_message(0, payload_bytes, 0))?.len();
        if serialized_bytes % 4 == 0 && (above == (serialized_bytes > wire_bytes)) {
            return Ok(payload_bytes);
        }
        payload_bytes = if above {
            payload_bytes.checked_add(1)
        } else {
            payload_bytes.checked_sub(1)
        }
        .ok_or("aligned wire boundary payload overflow")?;
    }
}

pub fn monotonic_ns() -> u64 {
    let mut value = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let result = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut value) };
    assert_eq!(result, 0, "CLOCK_MONOTONIC is unavailable");
    (value.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(value.tv_nsec as u64)
}

pub fn make_qos() -> Result<DdsQos, Box<dyn std::error::Error>> {
    let mut qos = DdsQos::create()?;
    qos.set_reliability(
        cyclonedds_rs::dds_reliability_kind::DDS_RELIABILITY_RELIABLE,
        Duration::from_secs(1),
    )
    .set_durability(cyclonedds_rs::dds_durability_kind::DDS_DURABILITY_VOLATILE)
    .set_history(cyclonedds_rs::dds_history_kind::DDS_HISTORY_KEEP_LAST, 32);
    Ok(qos)
}

pub fn matched_listener(matched: Arc<AtomicBool>, writer: bool) -> cyclonedds_rs::DdsListener {
    if writer {
        DdsListenerBuilder::new()
            .on_publication_matched(move |_entity, status| {
                if status.current_count > 0 {
                    matched.store(true, Ordering::Release);
                }
            })
            .build()
    } else {
        DdsListenerBuilder::new()
            .on_subscription_matched(move |_entity, status| {
                if status.current_count > 0 {
                    matched.store(true, Ordering::Release);
                }
            })
            .build()
    }
}

pub fn wait_for_match(matched: &AtomicBool, timeout: Duration) -> Result<(), String> {
    let started = Instant::now();
    while !matched.load(Ordering::Acquire) {
        if started.elapsed() >= timeout {
            return Err("DDS publication/subscription match timeout".to_string());
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    Ok(())
}

pub fn create_named_topic(
    participant: &DdsParticipant,
    name: &str,
) -> Result<DdsTopic<BenchmarkMessage>, Box<dyn std::error::Error>> {
    Ok(BenchmarkMessage::create_topic_with_name(
        participant,
        name,
        None,
        None,
    )?)
}

pub fn create_writer(
    participant: &DdsParticipant,
    topic: DdsTopic<BenchmarkMessage>,
    matched: Arc<AtomicBool>,
) -> Result<DdsWriter<BenchmarkMessage>, Box<dyn std::error::Error>> {
    let qos = make_qos()?;
    let listener = matched_listener(matched, true);
    Ok(WriterBuilder::new()
        .with_qos(qos)
        .with_listener(listener)
        .create(participant, topic)?)
}

pub fn create_reader(
    subscriber: &DdsSubscriber,
    topic: DdsTopic<BenchmarkMessage>,
    matched: Arc<AtomicBool>,
) -> Result<DdsReader<BenchmarkMessage>, Box<dyn std::error::Error>> {
    let qos = make_qos()?;
    let listener = matched_listener(matched, false);
    Ok(ReaderBuilder::new()
        .with_qos(qos)
        .with_listener(listener)
        .create(subscriber, topic)?)
}

pub fn wait_for_start() -> Result<(), Box<dyn std::error::Error>> {
    let stdin = io::stdin();
    let mut line = String::new();
    stdin.read_line(&mut line)?;
    if line.trim() == "START" {
        Ok(())
    } else {
        Err(format!("expected START, got {}", line.trim()).into())
    }
}

pub fn emit_control(line: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "{line}")?;
    stdout.flush()?;
    Ok(())
}

pub fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    let index = ((sorted.len() - 1) * percentile).div_ceil(100);
    sorted[index]
}

pub fn latency_summary(mut values: Vec<u64>) -> Option<LatencySummary> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    let sum = values.iter().map(|value| *value as u128).sum::<u128>();
    Some(LatencySummary {
        count: values.len(),
        min_ns: values[0],
        mean_ns: (sum / values.len() as u128) as u64,
        p50_ns: percentile(&values, 50),
        p95_ns: percentile(&values, 95),
        p99_ns: percentile(&values, 99),
        max_ns: *values.last().unwrap(),
    })
}

pub fn write_json_line(
    path: Option<&Path>,
    value: &Value,
) -> Result<(), Box<dyn std::error::Error>> {
    let line = serde_json::to_string(value)?;
    if let Some(path) = path {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        writeln!(file, "{line}")?;
    } else {
        println!("{line}");
    }
    Ok(())
}

pub fn run_id() -> String {
    format!("{}-{}", std::process::id(), monotonic_ns())
}

pub fn metadata(run_id: &str, cases: &CaseFile) -> Value {
    let mut tools = BTreeMap::new();
    for (name, args) in [
        ("rustc", vec!["--version", "--verbose"]),
        ("cargo", vec!["--version", "--verbose"]),
        ("cmake", vec!["--version"]),
        ("clang", vec!["--version"]),
        ("cc", vec!["--version"]),
        ("bindgen", vec!["--version"]),
    ] {
        tools.insert(name, command_output(name, &args));
    }
    json!({
        "schema_version": CASE_SCHEMA_VERSION,
        "run_id": run_id,
        "repository": repository_metadata(),
        "vendor": vendor_metadata(),
        "toolchain": tools,
        "build": {
            "target": rustc_target_value(),
            "profile": std::env::var("PROFILE").ok().or_else(|| Some("release".to_string())),
            "rustflags": std::env::var("RUSTFLAGS").ok()
        },
        "dds_runtime": {
            "udp": {
                "max_message_size": 14720,
                "fragment_size": 1344,
                "shared_memory": false
            },
            "shm": {
                "max_message_size": 14720,
                "fragment_size": 1344,
                "shared_memory": true
            }
        },
        "host": host_metadata(),
        "cases": cases,
    })
}

fn command_output(command: &str, args: &[&str]) -> Value {
    match Command::new(command).args(args).output() {
        Ok(output) => json!({
            "value": String::from_utf8_lossy(&output.stdout).trim().to_string(),
            "stderr": String::from_utf8_lossy(&output.stderr).trim().to_string(),
            "status": output.status.code()
        }),
        Err(error) => json!({ "value": null, "reason": error.to_string() }),
    }
}

fn repository_metadata() -> Value {
    json!({
        "commit": command_value("git", &["rev-parse", "HEAD"]),
        "dirty": command_string("git", &["status", "--porcelain=v1"]).map(|value| !value.is_empty()),
        "cargo_lock_sha256": file_hash_value("Cargo.lock"),
        "generated_binding_sha256": file_hash_value("cyclonedds-sys/src/generated.rs"),
        "package_versions": cargo_package_versions(),
    })
}

fn vendor_metadata() -> Value {
    json!({
        "cyclonedds_sha": command_value("git", &["-C", "vendor/cyclonedds", "rev-parse", "HEAD"]),
        "cyclonedds_describe": command_value("git", &["-C", "vendor/cyclonedds", "describe", "--always", "--dirty"]),
        "cyclonedds_dirty": command_string("git", &["-C", "vendor/cyclonedds", "status", "--porcelain=v1"]).map(|value| !value.is_empty()),
        "iceoryx_sha": command_value("git", &["-C", "vendor/iceoryx", "rev-parse", "HEAD"]),
        "iceoryx_describe": command_value("git", &["-C", "vendor/iceoryx", "describe", "--always", "--dirty"]),
    })
}

fn host_metadata() -> Value {
    json!({
        "runner": environment_value("RUNNER_NAME")
            .or_else(|| environment_value("HOSTNAME"))
            .unwrap_or_else(|| json!({ "value": null, "reason": "RUNNER_NAME and HOSTNAME are unavailable" })),
        "os": command_value("uname", &["-sro"]),
        "kernel": command_value("uname", &["-r"]),
        "cpu": command_value("uname", &["-m"]),
        "cpu_count": std::thread::available_parallelism().map(|value| value.get()).unwrap_or(0),
        "ram_bytes": memory_value(),
        "governor": file_text_value("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"),
        "perf": command_output("perf", &["stat", "--no-big-num", "true"]),
        "lo_mtu": file_u64_value("/sys/class/net/lo/mtu"),
    })
}

fn command_value(command: &str, args: &[&str]) -> Value {
    match Command::new(command).args(args).output() {
        Ok(output) if output.status.success() => {
            json!(String::from_utf8_lossy(&output.stdout).trim().to_string())
        }
        Ok(output) => json!({
            "value": null,
            "reason": format!(
                "{command} exited with {:?}: {}",
                output.status.code(),
                String::from_utf8_lossy(&output.stderr).trim()
            )
        }),
        Err(error) => json!({ "value": null, "reason": error.to_string() }),
    }
}

fn rustc_target_value() -> Value {
    match Command::new("rustc").args(["-vV"]).output() {
        Ok(output) if output.status.success() => output
            .stdout
            .split(|byte| *byte == b'\n')
            .find_map(|line| line.strip_prefix(b"host: "))
            .map(|value| json!(String::from_utf8_lossy(value).trim().to_string()))
            .unwrap_or_else(
                || json!({ "value": null, "reason": "rustc host target is unavailable" }),
            ),
        Ok(output) => json!({
            "value": null,
            "reason": String::from_utf8_lossy(&output.stderr).trim().to_string()
        }),
        Err(error) => json!({ "value": null, "reason": error.to_string() }),
    }
}

fn environment_value(name: &str) -> Option<Value> {
    std::env::var(name).ok().map(|value| json!(value))
}

fn cargo_package_versions() -> Value {
    match Command::new("cargo")
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .output()
    {
        Ok(output) if output.status.success() => {
            match serde_json::from_slice::<Value>(&output.stdout) {
                Ok(metadata) => metadata
                    .get("packages")
                    .and_then(Value::as_array)
                    .map(|packages| {
                        packages
                            .iter()
                            .filter_map(|package| {
                                Some((
                                    package.get("name")?.as_str()?.to_string(),
                                    json!(package.get("version")?.as_str()?),
                                ))
                            })
                            .collect::<BTreeMap<_, _>>()
                    })
                    .map(|packages| json!(packages))
                    .unwrap_or_else(
                        || json!({ "value": null, "reason": "cargo metadata has no packages" }),
                    ),
                Err(error) => json!({ "value": null, "reason": error.to_string() }),
            }
        }
        Ok(output) => json!({
            "value": null,
            "reason": String::from_utf8_lossy(&output.stderr).trim().to_string()
        }),
        Err(error) => json!({ "value": null, "reason": error.to_string() }),
    }
}

fn file_text_value(path: &str) -> Value {
    match fs::read_to_string(path) {
        Ok(value) => json!(value.trim()),
        Err(error) => json!({ "value": null, "reason": error.to_string() }),
    }
}

fn file_u64_value(path: &str) -> Value {
    match fs::read_to_string(path) {
        Ok(value) => match value.trim().parse::<u64>() {
            Ok(value) => json!(value),
            Err(error) => json!({ "value": null, "reason": error.to_string() }),
        },
        Err(error) => json!({ "value": null, "reason": error.to_string() }),
    }
}

fn memory_value() -> Value {
    match fs::read_to_string("/proc/meminfo") {
        Ok(contents) => match contents.lines().find_map(|line| {
            line.strip_prefix("MemTotal:")?
                .split_whitespace()
                .next()?
                .parse::<u64>()
                .ok()
        }) {
            Some(value) => json!(value * 1024),
            None => json!({ "value": null, "reason": "MemTotal is unavailable" }),
        },
        Err(error) => json!({ "value": null, "reason": error.to_string() }),
    }
}

fn command_string(command: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(command).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn file_hash_value(path: impl AsRef<Path>) -> Value {
    match Command::new("sha256sum").arg(path.as_ref()).output() {
        Ok(output) if output.status.success() => output
            .stdout
            .split(|byte| *byte == b' ' || *byte == b'\n')
            .find(|part| !part.is_empty())
            .map(|value| json!(String::from_utf8_lossy(value).to_string()))
            .unwrap_or_else(|| json!({ "value": null, "reason": "sha256sum returned no digest" })),
        Ok(output) => json!({
            "value": null,
            "reason": String::from_utf8_lossy(&output.stderr).trim().to_string()
        }),
        Err(error) => json!({ "value": null, "reason": error.to_string() }),
    }
}

pub fn xml_config(shm: bool) -> String {
    let max_message = "14720B";
    let fragment_size = "1344B";
    let shared_memory = if shm { "true" } else { "false" };
    format!(
        r#"<?xml version="1.0" encoding="UTF-8" ?>
<CycloneDDS xmlns="https://cdds.io/config">
  <Domain id="1">
    <General>
      <Interfaces>
        <NetworkInterface name="lo" priority="default" />
      </Interfaces>
      <AllowMulticast>true</AllowMulticast>
      <MaxMessageSize>{max_message}</MaxMessageSize>
      <FragmentSize>{fragment_size}</FragmentSize>
    </General>
    <SharedMemory>
      <Enable>{shared_memory}</Enable>
    </SharedMemory>
  </Domain>
</CycloneDDS>"#
    )
}

pub fn roudi_config() -> &'static str {
    r#"[general]
version = 1

[[segment]]

[[segment.mempool]]
size = 128
count = 10000

[[segment.mempool]]
size = 1024
count = 5000

[[segment.mempool]]
size = 16384
count = 1000

[[segment.mempool]]
size = 131072
count = 200

[[segment.mempool]]
size = 524288
count = 50

[[segment.mempool]]
size = 1048576
count = 30

[[segment.mempool]]
size = 4194304
count = 10
"#
}

#[cfg(test)]
mod tests {
    use super::{find_boundary_payload, latency_summary, serialize_message, BoundaryRelation};

    #[test]
    fn 境界ケースは整列済みシリアライズサイズを選ぶ() {
        let cases = [
            (BoundaryRelation::Below, 65_535, (65_508, 65_532)),
            (BoundaryRelation::Above, 65_535, (65_512, 65_536)),
        ];

        let actual = cases
            .iter()
            .map(|(relation, boundary, _)| {
                let payload = find_boundary_payload(*relation, *boundary).unwrap();
                let serialized = serialize_message(&super::make_message(0, payload, 0))
                    .unwrap()
                    .len();
                (payload, serialized)
            })
            .collect::<Vec<_>>();
        let expected = cases
            .iter()
            .map(|(_, _, expected)| *expected)
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn レイテンシ統計は空と代表値を扱う() {
        let cases = [
            (Vec::new(), None),
            (vec![10, 20, 30, 40], Some((4, 10, 25, 30, 40, 40, 40))),
        ];

        for (values, expected) in cases {
            let actual = latency_summary(values).map(|summary| {
                (
                    summary.count,
                    summary.min_ns,
                    summary.mean_ns,
                    summary.p50_ns,
                    summary.p95_ns,
                    summary.p99_ns,
                    summary.max_ns,
                )
            });
            assert_eq!(actual, expected);
        }
    }
}
