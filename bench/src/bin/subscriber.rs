use std::collections::HashSet;
use std::env;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use cyclonedds_bench::{
    BenchmarkCase, CaseFile, DOMAIN_ID, ResultRecord, create_named_topic, create_reader,
    emit_control, latency_summary, make_message, monotonic_ns, wait_for_match, wait_for_start,
};
use cyclonedds_rs::{DdsSubscriber, ParticipantBuilder, SampleBuffer};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = Arguments::parse()?;
    let case_file = CaseFile::load()?;
    let case_definition = case_file.find(&arguments.case_id)?;
    receive(&arguments, &case_definition, case_definition.resolve()?)
}

struct Arguments {
    case_id: String,
    topic: String,
    run_id: String,
}

impl Arguments {
    fn parse() -> Result<Self, Box<dyn std::error::Error>> {
        let mut case_id = None;
        let mut topic = None;
        let mut run_id = None;
        let mut arguments = env::args().skip(1);
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--case" => case_id = arguments.next(),
                "--topic" => topic = arguments.next(),
                "--run-id" => run_id = arguments.next(),
                value => return Err(format!("unknown argument: {value}").into()),
            }
        }
        Ok(Self {
            case_id: case_id.ok_or("--case is required")?,
            topic: topic.ok_or("--topic is required")?,
            run_id: run_id.ok_or("--run-id is required")?,
        })
    }
}

fn receive(
    arguments: &Arguments,
    case_definition: &BenchmarkCase,
    resolved_case: cyclonedds_bench::ResolvedCase,
) -> Result<(), Box<dyn std::error::Error>> {
    let total_messages = case_definition.warmup_messages + case_definition.measured_messages;
    let history_depth = i32::try_from(total_messages)?;
    let participant = unsafe { ParticipantBuilder::new().with_domain(DOMAIN_ID).create()? };
    let subscriber = DdsSubscriber::create(&participant, None, None)?;
    let topic = create_named_topic(&participant, &arguments.topic)?;
    let matched = Arc::new(AtomicBool::new(false));
    let reader = create_reader(&subscriber, topic, matched.clone(), history_depth)?;

    emit_control("READY role=subscriber")?;
    wait_for_match(
        &matched,
        Duration::from_secs(case_definition.timeout_seconds),
    )?;
    emit_control("MATCHED role=subscriber")?;
    wait_for_start()?;

    let deadline = Instant::now() + Duration::from_secs(case_definition.timeout_seconds);
    let mut samples = SampleBuffer::new(history_depth as usize);
    let mut seen = HashSet::new();
    let mut messages_received = 0u64;
    let mut bytes_received = 0u128;
    let mut duplicate = 0u64;
    let mut out_of_order = 0u64;
    let mut last_sequence = None;
    let mut latencies = Vec::new();
    let mut measurement_started = None;
    let mut measurement_ended = None;
    let mut payload_error = None;

    while Instant::now() < deadline && seen.len() < total_messages as usize {
        match reader.take_now(&mut samples) {
            Ok(_) => {
                for (message, info) in samples.iter_items() {
                    if !info.is_valid() {
                        payload_error = Some("received invalid sample".to_string());
                        continue;
                    }
                    messages_received += 1;
                    bytes_received += message.payload.len() as u128;
                    if !seen.insert(message.sequence) {
                        duplicate += 1;
                    }
                    if let Some(previous) = last_sequence
                        && message.sequence < previous
                    {
                        out_of_order += 1;
                    }
                    last_sequence = Some(message.sequence);
                    if message.sequence < total_messages {
                        let expected = make_message(
                            message.sequence,
                            resolved_case.payload_bytes,
                            message.sent_monotonic_ns,
                        );
                        if message.payload != expected.payload {
                            payload_error =
                                Some(format!("payload mismatch at sequence {}", message.sequence));
                        }
                    }
                    if message.sequence >= case_definition.warmup_messages
                        && message.sequence < total_messages
                    {
                        let received_at = monotonic_ns();
                        if measurement_started.is_none() {
                            measurement_started = Some(received_at);
                        }
                        measurement_ended = Some(received_at);
                        if received_at < message.sent_monotonic_ns {
                            payload_error =
                                Some(format!("negative latency at sequence {}", message.sequence));
                        } else {
                            latencies.push(received_at - message.sent_monotonic_ns);
                        }
                    }
                }
                samples.clear();
            }
            Err(cyclonedds_rs::DDSError::NoData) => {
                std::thread::sleep(Duration::from_micros(50));
            }
            Err(error) => return Err(error.into()),
        }
    }

    let missing = (0..total_messages)
        .filter(|sequence| !seen.contains(sequence))
        .count() as u64;
    let elapsed_ns = measurement_started
        .zip(measurement_ended)
        .map(|(start, end)| end.saturating_sub(start) as u128)
        .unwrap_or(0);
    let throughput = (elapsed_ns > 0).then(|| {
        (case_definition.measured_messages as f64 * resolved_case.payload_bytes as f64)
            / (elapsed_ns as f64 / 1_000_000_000.0)
    });
    let complete = missing == 0 && duplicate == 0 && out_of_order == 0 && payload_error.is_none();
    let record = ResultRecord {
        schema_version: 1,
        run_id: arguments.run_id.clone(),
        case_id: arguments.case_id.clone(),
        status: if complete { "ok" } else { "error" }.to_string(),
        kind: case_definition.kind.clone(),
        transport: case_definition.transport.clone(),
        shm: case_definition.shm,
        domain_id: DOMAIN_ID,
        interface: "lo".to_string(),
        payload_bytes: resolved_case.payload_bytes,
        serialized_bytes: resolved_case.serialized_bytes,
        rate_hz: case_definition.rate_hz,
        burst: case_definition.burst,
        warmup_messages: case_definition.warmup_messages,
        measured_messages: case_definition.measured_messages,
        elapsed_ns,
        messages_sent: total_messages,
        messages_received,
        bytes_sent: (total_messages as usize * resolved_case.payload_bytes) as u128,
        bytes_received,
        missing,
        duplicate,
        out_of_order,
        throughput_bytes_per_sec: throughput,
        latency: latency_summary(latencies),
        serialize_elapsed_ns: None,
        deserialize_elapsed_ns: None,
        roundtrip_ok: None,
        unsupported: Vec::new(),
        error: payload_error
            .or_else(|| (missing > 0).then(|| format!("missing {missing} samples"))),
    };
    emit_control(&format!("RESULT {}", serde_json::to_string(&record)?))?;
    Ok(())
}
