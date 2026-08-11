use std::env;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use cyclonedds_bench::{
    BenchmarkCase, CaseFile, DOMAIN_ID, create_named_topic, create_writer, emit_control,
    make_message, monotonic_ns, wait_for_match, wait_for_start, wait_for_stop,
};
use cyclonedds_rs::ParticipantBuilder;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = Arguments::parse()?;
    let case_file = CaseFile::load()?;
    let case_definition = case_file.find(&arguments.case_id)?;
    publish(&arguments, &case_definition, case_definition.resolve()?)
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

fn publish(
    arguments: &Arguments,
    case_definition: &BenchmarkCase,
    resolved_case: cyclonedds_bench::ResolvedCase,
) -> Result<(), Box<dyn std::error::Error>> {
    let total_messages = case_definition.warmup_messages + case_definition.measured_messages;
    let history_depth = i32::try_from(total_messages)?;
    let participant = unsafe { ParticipantBuilder::new().with_domain(DOMAIN_ID).create()? };
    let topic = create_named_topic(&participant, &arguments.topic)?;
    let matched = Arc::new(AtomicBool::new(false));
    let mut writer = create_writer(&participant, topic, matched.clone(), history_depth)?;

    emit_control("READY role=publisher")?;
    wait_for_match(
        &matched,
        Duration::from_secs(case_definition.timeout_seconds),
    )?;
    emit_control("MATCHED role=publisher")?;
    wait_for_start()?;

    let mut bytes_sent = 0u128;
    let period = case_definition
        .rate_hz
        .filter(|rate| *rate > 0)
        .map(|rate| Duration::from_nanos(1_000_000_000 / rate));
    let mut next_send = Instant::now();
    let mut measurement_started = None;
    let mut measurement_ended = None;
    let started = Instant::now();

    for sequence in 0..total_messages {
        if period.is_some() {
            let now = Instant::now();
            if next_send > now {
                std::thread::sleep(next_send - now);
            }
        }
        let sent_monotonic_ns = monotonic_ns();
        if sequence == case_definition.warmup_messages {
            measurement_started = Some(sent_monotonic_ns);
        }
        let message = make_message(sequence, resolved_case.payload_bytes, sent_monotonic_ns);
        bytes_sent += message.payload.len() as u128;
        writer.write(Arc::new(message))?;
        if sequence + 1 == total_messages {
            measurement_ended = Some(monotonic_ns());
        }
        if let Some(period) = period {
            next_send += period;
        }
    }

    let elapsed_ns = measurement_started
        .zip(measurement_ended)
        .map(|(start, end)| end.saturating_sub(start) as u128)
        .unwrap_or_else(|| started.elapsed().as_nanos());
    emit_control(&format!(
        "DONE role=publisher run_id={} messages_sent={} bytes_sent={} elapsed_ns={}",
        arguments.run_id, total_messages, bytes_sent, elapsed_ns
    ))?;
    wait_for_stop()?;
    Ok(())
}
