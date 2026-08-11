use std::collections::HashSet;
use std::env;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

use cyclonedds_bench::{
    BenchmarkCase, CASE_SCHEMA_VERSION, CaseFile, CaseKind, DOMAIN_ID, ResultRecord, make_message,
    metadata, roudi_config, serialize_message, write_json_line,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = Arguments::parse()?;
    let cases = CaseFile::load()?;
    let case_definition = cases.find(&arguments.case_id)?;
    let resolved_case = case_definition.resolve()?;
    let run_id = arguments.run_id.unwrap_or_else(cyclonedds_bench::run_id);

    if let Some(path) = arguments.metadata.as_deref() {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, serde_json::to_vec_pretty(&metadata(&run_id, &cases))?)?;
    }

    let record = match match case_definition.kind {
        CaseKind::Cdr => run_cdr(&run_id, &case_definition, &resolved_case),
        _ => run_process_case(&run_id, &case_definition),
    } {
        Ok(record) => record,
        Err(error) => failed_record(&run_id, &case_definition, &resolved_case, error.to_string()),
    };
    let status_ok = record.status == "ok";
    let value = serde_json::to_value(&record)?;
    write_json_line(arguments.output.as_deref(), &value)?;
    if status_ok {
        Ok(())
    } else {
        Err(record
            .error
            .unwrap_or_else(|| "benchmark case failed".to_string())
            .into())
    }
}

fn failed_record(
    run_id: &str,
    case_definition: &BenchmarkCase,
    resolved_case: &cyclonedds_bench::ResolvedCase,
    error: String,
) -> ResultRecord {
    ResultRecord {
        schema_version: CASE_SCHEMA_VERSION,
        run_id: run_id.to_string(),
        case_id: case_definition.id.clone(),
        status: "error".to_string(),
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
        elapsed_ns: 0,
        messages_sent: 0,
        messages_received: 0,
        bytes_sent: 0,
        bytes_received: 0,
        missing: 0,
        duplicate: 0,
        out_of_order: 0,
        throughput_bytes_per_sec: None,
        latency: None,
        serialize_elapsed_ns: None,
        deserialize_elapsed_ns: None,
        roundtrip_ok: None,
        unsupported: vec![],
        error: Some(error),
    }
}

struct Arguments {
    case_id: String,
    output: Option<PathBuf>,
    metadata: Option<PathBuf>,
    run_id: Option<String>,
}

impl Arguments {
    fn parse() -> Result<Self, Box<dyn std::error::Error>> {
        let mut case_id = None;
        let mut output = None;
        let mut metadata = None;
        let mut run_id = None;
        let mut arguments = env::args().skip(1);
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--case" => case_id = arguments.next(),
                "--output" => output = arguments.next().map(PathBuf::from),
                "--metadata" => metadata = arguments.next().map(PathBuf::from),
                "--run-id" => run_id = arguments.next(),
                value => return Err(format!("unknown argument: {value}").into()),
            }
        }
        Ok(Self {
            case_id: case_id.ok_or("--case is required")?,
            output,
            metadata,
            run_id,
        })
    }
}

fn run_cdr(
    run_id: &str,
    case_definition: &BenchmarkCase,
    resolved_case: &cyclonedds_bench::ResolvedCase,
) -> Result<ResultRecord, Box<dyn std::error::Error>> {
    let mut serialize_elapsed_ns = 0u128;
    let mut deserialize_elapsed_ns = 0u128;
    let mut roundtrip_ok = true;
    let total = case_definition.warmup_messages + case_definition.measured_messages;
    let mut serialized_bytes = resolved_case.serialized_bytes;

    for sequence in 0..total {
        let message = make_message(sequence, resolved_case.payload_bytes, 0);
        let serialize_started = Instant::now();
        let encoded = serialize_message(&message)?;
        if sequence >= case_definition.warmup_messages {
            serialize_elapsed_ns += serialize_started.elapsed().as_nanos();
        }
        serialized_bytes = encoded.len();

        let deserialize_started = Instant::now();
        let decoded: cyclonedds_bench::BenchmarkMessage = cdr::deserialize(&encoded)?;
        if sequence >= case_definition.warmup_messages {
            deserialize_elapsed_ns += deserialize_started.elapsed().as_nanos();
        }
        if sequence >= case_definition.warmup_messages && decoded != message {
            roundtrip_ok = false;
        }
    }

    let status = if roundtrip_ok { "ok" } else { "error" };
    Ok(ResultRecord {
        schema_version: CASE_SCHEMA_VERSION,
        run_id: run_id.to_string(),
        case_id: case_definition.id.clone(),
        status: status.to_string(),
        kind: case_definition.kind.clone(),
        transport: case_definition.transport.clone(),
        shm: false,
        domain_id: DOMAIN_ID,
        interface: "none".to_string(),
        payload_bytes: resolved_case.payload_bytes,
        serialized_bytes,
        rate_hz: case_definition.rate_hz,
        burst: case_definition.burst,
        warmup_messages: case_definition.warmup_messages,
        measured_messages: case_definition.measured_messages,
        elapsed_ns: serialize_elapsed_ns + deserialize_elapsed_ns,
        messages_sent: case_definition.measured_messages,
        messages_received: case_definition.measured_messages,
        bytes_sent: serialized_bytes as u128 * case_definition.measured_messages as u128,
        bytes_received: serialized_bytes as u128 * case_definition.measured_messages as u128,
        missing: 0,
        duplicate: 0,
        out_of_order: 0,
        throughput_bytes_per_sec: None,
        latency: None,
        serialize_elapsed_ns: Some(serialize_elapsed_ns),
        deserialize_elapsed_ns: Some(deserialize_elapsed_ns),
        roundtrip_ok: Some(roundtrip_ok),
        unsupported: vec![],
        error: (!roundtrip_ok).then(|| "CDR roundtrip mismatch".to_string()),
    })
}

fn run_process_case(
    run_id: &str,
    case_definition: &BenchmarkCase,
) -> Result<ResultRecord, Box<dyn std::error::Error>> {
    let topic = format!(
        "cyclonedds_bench_{}_{}",
        sanitize(run_id),
        sanitize(&case_definition.id)
    );
    let config_path = write_temp_file(
        "cyclonedds-bench",
        ".xml",
        &cyclonedds_bench::xml_config(case_definition.shm),
    )?;
    let roudi = if case_definition.shm {
        match start_roudi() {
            Ok(roudi) => Some(roudi),
            Err(error) => {
                let _ = fs::remove_file(&config_path);
                return Err(error);
            }
        }
    } else {
        None
    };
    let result = run_children(run_id, case_definition, &topic, &config_path);
    if let Err(error) = stop_roudi(roudi) {
        eprintln!("RouDi cleanup failed: {error}");
    }
    let _ = fs::remove_file(config_path);
    result
}

struct ChildStream {
    child: Child,
    stdin: ChildStdin,
    lines: Receiver<String>,
}

impl Drop for ChildStream {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn run_children(
    run_id: &str,
    case_definition: &BenchmarkCase,
    topic: &str,
    config_path: &Path,
) -> Result<ResultRecord, Box<dyn std::error::Error>> {
    let executable = env::current_exe()?;
    let publisher = executable.with_file_name("publisher");
    let subscriber = executable.with_file_name("subscriber");
    let environment = [(
        "CYCLONEDDS_URI",
        format!("file://{}", config_path.display()),
    )];
    let mut subscriber = spawn_child(
        &subscriber,
        "subscriber",
        run_id,
        case_definition,
        topic,
        &environment,
    )?;
    let mut publisher = spawn_child(
        &publisher,
        "publisher",
        run_id,
        case_definition,
        topic,
        &environment,
    )?;

    let handshake_deadline = Instant::now() + Duration::from_secs(case_definition.timeout_seconds);
    let mut handshake = HashSet::new();
    while handshake.len() < 4 {
        receive_handshake_line("subscriber", &subscriber.lines, &mut handshake)?;
        receive_handshake_line("publisher", &publisher.lines, &mut handshake)?;
        if Instant::now() >= handshake_deadline {
            terminate_child(&mut subscriber.child);
            terminate_child(&mut publisher.child);
            return Err("child ready/matched timeout".into());
        }
        thread::sleep(Duration::from_millis(1));
    }
    subscriber.stdin.write_all(b"START\n")?;
    subscriber.stdin.flush()?;
    publisher.stdin.write_all(b"START\n")?;
    publisher.stdin.flush()?;

    let result_deadline =
        Instant::now() + Duration::from_secs(case_definition.timeout_seconds.saturating_add(5));
    let mut result = None;
    let mut messages_sent = None;
    let mut bytes_sent = None;
    while result.is_none() || messages_sent.is_none() {
        receive_result_line(&subscriber.lines, &mut result)?;
        receive_publisher_line(&publisher.lines, &mut messages_sent, &mut bytes_sent)?;
        if Instant::now() >= result_deadline {
            terminate_child(&mut subscriber.child);
            terminate_child(&mut publisher.child);
            return Err("benchmark child timeout".into());
        }
        thread::sleep(Duration::from_millis(1));
    }

    let mut result = result.ok_or("subscriber did not return a result")?;
    result.messages_sent = messages_sent.unwrap_or(0);
    result.bytes_sent = bytes_sent.unwrap_or(0);
    let subscriber_status = subscriber.child.wait()?.success();
    let publisher_status = publisher.child.wait()?.success();
    if !subscriber_status || !publisher_status {
        result.status = "error".to_string();
        result.error = Some("publisher or subscriber exited unsuccessfully".to_string());
    }
    Ok(result)
}

fn spawn_child(
    executable: &Path,
    role: &str,
    run_id: &str,
    case_definition: &BenchmarkCase,
    topic: &str,
    environment: &[(&str, String)],
) -> Result<ChildStream, Box<dyn std::error::Error>> {
    let mut command = Command::new(executable);
    let mut child = command
        .arg("--case")
        .arg(&case_definition.id)
        .arg("--topic")
        .arg(topic)
        .arg("--run-id")
        .arg(run_id)
        .envs(environment.iter().map(|(key, value)| (*key, value)))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|error| format!("failed to start {role}: {error}"))?;
    let stdin = child.stdin.take().ok_or("child stdin unavailable")?;
    let stdout = child.stdout.take().ok_or("child stdout unavailable")?;
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if sender.send(line).is_err() {
                break;
            }
        }
    });
    Ok(ChildStream {
        child,
        stdin,
        lines: receiver,
    })
}

fn receive_handshake_line(
    role: &str,
    receiver: &Receiver<String>,
    handshake: &mut HashSet<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    match receiver.try_recv() {
        Ok(line) if line == format!("READY role={role}") => {
            handshake.insert(format!("ready-{role}"));
        }
        Ok(line) if line == format!("MATCHED role={role}") => {
            handshake.insert(format!("matched-{role}"));
        }
        Ok(line) if line.starts_with("ERROR") => return Err(line.into()),
        Ok(_) | Err(TryRecvError::Empty) => {}
        Err(TryRecvError::Disconnected) => {
            return Err(format!("{role} exited before handshake").into());
        }
    }
    Ok(())
}

fn receive_result_line(
    receiver: &Receiver<String>,
    result: &mut Option<ResultRecord>,
) -> Result<(), Box<dyn std::error::Error>> {
    match receiver.try_recv() {
        Ok(line) if line.starts_with("RESULT ") => {
            *result = Some(serde_json::from_str(line.trim_start_matches("RESULT "))?);
        }
        Ok(line) if line.starts_with("ERROR") => return Err(line.into()),
        Ok(_) | Err(TryRecvError::Empty) => {}
        Err(TryRecvError::Disconnected) => {}
    }
    Ok(())
}

fn receive_publisher_line(
    receiver: &Receiver<String>,
    messages_sent: &mut Option<u64>,
    bytes_sent: &mut Option<u128>,
) -> Result<(), Box<dyn std::error::Error>> {
    match receiver.try_recv() {
        Ok(line) if line.starts_with("DONE ") => {
            *messages_sent =
                parse_value(&line, "messages_sent").and_then(|value| value.parse().ok());
            *bytes_sent = parse_value(&line, "bytes_sent").and_then(|value| value.parse().ok());
        }
        Ok(line) if line.starts_with("ERROR") => return Err(line.into()),
        Ok(_) | Err(TryRecvError::Empty) => {}
        Err(TryRecvError::Disconnected) => {}
    }
    Ok(())
}

fn parse_value<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    line.split_whitespace()
        .find_map(|field| field.strip_prefix(&format!("{key}=")))
}

struct RoudiProcess {
    child: Child,
    config_path: PathBuf,
}

fn start_roudi() -> Result<RoudiProcess, Box<dyn std::error::Error>> {
    let executable = env::var_os("ICEORYX_BIN")
        .map(PathBuf::from)
        .or_else(|| {
            let path = PathBuf::from("vendor/iceoryx/install/bin/iox-roudi");
            path.is_file().then_some(path)
        })
        .unwrap_or_else(|| PathBuf::from("iox-roudi"));
    let config_path = write_temp_file("cyclonedds-bench-roudi", ".toml", roudi_config())?;
    let mut child = Command::new(executable)
        .arg("--config-file")
        .arg(&config_path)
        .arg("--monitoring-mode")
        .arg("off")
        .arg("--log-level")
        .arg("error")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let startup_failed = Arc::new(AtomicBool::new(false));
    if let Some(stream) = child.stdout.take() {
        watch_roudi_output(stream, startup_failed.clone());
    }
    if let Some(stream) = child.stderr.take() {
        watch_roudi_output(stream, startup_failed.clone());
    }
    thread::sleep(Duration::from_secs(1));
    if startup_failed.load(Ordering::Acquire) {
        terminate_child(&mut child);
        let _ = fs::remove_file(&config_path);
        return Err("iox-roudi reported a startup failure".into());
    }
    if let Some(status) = child.try_wait()? {
        let _ = fs::remove_file(&config_path);
        return Err(format!("iox-roudi exited during startup: {status}").into());
    }
    Ok(RoudiProcess { child, config_path })
}

fn watch_roudi_output<R: Read + Send + 'static>(stream: R, startup_failed: Arc<AtomicBool>) {
    thread::spawn(move || {
        for line in BufReader::new(stream).lines().map_while(Result::ok) {
            if line.contains("Could not acquire lock")
                || line.contains("ICEORYX error!")
                || line.contains("Couldn't parse config file")
                || line.contains("Fatal")
            {
                startup_failed.store(true, Ordering::Release);
            }
        }
    });
}

fn stop_roudi(roudi: Option<RoudiProcess>) -> Result<(), Box<dyn std::error::Error>> {
    let Some(mut roudi) = roudi else {
        return Ok(());
    };
    let child = &mut roudi.child;
    unsafe {
        libc::kill(child.id() as i32, libc::SIGINT);
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if child.try_wait()?.is_some() {
            fs::remove_file(roudi.config_path)?;
            return Ok(());
        }
        thread::sleep(Duration::from_millis(50));
    }
    child.kill()?;
    child.wait()?;
    fs::remove_file(roudi.config_path)?;
    Ok(())
}

fn terminate_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn write_temp_file(
    prefix: &str,
    suffix: &str,
    contents: &str,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let path = env::temp_dir().join(format!(
        "{prefix}-{}-{}{}",
        std::process::id(),
        cyclonedds_bench::monotonic_ns(),
        suffix
    ));
    fs::write(&path, contents)?;
    Ok(path)
}

fn sanitize(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '_'
            }
        })
        .collect()
}
