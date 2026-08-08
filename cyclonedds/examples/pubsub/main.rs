use std::{
    collections::{HashMap, VecDeque},
    fmt::Display,
    sync::Arc,
    time::{Duration, Instant},
};

use cyclonedds_rs::{
    DdsParticipant, DdsPublisher, DdsReader, DdsSubscriber, DdsTopic, DdsWriter, SampleBuffer,
    TopicType, untyped::Untyped,
};
use tracing::{debug, info, warn};

use crate::{
    opt::{Cmd, Opt, PubCmd, PubOpt, SubCmd, SubOpt},
    topic::{ExTopic1, ExTopic2, LargeChunk, SampleImpl},
};

pub mod opt;
pub mod topic;
pub mod util;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let opt = <Opt as clap::Parser>::parse();
    util::init();
    if opt.use_shm {
        util::use_shm_config();
        info!("use shared memory transport");
    }

    match opt.cmd {
        Cmd::Pub(opt) => pub_cmd(opt).await?,
        Cmd::Sub(opt) => sub_cmd(opt).await?,
    }
    Ok(())
}

struct LargeChunkTask {
    config: LargeChunkTaskConfig,
    seq: usize,
    current_size: usize,
    current_repeat: usize,
}

impl LargeChunkTask {
    fn new(config: LargeChunkTaskConfig) -> Self {
        let current_size = config.start_byte;
        Self {
            config,
            seq: 0,
            current_size,
            current_repeat: 0,
        }
    }

    fn size_tick(&mut self) -> bool {
        match self.config.step {
            StepOps::Add(v) => {
                self.current_size += v;
            }
            StepOps::Mul(f) => {
                self.current_size = ((self.current_size as f64) * f) as usize;
            }
        }
        if self.current_size > self.config.end_byte {
            return false;
        }
        self.current_repeat = 0;
        true
    }

    fn next(&mut self) -> Option<LargeChunk> {
        if self.current_repeat >= self.config.repeat && !self.size_tick() {
            return None;
        }
        let chunk = LargeChunk::new(
            self.seq,
            (self.seq % u8::MAX as usize) as u8,
            self.current_size,
        );
        self.current_repeat += 1;
        self.seq += 1;
        Some(chunk)
    }
}

pub struct LargeChunkTaskConfig {
    pub start_byte: usize,
    pub end_byte: usize,
    pub repeat: usize,
    pub step: StepOps,
    pub dur: Duration,
}

impl LargeChunkTaskConfig {
    fn new(
        start_byte: usize,
        end_byte: usize,
        repeat: usize,
        step: StepOps,
        dur: Duration,
    ) -> Self {
        Self {
            start_byte,
            end_byte,
            repeat,
            step,
            dur,
        }
    }
}

pub enum StepOps {
    Add(usize),
    Mul(f64),
}

// 送信コマンドタスク
async fn pub_cmd(opt: PubOpt) -> anyhow::Result<()> {
    let p = DdsParticipant::get_or_create(Some(opt.dds.domain_id))?;
    let pb = DdsPublisher::create(p, None, None)?;
    match opt.cmd {
        PubCmd::Any => {
            let task1 = send_task::<ExTopic1>(p, &pb, "ExTopic1", Duration::from_secs(1))?;
            let task2 = send_task::<ExTopic2>(p, &pb, "ExTopic2", Duration::from_millis(20))?;
            let _ = tokio::join!(task1, task2);
        }
        PubCmd::Large(large_opt) => {
            let topic = LargeChunk::create_topic(p, None, None, None)?;
            let mut w = DdsWriter::create(&pb, topic, None, None)?;

            let config = large_opt.config();
            let mut interval = tokio::time::interval(config.dur);
            let mut task_state = LargeChunkTask::new(config);
            while let Some(chunk) = task_state.next() {
                let arc = Arc::new(chunk);
                let start = Instant::now();
                w.write(arc.clone())?;
                let write_elapsed = humantime::format_duration(start.elapsed());
                let size = readable_byte::readable_byte::b(arc.bytes.len() as u64);
                info!(
                    "Publishing: id={} repeat={} size={size} write_cost={write_elapsed}",
                    task_state.seq, task_state.current_repeat
                );
                interval.tick().await;
            }
        }
    }
    Ok(())
}

// 1topicの送信タスク
fn send_task<T: SampleImpl + TopicType>(
    p: &DdsParticipant,
    pb: &DdsPublisher,
    topic_name: &str,
    dur: Duration,
) -> anyhow::Result<impl std::future::Future<Output = ()>> {
    let t = DdsTopic::create(p, topic_name, None, None)?;
    let mut w = DdsWriter::create(pb, t, None, None)?;
    Ok(async move {
        let mut id = 0;
        let mut interval = tokio::time::interval(dur);
        loop {
            let data = T::sample(id);
            let arc = Arc::new(data);
            w.write(arc).unwrap();
            id += 1;
            interval.tick().await;
        }
    })
}

async fn sub_cmd(opt: SubOpt) -> anyhow::Result<()> {
    let p = DdsParticipant::get_or_create(Some(opt.dds.domain_id))?;
    let sb = DdsSubscriber::create(p, None, None)?;
    match opt.cmd {
        SubCmd::Untyped => {
            let t1 = recv_untyped_task(p, &sb, "topic/ExTopic1", "ExTopic1")?;
            let t2 = recv_untyped_task(p, &sb, "topic/ExTopic2", "ExTopic2")?;
            let t3 = recv_untyped_task(p, &sb, "topic/LargeChunk", "/topic/LargeChunk")?;
            let _ = tokio::join!(t1, t2, t3);
        }
        SubCmd::Large => {
            let topic = LargeChunk::create_topic(p, None, None, None)?;
            let r = DdsReader::create_async(&sb, topic, None)?;
            let mut samples = SampleBuffer::<LargeChunk>::new(4);
            let mut record_map = HashMap::<usize, Vec<Duration>>::new();
            loop {
                let res = r.take_async(&mut samples).await;
                if let Err(_e) = res {
                    show_transport_log(&record_map);
                    continue;
                }
                for s in samples.iter() {
                    let delta = s.elapsed();
                    record_map.entry(s.bytes.len()).or_default().push(delta);
                    let delta = humantime::format_duration(delta);
                    let size = readable_byte::readable_byte::b(s.bytes.len() as u64);
                    info!(
                        "Received: size={size} delta={delta}, verified={}",
                        s.verify()
                    );
                }
                samples.clear();
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct StateSnapshot {
    ts: Duration,
    count: usize,
    bytes: usize,
}

impl StateSnapshot {
    fn next_span(&self, ts: Duration) -> Self {
        Self {
            ts,
            count: self.count,
            bytes: self.bytes,
        }
    }

    fn update(&mut self, count: usize, bytes: usize) {
        self.count += count;
        self.bytes += bytes;
    }

    fn delta_since(&self, earlier: Option<Self>) -> Delta {
        let earler = earlier.unwrap_or_default();
        let dur = self.ts.saturating_sub(earler.ts);
        let count = self.count.saturating_sub(earler.count);
        let bytes = self.bytes.saturating_sub(earler.bytes);

        Delta { dur, count, bytes }
    }
}

impl Default for StateSnapshot {
    fn default() -> Self {
        Self {
            ts: Duration::from_secs(0),
            count: 0,
            bytes: 0,
        }
    }
}

struct Delta {
    dur: Duration,
    count: usize,
    bytes: usize,
}

impl Delta {
    fn rate(&self) -> (f64, f64) {
        let sec = self.dur.as_secs_f64();
        if sec > 0.0 {
            (self.count as f64 / sec, self.bytes as f64 / sec)
        } else {
            (0.0, 0.0)
        }
    }
}

struct RecvState {
    name: String,
    next_print: bool,
    log: VecDeque<StateSnapshot>,
}

impl RecvState {
    fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            next_print: false,
            log: [StateSnapshot::default()].into(),
        }
    }

    fn last(&self) -> Option<&StateSnapshot> {
        self.log.back()
    }

    fn last_mut(&mut self) -> Option<&mut StateSnapshot> {
        self.log.back_mut()
    }

    fn latest_delta(&self) -> Delta {
        if self.log.len() < 2 {
            self.last().unwrap().delta_since(None)
        } else {
            let latests = self.log.iter().rev().take(2).collect::<Vec<_>>();
            let last = latests[0];
            let prev = latests[1];
            debug!("last={:?} prev={:?}", last, prev);
            last.delta_since(Some(*prev))
        }
    }

    fn apply(&mut self, count: usize, bytes: usize) {
        if let Some(last) = self.last_mut() {
            last.update(count, bytes);
            self.next_print = true;
        }
    }

    fn update_tick(&mut self, ts: Duration) {
        if self.log.len() > 4 {
            self.log.pop_front();
        }
        let next = self.last().unwrap().next_span(ts);
        self.log.push_back(next);
    }

    fn show_if_updated(&mut self) {
        if self.next_print {
            info!("{}", self);
            self.next_print = false;
        }
    }
}

impl Display for RecvState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let delta = self.latest_delta();
        let count = self.last().unwrap().count;
        let (rate_count, rate_bytes) = delta.rate();
        let rate_bytes = readable_byte::readable_byte::b(rate_bytes as u64);
        write!(
            f,
            "{}: count={count:5} msg={rate_count:>6.1}/s speed={rate_bytes}/s",
            self.name
        )
    }
}

fn recv_untyped_task(
    p: &DdsParticipant,
    sb: &DdsSubscriber,
    type_name: &str,
    topic_name: &str,
) -> anyhow::Result<impl std::future::Future<Output = ()>> {
    let t = DdsTopic::<Untyped>::create_untyped(p, topic_name, type_name, None, None)?;
    let r = DdsReader::create_async(sb, t, None)?;
    info!(type_name = type_name, "create recv task");
    let mut stat = RecvState::new(format!("{}:{}", topic_name, type_name));
    Ok(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let start = tokio::time::Instant::now();
        let mut samples = SampleBuffer::new(8);
        info!("start recv loop {}", stat.name);
        loop {
            tokio::select! {
                tick = interval.tick() => {
                    stat.show_if_updated();
                    stat.update_tick(tick.duration_since(start));
                }
                res = r.takecdr_async(&mut samples) => {
                    if let Err(e) = res {
                        warn!("take error: {:?}", e);
                        continue;
                    }
                    for s in samples.iter_sample() {
                        let cdr = s.cdr().unwrap();
                        stat.apply(1, cdr.len());
                    }
                    samples.clear();
                }
            }
        }
    })
}

fn show_transport_log(map: &HashMap<usize, Vec<Duration>>) {
    let keys = {
        let mut ks: Vec<_> = map.keys().cloned().collect();
        ks.sort();
        ks
    };
    for k in keys {
        let vec = &map[&k];
        let count = vec.len();
        let sum = vec.iter().sum::<Duration>();
        let avg = sum / (count as u32);
        let max = *vec.iter().max().unwrap();
        let min = *vec.iter().min().unwrap();
        let size = readable_byte::readable_byte::b(k as u64);
        println!(
            "size={size} count={} avg={} min={} max={}",
            count,
            humantime::format_duration(avg),
            humantime::format_duration(min),
            humantime::format_duration(max)
        );
    }
}
