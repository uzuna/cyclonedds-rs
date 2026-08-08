use std::time::Duration;

use crate::{LargeChunkTaskConfig, StepOps};

#[derive(clap::Parser)]
pub struct Opt {
    #[arg(long, short = 's', help = "Use shared memory transport")]
    pub use_shm: bool,
    #[command(subcommand)]
    pub cmd: Cmd,
}

#[derive(clap::Subcommand)]
pub enum Cmd {
    /// Publish messages
    Pub(PubOpt),
    /// Subscribe messages
    Sub(SubOpt),
}

#[derive(clap::Args)]
pub struct PubOpt {
    #[command(subcommand)]
    pub cmd: PubCmd,
    #[command(flatten)]
    pub dds: DdsOpt,
}

#[derive(clap::Args)]
pub struct SubOpt {
    #[command(subcommand)]
    pub cmd: SubCmd,
    #[command(flatten)]
    pub dds: DdsOpt,
}

#[derive(clap::Subcommand)]
pub enum PubCmd {
    /// Publish Sample messages
    Any,
    /// Publish Large messages
    Large(PubLargeOpt),
}

#[derive(clap::Subcommand)]
pub enum SubCmd {
    /// Subscribe as Untyped reader
    Untyped,
    /// Subscribe Large Message as Typed reader
    Large,
}

#[derive(clap::Args)]
pub struct DdsOpt {
    #[arg(long, default_value_t = 0, help = "DDS Domain ID")]
    pub domain_id: u32,
}

#[derive(clap::Args)]
pub struct PubLargeOpt {
    #[arg(
        long,
        short = 's',
        default_value_t = 1024,
        help = "Start size in bytes"
    )]
    pub start_byte: usize,
    #[arg(long, short = 'e', default_value_t = 2 * 1024 * 1024, help = "End size in bytes")]
    pub end_byte: usize,
    #[arg(long, short = 'r', default_value_t = 10, help = "Repeat count")]
    pub repeat: usize,
    #[arg(long, short = 'd', value_parser = humantime::parse_duration, default_value = "100ms")]
    pub dur: Duration,
    #[arg(
        long,
        short = 'p',
        default_value = "*2.0",
        help = "Step operation (add (+N) or mul (*N))"
    )]
    pub step: String,
}

impl PubLargeOpt {
    fn parse_step(&self) -> StepOps {
        if let Some(s) = self.step.strip_prefix('*') {
            if let Ok(f) = s.parse::<f64>() {
                return StepOps::Mul(f);
            }
        } else if let Some(s) = self.step.strip_prefix('+')
            && let Ok(n) = s.parse::<usize>()
        {
            return StepOps::Add(n);
        }
        StepOps::Mul(2.0)
    }

    pub fn config(&self) -> LargeChunkTaskConfig {
        LargeChunkTaskConfig::new(
            self.start_byte,
            self.end_byte,
            self.repeat,
            self.parse_step(),
            self.dur,
        )
    }
}
