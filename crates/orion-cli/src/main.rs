//! OrionMesh CLI (`orion`).
//!
//! Subcommand modules live under `cmd/`. This file is a thin Clap dispatcher
//! plus the global flags (controller URL, NATS URL, cluster token, output
//! format). Each `cmd::*` module implements one verb (or a small group).
//!
//! Global env precedence:
//!   `--controller`    >  `ORION_CONTROLLER_URL`   >  default http://127.0.0.1:7878
//!   `--nats-url`      >  `NATS_URL`               >  default nats://127.0.0.1:4222
//!   `--token`         >  `ORION_CLUSTER_TOKEN`    >  none (auth-disabled dev mode)
//!   `--output`/`-o`   >  default table for ls/get, yaml/json otherwise

use anyhow::Result;
use clap::{Parser, Subcommand};

mod http;
mod nats;
mod output;

mod cmd;

#[derive(Parser, Debug)]
#[command(
    name = "orion",
    version,
    about = "OrionMesh CLI — full parity with the controller REST API"
)]
struct Cli {
    /// Controller base URL.
    #[arg(
        long,
        env = "ORION_CONTROLLER_URL",
        default_value = "http://127.0.0.1:7878",
        global = true
    )]
    controller: String,

    /// NATS broker URL (for `orion queue ...` data-plane traffic).
    #[arg(
        long,
        env = "NATS_URL",
        default_value = "nats://127.0.0.1:4222",
        global = true
    )]
    nats_url: String,

    /// Cluster shared token (for both NATS and HTTP). Defaults to the contents
    /// of `~/.config/orion/cluster.token` when unset.
    #[arg(long, env = "ORION_CLUSTER_TOKEN", global = true)]
    token: Option<String>,

    /// Output format for commands that return data.
    #[arg(long, short = 'o', global = true, value_enum, default_value_t = output::Format::Table)]
    output: output::Format,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Fetch resources (`orion get nodes`, `orion get services`, `orion get queue ps-rows`).
    Get(cmd::get::Args),
    /// Describe a single resource — pretty-printed.
    Describe(cmd::describe::Args),
    /// Apply (create or update) a resource from YAML/JSON. `-f -` reads stdin.
    Apply(cmd::apply::Args),
    /// Delete a resource by kind + name.
    Delete(cmd::delete::Args),
    /// Dispatch a Service or Task — controller picks a node and runs it.
    Dispatch(cmd::dispatch::Args),
    /// Stop a running Service / Task by kind + name.
    Stop(cmd::stop_restart::StopArgs),
    /// Restart a running Service by kind + name.
    Restart(cmd::stop_restart::RestartArgs),
    /// Get logs for a workload. `--follow` polls for new entries.
    Logs(cmd::logs::Args),
    /// List running instances cluster-wide or filtered.
    Instances(cmd::instances::Args),
    /// Validate a local YAML file (or stdin via `-f -`) without contacting the controller.
    Validate(cmd::validate::Args),
    /// Apply + dispatch a Service/Task in one shot.
    Run(cmd::run::Args),
    /// Schedule subcommands (list observed fires, create from cron + task).
    #[command(subcommand)]
    Schedule(cmd::schedules::Sub),
    /// Diagnostics (system, jetstream).
    #[command(subcommand)]
    Diag(cmd::diag::Sub),
    /// Parse free-form text (column-headers / TSV / regex) on stdin into ndjson on stdout.
    Json(cmd::json::Args),
    /// Named queue operations: pub / sub / ls / describe / purge.
    #[command(subcommand)]
    Queue(cmd::queue::Sub),
    /// Generate Resource YAML (queue, service, task, schedule, processor) on stdout.
    #[command(name = "gen", subcommand)]
    Gen(cmd::generate::Sub),
    /// Bring up a full local OrionMesh stack — NATS + controller + agent (+ UI).
    Up(cmd::up::Args),
    /// Health check across broker / controller / agent.
    Doctor(cmd::doctor::Args),
    /// Benchmark queue throughput / latency.
    #[command(subcommand)]
    Bench(cmd::bench::Sub),
    /// Scaffold a runnable processor project (python/java/rust) on disk.
    #[command(subcommand)]
    Init(cmd::init::Sub),
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let cli = Cli::parse();
    let token = cli.token.clone().or_else(|| {
        // Mirror orion-auth: look at ~/.config/orion/cluster.token when no env / flag.
        let p = dirs_token_path();
        std::fs::read_to_string(p).ok().map(|s| s.trim().to_owned())
    });
    let ctx = Ctx {
        controller: cli.controller.trim_end_matches('/').to_owned(),
        nats_url: cli.nats_url,
        token,
        output: cli.output,
    };

    match cli.cmd {
        Cmd::Get(a) => cmd::get::run(&ctx, a).await,
        Cmd::Describe(a) => cmd::describe::run(&ctx, a).await,
        Cmd::Apply(a) => cmd::apply::run(&ctx, a).await,
        Cmd::Delete(a) => cmd::delete::run(&ctx, a).await,
        Cmd::Dispatch(a) => cmd::dispatch::run(&ctx, a).await,
        Cmd::Stop(a) => cmd::stop_restart::run_stop(&ctx, a).await,
        Cmd::Restart(a) => cmd::stop_restart::run_restart(&ctx, a).await,
        Cmd::Logs(a) => cmd::logs::run(&ctx, a).await,
        Cmd::Instances(a) => cmd::instances::run(&ctx, a).await,
        Cmd::Validate(a) => cmd::validate::run(&ctx, a).await,
        Cmd::Run(a) => cmd::run::run(&ctx, a).await,
        Cmd::Schedule(s) => cmd::schedules::run(&ctx, s).await,
        Cmd::Diag(s) => cmd::diag::run(&ctx, s).await,
        Cmd::Json(a) => cmd::json::run(&ctx, a).await,
        Cmd::Queue(s) => cmd::queue::run(&ctx, s).await,
        Cmd::Gen(s) => cmd::generate::run(&ctx, s).await,
        Cmd::Up(a) => cmd::up::run(&ctx, a).await,
        Cmd::Doctor(a) => cmd::doctor::run(&ctx, a).await,
        Cmd::Bench(s) => cmd::bench::run(&ctx, s).await,
        Cmd::Init(s) => cmd::init::run(&ctx, s).await,
    }
}

fn dirs_token_path() -> std::path::PathBuf {
    if let Ok(home) = std::env::var("HOME") {
        std::path::Path::new(&home).join(".config/orion/cluster.token")
    } else {
        std::path::PathBuf::from(".orion-cluster-token")
    }
}

/// Resolved global context passed to every subcommand.
#[derive(Clone, Debug)]
pub struct Ctx {
    pub controller: String,
    pub nats_url: String,
    pub token: Option<String>,
    pub output: output::Format,
}
