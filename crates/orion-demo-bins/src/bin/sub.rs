//! orion-demo-sub — subscribe to a NATS subject and print each message.
//!
//! Two subscription modes:
//!   - Plain pub/sub (default)         — every subscriber gets every message
//!   - Queue group (`--queue-group X`) — one subscriber per group gets each message
//!
//! Reads `ORION_REPLICA_INDEX` to identify itself when running as one of N replicas
//! (the agent injects this env var per replica).

use anyhow::Result;
use clap::Parser;
use futures::StreamExt;

#[derive(Parser)]
#[command(name = "orion-demo-sub")]
struct Args {
    #[arg(long, env = "NATS_URL", default_value = "nats://127.0.0.1:4222")]
    nats_url: String,
    #[arg(long, default_value = "orion.demo")]
    subject: String,
    /// When set, subscribes via a NATS queue group — only ONE subscriber in
    /// the group receives each message (load-balancing). Without this flag,
    /// every subscriber receives every message (fan-out).
    #[arg(long)]
    queue_group: Option<String>,
    /// Logical label shown in stdout. Defaults to `r<ORION_REPLICA_INDEX>`
    /// if the agent set that env, else "demo".
    #[arg(long)]
    label: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let label = args
        .label
        .clone()
        .or_else(|| std::env::var("ORION_REPLICA_INDEX").ok().map(|i| format!("r{i}")))
        .unwrap_or_else(|| "demo".into());

    let mode = match &args.queue_group {
        Some(g) => format!("queue-group '{g}'"),
        None => "fan-out (no queue group)".into(),
    };
    println!(
        "[demo-sub:{label}] connecting to {} -> {} ({mode})",
        args.nats_url, args.subject
    );

    let nc = async_nats::connect(&args.nats_url).await?;
    println!("[demo-sub:{label}] connected");

    let mut sub = match &args.queue_group {
        Some(g) => nc.queue_subscribe(args.subject.clone(), g.clone()).await?,
        None => nc.subscribe(args.subject.clone()).await?,
    };
    println!("[demo-sub:{label}] subscribed");

    while let Some(msg) = sub.next().await {
        let body = String::from_utf8_lossy(&msg.payload);
        println!(
            "[demo-sub:{label}] recv: {} (subject={})",
            body, msg.subject
        );
    }
    Ok(())
}
