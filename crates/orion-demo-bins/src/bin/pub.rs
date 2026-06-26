//! orion-demo-pub — publish "tick N at HH:MM:SS" to a NATS subject.
//!
//! Two modes:
//!   Core NATS (default)              — fire-and-forget, no persistence
//!   `--jetstream`                    — publishes through JetStream; the
//!                                       broker persists every message in a
//!                                       stream so subscribers can replay or
//!                                       come up after the message was sent.
//!
//! In JetStream mode the publisher auto-creates a stream named `--stream`
//! (default `ORION_DEMO_JS`) bound to a wildcard derived from `--subject`.
//! Re-running with the same stream name is idempotent.

use anyhow::Result;
use async_nats::jetstream;
use clap::Parser;
use std::time::Duration;

#[derive(Parser)]
#[command(name = "orion-demo-pub")]
struct Args {
    #[arg(long, env = "NATS_URL", default_value = "nats://127.0.0.1:4222")]
    nats_url: String,
    #[arg(long, default_value = "orion.demo")]
    subject: String,
    #[arg(long, default_value_t = 1.0)]
    interval_seconds: f32,
    #[arg(long)]
    label: Option<String>,
    /// Use JetStream (persistent + ack'd). Without this flag, plain core NATS.
    #[arg(long)]
    jetstream: bool,
    /// JetStream stream name. Auto-created if missing. Ignored in core mode.
    #[arg(long, default_value = "ORION_DEMO_JS")]
    stream: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let label = args
        .label
        .clone()
        .or_else(|| std::env::var("ORION_REPLICA_INDEX").ok().map(|i| format!("r{i}")))
        .unwrap_or_else(|| "demo".into());

    let mode = if args.jetstream { "JetStream" } else { "core NATS" };
    println!(
        "[demo-pub:{label}] connecting to {} -> {} ({mode})",
        args.nats_url, args.subject
    );
    let nc = async_nats::connect(&args.nats_url).await?;
    println!("[demo-pub:{label}] connected");

    let js = if args.jetstream {
        let js = jetstream::new(nc.clone());
        // Subjects use `>` so a single-segment subject like `orion.demo.js.tick`
        // is covered by the stream's wildcard. We auto-create — idempotent.
        let subj_wildcard = if args.subject.contains('>') || args.subject.contains('*') {
            args.subject.clone()
        } else {
            format!("{}.>", args.subject)
        };
        let cfg = jetstream::stream::Config {
            name: args.stream.clone(),
            subjects: vec![subj_wildcard.clone()],
            ..Default::default()
        };
        js.get_or_create_stream(cfg).await?;
        println!(
            "[demo-pub:{label}] stream {} ready (subjects: {subj_wildcard})",
            args.stream
        );
        Some(js)
    } else {
        None
    };

    let mut i: u64 = 0;
    let interval = Duration::from_millis((args.interval_seconds * 1000.0) as u64);
    loop {
        i += 1;
        let line = format!(
            "tick {i} from {label} at {ts}",
            ts = chrono::Utc::now().format("%H:%M:%S%.3f")
        );
        // Per-tick subject suffix in JetStream mode so the stream stores
        // distinct subjects (`orion.demo.js.tick`) rather than the whole prefix.
        let publish_subj = if args.jetstream {
            format!("{}.tick", args.subject)
        } else {
            args.subject.clone()
        };

        if let Some(js) = &js {
            let ack = js
                .publish(publish_subj.clone(), line.clone().into())
                .await?
                .await?;
            println!(
                "[demo-pub:{label}] sent (js seq={}): {line}",
                ack.sequence
            );
        } else {
            nc.publish(publish_subj.clone(), line.clone().into()).await?;
            println!("[demo-pub:{label}] sent: {line}");
        }
        tokio::time::sleep(interval).await;
    }
}
