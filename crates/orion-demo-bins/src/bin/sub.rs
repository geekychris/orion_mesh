//! orion-demo-sub — subscribe to a NATS subject and print each message.
//!
//! Three subscription modes:
//!   Core NATS, no queue group        — fan-out (every subscriber gets each msg)
//!   Core NATS, `--queue-group X`     — load-balanced (one sub per group per msg)
//!   `--jetstream --durable <name>`   — durable JetStream consumer; explicit ack
//!                                       per message; backlog survives sub restart
//!
//! Reads `ORION_REPLICA_INDEX` for the default `--label`.

use anyhow::Result;
use async_nats::jetstream;
use clap::Parser;
use futures::StreamExt;

#[derive(Parser)]
#[command(name = "orion-demo-sub")]
struct Args {
    #[arg(long, env = "NATS_URL", default_value = "nats://127.0.0.1:4222")]
    nats_url: String,
    #[arg(long, default_value = "orion.demo")]
    subject: String,
    /// Queue group name. Only one subscriber in the group receives each message.
    /// Ignored in JetStream mode (use `--durable` for shared consumption).
    #[arg(long)]
    queue_group: Option<String>,
    /// Use JetStream (durable, ack-based, replayable). Without this flag, plain core NATS.
    #[arg(long)]
    jetstream: bool,
    /// JetStream stream to bind to. Default `ORION_DEMO_JS` (matches the pub default).
    #[arg(long, default_value = "ORION_DEMO_JS")]
    stream: String,
    /// JetStream durable consumer name. Multiple subscribers using the same
    /// durable name share the load AND survive restart with replay from where
    /// the last ack stopped.
    #[arg(long)]
    durable: Option<String>,
    /// Logical label shown in stdout.
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

    let mode = if args.jetstream {
        format!(
            "JetStream stream={} durable={}",
            args.stream,
            args.durable.as_deref().unwrap_or("(ephemeral)")
        )
    } else if let Some(g) = &args.queue_group {
        format!("core queue-group '{g}'")
    } else {
        "core fan-out (no queue group)".into()
    };
    println!(
        "[demo-sub:{label}] connecting to {} -> {} ({mode})",
        args.nats_url, args.subject
    );

    let nc = async_nats::connect(&args.nats_url).await?;
    println!("[demo-sub:{label}] connected");

    if args.jetstream {
        run_jetstream(&label, &args, nc).await
    } else {
        run_core(&label, &args, nc).await
    }
}

async fn run_core(label: &str, args: &Args, nc: async_nats::Client) -> Result<()> {
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

async fn run_jetstream(label: &str, args: &Args, nc: async_nats::Client) -> Result<()> {
    let js = jetstream::new(nc);
    // Auto-create the stream if missing — same config as the publisher uses.
    // Idempotent if the stream already exists.
    let subj_wildcard = if args.subject.contains('>') || args.subject.contains('*') {
        args.subject.clone()
    } else {
        format!("{}.>", args.subject)
    };
    let stream_cfg = jetstream::stream::Config {
        name: args.stream.clone(),
        subjects: vec![subj_wildcard.clone()],
        ..Default::default()
    };
    let stream = js.get_or_create_stream(stream_cfg).await?;
    println!("[demo-sub:{label}] stream {} ready (subjects: {subj_wildcard})", args.stream);
    // When `durable_name` is set, multiple subscribers using the same name
    // share the load AND survive restart with replay-from-last-ack.
    let durable_name = args.durable.clone().unwrap_or_else(|| format!("ephemeral-{label}"));
    let cfg = jetstream::consumer::pull::Config {
        durable_name: Some(durable_name.clone()),
        ..Default::default()
    };
    let consumer = stream.get_or_create_consumer(&durable_name, cfg).await?;
    println!("[demo-sub:{label}] consumer '{durable_name}' bound");

    let mut messages = consumer.messages().await?;
    while let Some(msg) = messages.next().await {
        let msg = msg?;
        let body = String::from_utf8_lossy(&msg.payload);
        let seq = msg.info().ok().map(|i| i.stream_sequence).unwrap_or(0);
        println!(
            "[demo-sub:{label}] recv (seq={seq}): {} (subject={})",
            body, msg.subject
        );
        msg.ack().await.map_err(|e| anyhow::anyhow!("{e}"))?;
    }
    Ok(())
}
