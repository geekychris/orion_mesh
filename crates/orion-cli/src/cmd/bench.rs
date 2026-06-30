//! `orion bench queue` — throughput / latency probe for a named queue.
//!
//! Modes:
//!   `pub`   publish N messages of size S, measure publish rate
//!   `sub`   subscribe and report end-to-end latency (publisher embeds a
//!           wall-clock stamp; subscriber computes `now - stamp`)
//!   `rt`    round-trip — pub + sub in the same process, end-to-end report

use crate::{Ctx, nats};
use anyhow::{Context, Result};
use async_nats::jetstream::{self, consumer};
use clap::{Args as ClapArgs, Subcommand};
use futures::StreamExt;
use orion_types::{Resource, ResourceBody};
use serde_json::{json, Value};
use std::time::{Duration, Instant};

#[derive(Subcommand, Debug)]
pub enum Sub {
    /// Pub + sub in one process; reports E2E latency p50/p95/p99 + rate.
    Queue(QueueArgs),
}

#[derive(ClapArgs, Debug)]
pub struct QueueArgs {
    pub name: String,
    /// Number of messages.
    #[arg(short = 'n', long, default_value_t = 1000)]
    pub count: u64,
    /// Payload size in bytes (above the timestamp + index overhead).
    #[arg(short = 's', long, default_value_t = 256)]
    pub size: usize,
    /// Concurrent publishers.
    #[arg(long, default_value_t = 1)]
    pub publishers: u32,
    /// Skip the consumer side — just measure publish rate.
    #[arg(long)]
    pub pub_only: bool,
    /// Skip the publisher side — just consume and report (requires a separate publisher).
    #[arg(long)]
    pub sub_only: bool,
    /// Durable name on the consumer side (default: bench-<random>).
    #[arg(long)]
    pub durable: Option<String>,
}

pub async fn run(ctx: &Ctx, sub: Sub) -> Result<()> {
    match sub {
        Sub::Queue(a) => run_queue(ctx, a).await,
    }
}

async fn run_queue(ctx: &Ctx, args: QueueArgs) -> Result<()> {
    let queue = lookup_queue(ctx, &args.name).await?;
    let nc = nats::connect(ctx).await?;
    let js = jetstream::new(nc);
    let (subject, cfg) = orion_bus::client::queue_stream_config(&args.name, &queue);
    orion_bus::client::ensure_stream(&js, cfg.clone())
        .await
        .with_context(|| format!("ensuring stream for queue {}", args.name))?;

    let durable = args
        .durable
        .clone()
        .unwrap_or_else(|| format!("bench-{}", std::process::id()));

    println!(
        "bench queue={} subject={} n={} size={} pubs={}",
        args.name, subject, args.count, args.size, args.publishers
    );

    // Spawn consumer first so it doesn't miss publishes.
    let cons_handle = if !args.pub_only {
        let js2 = js.clone();
        let subject2 = subject.clone();
        let stream2 = cfg.name.clone();
        let durable2 = durable.clone();
        let want = args.count;
        Some(tokio::spawn(async move {
            consumer_run(js2, stream2, durable2, subject2, want).await
        }))
    } else {
        None
    };
    // Wait briefly for the consumer to bind before pubs start.
    if cons_handle.is_some() {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let pub_stats = if !args.sub_only {
        producer_run(&js, &subject, args.count, args.size, args.publishers).await?
    } else {
        ProducerStats::default()
    };

    if !args.sub_only {
        println!(
            "pub: {n} msgs in {ms:.0}ms → {rate:.1} msg/s ({mbps:.2} MiB/s)",
            n = pub_stats.n,
            ms = pub_stats.elapsed.as_millis() as f64,
            rate = pub_stats.n as f64 / pub_stats.elapsed.as_secs_f64().max(1e-6),
            mbps = (pub_stats.bytes as f64 / 1024.0 / 1024.0)
                / pub_stats.elapsed.as_secs_f64().max(1e-6),
        );
    }

    if let Some(h) = cons_handle {
        match tokio::time::timeout(Duration::from_secs(120), h).await {
            Ok(Ok(Ok(cs))) => print_consumer_stats(&cs),
            Ok(Ok(Err(e))) => eprintln!("consumer error: {e:?}"),
            Ok(Err(e)) => eprintln!("consumer join error: {e:?}"),
            Err(_) => eprintln!("consumer timed out waiting for {} msgs", args.count),
        }
    }
    Ok(())
}

#[derive(Debug, Default)]
struct ProducerStats {
    n: u64,
    bytes: u64,
    elapsed: Duration,
}

async fn producer_run(
    js: &jetstream::Context,
    subject: &str,
    n: u64,
    payload_size: usize,
    publishers: u32,
) -> Result<ProducerStats> {
    let per_pub = n / publishers as u64;
    let leftover = n - per_pub * publishers as u64;
    let start = Instant::now();
    let mut handles = Vec::new();
    let mut total_bytes: u64 = 0;
    for p in 0..publishers {
        let extra = if p == 0 { leftover } else { 0 };
        let n_here = per_pub + extra;
        let js = js.clone();
        let subject = subject.to_owned();
        let filler: Vec<u8> = vec![b'x'; payload_size];
        let h = tokio::spawn(async move {
            let mut bytes = 0u64;
            for i in 0..n_here {
                let ts = chrono::Utc::now().timestamp_micros();
                let payload = json!({
                    "_ts_us": ts,
                    "_i": i,
                    "_p": p,
                    "_filler": String::from_utf8_lossy(&filler),
                });
                let bytes_payload = serde_json::to_vec(&payload).unwrap();
                bytes += bytes_payload.len() as u64;
                let ack = js
                    .publish(subject.clone(), bytes_payload.into())
                    .await
                    .unwrap();
                let _ = ack.await;
            }
            bytes
        });
        handles.push(h);
    }
    for h in handles {
        total_bytes += h.await.unwrap_or(0);
    }
    Ok(ProducerStats {
        n,
        bytes: total_bytes,
        elapsed: start.elapsed(),
    })
}

#[derive(Debug, Default)]
struct ConsumerStats {
    n: u64,
    latencies_us: Vec<i64>,
    elapsed: Duration,
}

async fn consumer_run(
    js: jetstream::Context,
    stream: String,
    durable: String,
    subject: String,
    want: u64,
) -> Result<ConsumerStats> {
    let stream = js.get_stream(&stream).await?;
    let consumer = stream
        .get_or_create_consumer(
            &durable,
            consumer::pull::Config {
                durable_name: Some(durable.clone()),
                filter_subject: subject.clone(),
                ack_policy: consumer::AckPolicy::Explicit,
                ..Default::default()
            },
        )
        .await?;
    let start = Instant::now();
    let mut messages = consumer.messages().await?;
    let mut lat = Vec::with_capacity(want as usize);
    while lat.len() < want as usize {
        let next = tokio::time::timeout(Duration::from_secs(60), messages.next()).await;
        let m = match next {
            Ok(Some(Ok(m))) => m,
            Ok(Some(Err(_))) | Ok(None) => continue,
            Err(_) => break,
        };
        let v: Value = serde_json::from_slice(&m.payload).unwrap_or(Value::Null);
        if let Some(ts) = v.get("_ts_us").and_then(|n| n.as_i64()) {
            let now = chrono::Utc::now().timestamp_micros();
            lat.push(now - ts);
        }
        let _ = m.ack().await;
    }
    Ok(ConsumerStats {
        n: lat.len() as u64,
        latencies_us: lat,
        elapsed: start.elapsed(),
    })
}

fn print_consumer_stats(s: &ConsumerStats) {
    let mut lat = s.latencies_us.clone();
    lat.sort_unstable();
    let p = |q: f64| {
        if lat.is_empty() {
            0
        } else {
            lat[((lat.len() as f64) * q) as usize - usize::from((q - 0.0).abs() > f64::EPSILON)]
        }
    };
    let p50 = p(0.50);
    let p95 = p(0.95);
    let p99 = p(0.99);
    let mean = if lat.is_empty() {
        0
    } else {
        lat.iter().sum::<i64>() / lat.len() as i64
    };
    println!(
        "sub: {n} msgs in {ms:.0}ms → {rate:.1} msg/s · latency mean={mean}µs p50={p50}µs p95={p95}µs p99={p99}µs",
        n = s.n,
        ms = s.elapsed.as_millis() as f64,
        rate = s.n as f64 / s.elapsed.as_secs_f64().max(1e-6),
    );
}

async fn lookup_queue(ctx: &Ctx, name: &str) -> Result<orion_types::QueueSpec> {
    let r: Resource = crate::http::get_json(ctx, &format!("/v1/resources/Queue/{name}")).await?;
    match r.body {
        ResourceBody::Queue { spec, .. } => Ok(spec),
        _ => anyhow::bail!("resource {name} is not a Queue"),
    }
}
