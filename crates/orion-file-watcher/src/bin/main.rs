//! orion-file-watcher binary.
//!
//! Reads ORION_QUEUE_NAME / ORION_QUEUE_SUBJECT / ORION_QUEUE_STREAM from
//! the environment (set by the agent when this is dispatched as a
//! Service) and tails the file paths listed in --path / $WATCH_PATHS.

use anyhow::{Context, Result};
use clap::Parser;
use orion_file_watcher::{make_event, CursorMap};
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};

#[derive(Parser, Debug)]
#[command(name = "orion-file-watcher")]
struct Args {
    /// One or more file paths to watch. Repeatable.
    #[arg(long = "path", value_name = "FILE", required = false)]
    paths: Vec<PathBuf>,
    /// Poll interval in milliseconds.
    #[arg(long, default_value_t = 500)]
    interval_ms: u64,
    /// Read from start (default false — only emit lines added after we start).
    #[arg(long)]
    from_start: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let mut args = Args::parse();
    if args.paths.is_empty() {
        if let Ok(s) = std::env::var("WATCH_PATHS") {
            args.paths = s.split(':').map(PathBuf::from).collect();
        }
    }
    if args.paths.is_empty() {
        anyhow::bail!("no paths to watch — pass --path or set WATCH_PATHS");
    }

    let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let token = std::env::var("ORION_CLUSTER_TOKEN").ok();
    let subject = std::env::var("ORION_QUEUE_SUBJECT")
        .context("ORION_QUEUE_SUBJECT must be set (the agent populates it from the Service spec)")?;
    let stream = std::env::var("ORION_QUEUE_STREAM")
        .context("ORION_QUEUE_STREAM must be set")?;

    let nc = orion_bus::client::connect(&nats_url, token.as_deref())
        .await
        .with_context(|| format!("connecting to NATS at {nats_url}"))?;
    let js = async_nats::jetstream::new(nc);

    // Ensure stream exists.
    let cfg = async_nats::jetstream::stream::Config {
        name: stream.clone(),
        subjects: vec![subject.clone()],
        ..Default::default()
    };
    let _ = orion_bus::client::ensure_stream(&js, cfg).await;

    tracing::info!(?args.paths, subject = %subject, "orion-file-watcher starting");

    let mut cursors = CursorMap::default();
    // Seed cursors at the current file size if not from_start.
    if !args.from_start {
        for p in &args.paths {
            if let Ok(meta) = tokio::fs::metadata(p).await {
                cursors.update(p, meta.len());
            }
        }
    }

    let mut ticker = tokio::time::interval(Duration::from_millis(args.interval_ms));
    loop {
        ticker.tick().await;
        for path in &args.paths {
            if let Err(e) = poll_one(path, &mut cursors, &js, &subject).await {
                tracing::warn!(?path, error = %e, "poll error");
            }
        }
    }
}

async fn poll_one(
    path: &std::path::Path,
    cursors: &mut CursorMap,
    js: &async_nats::jetstream::Context,
    subject: &str,
) -> Result<()> {
    let meta = match tokio::fs::metadata(path).await {
        Ok(m) => m,
        Err(_) => return Ok(()), // file doesn't exist yet — try next tick
    };
    let size = meta.len();
    let cur = cursors.get(path);
    let (start, _rotated) = cur.advance(size);
    if start >= size {
        return Ok(());
    }

    let mut f = tokio::fs::File::open(path).await?;
    f.seek(SeekFrom::Start(start)).await?;
    let mut buf = Vec::with_capacity((size - start) as usize);
    f.read_to_end(&mut buf).await?;
    let new_offset = start + buf.len() as u64;
    cursors.update(path, new_offset);

    // Split into lines and publish each.
    let text = String::from_utf8_lossy(&buf);
    for line in text.split_inclusive('\n') {
        if line.trim().is_empty() {
            continue;
        }
        let ev = make_event(subject, path, line, chrono::Utc::now());
        let payload = serde_json::to_vec(&ev)?;
        let _ = js.publish(subject.to_owned(), payload.into()).await?.await;
    }
    Ok(())
}
