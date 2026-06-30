//! `orion logs <kind> <name>` — fetch (and optionally follow) workload logs.

use crate::{Ctx, http};
use anyhow::Result;
use clap::Args as ClapArgs;
use serde::Deserialize;
use std::time::Duration;

#[derive(ClapArgs, Debug)]
pub struct Args {
    pub kind: String,
    pub name: String,
    /// Poll for new lines after the snapshot.
    #[arg(short, long)]
    pub follow: bool,
    /// Trim output to the last N lines on first read.
    #[arg(long)]
    pub tail: Option<usize>,
    /// Sleep between polls when `--follow` (seconds, default 1).
    #[arg(long, default_value_t = 1.0)]
    pub interval: f32,
}

#[derive(Debug, Deserialize, Clone)]
struct LogEntry {
    at: String,
    node_id: String,
    stream: String,
    line: String,
}

#[derive(Debug, Deserialize)]
struct LogsView {
    #[serde(default)]
    total: usize,
    #[serde(default)]
    entries: Vec<LogEntry>,
}

pub async fn run(ctx: &Ctx, args: Args) -> Result<()> {
    let kind = super::util::canonical_kind(&args.kind);
    let base = format!("/v1/logs/{kind}/{}", args.name);
    let initial: LogsView = http::get_json(ctx, &base).await?;
    let mut since = initial.total;
    let entries = match args.tail {
        Some(n) if n < initial.entries.len() => {
            initial.entries[initial.entries.len() - n..].to_vec()
        }
        _ => initial.entries,
    };
    for e in &entries {
        emit(e);
    }
    if !args.follow {
        return Ok(());
    }
    let interval = Duration::from_millis((args.interval * 1000.0) as u64);
    loop {
        tokio::time::sleep(interval).await;
        let url = format!("{base}?since={since}");
        let resp: LogsView = match http::get_json(ctx, &url).await {
            Ok(v) => v,
            Err(e) => {
                eprintln!("(log poll error: {e})");
                continue;
            }
        };
        for e in &resp.entries {
            emit(e);
        }
        since = resp.total;
    }
}

fn emit(e: &LogEntry) {
    println!("{} [{}/{}] {}", e.at, e.node_id, e.stream, e.line);
}
