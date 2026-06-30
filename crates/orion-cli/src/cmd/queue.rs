//! `orion queue {pub,sub,ls,describe,purge}` — work with named queues.
//!
//! Subjects + streams follow the conventions from `orion_types`:
//!     subject = `orion.queue.<name>`        (override: spec.subject)
//!     stream  = `ORION_QUEUE_<NAME_UPPER>`  (override: spec.stream)
//!
//! Delivery semantics are enforced on the *subscriber* side: a `work` queue
//! requires consumers to share a durable name (load-balanced), a `topic` queue
//! forbids sharing (broadcast).

use crate::{Ctx, http, nats, output};
use anyhow::{Context, Result};
use async_nats::jetstream::{self, consumer};
use clap::{Args as ClapArgs, Subcommand};
use futures::StreamExt;
use orion_types::{QueueSpec, QueueType, Resource, ResourceBody};
use serde_json::Value;
use std::io::{BufRead, Write};
use std::time::Duration;

#[derive(Subcommand, Debug)]
pub enum Sub {
    /// Read ndjson on stdin, publish each line to the queue.
    Pub(PubArgs),
    /// Subscribe and print rows. `--group` joins as a shared durable;
    /// otherwise a unique per-process durable is used.
    Sub(SubArgs),
    /// List queues with declared type and live stream/consumer counts.
    Ls,
    /// Show details for one queue: spec + JetStream state + active consumers.
    Describe(DescribeArgs),
    /// Drop all messages from the queue's stream (destructive).
    Purge(PurgeArgs),
}

#[derive(ClapArgs, Debug)]
pub struct PubArgs {
    pub name: String,
    /// Append `.${row[field]}` to the subject to enable per-row routing.
    #[arg(long)]
    pub subject_from: Option<String>,
    /// Skip if the queue resource doesn't exist (auto-fail otherwise).
    #[arg(long)]
    pub allow_missing: bool,
}

#[derive(ClapArgs, Debug)]
pub struct SubArgs {
    pub name: String,
    /// Consumer durable name. For `work` queues, sharing this across processes
    /// load-balances messages. For `topic` queues, must be unique per process
    /// (default: auto-generated from PID).
    #[arg(long)]
    pub group: Option<String>,
    /// Run forever; otherwise stop after `--limit` messages.
    #[arg(long, conflicts_with = "limit")]
    pub forever: bool,
    /// Stop after this many messages.
    #[arg(long, default_value_t = 0)]
    pub limit: usize,
    /// Don't ack messages (useful for tail-style inspection on a work queue).
    #[arg(long)]
    pub no_ack: bool,
}

#[derive(ClapArgs, Debug)]
pub struct DescribeArgs {
    pub name: String,
}

#[derive(ClapArgs, Debug)]
pub struct PurgeArgs {
    pub name: String,
    /// Skip the confirmation prompt.
    #[arg(short, long)]
    pub yes: bool,
}

pub async fn run(ctx: &Ctx, sub: Sub) -> Result<()> {
    match sub {
        Sub::Pub(a) => run_pub(ctx, a).await,
        Sub::Sub(a) => run_sub(ctx, a).await,
        Sub::Ls => run_ls(ctx).await,
        Sub::Describe(a) => run_describe(ctx, a).await,
        Sub::Purge(a) => run_purge(ctx, a).await,
    }
}

// --------------------------------------------------------------------------- pub

async fn run_pub(ctx: &Ctx, args: PubArgs) -> Result<()> {
    let queue = match fetch_queue(ctx, &args.name).await? {
        Some(q) => q,
        None if args.allow_missing => {
            // Auto-create a default work queue resource via apply.
            let yaml = format!(
                "apiVersion: orionmesh.dev/v1\nkind: Queue\nmetadata:\n  name: {}\nspec:\n  type: work\n",
                args.name
            );
            http::post_yaml(ctx, "/v1/resources/apply", yaml).await?;
            fetch_queue(ctx, &args.name)
                .await?
                .ok_or_else(|| anyhow::anyhow!("queue auto-create failed"))?
        }
        None => anyhow::bail!(
            "queue {} not found. Run:\n  orion gen queue {} --type work | orion apply -f -",
            args.name,
            args.name
        ),
    };
    let nc = nats::connect(ctx).await?;
    let js = jetstream::new(nc.clone());
    let (subject, cfg) = orion_bus::client::queue_stream_config(&args.name, &queue);
    orion_bus::client::ensure_stream(&js, cfg)
        .await
        .with_context(|| format!("ensuring stream for queue {}", args.name))?;

    let stdin = std::io::stdin();
    let stderr = std::io::stderr();
    let mut errlock = stderr.lock();

    let mut n = 0u64;
    for line in stdin.lock().lines() {
        let line = line.context("reading stdin")?;
        if line.trim().is_empty() {
            continue;
        }
        let parsed: Value = serde_json::from_str(&line)
            .with_context(|| format!("invalid json on stdin: {}", &line[..line.len().min(80)]))?;
        let publish_subject = match &args.subject_from {
            Some(field) => match parsed.get(field).and_then(|v| v.as_str()) {
                Some(suffix) => format!("{subject}.{suffix}"),
                None => subject.clone(),
            },
            None => subject.clone(),
        };
        let bytes = line.into_bytes();
        let _seq = orion_bus::client::publish_bytes(&js, &publish_subject, bytes).await?;
        n += 1;
    }
    let _ = writeln!(errlock, "published {n} messages to {subject}");
    Ok(())
}

// --------------------------------------------------------------------------- sub

async fn run_sub(ctx: &Ctx, args: SubArgs) -> Result<()> {
    let queue = fetch_queue(ctx, &args.name)
        .await?
        .ok_or_else(|| anyhow::anyhow!("queue {} not found", args.name))?;
    let nc = nats::connect(ctx).await?;
    let js = jetstream::new(nc);

    let (subject, cfg) = orion_bus::client::queue_stream_config(&args.name, &queue);
    let stream = orion_bus::client::ensure_stream(&js, cfg).await?;

    let durable = match queue.queue_type {
        QueueType::Work => args
            .group
            .clone()
            .unwrap_or_else(|| format!("{}-workers", args.name)),
        QueueType::Topic => {
            // Each subscriber needs its own durable so JetStream tracks an
            // independent cursor per process — that's how broadcast works.
            let pid = std::process::id();
            let host = hostname();
            args.group
                .clone()
                .unwrap_or_else(|| format!("{}-{host}-{pid}", args.name))
        }
    };
    if matches!(queue.queue_type, QueueType::Topic) && args.group.is_some() {
        eprintln!(
            "warning: --group {} on a topic queue causes load-balancing across processes that share it. \
             For broadcast, omit --group.",
            args.group.as_deref().unwrap_or("")
        );
    }

    eprintln!(
        "[orion queue sub] queue={} type={:?} subject={} durable={}",
        args.name, queue.queue_type, subject, durable
    );

    let consumer = stream
        .get_or_create_consumer(
            &durable,
            consumer::pull::Config {
                durable_name: Some(durable.clone()),
                filter_subject: subject.clone(),
                ack_policy: if args.no_ack {
                    consumer::AckPolicy::None
                } else {
                    consumer::AckPolicy::Explicit
                },
                ..Default::default()
            },
        )
        .await
        .with_context(|| format!("creating consumer {durable}"))?;

    let mut messages = consumer
        .messages()
        .await
        .context("opening consumer message stream")?;

    let mut n = 0usize;
    let stdout = std::io::stdout();
    let mut outlock = stdout.lock();
    loop {
        let next = tokio::time::timeout(Duration::from_secs(60 * 60), messages.next()).await;
        let m = match next {
            Ok(Some(Ok(m))) => m,
            Ok(Some(Err(e))) => {
                eprintln!("(consumer error: {e})");
                continue;
            }
            Ok(None) => break,
            Err(_) => continue,
        };
        let info = m.info().ok();
        let seq = info.as_ref().map(|i| i.stream_sequence).unwrap_or(0);
        let payload = std::str::from_utf8(&m.payload)
            .map(|s| s.to_owned())
            .unwrap_or_else(|_| format!("<{} bytes>", m.payload.len()));
        let _ = writeln!(outlock, "{seq}\t{}\t{payload}", m.subject);
        if !args.no_ack {
            if let Err(e) = m.ack().await {
                eprintln!("(ack failed: {e})");
            }
        }
        n += 1;
        if !args.forever && args.limit > 0 && n >= args.limit {
            break;
        }
    }
    eprintln!("consumed {n} messages");
    Ok(())
}

// --------------------------------------------------------------------------- ls

async fn run_ls(ctx: &Ctx) -> Result<()> {
    let resources: Vec<Resource> = http::get_json(ctx, "/v1/resources/Queue").await?;
    let nc = nats::connect(ctx).await;
    let js = nc.as_ref().ok().map(|c| jetstream::new(c.clone()));

    if matches!(ctx.output, output::Format::Json) {
        output::print_json(&resources)?;
        return Ok(());
    }

    let mut rows = Vec::with_capacity(resources.len());
    for r in &resources {
        let (name, spec) = match &r.body {
            ResourceBody::Queue { spec, .. } => (r.metadata.name.0.clone(), spec.clone()),
            _ => continue,
        };
        let qtype = match spec.queue_type {
            QueueType::Topic => "topic",
            QueueType::Work => "work",
        };
        let (subject, cfg) = orion_bus::client::queue_stream_config(&name, &spec);
        let mut msgs = String::from("-");
        let mut consumers = String::from("-");
        if let Some(js) = &js {
            if let Ok(mut stream) = js.get_stream(&cfg.name).await {
                if let Ok(info) = stream.info().await {
                    msgs = info.state.messages.to_string();
                    consumers = info.state.consumer_count.to_string();
                }
            }
        }
        rows.push(vec![name, qtype.to_owned(), subject, msgs, consumers]);
    }
    output::render_table(
        &["name", "type", "subject", "messages", "consumers"],
        &rows,
    );
    Ok(())
}

// --------------------------------------------------------------------------- describe

async fn run_describe(ctx: &Ctx, args: DescribeArgs) -> Result<()> {
    let resource = fetch_queue_resource(ctx, &args.name)
        .await?
        .ok_or_else(|| anyhow::anyhow!("queue {} not found", args.name))?;
    let spec = match &resource.body {
        ResourceBody::Queue { spec, .. } => spec.clone(),
        _ => anyhow::bail!("resource {} is not a Queue", args.name),
    };
    let (subject, cfg) = orion_bus::client::queue_stream_config(&args.name, &spec);

    println!("name:    {}", args.name);
    println!("type:    {:?}", spec.queue_type);
    println!("subject: {subject}");
    println!("stream:  {}", cfg.name);

    if let Ok(nc) = nats::connect(ctx).await {
        let js = jetstream::new(nc);
        if let Ok(stream) = js.get_stream(&cfg.name).await {
            if let Ok(info) = stream.get_info().await {
                println!("messages: {}", info.state.messages);
                println!("bytes:    {}", info.state.bytes);
                println!("first_seq: {}", info.state.first_sequence);
                println!("last_seq:  {}", info.state.last_sequence);
                println!("consumers: {}", info.state.consumer_count);
            }
            println!("\n-- consumers --");
            let mut consumers = stream.consumers();
            while let Some(c) = consumers.next().await {
                match c {
                    Ok(info) => println!(
                        "  {} pending_acks={} delivered={} ack_floor={}",
                        info.name,
                        info.num_ack_pending,
                        info.delivered.stream_sequence,
                        info.ack_floor.stream_sequence,
                    ),
                    Err(e) => eprintln!("  (consumer info error: {e})"),
                }
            }
        } else {
            println!("stream not yet created — publish a message first");
        }
    }
    Ok(())
}

// --------------------------------------------------------------------------- purge

async fn run_purge(ctx: &Ctx, args: PurgeArgs) -> Result<()> {
    if !args.yes {
        eprint!("This will delete all messages from queue {}. Type the name to confirm: ", args.name);
        let _ = std::io::stderr().flush();
        let mut line = String::new();
        std::io::stdin().read_line(&mut line)?;
        if line.trim() != args.name {
            anyhow::bail!("confirmation did not match — aborting");
        }
    }
    let queue = fetch_queue(ctx, &args.name)
        .await?
        .ok_or_else(|| anyhow::anyhow!("queue {} not found", args.name))?;
    let nc = nats::connect(ctx).await?;
    let js = jetstream::new(nc);
    let (_, cfg) = orion_bus::client::queue_stream_config(&args.name, &queue);
    let stream = js.get_stream(&cfg.name).await?;
    let n = stream.purge().await?;
    println!("purged {} messages from {}", n.purged, args.name);
    Ok(())
}

// --------------------------------------------------------------------------- helpers

async fn fetch_queue(ctx: &Ctx, name: &str) -> Result<Option<QueueSpec>> {
    Ok(fetch_queue_resource(ctx, name)
        .await?
        .and_then(|r| match r.body {
            ResourceBody::Queue { spec, .. } => Some(spec),
            _ => None,
        }))
}

async fn fetch_queue_resource(ctx: &Ctx, name: &str) -> Result<Option<Resource>> {
    let url = format!("{}/v1/resources/Queue/{name}", ctx.controller);
    let mut headers = reqwest::header::HeaderMap::new();
    if let Some(t) = &ctx.token {
        if !t.is_empty() {
            headers.insert(
                reqwest::header::AUTHORIZATION,
                reqwest::header::HeaderValue::from_str(&format!("Bearer {t}"))?,
            );
        }
    }
    let resp = reqwest::Client::builder()
        .default_headers(headers)
        .build()?
        .get(&url)
        .send()
        .await?;
    if resp.status().as_u16() == 404 {
        return Ok(None);
    }
    let r = resp.error_for_status()?.json::<Resource>().await?;
    Ok(Some(r))
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("HOST"))
        .unwrap_or_else(|_| "host".into())
        .replace(|c: char| !(c.is_ascii_alphanumeric() || c == '-' || c == '_'), "_")
}
