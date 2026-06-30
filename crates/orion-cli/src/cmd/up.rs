//! `orion up` — bring up a full local OrionMesh stack for development.
//!
//! Spawns NATS (via Docker), then the controller + agent (and optionally the
//! UI) using whichever binaries are on `$PATH` (so the installed ones take
//! precedence; falls back to `target/debug` when run from the workspace).
//! Combined logs stream to stdout with a per-component prefix. Ctrl-C kills
//! the lot in reverse start order.

use crate::Ctx;
use anyhow::{Context, Result};
use clap::{Args as ClapArgs, ValueEnum};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::signal;

#[derive(ClapArgs, Debug)]
pub struct Args {
    /// How to start the NATS broker.
    ///   auto    — try `nats-server` on PATH first; fall back to Docker if missing (default)
    ///   native  — require `nats-server` on PATH; error if not installed
    ///   docker  — always use `docker run nats:2.10`
    ///   none    — assume something else is already running at --nats-url
    #[arg(long, value_enum, default_value_t = NatsBackend::Auto)]
    pub nats: NatsBackend,
    /// Skip starting the broker entirely. Equivalent to `--nats none`.
    #[arg(long, conflicts_with = "nats")]
    pub no_nats: bool,
    /// Skip starting the UI.
    #[arg(long)]
    pub no_ui: bool,
    /// Skip starting the agent.
    #[arg(long)]
    pub no_agent: bool,
    /// Node id for the agent.
    #[arg(long, default_value = "local-dev")]
    pub node_id: String,
    /// HTTP bind address for the controller.
    #[arg(long, default_value = "127.0.0.1:7878")]
    pub controller_bind: String,
    /// Persist controller state to this path (default: in-memory).
    #[arg(long)]
    pub store: Option<String>,
    /// Enforce auth (default: dev mode with `ORION_AUTH_DISABLED=1`).
    #[arg(long)]
    pub auth: bool,
    /// Container name when --nats=docker.
    #[arg(long, default_value = "orion-nats")]
    pub nats_container: String,
    /// Bind the agent to a non-default NATS URL.
    #[arg(long)]
    pub agent_nats_url: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
#[clap(rename_all = "lowercase")]
pub enum NatsBackend {
    Auto,
    Native,
    Docker,
    None,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ResolvedNats {
    Native,
    Docker,
    None,
}

pub async fn run(ctx: &Ctx, args: Args) -> Result<()> {
    // ---------- NATS ----------
    let nats_choice = if args.no_nats {
        NatsBackend::None
    } else {
        args.nats
    };
    let resolved = match nats_choice {
        NatsBackend::Auto => {
            if which("nats-server").is_some() {
                eprintln!("[up] nats-server found on PATH → using native broker");
                ResolvedNats::Native
            } else {
                eprintln!("[up] nats-server not on PATH → falling back to Docker (install nats-server to avoid Docker — `brew install nats-server` or https://nats.io)");
                ResolvedNats::Docker
            }
        }
        NatsBackend::Native => ResolvedNats::Native,
        NatsBackend::Docker => ResolvedNats::Docker,
        NatsBackend::None => ResolvedNats::None,
    };

    let mut children: Vec<(&'static str, Child)> = Vec::new();

    match resolved {
        ResolvedNats::Native => {
            let child = start_native_nats(&mut children).await?;
            let _ = child; // pushed into `children`
        }
        ResolvedNats::Docker => ensure_docker_nats(&args.nats_container).await?,
        ResolvedNats::None => eprintln!("[up] skipping NATS (--nats none)"),
    }

    // ---------- controller ----------
    let mut ctrl = Command::new(find_bin("orion-controller")?);
    ctrl.arg("--bind").arg(&args.controller_bind);
    if !args.auth {
        ctrl.env("ORION_AUTH_DISABLED", "1");
    }
    ctrl.env(
        "ORION_STORE_PATH",
        args.store.clone().unwrap_or_else(|| "sqlite::memory:".into()),
    );
    ctrl.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut ctrl_child = ctrl.spawn().context("spawning orion-controller")?;
    pipe("controller", ctrl_child.stdout.take(), ctrl_child.stderr.take());
    children.push(("controller", ctrl_child));
    eprintln!("[up] controller starting on http://{}", args.controller_bind);

    // Probe /health for a couple of seconds.
    let controller_url = format!("http://{}/health", args.controller_bind);
    for i in 0..20 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if reqwest::get(&controller_url).await.is_ok() {
            eprintln!("[up] controller ready after {}ms", (i + 1) * 200);
            break;
        }
    }

    // ---------- agent ----------
    if !args.no_agent {
        let mut a = Command::new(find_bin("orion-agent")?);
        a.arg("--node-id").arg(&args.node_id);
        a.arg("--heartbeat-interval").arg("2");
        if !args.auth {
            a.env("ORION_AUTH_DISABLED", "1");
        }
        if let Some(url) = &args.agent_nats_url {
            a.env("NATS_URL", url);
        }
        a.stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = a.spawn().context("spawning orion-agent")?;
        pipe("agent", child.stdout.take(), child.stderr.take());
        children.push(("agent", child));
        eprintln!("[up] agent {} started", args.node_id);
    }

    // ---------- ui ----------
    if !args.no_ui {
        if let Some(ui_bin) = find_bin_optional("orion-ui") {
            let mut u = Command::new(ui_bin);
            u.stdout(Stdio::piped()).stderr(Stdio::piped());
            u.env("ORION_CONTROLLER_URL", &ctx.controller);
            match u.spawn() {
                Ok(mut child) => {
                    pipe("ui", child.stdout.take(), child.stderr.take());
                    children.push(("ui", child));
                    eprintln!("[up] ui started");
                }
                Err(e) => eprintln!("[up] could not start ui: {e}"),
            }
        } else {
            eprintln!("[up] orion-ui not on PATH — skipping UI");
        }
    }

    eprintln!("[up] all components running. Ctrl-C to stop.");

    let _ = signal::ctrl_c().await;
    eprintln!("\n[up] Ctrl-C received, shutting down");

    // Reverse-order shutdown — children includes the native nats-server if we started one.
    while let Some((name, mut child)) = children.pop() {
        eprintln!("[up] stopping {name}");
        let _ = child.start_kill();
        let _ = tokio::time::timeout(Duration::from_secs(3), child.wait()).await;
    }
    if matches!(resolved, ResolvedNats::Docker) {
        eprintln!("[up] stopping docker container {}", args.nats_container);
        let _ = Command::new("docker")
            .args(["stop", &args.nats_container])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await;
    }
    eprintln!("[up] done");
    Ok(())
}

async fn ensure_docker_nats(container: &str) -> Result<()> {
    // Already running?
    let out = Command::new("docker")
        .args(["ps", "--format", "{{.Names}}"])
        .output()
        .await
        .context("running `docker ps` — is docker installed?")?;
    let names = String::from_utf8_lossy(&out.stdout);
    if names.lines().any(|l| l.trim() == container) {
        eprintln!("[up] NATS already running (container {container})");
        return wait_for_nats_ready().await;
    }
    eprintln!("[up] starting NATS via docker (docker run nats:2.10 -js)");
    let status = Command::new("docker")
        .args([
            "run", "-d", "--rm",
            "--name", container,
            "-p", "4222:4222",
            "nats:2.10", "-js",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()
        .await
        .context("spawning docker run")?;
    if !status.success() {
        anyhow::bail!("docker run failed");
    }
    wait_for_nats_ready().await
}

async fn start_native_nats(children: &mut Vec<(&'static str, Child)>) -> Result<()> {
    let bin = which("nats-server").ok_or_else(|| {
        anyhow::anyhow!(
            "--nats=native requested but `nats-server` is not on PATH. \
             Install it (`brew install nats-server`, apt, or https://nats.io), \
             or use --nats=docker / --nats=auto."
        )
    })?;
    eprintln!("[up] starting NATS via native nats-server ({bin})");
    let mut cmd = Command::new(bin);
    cmd.arg("-js")
        .arg("--addr").arg("127.0.0.1")
        .arg("-p").arg("4222")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd.spawn().context("spawning nats-server")?;
    pipe("nats", child.stdout.take(), child.stderr.take());
    children.push(("nats-server", child));
    wait_for_nats_ready().await
}

async fn wait_for_nats_ready() -> Result<()> {
    for i in 0..40 {
        if tokio::net::TcpStream::connect("127.0.0.1:4222").await.is_ok() {
            eprintln!("[up] NATS ready after {}ms", (i + 1) * 100);
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    anyhow::bail!("NATS did not become ready on 127.0.0.1:4222 within 4s")
}

fn find_bin(name: &str) -> Result<String> {
    find_bin_optional(name).ok_or_else(|| {
        anyhow::anyhow!(
            "{name} not found on PATH or in target/{{debug,release}}. \
             Run scripts/install-bins.sh."
        )
    })
}

fn find_bin_optional(name: &str) -> Option<String> {
    // PATH lookup first (covers ~/.orion/bin via .zshrc).
    if let Some(p) = which(name) {
        return Some(p);
    }
    // Fallback: workspace target dirs (handy when run from `cargo run`).
    for dir in ["target/release", "target/debug"] {
        let path = std::path::Path::new(dir).join(name);
        if path.is_file() {
            return Some(path.to_string_lossy().into_owned());
        }
    }
    None
}

fn which(name: &str) -> Option<String> {
    let path = std::env::var("PATH").unwrap_or_default();
    for dir in path.split(':') {
        let p = std::path::Path::new(dir).join(name);
        if p.is_file() {
            return Some(p.to_string_lossy().into_owned());
        }
    }
    None
}

fn pipe(
    prefix: &'static str,
    stdout: Option<tokio::process::ChildStdout>,
    stderr: Option<tokio::process::ChildStderr>,
) {
    if let Some(out) = stdout {
        tokio::spawn(forward(prefix, "out", BufReader::new(out)));
    }
    if let Some(err) = stderr {
        tokio::spawn(forward(prefix, "err", BufReader::new(err)));
    }
}

async fn forward<R>(prefix: &'static str, stream: &'static str, mut r: BufReader<R>)
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut line = String::new();
    loop {
        line.clear();
        match r.read_line(&mut line).await {
            Ok(0) => break,
            Ok(_) => {
                let trimmed = line.trim_end_matches('\n');
                let tag = if stream == "err" {
                    format!("{prefix}!")
                } else {
                    prefix.to_owned()
                };
                println!("[{tag}] {trimmed}");
            }
            Err(_) => break,
        }
    }
}
