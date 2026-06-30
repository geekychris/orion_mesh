//! `orion doctor` — health probe across NATS / controller / agent.
//!
//! Designed to be the first thing you run when something looks off. Prints a
//! checklist with pass/fail; exits 1 if anything is unhealthy so CI can use
//! it as a gate.

use crate::{Ctx, http};
use anyhow::Result;
use clap::Args as ClapArgs;
use serde_json::Value;
use std::time::Duration;

#[derive(ClapArgs, Debug)]
pub struct Args {
    /// Skip the NATS probe (e.g. when running against a managed broker).
    #[arg(long)]
    pub no_nats: bool,
    /// Don't return a non-zero exit code on failure.
    #[arg(long)]
    pub no_fail: bool,
}

pub async fn run(ctx: &Ctx, args: Args) -> Result<()> {
    let mut report = Report::default();

    println!("orion doctor — {ts}", ts = chrono::Utc::now().to_rfc3339());
    println!();

    // ---- controller --------------------------------------------------------
    match tokio::time::timeout(
        Duration::from_secs(2),
        http::get_text(ctx, "/health"),
    )
    .await
    {
        Ok(Ok(_)) => report.ok("controller", "/health → ok", &ctx.controller),
        Ok(Err(e)) => report.fail("controller", &format!("/health → {e}"), &ctx.controller),
        Err(_) => report.fail("controller", "/health → timeout (2s)", &ctx.controller),
    }

    // ---- nodes -------------------------------------------------------------
    match http::get_json::<Value>(ctx, "/v1/nodes").await {
        Ok(v) => {
            let n = v.as_array().map(|a| a.len()).unwrap_or(0);
            if n == 0 {
                report.warn(
                    "agents",
                    "/v1/nodes → 0 nodes (no agent has reported)",
                    "start an agent: orion-agent --node-id <id>",
                );
            } else {
                report.ok(
                    "agents",
                    &format!("/v1/nodes → {n} reporting"),
                    &v.to_string()[..v.to_string().len().min(120)],
                );
            }
        }
        Err(e) => report.fail("agents", &format!("/v1/nodes failed: {e}"), ""),
    }

    // ---- nats ----------------------------------------------------------------
    if !args.no_nats {
        match orion_bus::client::connect(&ctx.nats_url, ctx.token.as_deref()).await {
            Ok(nc) => {
                let info = nc.server_info();
                report.ok(
                    "broker",
                    &format!(
                        "NATS connect → server={} v{}",
                        info.server_name, info.version
                    ),
                    &ctx.nats_url,
                );
                // JetStream probe — list_streams returns an iterator; success
                // means JS is enabled and we have permission.
                let js = async_nats::jetstream::new(nc);
                match tokio::time::timeout(Duration::from_secs(2), async {
                    use futures::StreamExt;
                    let mut s = js.streams();
                    let mut count = 0;
                    while let Some(item) = s.next().await {
                        if item.is_ok() {
                            count += 1;
                        }
                    }
                    count
                })
                .await
                {
                    Ok(n) => report.ok(
                        "jetstream",
                        &format!("streams iterable → {n} stream(s)"),
                        "",
                    ),
                    Err(_) => report.warn(
                        "jetstream",
                        "stream iteration timed out",
                        "verify broker was started with -js",
                    ),
                }
            }
            Err(e) => report.fail("broker", &format!("NATS connect failed: {e}"), &ctx.nats_url),
        }
    }

    // ---- controller diag --------------------------------------------------
    if let Ok(v) = http::get_json::<Value>(ctx, "/v1/diag/system").await {
        let auth_disabled = v
            .get("controller")
            .and_then(|c| c.get("auth_disabled"))
            .and_then(|b| b.as_bool())
            .unwrap_or(false);
        if auth_disabled {
            report.warn(
                "auth",
                "controller running with ORION_AUTH_DISABLED=1",
                "dev mode is fine locally; do not run this way in production",
            );
        } else {
            report.ok("auth", "cluster token enforced", "");
        }
        let inst = v
            .get("instances")
            .and_then(|i| i.get("total"))
            .and_then(|n| n.as_u64())
            .unwrap_or(0);
        let logs = v
            .get("logs")
            .and_then(|l| l.get("buffered_lines"))
            .and_then(|n| n.as_u64())
            .unwrap_or(0);
        report.note(format!(
            "instances: {inst} running · log buffer: {logs} lines"
        ));
    }

    // ---- output ----------------------------------------------------------
    println!();
    for line in report.lines() {
        println!("{line}");
    }
    println!();
    println!(
        "summary: {pass} pass, {warn} warn, {fail} fail",
        pass = report.n_ok,
        warn = report.n_warn,
        fail = report.n_fail
    );
    if report.n_fail > 0 && !args.no_fail {
        std::process::exit(1);
    }
    Ok(())
}

#[derive(Default)]
struct Report {
    out: Vec<String>,
    n_ok: usize,
    n_warn: usize,
    n_fail: usize,
}
impl Report {
    fn ok(&mut self, area: &str, msg: &str, detail: &str) {
        self.n_ok += 1;
        self.out
            .push(format!("  ✓ {:<12} {msg}{}", area, fmt_detail(detail)));
    }
    fn warn(&mut self, area: &str, msg: &str, detail: &str) {
        self.n_warn += 1;
        self.out
            .push(format!("  ~ {:<12} {msg}{}", area, fmt_detail(detail)));
    }
    fn fail(&mut self, area: &str, msg: &str, detail: &str) {
        self.n_fail += 1;
        self.out
            .push(format!("  ✗ {:<12} {msg}{}", area, fmt_detail(detail)));
    }
    fn note(&mut self, msg: String) {
        self.out.push(format!("    {msg}"));
    }
    fn lines(&self) -> &[String] {
        &self.out
    }
}

fn fmt_detail(s: &str) -> String {
    if s.is_empty() {
        String::new()
    } else {
        format!("  ({s})")
    }
}
