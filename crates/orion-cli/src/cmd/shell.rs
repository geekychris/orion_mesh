//! `orion completions`, `orion exec`, `orion env`.
//!
//! Three small commands that round out the day-to-day shell experience:
//!
//! * `completions` — emit bash/zsh/fish/elvish completions.
//! * `env`         — emit shell-eval'able `KEY=value` lines so a script
//!                   can pick up `ORION_CONTROLLER_URL` etc. without
//!                   remembering them.
//! * `exec`        — wrap an arbitrary shell command as a one-shot Task,
//!                   dispatch it, stream logs back. The "I want to run
//!                   `python my-script.py` somewhere on the cluster" verb.

use crate::Ctx;
use anyhow::{Context, Result};
use clap::{Args as ClapArgs, CommandFactory, Subcommand};
use clap_complete::Shell;
use serde_json::json;
use std::io;
use uuid::Uuid;

#[derive(Subcommand, Debug)]
pub enum CompletionsSub {
    /// Bash completion script.
    Bash,
    /// Zsh completion script.
    Zsh,
    /// Fish completion script.
    Fish,
    /// PowerShell completion script.
    Powershell,
    /// Elvish completion script.
    Elvish,
}

#[derive(ClapArgs, Debug)]
pub struct CompletionsArgs {
    #[command(subcommand)]
    pub shell: CompletionsSub,
}

pub fn run_completions(args: CompletionsArgs) -> Result<()> {
    let mut cmd = crate::Cli::command();
    let bin = "orion".to_owned();
    let mut out = io::stdout();
    let shell = match args.shell {
        CompletionsSub::Bash => Shell::Bash,
        CompletionsSub::Zsh => Shell::Zsh,
        CompletionsSub::Fish => Shell::Fish,
        CompletionsSub::Powershell => Shell::PowerShell,
        CompletionsSub::Elvish => Shell::Elvish,
    };
    clap_complete::generate(shell, &mut cmd, bin, &mut out);
    Ok(())
}

// ============================================================ env

#[derive(ClapArgs, Debug, Clone)]
pub struct EnvArgs {
    /// Output format. `sh` is the default and works for bash/zsh/sh; `fish`
    /// emits `set -x` lines; `json` emits a JSON object.
    #[arg(long, value_parser = ["sh", "fish", "json"], default_value = "sh")]
    pub format: String,
}

pub fn run_env(ctx: &Ctx, args: EnvArgs) -> Result<()> {
    let pairs = [
        ("ORION_CONTROLLER_URL", ctx.controller.as_str()),
        ("NATS_URL", ctx.nats_url.as_str()),
    ];
    let token = ctx.token.clone().unwrap_or_default();
    match args.format.as_str() {
        "sh" => {
            for (k, v) in pairs {
                println!("export {k}={}", shell_quote(v));
            }
            if !token.is_empty() {
                println!("export ORION_CLUSTER_TOKEN={}", shell_quote(&token));
            }
        }
        "fish" => {
            for (k, v) in pairs {
                println!("set -x {k} {}", shell_quote(v));
            }
            if !token.is_empty() {
                println!("set -x ORION_CLUSTER_TOKEN {}", shell_quote(&token));
            }
        }
        "json" => {
            let mut obj = serde_json::Map::new();
            for (k, v) in pairs {
                obj.insert(k.to_owned(), json!(v));
            }
            if !token.is_empty() {
                obj.insert("ORION_CLUSTER_TOKEN".into(), json!(token));
            }
            println!("{}", serde_json::to_string_pretty(&obj)?);
        }
        other => anyhow::bail!("unknown format: {other}"),
    }
    Ok(())
}

pub(crate) fn shell_quote(s: &str) -> String {
    if !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '/' | ':' | '@'))
    {
        s.to_owned()
    } else {
        let escaped = s.replace('\'', r"'\''");
        format!("'{escaped}'")
    }
}

// ============================================================ exec

#[derive(ClapArgs, Debug)]
pub struct ExecArgs {
    /// Optional Task name. Default: `oexec-<short-uuid>`.
    #[arg(long)]
    pub name: Option<String>,
    /// Runtime kind — `native` (default) or `docker`.
    #[arg(long, value_parser = ["native", "docker"], default_value = "native")]
    pub runtime: String,
    /// For docker: image name.
    #[arg(long)]
    pub image: Option<String>,
    /// Add an env var (repeatable). Format: `K=V`.
    #[arg(long = "env", value_name = "K=V")]
    pub env: Vec<String>,
    /// Tail logs after dispatch (default true).
    #[arg(long, default_value_t = true)]
    pub watch: bool,
    /// Don't auto-delete the Task after it exits.
    #[arg(long)]
    pub keep: bool,
    /// The command to run. Everything after `--` is the argv.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true, num_args = 1..)]
    pub argv: Vec<String>,
}

pub async fn run_exec(ctx: &Ctx, args: ExecArgs) -> Result<()> {
    use crate::http;
    if args.argv.is_empty() {
        anyhow::bail!("nothing to exec — pass the command after `--`");
    }
    let name = args
        .name
        .clone()
        .unwrap_or_else(|| format!("oexec-{}", short_id()));
    // Build a Task YAML inline.
    let mut env_map = serde_yml::Mapping::new();
    for kv in &args.env {
        if let Some((k, v)) = kv.split_once('=') {
            env_map.insert(k.into(), v.into());
        }
    }
    let mut runtime = serde_yml::Mapping::new();
    runtime.insert("kind".into(), args.runtime.as_str().into());
    match args.runtime.as_str() {
        "native" => {
            runtime.insert("exec".into(), args.argv[0].as_str().into());
            let rest: Vec<serde_yml::Value> = args.argv[1..]
                .iter()
                .map(|s| s.as_str().into())
                .collect();
            runtime.insert("args".into(), rest.into());
        }
        "docker" => {
            let image = args
                .image
                .clone()
                .ok_or_else(|| anyhow::anyhow!("--image required for docker runtime"))?;
            runtime.insert("image".into(), image.into());
            let argv: Vec<serde_yml::Value> = args.argv.iter().map(|s| s.as_str().into()).collect();
            runtime.insert("args".into(), argv.into());
        }
        _ => unreachable!(),
    }
    if !env_map.is_empty() {
        runtime.insert("env".into(), env_map.into());
    }
    let mut spec = serde_yml::Mapping::new();
    spec.insert("runtime".into(), runtime.into());
    let mut metadata = serde_yml::Mapping::new();
    metadata.insert("name".into(), name.as_str().into());
    let mut top = serde_yml::Mapping::new();
    top.insert("apiVersion".into(), "orionmesh.dev/v1".into());
    top.insert("kind".into(), "Task".into());
    top.insert("metadata".into(), metadata.into());
    top.insert("spec".into(), spec.into());
    let yaml = serde_yml::to_string(&serde_yml::Value::Mapping(top))?;

    http::post_yaml(ctx, "/v1/resources/apply", yaml.clone())
        .await
        .with_context(|| "applying oexec Task")?;
    eprintln!("[exec] applied Task/{name}");
    let resp = http::post_empty(ctx, &format!("/v1/dispatch/Task/{name}"))
        .await
        .with_context(|| "dispatching oexec Task")?;
    let node = resp.get("node").and_then(|v| v.as_str()).unwrap_or("?");
    eprintln!("[exec] dispatched to {node}");

    if args.watch {
        let logs = crate::cmd::logs::Args {
            kind: "Task".into(),
            name: name.clone(),
            follow: false,
            tail: None,
            interval: 1.0,
        };
        // Poll until we see at least one line and the workload has had time to exit.
        // Simpler: wait + fetch once.
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        crate::cmd::logs::run(ctx, logs).await?;
    }
    if !args.keep {
        let _ = http::delete_path(ctx, &format!("/v1/resources/Task/{name}")).await;
        eprintln!("[exec] deleted Task/{name}");
    }
    Ok(())
}

fn short_id() -> String {
    Uuid::new_v4().to_string().chars().take(8).collect()
}

// ============================================================ tests

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shell_quote_passes_safe_strings_through() {
        assert_eq!(shell_quote("simple"), "simple");
        assert_eq!(shell_quote("http://127.0.0.1:7878"), "http://127.0.0.1:7878");
        assert_eq!(shell_quote("a-b_c.d/e:f@g"), "a-b_c.d/e:f@g");
    }

    #[test]
    fn shell_quote_escapes_dangerous_strings() {
        assert_eq!(shell_quote("has space"), "'has space'");
        assert_eq!(shell_quote("with$dollar"), "'with$dollar'");
        // Single quote in the middle escapes via the '\'' trick.
        assert_eq!(shell_quote("it's"), r"'it'\''s'");
    }

    #[test]
    fn shell_quote_empty_string() {
        assert_eq!(shell_quote(""), "''");
    }
}
