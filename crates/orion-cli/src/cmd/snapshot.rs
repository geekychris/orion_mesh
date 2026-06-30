//! `orion snapshot create / restore` — pack every resource into a single
//! YAML stream and re-apply it. Useful for moving a cluster between machines,
//! for diffing two clusters, or for a panic-button "what did I have running
//! yesterday" recovery.

use crate::{Ctx, http};
use anyhow::{Context, Result};
use clap::{Args as ClapArgs, Subcommand};
use serde_json::Value;
use std::fs;
use std::path::PathBuf;

#[derive(Subcommand, Debug)]
pub enum Sub {
    /// Write every resource the controller knows about to a file (or stdout).
    Create(CreateArgs),
    /// Re-apply every resource document in a snapshot file.
    Restore(RestoreArgs),
}

#[derive(ClapArgs, Debug)]
pub struct CreateArgs {
    /// Output path. `-` (default) writes to stdout.
    #[arg(short = 'o', long = "out", default_value = "-")]
    pub out: PathBuf,
    /// Comma-separated kinds to include. Default: every declarative kind.
    #[arg(long, default_value = "Service,Task,Schedule,Dataset,Model,Project,Volume,Network,Queue,Runtime,Capability,Policy,Integration,Secret")]
    pub kinds: String,
}

#[derive(ClapArgs, Debug)]
pub struct RestoreArgs {
    /// Snapshot file. `-` (default) reads from stdin.
    #[arg(short = 'f', long = "file", default_value = "-")]
    pub file: PathBuf,
    /// Re-apply even if a resource with the same kind+name already exists.
    /// (Apply is idempotent already; this exists for future "skip-if-present" mode.)
    #[arg(long)]
    pub force: bool,
}

pub async fn run(ctx: &Ctx, sub: Sub) -> Result<()> {
    match sub {
        Sub::Create(a) => create(ctx, a).await,
        Sub::Restore(a) => restore(ctx, a).await,
    }
}

async fn create(ctx: &Ctx, args: CreateArgs) -> Result<()> {
    let kinds: Vec<&str> = args.kinds.split(',').map(|s| s.trim()).collect();
    let mut out = String::with_capacity(4096);
    let mut count = 0u32;
    for kind in &kinds {
        let path = format!("/v1/resources/{kind}");
        let v: Value = match http::get_json(ctx, &path).await {
            Ok(v) => v,
            Err(e) => {
                eprintln!("skip {kind}: {e}");
                continue;
            }
        };
        let arr = match v.as_array() {
            Some(a) => a,
            None => continue,
        };
        for r in arr {
            let mut clean = r.clone();
            // Strip status — restore is a "desired state" replay.
            if let Some(obj) = clean.as_object_mut() {
                obj.remove("status");
            }
            let yaml = serde_yml::to_string(&clean).context("encoding resource")?;
            out.push_str("---\n");
            out.push_str(&yaml);
            count += 1;
        }
    }
    if args.out.as_os_str() == "-" {
        print!("{out}");
    } else {
        fs::write(&args.out, &out).with_context(|| format!("writing {}", args.out.display()))?;
        eprintln!("wrote {count} resources to {}", args.out.display());
    }
    Ok(())
}

async fn restore(ctx: &Ctx, args: RestoreArgs) -> Result<()> {
    let body = super::util::read_yaml_input(&args.file)?;
    let mut applied = 0u32;
    let mut failed = 0u32;
    for doc in body.split("\n---\n") {
        let trimmed = doc.trim();
        if trimmed.is_empty() {
            continue;
        }
        match http::post_yaml(ctx, "/v1/resources/apply", trimmed.to_owned()).await {
            Ok(_) => applied += 1,
            Err(e) => {
                failed += 1;
                eprintln!("failed: {e}");
            }
        }
    }
    eprintln!("restore done: {applied} applied, {failed} failed");
    let _ = args.force;
    Ok(())
}
