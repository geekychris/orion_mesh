//! `orion run -f <file>` — apply + dispatch in one shot, optionally tail logs.

use crate::{Ctx, http};
use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::path::PathBuf;

#[derive(ClapArgs, Debug)]
pub struct Args {
    /// File (or `-` for stdin) containing a Service or Task YAML/JSON.
    #[arg(short = 'f', long = "file", default_value = "-")]
    file: PathBuf,
    /// Tail logs after dispatch.
    #[arg(short, long)]
    watch: bool,
}

pub async fn run(ctx: &Ctx, args: Args) -> Result<()> {
    let body = super::util::read_yaml_input(&args.file)?;
    let r = orion_types::Resource::from_yaml(&body).context("parsing resource yaml")?;
    r.validate().context("validating resource")?;
    let kind = r.kind_str();
    let name = r.metadata.name.0.clone();

    if !matches!(kind, "Service" | "Task") {
        anyhow::bail!(
            "orion run only supports kind=Service|Task — got {kind}. \
             Use `orion apply -f {}` to apply a {kind} without dispatching.",
            args.file.display()
        );
    }

    http::post_yaml(ctx, "/v1/resources/apply", body).await?;
    println!("applied {kind}/{name}");
    let resp = http::post_empty(ctx, &format!("/v1/dispatch/{kind}/{name}")).await?;
    let node = resp
        .get("node")
        .and_then(|s| s.as_str())
        .unwrap_or("?");
    println!("dispatched {kind}/{name} to {node}");
    if args.watch {
        let logs_args = crate::cmd::logs::Args {
            kind: kind.to_owned(),
            name,
            follow: true,
            tail: None,
            interval: 1.0,
        };
        crate::cmd::logs::run(ctx, logs_args).await?;
    }
    Ok(())
}
