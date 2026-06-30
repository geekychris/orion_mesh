//! `orion stop <kind> <name>` and `orion restart <kind> <name>`.

use crate::{Ctx, http, output};
use anyhow::Result;
use clap::Args as ClapArgs;

#[derive(ClapArgs, Debug)]
pub struct StopArgs {
    pub kind: String,
    pub name: String,
}

#[derive(ClapArgs, Debug)]
pub struct RestartArgs {
    pub kind: String,
    pub name: String,
}

pub async fn run_stop(ctx: &Ctx, args: StopArgs) -> Result<()> {
    let kind = super::util::canonical_kind(&args.kind);
    let resp = http::post_empty(
        ctx,
        &format!("/v1/control/{kind}/{}/stop", args.name),
    )
    .await?;
    let n = resp.get("stopped").and_then(|n| n.as_u64()).unwrap_or(0);
    println!("stopped {kind}/{} ({n} instance(s))", args.name);
    if matches!(ctx.output, output::Format::Json | output::Format::Yaml) {
        output::render(ctx.output, &resp)?;
    }
    Ok(())
}

pub async fn run_restart(ctx: &Ctx, args: RestartArgs) -> Result<()> {
    let kind = super::util::canonical_kind(&args.kind);
    let resp = http::post_empty(
        ctx,
        &format!("/v1/control/{kind}/{}/restart", args.name),
    )
    .await?;
    println!("restart requested for {kind}/{}", args.name);
    if matches!(ctx.output, output::Format::Json | output::Format::Yaml) {
        output::render(ctx.output, &resp)?;
    }
    Ok(())
}
