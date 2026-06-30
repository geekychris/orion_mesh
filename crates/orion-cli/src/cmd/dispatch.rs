//! `orion dispatch <kind> <name>` — POST /v1/dispatch.

use crate::{Ctx, http, output};
use anyhow::Result;
use clap::Args as ClapArgs;

#[derive(ClapArgs, Debug)]
pub struct Args {
    pub kind: String,
    pub name: String,
}

pub async fn run(ctx: &Ctx, args: Args) -> Result<()> {
    let kind = super::util::canonical_kind(&args.kind);
    let resp = http::post_empty(ctx, &format!("/v1/dispatch/{kind}/{}", args.name)).await?;
    let node = resp
        .get("node")
        .and_then(|s| s.as_str())
        .unwrap_or("?");
    let id = resp
        .get("instance_id")
        .and_then(|s| s.as_str())
        .unwrap_or("?");
    println!("dispatched {kind}/{} to {node} (instance {id})", args.name);
    if matches!(ctx.output, output::Format::Json | output::Format::Yaml) {
        output::render(ctx.output, &resp)?;
    }
    Ok(())
}
