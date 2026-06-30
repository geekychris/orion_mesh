//! `orion describe <kind> <name>` — GET a single resource and pretty-print.

use crate::{Ctx, http, output};
use anyhow::Result;
use clap::Args as ClapArgs;
use serde_json::Value;

#[derive(ClapArgs, Debug)]
pub struct Args {
    pub kind: String,
    pub name: String,
}

pub async fn run(ctx: &Ctx, args: Args) -> Result<()> {
    let kind = super::util::canonical_kind(&args.kind);
    let v: Value =
        http::get_json(ctx, &format!("/v1/resources/{kind}/{}", args.name)).await?;
    match ctx.output {
        output::Format::Json => output::print_json(&v)?,
        _ => output::print_yaml(&v)?,
    }
    Ok(())
}
