//! `orion delete <kind> <name>` — DELETE the resource from the controller.

use crate::{Ctx, http};
use anyhow::Result;
use clap::Args as ClapArgs;

#[derive(ClapArgs, Debug)]
pub struct Args {
    /// Resource kind (e.g. Service, Queue, Schedule). Case-insensitive; plurals accepted.
    pub kind: String,
    pub name: String,
}

pub async fn run(ctx: &Ctx, args: Args) -> Result<()> {
    let kind = super::util::canonical_kind(&args.kind);
    let resp = http::delete_path(ctx, &format!("/v1/resources/{kind}/{}", args.name)).await?;
    let deleted = resp
        .get("deleted")
        .and_then(|b| b.as_bool())
        .unwrap_or(false);
    if deleted {
        println!("deleted {kind}/{}", args.name);
    } else {
        println!("no-op (not present) {kind}/{}", args.name);
    }
    Ok(())
}
