//! `orion apply` — POST a YAML/JSON resource to the controller.

use crate::{Ctx, http, output};
use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::path::PathBuf;

#[derive(ClapArgs, Debug)]
pub struct Args {
    /// File path to read the resource from. Use `-` for stdin.
    #[arg(short = 'f', long = "file", default_value = "-")]
    file: PathBuf,
    /// Validate locally and call the controller's dry-run, but don't persist.
    #[arg(long)]
    dry_run: bool,
}

pub async fn run(ctx: &Ctx, args: Args) -> Result<()> {
    let body = super::util::read_yaml_input(&args.file)?;
    // Local sanity check first so the user gets a fast error.
    let r = orion_types::Resource::from_yaml(&body).context("parsing resource yaml")?;
    r.validate().context("validating resource")?;
    let path = if args.dry_run {
        "/v1/resources/apply?dry_run=1"
    } else {
        "/v1/resources/apply"
    };
    let resp = http::post_yaml(ctx, path, body).await?;
    let kind = resp
        .get("kind")
        .and_then(|s| s.as_str())
        .unwrap_or(r.kind_str());
    let name = resp
        .get("name")
        .and_then(|s| s.as_str())
        .unwrap_or(&r.metadata.name.0);
    let generation = resp
        .get("generation")
        .map(|g| g.to_string())
        .unwrap_or_else(|| "?".into());
    let dry = resp
        .get("dry_run")
        .and_then(|b| b.as_bool())
        .unwrap_or(false);
    if dry {
        println!("[dry-run] would apply {kind}/{name}");
    } else {
        println!("applied {kind}/{name} (generation {generation})");
    }
    match ctx.output {
        output::Format::Json => output::print_json(&resp)?,
        output::Format::Yaml => output::print_yaml(&resp)?,
        _ => {}
    }
    Ok(())
}
