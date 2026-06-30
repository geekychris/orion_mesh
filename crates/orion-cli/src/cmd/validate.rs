//! `orion validate -f <file|-` — local-only YAML validation.

use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::path::PathBuf;

#[derive(ClapArgs, Debug)]
pub struct Args {
    #[arg(short = 'f', long = "file", default_value = "-")]
    file: PathBuf,
}

pub async fn run(_ctx: &crate::Ctx, args: Args) -> Result<()> {
    let yaml = super::util::read_yaml_input(&args.file)?;
    let r = orion_types::Resource::from_yaml(&yaml).context("parsing resource yaml")?;
    r.validate().context("validating resource")?;
    println!("ok: kind={} name={}", r.kind_str(), r.metadata.name.0);
    Ok(())
}
