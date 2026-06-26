//! OrionMesh CLI (`orion`).
//!
//! Phase 1 scope: `orion get nodes`, `orion validate <file.yaml>`. Apply / delete /
//! describe come in later phases.

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use orion_types::Resource;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "orion", version, about = "OrionMesh CLI")]
struct Cli {
    /// Controller URL.
    #[arg(long, env = "ORION_CONTROLLER_URL", default_value = "http://127.0.0.1:7878")]
    controller: String,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Fetch resources from the controller.
    Get {
        #[command(subcommand)]
        what: GetWhat,
    },
    /// Validate an OrionMesh resource YAML locally — no controller call.
    Validate { path: PathBuf },
}

#[derive(Subcommand, Debug)]
enum GetWhat {
    Nodes,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Get { what: GetWhat::Nodes } => {
            let url = format!("{}/v1/nodes", cli.controller.trim_end_matches('/'));
            let resp = reqwest::get(&url)
                .await
                .with_context(|| format!("GET {url}"))?
                .error_for_status()?
                .text()
                .await?;
            println!("{resp}");
        }
        Cmd::Validate { path } => {
            let yaml = std::fs::read_to_string(&path)
                .with_context(|| format!("reading {}", path.display()))?;
            let resource = Resource::from_yaml(&yaml).context("parsing resource yaml")?;
            resource.validate().context("validating resource")?;
            println!("ok: kind={} name={}", resource.kind_str(), resource.metadata.name);
        }
    }
    Ok(())
}
